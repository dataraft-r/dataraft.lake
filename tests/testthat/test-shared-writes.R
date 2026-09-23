test_that("libpq keyword values preserve quoting without exposing secrets", {
  expect_equal(
    postgres_parameters(
      "host=localhost dbname='team lake' user=test password='a\\'b\\\\c'"
    ),
    list(
      host = "localhost",
      dbname = "team lake",
      user = "test",
      password = "a'b\\c"
    )
  )
  expect_equal(
    postgres_parameters("service=team connect_timeout=10"),
    list(service = "team", connect_timeout = "10")
  )
  for (invalid in c(
    "postgres://secret@host/db",
    "password='secret",
    "drv=secret",
    "password='secret'x"
  )) {
    error <- tryCatch(postgres_parameters(invalid), error = identity)
    expect_s3_class(error, "dataraft_error")
    expect_equal(grepl("secret", conditionMessage(error)), FALSE)
  }
})

test_that("publication rechecks caller previous after preparation", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  first$asset <- "risk.validated"
  first$output_config <- f$lake$config
  second <- dr_run(f$pipeline, f$lake, cache = FALSE)
  expect_error(
    publish_candidate(
      f$lake,
      "not-published",
      list(asset = "risk.validated"),
      list(parent = second$release_id),
      f$contract,
      tibble::tibble(),
      "definition",
      "input",
      NA_character_,
      list(),
      previous = first
    ),
    class = "dr_publication_conflict"
  )
  expect_identical(
    dr_releases(f$lake)$release_id,
    c(second$release_id, first$release_id)
  )
})

test_that("publication rechecks asset kind under its lock", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  DBI::dbExecute(
    f$lake$con,
    "UPDATE lake._dl.releases SET table_name = 'model_existing'"
  )
  expect_error(
    publish_candidate(
      f$lake,
      "not-published",
      list(asset = "risk.validated"),
      list(parent = first$release_id),
      f$contract,
      tibble::tibble(),
      "definition",
      "input",
      NA_character_,
      list()
    ),
    class = "dataraft_error_lake"
  )
  expect_equal(nrow(dr_releases(f$lake)), 1L)
})

test_that("overlapping preparation uses separate slots and rejects stale previous", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_publish(dr_product("shared", data.frame(id = 1L)), to = f$lake)
  original <- compose_candidate
  entered <- FALSE
  newer <- NULL
  testthat::local_mocked_bindings(compose_candidate = function(
    lake,
    data,
    publish,
    run
  ) {
    if (!entered) {
      entered <<- TRUE
      newer <<- dr_publish(
        dr_product("shared", data.frame(id = 3L)),
        to = lake,
        previous = first,
        stop_on_failure = FALSE
      )
    }
    original(lake, data, publish, run)
  })
  stale <- dr_publish(
    dr_product("shared", data.frame(id = 2L)),
    to = f$lake,
    previous = first,
    stop_on_failure = FALSE
  )
  expect_identical(newer$status, "published")
  expect_identical(stale$status, "error")
  expect_s3_class(stale$error, "dr_publication_conflict")
  expect_identical(dr_read_release(f$lake, "shared")$id, 3L)
  expect_identical(dr_read_release(f$lake, "shared", first$release_id)$id, 1L)
  expect_length(staging_slots(f$lake, "shared"), 0L)
})

test_that("staging discovery protects unrelated assets and symlink targets", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  parent <- file.path(f$lake$config$landing, ".dataraft-staging")
  a <- create_staging_slot(f$lake, "orders", "r123")
  b <- create_staging_slot(f$lake, "orders", "r456")
  other <- create_staging_slot(f$lake, "orders_other", "r123")
  unrelated <- file.path(parent, "orders")
  dir.create(unrelated)
  expect_setequal(
    staging_slots(f$lake, "orders"),
    c("orders--r123", "orders--r456")
  )
  expect_error(
    create_staging_slot(f$lake, "orders", "../escape"),
    class = "dr_staging_invalid"
  )
  outside <- withr::local_tempdir()
  link <- file.path(parent, "orders--r789")
  # fs creates a directory junction on Windows without symlink privileges.
  fs::link_create(outside, link)
  withr::defer(fs::link_delete(link))
  expect_true(fs::is_link(link))
  expect_false("orders--r789" %in% staging_slots(f$lake, "orders"))
  # Exercise recovery policy independently of Linux-only process discovery.
  local_identity <- list(
    host = "test-host",
    pid = 123L,
    boot = "current-boot",
    process_start = "test-start"
  )
  testthat::local_mocked_bindings(
    writer_identity = function() local_identity,
    process_start = function(pid) "test-start"
  )
  writeLines(jencode(writer_identity()), file.path(a, "writer.json"))
  expect_error(
    dr_recover(
      f$lake,
      staging_assets = "orders",
      dry_run = FALSE,
      writer_stopped = TRUE
    ),
    "still alive",
    class = "dataraft_error_lake"
  )
  owner <- writer_identity()
  owner$boot <- "previous-boot"
  writeLines(jencode(owner), file.path(a, "writer.json"))
  removed <- dr_recover(
    f$lake,
    staging_assets = "orders",
    dry_run = FALSE,
    writer_stopped = TRUE
  )
  expect_setequal(removed$id, c("orders--r123", "orders--r456"))
  expect_identical(dir.exists(other), TRUE)
  expect_identical(dir.exists(outside), TRUE)
  expect_true(fs::is_link(link))
})
