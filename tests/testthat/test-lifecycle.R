test_that("good, invalid, missing and corrected deliveries preserve history", {
  skip_if_not_installed("dataraft.adapters")
  f <- fixture()
  on.exit(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake, business_date = "2026-08-31")
  expect_equal(first$status, "published")
  expect_equal(
    sum(dplyr::collect(dr_tbl(f$lake, "risk.validated"))$reserve),
    300
  )
  bad <- f$good
  bad$reserve[1] <- -100
  f$write(bad)
  event <- NULL
  blocked <- dr_run(
    f$pipeline,
    f$lake,
    notify = function(x) event <<- x,
    stop_on_failure = FALSE
  )
  expect_equal(blocked$status, "blocked")
  expect_equal(event$type, "quality_failed")
  expect_equal(event$recipient, "Risk")
  expect_equal(
    sum(dplyr::collect(dr_tbl(f$lake, "risk.validated"))$reserve),
    300
  )
  expect_true(any(blocked$quality$status == "failed"))
  expect_true(nrow(dr_registry(f$lake, "quality_results")) > 0)
  unlink(f$path)
  missing <- dr_run(f$pipeline, f$lake, stop_on_failure = FALSE)
  expect_equal(missing$status, "missing")
  corrected <- f$good
  corrected$reserve[1] <- 150
  f$write(corrected)
  fixed <- dr_run(f$pipeline, f$lake)
  expect_equal(fixed$status, "published")
  expect_equal(
    sum(dplyr::collect(dr_tbl(f$lake, "risk.validated"))$reserve),
    350
  )
  expect_equal(
    sum(
      dplyr::collect(dr_tbl(f$lake, "risk.validated", first$release_id))$reserve
    ),
    300
  )
  expect_equal(nrow(dr_registry(f$lake, "releases")), 2)
  expect_equal(dr_freshness(f$lake)$latest_attempt, "published")
})

test_that("retry is idempotent and never promotes an old cached release", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  second <- dr_run(f$pipeline, f$lake)
  expect_equal(second$status, "cached")
  expect_equal(second$release_id, first$release_id)
  modified <- f$good
  modified$reserve <- c(300, 400)
  f$write(modified)
  third <- dr_run(f$pipeline, f$lake)
  f$write()
  old_retry <- dr_run(f$pipeline, f$lake)
  expect_equal(old_retry$release_id, first$release_id)
  expect_equal(
    sum(dplyr::collect(dr_tbl(f$lake, "risk.validated"))$reserve),
    700
  )
  expect_equal(nrow(dr_registry(f$lake, "releases")), 2)
  expect_false(identical(third$release_id, first$release_id))
})

test_that("schema failures and duplicate keys block publication", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  f$write(f$good[c(1, 1), ])
  result <- dr_run(f$pipeline, f$lake, stop_on_failure = FALSE)
  expect_equal(result$status, "blocked")
  expect_equal(
    result$quality$status[result$quality$rule == "unique_key"],
    "failed"
  )
  expect_error(dr_tbl(f$lake, "risk.validated"), class = "dr_no_release")
  q <- dr_validate(data.frame(unexpected = 1), f$contract)
  expect_equal(q$status, "failed")
})

test_that("missing, errored and empty rules never pass", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  cases <- list(
    failed = function(x) NA,
    error = function(x) stop("secret or row data"),
    not_checked = function(x) dr_quality_counts(0, 0)
  )
  for (expected in names(cases)) {
    c <- f$contract
    c$rules <- list(dr_quality_rule("test", cases[[expected]]))
    q <- dr_validate(f$good, c)
    expect_equal(q$status[q$rule == "test"], expected)
    expect_false(dataraft.core:::quality_ok(q))
    expect_false(any(grepl("secret", q$message)))
  }
  empty <- dr_validate(f$good[0, ], f$contract)
  expect_false(dataraft.core:::quality_ok(empty))
})

test_that("warnings and explicit count thresholds work", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  c <- f$contract
  c$rules <- list(
    dr_quality_rule("warning", function(x) FALSE, action = "warn"),
    dr_quality_rule(
      "within tolerance",
      function(x) dr_quality_counts(1, 10),
      threshold = .1
    )
  )
  q <- dr_validate(f$good, c)
  expect_true(dataraft.core:::quality_ok(q))
  expect_equal(q$status[q$rule == "warning"], "warning")
  expect_equal(q$status[q$rule == "within tolerance"], "passed")
})

test_that("contracts cannot silently change under an existing version", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  dr_register(f$lake, f$contract)
  changed <- f$contract
  changed$grain <- "different grain"
  expect_error(dr_register(f$lake, changed), "version bump")
})

test_that("notifications deduplicate delivered events but retry transport failures", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  bad <- f$good
  bad$reserve[1] <- -1
  f$write(bad)
  sent <- 0L
  notify <- function(e) sent <<- sent + 1L
  dr_run(f$pipeline, f$lake, notify = notify, stop_on_failure = FALSE)
  dr_run(f$pipeline, f$lake, notify = notify, stop_on_failure = FALSE)
  expect_equal(sent, 1L)
  expect_true("suppressed" %in% dr_registry(f$lake, "events")$status)
  f$write()
  dr_run(f$pipeline, f$lake)
  f$write(bad)
  dr_run(f$pipeline, f$lake, notify = notify, stop_on_failure = FALSE)
  expect_equal(sent, 2L) # An incident recurring after recovery must notify again.
  bad$reserve[1] <- -2
  f$write(bad)
  x <- dr_run(
    f$pipeline,
    f$lake,
    notify = function(e) stop("mail down"),
    stop_on_failure = FALSE
  )
  expect_equal(x$status, "blocked")
  expect_true("delivery_failed" %in% dr_registry(f$lake, "events")$status)
})

test_that("freshness changes even when no new job runs", {
  skip_if_not_installed("dataraft.adapters")
  f <- fixture()
  on.exit(fixture_cleanup(f))
  dr_run(f$pipeline, f$lake)
  expect_equal(dr_freshness(f$lake)$freshness, "current")
  expect_equal(
    dr_freshness(f$lake, at = Sys.time() + 72 * 3600)$freshness,
    "stale"
  )
})

test_that("connection-free specifications reopen the same lake", {
  f <- fixture()
  on.exit(unlink(f$root, recursive = TRUE))
  expect_false("con" %in% names(f$pipeline))
  dr_disconnect_lake(f$lake)
  out <- dr_run(f$pipeline)
  expect_equal(out$status, "published")
})

test_that("nonexistent relative paths are frozen to the original working directory", {
  original <- getwd()
  catalog <- dr_registry_duckdb("not-created-yet.ducklake")
  source <- dr_source_file("test.source", "not-created-yet.csv")
  expect_equal(catalog$path, file.path(original, "not-created-yet.ducklake"))
  expect_equal(source$path, file.path(original, "not-created-yet.csv"))
})
