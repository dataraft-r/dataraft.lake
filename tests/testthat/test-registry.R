test_that("the current registry reopens without changing release evidence", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  result <- dr_run(f$pipeline, f$lake)
  before <- dr_registry(f$lake, "quality_results")
  registry_init(f$lake)
  expect_identical(dr_registry(f$lake, "quality_results"), before)
  expect_identical(dr_registry(f$lake, "schema_version")$version, 6L)
  expect_equal(dr_releases(f$lake)$release_id, result$release_id)
})

test_that("unsupported registries are rejected without rewriting evidence", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  dr_run(f$pipeline, f$lake)
  before <- dr_registry(f$lake, "quality_results")
  DBI::dbExecute(f$lake$con, "UPDATE lake._dl.schema_version SET version = 2")
  expect_snapshot(error = TRUE, registry_init(f$lake))
  expect_identical(dr_registry(f$lake, "schema_version")$version, 2L)
  expect_identical(dr_registry(f$lake, "quality_results"), before)
  config <- f$lake$config
  dr_close_lake(f$lake)
  expect_snapshot(error = TRUE, dr_connect_lake(config))
  expect_snapshot(error = TRUE, dr_connect_lake(config, read_only = TRUE))
})

test_that("release order is independent of writer clock drift", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  DBI::dbExecute(
    f$lake$con,
    "UPDATE lake._dl.releases SET published_at = '9999-01-01T00:00:00Z'"
  )
  changed <- f$good
  changed$reserve <- changed$reserve + 1
  f$write(changed)
  second <- dr_run(f$pipeline, f$lake)
  expect_identical(
    resolve_release(f$lake, "risk.validated")$release_id[[1]],
    second$release_id
  )
  expect_identical(
    dr_releases(f$lake)$release_id,
    c(second$release_id, first$release_id)
  )
  expect_equal(as.numeric(dr_releases(f$lake)$release_order), c(2, 1))
})

test_that("v4 migration retains history and explicitly labels legacy ordering", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  dr_run(f$pipeline, f$lake)
  insert_meta(
    f$lake,
    "reports",
    list(id = "report", created_at = now(), manifest = "{}")
  )
  before <- lapply(
    c("reports", "lineage_edges", "quality_results"),
    function(name) dr_registry(f$lake, name)
  )
  if (identical(f$lake$config$backend, "duckdb")) {
    DBI::dbExecute(f$lake$con, "DROP INDEX lake._dl.dr_unique_releases")
  }
  DBI::dbExecute(
    f$lake$con,
    "ALTER TABLE lake._dl.releases DROP COLUMN release_order"
  )
  DBI::dbExecute(f$lake$con, "DROP TABLE lake._dl.release_counter")
  DBI::dbExecute(f$lake$con, "DROP TABLE lake._dl.release_integrity")
  DBI::dbExecute(f$lake$con, "UPDATE lake._dl.schema_version SET version = 4")
  expect_warning(registry_init(f$lake), class = "dr_legacy_release_order")
  expect_identical(dr_registry(f$lake, "schema_version")$version, 6L)
  after <- lapply(
    c("reports", "lineage_edges", "quality_results"),
    function(name) dr_registry(f$lake, name)
  )
  expect_identical(after, before)
  expect_equal(as.numeric(dr_releases(f$lake)$release_order), 1)
})

test_that("publication rollback rolls back its catalog counter", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  before <- query(f$lake, "SELECT value FROM lake._dl.release_counter")
  expect_error(
    DBI::dbWithTransaction(f$lake$con, {
      insert_meta(
        f$lake,
        "releases",
        list(
          release_id = "rolled-back",
          schema_name = "_dl",
          table_name = "runs"
        )
      )
      rlang::abort("simulated commit failure", class = "test_commit_failure")
    }),
    class = "test_commit_failure"
  )
  expect_identical(
    query(f$lake, "SELECT value FROM lake._dl.release_counter"),
    before
  )
  expect_equal(nrow(dr_releases(f$lake)), 0)
})


test_that("DuckLake allocates release order and previews maintenance natively", {
  skip_if(
    Sys.getenv("DATARAFT_TEST_DUCKLAKE") != "true",
    "Enable real DuckLake integration"
  )
  f <- fixture(backend = "ducklake")
  withr::defer(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  second <- dr_run(f$pipeline, f$lake, cache = FALSE)
  expect_equal(as.numeric(dr_releases(f$lake)$release_order), c(2, 1))
  expect_equal(
    resolve_release(f$lake, "risk.validated")$release_id,
    second$release_id
  )
  expect_named(dr_expire_snapshots(f$lake), c("snapshots", "files"))
  expect_equal(
    nrow(dr_read_release(f$lake, "risk.validated", first$release_id)),
    2
  )
})
