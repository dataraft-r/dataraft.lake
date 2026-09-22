test_that("cleanup previews by default and preserves releases and diagnostics", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  good <- dr_run(f$pipeline, f$lake)
  bad <- f$good
  bad$reserve[1] <- -1
  f$write(bad)
  failed <- dr_run(f$pipeline, f$lake, stop_on_failure = FALSE)
  reference_time <- Sys.time() + 40 * 86400
  plan <- dr_cleanup(f$lake, at = reference_time)
  expect_equal(nrow(plan), 3L)
  expect_setequal(unique(plan$run_id), c(good$run_id, failed$run_id))
  expect_equal(all(plan$action == "would_drop"), TRUE)
  expect_equal(
    nrow(dr_quality(f$lake, run_id = failed$run_id)),
    nrow(failed$quality)
  )
  removed <- dr_cleanup(f$lake, at = reference_time, dry_run = FALSE)
  expect_equal(removed$action, rep("dropped", 3))
  expect_equal(nrow(dr_cleanup(f$lake, at = reference_time)), 0L)
  expect_equal(
    dplyr::collect(dr_tbl(f$lake, "risk.validated", good$release_id))$reserve,
    f$good$reserve
  )
  expect_equal(
    nrow(dr_quality(f$lake, run_id = failed$run_id)),
    nrow(failed$quality)
  )
})

test_that("DuckLake maintenance previews both operations and refuses active use", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  f$lake$config$backend <- "ducklake"
  calls <- character()
  testthat::local_mocked_bindings(query = function(lake, sql, params = NULL) {
    if (grepl("AS cutoff", sql, fixed = TRUE)) {
      return(tibble::tibble(cutoff = "2026-01-01 00:00:00"))
    }
    calls <<- c(calls, sql)
    tibble::tibble()
  })
  result <- dr_expire_snapshots(f$lake)
  expect_named(result, c("snapshots", "files"))
  expect_equal(length(calls), 2L)
  expect_match(calls[[1]], "ducklake_expire_snapshots")
  expect_match(calls[[2]], "ducklake_cleanup_old_files")
  expect_equal(all(grepl("dry_run => true", calls, fixed = TRUE)), TRUE)
  expect_error(
    dr_expire_snapshots(f$lake, dry_run = FALSE),
    class = "dr_maintenance_busy"
  )
  expect_error(
    dr_expire_snapshots(f$lake, older_than_days = 0),
    class = "dr_retention_period"
  )
})

test_that("cleanup owns quarantine suffixes but not similarly prefixed tables", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  run <- new_run(f$lake, "blocked", "risk.validated", "d", "v")
  finish_run(f$lake, run, "blocked")
  clean <- paste0("candidate_", run, "_clean")
  unrelated <- paste0("candidate_", run, "_unrelated")
  materialize(f$lake, f$good, "raw", clean)
  materialize(f$lake, f$good, "raw", unrelated)
  plan <- dr_cleanup(f$lake, at = Sys.time() + 40 * 86400, dry_run = FALSE)
  expect_equal(plan$table, clean)
  expect_equal(DBI::dbExistsTable(f$lake$con, table_id("raw", unrelated)), TRUE)
})
