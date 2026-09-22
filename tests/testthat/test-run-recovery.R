test_that("run recovery requires explicit selection and preserves releases", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  published <- dr_run(f$pipeline, f$lake)
  run <- new_run(f$lake, "abandoned", "risk.validated", "definition", "code")
  exec(f$lake, "UPDATE lake._dl.runs SET started_at = '2000-01-01T00:00:00Z' WHERE run_id = ?", list(run))
  expect_equal(dr_interrupted(f$lake)$run_id, run)
  testthat::local_mocked_bindings(writer_state = function(owner) "stopped")
  expect_equal(dr_recover(f$lake)$action, "would_mark_error")
  expect_error(dr_recover(f$lake, dry_run = FALSE), class = "dataraft_error_lake")
  recovered <- dr_recover(f$lake, run_ids = run, dry_run = FALSE)
  expect_equal(recovered$action, "marked_error")
  expect_equal(nrow(dr_interrupted(f$lake)), 0L)
  expect_equal(dr_releases(f$lake)$release_id, published$release_id)
  expect_equal(dr_verify_releases(f$lake)$status, "verified")
  expect_true("run_recovered" %in% dr_registry(f$lake, "events")$type)
})

test_that("unknown liveness needs confirmation and live writers cannot be overridden", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  run <- new_run(f$lake, "abandoned", "risk.validated", "definition", "code")
  state <- "unknown"
  testthat::local_mocked_bindings(writer_state = function(owner) state)
  expect_error(dr_recover(f$lake, run_ids = run, dry_run = FALSE), "liveness is unknown")
  state <- "alive"
  expect_error(dr_recover(f$lake, run_ids = run, dry_run = FALSE, writer_stopped = TRUE), "still alive")
  state <- "unknown"
  expect_equal(dr_recover(f$lake, run_ids = run, dry_run = FALSE, writer_stopped = TRUE)$action, "marked_error")
})
