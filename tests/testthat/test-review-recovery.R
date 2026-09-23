test_that("run recovery previews and retains published history", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  published <- dr_run(f$pipeline, f$lake)
  run <- new_run(f$lake, "abandoned", "risk.validated", "definition", "test")
  local_mocked_bindings(writer_state = function(owner) "stopped")
  expect_contains(dr_interrupted(f$lake, older_than_hours = 0)$run_id, run)
  plan <- dr_recover(f$lake, run_ids = run)
  expect_identical(plan$action, "would_mark_error")
  expect_identical(
    dr_registry(f$lake, "runs")$status[
      dr_registry(f$lake, "runs")$run_id == run
    ],
    "running"
  )
  recovered <- dr_recover(f$lake, run_ids = run, dry_run = FALSE)
  expect_identical(recovered$action, "marked_error")
  expect_contains(dr_releases(f$lake)$release_id, published$release_id)
  expect_equal(nrow(dr_interrupted(f$lake)), 0L)
  expect_contains(dr_registry(f$lake, "events")$type, "run_recovered")
})

test_that("known live and unknown writers are not silently recovered", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  run <- new_run(f$lake, "abandoned", "risk.validated", "definition", "test")
  state <- "alive"
  local_mocked_bindings(writer_state = function(owner) state)
  expect_error(
    dr_recover(f$lake, run_ids = run, dry_run = FALSE, writer_stopped = TRUE),
    class = "dataraft_error"
  )
  state <- "unknown"
  expect_error(
    dr_recover(f$lake, run_ids = run, dry_run = FALSE),
    class = "dataraft_error"
  )
  expect_identical(
    dr_recover(
      f$lake,
      run_ids = run,
      dry_run = FALSE,
      writer_stopped = TRUE
    )$action,
    "marked_error"
  )
})

test_that("cleanup includes clean candidates but not other run prefixes", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  run <- new_run(f$lake, "abandoned", "risk.validated", "definition", "test")
  finish_run(f$lake, run, "blocked")
  clean <- paste0("candidate_", run, "_clean")
  other <- paste0("candidate_", run, "other_clean")
  for (name in c(clean, other)) {
    DBI::dbWriteTable(f$lake$con, table_id("raw", name), data.frame(id = 1))
  }
  plan <- dr_cleanup(f$lake, at = Sys.time() + 40 * 86400)
  expect_contains(plan$table, clean)
  expect_false(other %in% plan$table)
  dr_cleanup(f$lake, at = Sys.time() + 40 * 86400, dry_run = FALSE)
  expect_false(DBI::dbExistsTable(f$lake$con, table_id("raw", clean)))
  expect_true(DBI::dbExistsTable(f$lake$con, table_id("raw", other)))
})

test_that("release verification detects mutation and missing data", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  result <- dr_run(f$pipeline, f$lake)
  expect_identical(dr_verify_releases(f$lake)$status, "verified")
  release <- dr_releases(f$lake)
  exec(
    f$lake,
    paste(
      "UPDATE",
      table_sql(f$lake, release$schema_name, release$table_name),
      "SET reserve = reserve + 1"
    )
  )
  expect_identical(dr_verify_releases(f$lake)$status, "modified")
  exec(
    f$lake,
    paste(
      "DROP TABLE",
      table_sql(f$lake, release$schema_name, release$table_name)
    )
  )
  expect_identical(dr_verify_releases(f$lake)$status, "missing")
})

test_that("registry identity and missing historical baselines are explicit", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  result <- dr_run(f$pipeline, f$lake)
  run <- dr_registry(f$lake, "runs")[1, ]
  expect_error(insert_meta(f$lake, "runs", as.list(run)), "unique")
  expect_error(insert_meta(f$lake, "runs", list(run_id = NULL)), "nonmissing")
  exec(f$lake, paste("DELETE FROM", meta(f$lake, "release_integrity")))
  expect_identical(dr_verify_releases(f$lake)$status, "unverifiable")
  exec(f$lake, paste("DELETE FROM", meta(f$lake, "runs")))
  expect_identical(dr_verify_releases(f$lake)$status, "registry_inconsistent")
})
