test_that("partition replacement retains other periods and validates global keys", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  f$pipeline$steps$publish$mode <- "replace_partition"
  f$pipeline$steps$publish$partition_by <- "date"
  a <- dr_run(f$pipeline, f$lake)
  sep <- f$good
  sep$date <- as.Date("2026-09-30")
  sep$reserve <- c(400, 500)
  f$write(sep)
  b <- dr_run(f$pipeline, f$lake)
  expect_equal(nrow(dplyr::collect(dr_tbl(f$lake, "risk.validated"))), 4)
  fixed <- f$good
  fixed$reserve <- c(150, 250)
  f$write(fixed)
  dr_run(f$pipeline, f$lake)
  current <- dplyr::collect(dr_tbl(f$lake, "risk.validated"))
  expect_equal(nrow(current), 4)
  expect_equal(sum(current$reserve), 1300)
  expect_equal(
    nrow(dplyr::collect(dr_tbl(f$lake, "risk.validated", a$release_id))),
    2
  )
  expect_equal(
    sum(dplyr::collect(dr_tbl(f$lake, "risk.validated", b$release_id))$reserve),
    1200
  )
  f$write(f$good[0, ])
  expect_equal(
    dr_run(f$pipeline, f$lake, stop_on_failure = FALSE)$status,
    "error"
  )
})

test_that("publication transaction rolls back release and successful run together", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  first <- dr_run(f$pipeline, f$lake)
  run <- dataraft.lake:::new_run(f$lake, "test", "risk.validated", "d", "v")
  raw <- dataraft.lake:::materialize(f$lake, f$good, "raw", "rollback_raw")
  pub <- f$pipeline$steps$publish
  candidate <- dataraft.lake:::compose_candidate(f$lake, raw, pub, run)
  quality <- dr_validate(candidate$data, f$contract)
  expect_error(
    dataraft.lake:::publish_candidate(
      f$lake,
      run,
      pub,
      candidate,
      f$contract,
      quality,
      "d",
      "i",
      NA_character_,
      list(),
      before_commit = function() stop("simulated crash before commit")
    ),
    "simulated crash"
  )
  expect_equal(nrow(dr_registry(f$lake, "releases")), 1)
  expect_equal(
    dr_registry(f$lake, "runs")$status[
      dr_registry(f$lake, "runs")$run_id == run
    ],
    "running"
  )
  expect_equal(dr_run(f$pipeline, f$lake)$release_id, first$release_id)
})
