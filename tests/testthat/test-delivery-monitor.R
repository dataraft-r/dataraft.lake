test_that("delivery monitoring detects missing business dates without an ingest attempt", {
  skip_if_not_installed("dataraft.catalog")
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  due <- as.POSIXct("2026-09-01 09:00:00", tz = "UTC")
  date <- as.Date("2026-08-31")
  sent <- 0L
  notify <- function(event) sent <<- sent + 1L
  check <- function(at = due + 60, notify_fn = notify) {
    dr_check_delivery(
      f$lake,
      "risk.validated",
      f$contract,
      date,
      due,
      at = at,
      notify = notify_fn
    )
  }
  expect_equal(check(due - 60)$status, "pending")
  expect_equal(nrow(dr_registry(f$lake, "events")), 0L)
  expect_equal(check()$status, "missing")
  expect_equal(dr_freshness(f$lake)$delivery_status, "missing")
  check()
  check()
  expect_equal(sent, 1L)
  dr_run(f$pipeline, f$lake, business_date = as.Date("2026-07-31"))
  expect_equal(check()$status, "missing")
  release <- dr_run(f$pipeline, f$lake, business_date = date)
  expect_equal(check()$release_id, release$release_id)
  expect_equal(check()$status, "received")
})

test_that("failed notifications remain retryable", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  due <- as.POSIXct("2026-09-01 09:00:00", tz = "UTC")
  dr_check_delivery(
    f$lake,
    "risk.validated",
    f$contract,
    "2026-08-31",
    due,
    at = due + 60,
    notify = function(event) stop("Transport unavailable")
  )
  expect_equal(dr_registry(f$lake, "events")$status, "delivery_failed")
  sent <- 0L
  dr_check_delivery(
    f$lake,
    "risk.validated",
    f$contract,
    "2026-08-31",
    due,
    at = due + 60,
    notify = function(event) sent <<- sent + 1L
  )
  expect_equal(sent, 1L)
})
