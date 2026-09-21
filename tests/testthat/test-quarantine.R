test_that("lake releases contain clean rows and retain quarantine evidence", {
  f <- fixture()
  on.exit(fixture_cleanup(f))
  product <- dataraft.core::dr_product(
    "orders",
    data.frame(id = 1:3, amount = c(10, -1, 20))
  ) |>
    dataraft.core::dr_add_quality(~ amount >= 0, action = "quarantine") |>
    dataraft.core::dr_set_target(dr_target_lake(f$lake))
  result <- dataraft.core::dr_run(product)
  expect_equal(result$status, "published")
  expect_equal(dataraft.core::dr_collect(result)$amount, c(10, 20))
  expect_equal(dataraft.core::dr_quarantine_rows(result)$amount, -1)
  expect_true(any(result$quality$stage == "quarantine"))
})
