test_that("product transitions persist and a retired product cannot publish", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  contract <- dataraft.core::dr_contract("lifecycle.sample", columns = c(id = "integer"))
  product <- dataraft.core::dr_product("lifecycle.sample", data.frame(id = 1L), contract = contract)
  validation <- dataraft.core::dr_run(product, write = FALSE)
  expect_equal(dr_product_state(f$lake, product), "draft")
  expect_error(dr_promote(f$lake, product, "active"), class = "dataraft_error_lake")
  expect_equal(dr_promote(f$lake, product, "validated", validation = validation, actor = "test"), "validated")
  dr_promote(f$lake, product, "active", actor = "test")
  expect_equal(as.numeric(dr_product_transitions(f$lake, product$id)$sequence), c(1, 2))
  expect_equal(dr_product_state(f$lake, product), "active")
  dr_deprecate(f$lake, product, actor = "test")
  dr_retire(f$lake, product, actor = "test")
  expect_equal(dr_product_state(f$lake, product), "retired")
  expect_equal(nrow(dr_product_transitions(f$lake, product$id)), 4L)
  expect_error(dataraft.core::dr_publish(product, to = f$lake), class = "dataraft_error_lake")
})

test_that("backfill rejects a delivery for the wrong partition", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  product <- dataraft.core::dr_product("backfill.sample", data.frame(reporting_date = as.Date("2026-09-23"), id = 1L))
  expect_error(dr_backfill(f$lake, product, "2026-09-23", "2026-09-23",
    partition_by = "reporting_date", source_for_date = function(date) {
      data.frame(reporting_date = as.Date("2026-09-22"), id = 1L)
    }), class = "dataraft_error_lake")
})
