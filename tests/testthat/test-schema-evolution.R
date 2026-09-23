test_that("schema evolution requires a new contract version and preserves old releases", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  first_contract <- dr_contract(
    "evolving.contract",
    version = "1",
    columns = c(id = "integer"),
    key = "id"
  )
  first <- dr_publish(
    dr_product("evolving", data.frame(id = 1L), contract = first_contract),
    to = f$lake
  )
  changed <- dr_contract(
    "evolving.contract",
    version = "1",
    columns = c(id = "character"),
    key = "id"
  )
  expect_error(dr_publish(
    dr_product("evolving", data.frame(id = "new"), contract = changed),
    to = f$lake
  ))
  expect_equal(dr_collect(first)$id, 1L)
  expect_equal(nrow(dr_releases(f$lake, "evolving")), 1L)
  changed$version <- "2"
  second <- dr_publish(
    dr_product("evolving", data.frame(id = "new"), contract = changed),
    to = f$lake,
    previous = first
  )
  expect_equal(dr_collect(second)$id, "new")
  expect_equal(dr_collect(first)$id, 1L)
  expect_equal(nrow(dr_releases(f$lake, "evolving")), 2L)
})
