test_that("the canonical opener preserves configured read-only ownership", {
  local_mocked_bindings(dr_connect_lake = function(config, read_only) {
    list(read_only = read_only, extensions = config$install_extensions)
  })
  config <- dr_lake_config(backend = "duckdb", read_only = TRUE)
  expect_true(dr_open_lake(config)$read_only)
  expect_false(dr_open_lake(config, read_only = FALSE)$read_only)
  expect_false(dr_open_lake(config, install_extensions = FALSE)$extensions)
  expect_true(config$read_only)
})
