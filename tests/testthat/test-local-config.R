test_that("local shorthand defines a lake without creating anything", {
  # Resolve existing temporary-directory aliases before adding the absent lake.
  parent <- normalizePath(
    withr::local_tempdir(),
    winslash = "/",
    mustWork = TRUE
  )
  root <- file.path(parent, "new-lake")
  config <- dr_lake_config(path = root)
  expect_s3_class(config, "dr_config")
  expect_identical(config$backend, "duckdb")
  expect_identical(config$layers, c("raw", "validated", "products"))
  expect_identical(config$catalog$path, file.path(root, "metadata.duckdb"))
  expect_identical(config$storage$path, file.path(root, "data"))
  expect_identical(config$landing, file.path(root, "landing"))
  expect_false(dir.exists(root))
  expect_identical(dr_lake_config()$backend, "ducklake")
  expect_identical(
    dr_lake_config(path = root, backend = "ducklake")$backend,
    "ducklake"
  )
  expect_false(dir.exists(root))
  expect_error(
    dr_lake_config(path = root, catalog = dr_registry_duckdb("elsewhere.db")),
    "not both"
  )
  expect_error(
    dr_lake_config(path = root, storage = dr_storage_local("elsewhere")),
    "not both"
  )
  expect_error(dr_lake_config(path = root, landing = "elsewhere"), "not both")
  expect_false(dir.exists(root))
})
