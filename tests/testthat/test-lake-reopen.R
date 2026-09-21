test_that("custom layers survive folder reopening and permit another publication", {
  skip_if_not_installed("duckdb")
  root <- file.path(withr::local_tempdir(), "lake")
  layers <- c("raw", "staging", "core", "marts")
  lake <- dr_connect_lake(dr_lake_config(path = root, layers = layers))
  first <- dr_publish(
    dr_product("orders", data.frame(amount = 350)),
    to = lake,
    layer = "core"
  )
  dr_close_lake(lake)
  lake <- dr_open_lake(root)
  withr::defer(dr_close_lake(lake))
  expect_identical(lake$config$layers, layers)
  expect_equal(dr_read_release(lake, "orders")$amount, 350)
  second <- dr_publish(
    dr_product("orders", data.frame(amount = 380)),
    to = lake,
    layer = "core"
  )
  expect_identical(second$status, "published")
  expect_equal(dr_read_release(lake, "orders", first$release_id)$amount, 350)
  schemas <- DBI::dbGetQuery(
    lake$con,
    "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'lake'"
  )$schema_name
  expect_setequal(setdiff(schemas, c("main", "_dl")), layers)
})

test_that("both folder entry points retain named roles and read-only opens do not write", {
  skip_if_not_installed("duckdb")
  root <- file.path(withr::local_tempdir(), "lake")
  layers <- c(
    raw = "raw",
    staging = "prep",
    core = "business",
    marts = "reporting"
  )
  lake <- dr_setup_lake(path = root, layers = layers, backend = "duckdb")
  dr_close_lake(lake)
  marker <- file.path(root, "dataraft.json")
  before <- readLines(marker)
  config <- dr_lake_config(path = root)
  expect_identical(config$layers, layers)
  lake <- dr_open_lake(root, read_only = TRUE)
  expect_identical(lake$config$layers, layers)
  dr_close_lake(lake)
  expect_identical(readLines(marker), before)
  lake <- dr_setup_lake(path = root)
  expect_identical(lake$config$layers, layers)
  dr_close_lake(lake)
  expect_snapshot(
    error = TRUE,
    dr_open_lake(root, layers = c("raw", "products"))
  )
  expect_snapshot(
    error = TRUE,
    dr_setup_lake(path = root, landing = "elsewhere")
  )
  expect_snapshot(error = TRUE, dr_open_lake(root, backend = "ducklake"))
  expect_identical(readLines(marker), before)
})

test_that("a single layer is preserved without JSON scalar conversion", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root, layers = "raw")
  dr_close_lake(lake)
  expect_identical(dr_lake_config(path = root)$layers, "raw")
})

test_that("saved configuration cannot be bypassed by an older definition", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  stale <- dr_lake_config(path = root)
  lake <- dr_open_lake(root, layers = c("raw", "core"))
  dr_close_lake(lake)
  expect_snapshot(error = TRUE, dr_connect_lake(stale))
})

test_that("DuckLake folder setup survives reconnect and another checked publication", {
  skip_if(
    Sys.getenv("DATARAFT_TEST_DUCKLAKE") != "true",
    "Enable real DuckLake integration"
  )
  root <- file.path(withr::local_tempdir(), "lake")
  layers <- c("raw", "staging", "core", "marts")
  lake <- dr_setup_lake(path = root, backend = "ducklake", layers = layers)
  first <- dr_publish(
    dr_product("orders", data.frame(amount = 350)),
    to = lake,
    layer = "core"
  )
  dr_close_lake(lake)
  lake <- dr_open_lake(root)
  expect_identical(lake$config$backend, "ducklake")
  expect_identical(lake$config$layers, layers)
  expect_identical(
    DBI::dbGetQuery(
      lake$con,
      "SELECT type FROM duckdb_databases() WHERE database_name = 'lake'"
    )$type,
    "ducklake"
  )
  second <- dr_publish(
    dr_product("orders", data.frame(amount = 380)),
    to = lake,
    layer = "core"
  )
  expect_identical(second$status, "published")
  expect_equal(dr_read_release(lake, "orders", first$release_id)$amount, 350)
  dr_close_lake(lake)
  lake <- dr_open_lake(root, read_only = TRUE)
  expect_equal(dr_read_release(lake, "orders")$amount, 380)
  dr_close_lake(lake)
})
