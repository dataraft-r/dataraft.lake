test_that("local shorthand preserves saved backends and refuses unknown folders", {
  root <- withr::local_tempdir()
  writeLines("keep this", file.path(root, "important.txt"))
  expect_error(dr_lake_config(path = root), "not empty")
  expect_identical(readLines(file.path(root, "important.txt")), "keep this")
  expect_error(
    dr_lake_config(path = file.path(root, "important.txt")),
    "is a file"
  )
  manifest <- file.path(root, "dataraft.json")
  writeLines(
    '{"format":2,"backend":"ducklake","layers":["raw","validated","products"]}',
    manifest
  )
  expect_identical(dr_lake_config(path = root)$backend, "ducklake")
  expect_error(
    dr_lake_config(path = root, backend = "duckdb"),
    "different backend"
  )
  writeLines("not json", manifest)
  expect_error(dr_lake_config(path = root), "Invalid dataraft.json")
})

test_that("shorthand and open_lake reconnect to the same local storage", {
  skip_if_not_installed("duckdb")
  root <- file.path(withr::local_tempdir(), "lake")
  config <- dr_lake_config(path = root)
  expect_message(lake <- dr_connect_lake(config), NA)
  accepted <- dr_ingest(data.frame(id = 1L), to = lake, name = "orders")
  dr_close_lake(lake)
  expect_true(file.exists(file.path(root, "dataraft.json")))
  lake <- dr_open_lake(root)
  expect_equal(dr_read_release(lake, "orders", accepted$release_id)$id, 1L)
  dr_close_lake(lake)
  manifest <- readLines(file.path(root, "dataraft.json"))
  readonly <- dr_lake_config(path = root, read_only = TRUE)
  lake <- dr_connect_lake(readonly)
  expect_true(lake$config$read_only)
  expect_equal(dr_read_release(lake, "orders", accepted$release_id)$id, 1L)
  dr_close_lake(lake)
  expect_identical(readLines(file.path(root, "dataraft.json")), manifest)
  other <- file.path(withr::local_tempdir(), "from-open-lake")
  lake <- dr_open_lake(other)
  dr_close_lake(lake)
  expect_identical(dr_lake_config(path = other)$backend, "duckdb")
  absent <- file.path(withr::local_tempdir(), "readonly-absent")
  expect_error(
    dr_connect_lake(dr_lake_config(path = absent, read_only = TRUE)),
    "must already exist"
  )
  expect_false(dir.exists(absent))
})

test_that("a failed initial connection retains a retryable backend identity", {
  skip_if_not_installed("duckdb")
  root <- file.path(withr::local_tempdir(), "lake")
  config <- dr_lake_config(path = root)
  local_family_bindings(
    dbConnect = function(...) stop("connection unavailable"),
    .package = "DBI"
  )
  expect_error(dr_connect_lake(config), "connection unavailable")
  expect_true(file.exists(file.path(root, "dataraft.json")))
  expect_identical(dr_lake_config(path = root)$backend, "duckdb")
  expect_error(dr_connect_lake(config), "connection unavailable")
})

test_that("driver information is quiet while warnings and connection failures survive", {
  skip_if_not_installed("duckdb")
  config <- dr_lake_config(path = file.path(withr::local_tempdir(), "lake"))
  local_family_bindings(
    duckdb = function(...) {
      message("driver storage information")
      warning("driver warning", call. = FALSE)
      structure(list(), class = "mock_driver")
    },
    .package = "duckdb"
  )
  local_family_bindings(
    dbConnect = function(drv, ...) {
      force(drv)
      message("connection diagnostic")
      stop("connection failed", call. = FALSE)
    },
    .package = "DBI"
  )
  messages <- character()
  expect_warning(
    expect_error(
      withCallingHandlers(
        dr_connect_lake(config),
        message = function(condition) {
          messages <<- c(messages, conditionMessage(condition))
          invokeRestart("muffleMessage")
        }
      ),
      "connection failed"
    ),
    "driver warning"
  )
  expect_identical(messages, "connection diagnostic\n")
})
