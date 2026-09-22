test_that("config release sources read approved dbt output without registry changes", {
  skip_if_not_installed("dataraft.adapters")
  skip_if_not_installed("dataraft.dbt")
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  DBI::dbExecute(lake$con, "CREATE SCHEMA lake.marts")
  DBI::dbExecute(
    lake$con,
    paste(
      "CREATE TABLE lake.marts.customer_revenue AS",
      "SELECT 101 AS customer_id, 100.0::DOUBLE AS revenue"
    )
  )
  artifacts <- dbt_read_artifacts(system.file(
    "extdata",
    "dbt-artifacts",
    package = "dataraft.dbt"
  ))
  build <- structure(
    list(
      success = TRUE,
      status = 0L,
      command = "build",
      results = artifacts$results,
      manifest = artifacts$manifest
    ),
    class = "dr_dbt_result"
  )
  config <- lake$config
  dr_close_lake(lake)
  approved <- dr_dbt_publish(config, build, "customer_revenue",
    contract = dr_contract(columns = c(customer_id = "integer", revenue = "numeric")))
  checksum <- digest::digest(file = config$catalog$path, algo = "sha256")
  source <- dr_source_release(config, approved$asset, approved$release_id)
  expect_equal(dr_inspect(source)$backend, config$backend)
  expect_false(dr_capabilities(source)$lazy)
  expect_false(source$lake$read_only)
  read <- dr_read_source(source)
  expect_s3_class(read, "tbl_df")
  expect_equal(read$revenue, 100)
  expect_equal(attr(read, "dr_input_reference")$release_id, approved$release_id)
  expect_equal(
    digest::digest(file = config$catalog$path, algo = "sha256"),
    checksum
  )
  result <- dr_product("export") |> dr_add_source(source) |> dr_run()
  expect_equal(dr_collect(result)$revenue, 100)
  expect_equal(result$inputs$release_id, approved$release_id)
  expect_equal(result$inputs$asset, approved$asset)
  destination <- DBI::dbConnect(duckdb::duckdb(), bigint = "integer64")
  withr::defer(DBI::dbDisconnect(destination, shutdown = TRUE))
  exported <- dr_product("database_export") |>
    dr_add_source(source) |>
    dr_add_contract(c(customer_id = "integer", revenue = "numeric")) |>
    dr_set_target(dr_target_database(destination, "approved")) |>
    dr_run()
  expect_equal(DBI::dbReadTable(destination, "approved")$revenue, 100)
  expect_equal(exported$inputs$release_id, approved$release_id)
  expect_equal(
    digest::digest(file = config$catalog$path, algo = "sha256"),
    checksum
  )
  # Reopening writable proves the adapter closed its read-only attachment.
  writable <- dr_connect_lake(config)
  withr::defer(dr_close_lake(writable))
  expect_true(DBI::dbIsValid(writable$con))
  expect_equal(nrow(dr_registry(writable, "runs")), 1L)
})

test_that("config source pins remain unchanged after later publications", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  config <- lake$config
  first <- dr_ingest(data.frame(id = 1L), lake, "orders", contract = c(id = "integer"))
  pinned <- dr_source_release(config, "orders", first$release_id)
  dr_close_lake(lake)
  later <- dr_ingest(data.frame(id = 2L), config, "orders", contract = c(id = "integer"))
  data <- dr_read_source(pinned)
  expect_equal(data$id, 1L)
  expect_equal(attr(data, "dr_input_reference")$release_id, first$release_id)
  latest <- dr_read_source(dr_source_release(config, "orders"))
  expect_equal(latest$id, 2L)
  expect_equal(attr(latest, "dr_input_reference")$release_id, later$release_id)
  expect_equal(dr_collect(first)$id, 1L)
})

test_that("latest is resolved once and records that release on both source forms", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  accepted <- dr_ingest(data.frame(id = 1L), lake, "orders", contract = c(id = "integer"))
  connected <- dr_source_release(lake, "orders")
  config <- lake$config
  actual_resolver <- resolve_release
  calls <- 0L
  local_family_bindings(
    resolve_release = function(...) {
      calls <<- calls + 1L
      actual_resolver(...)
    },
    .package = "dataraft.dbt"
  )
  lazy <- dr_read_source(connected)
  expect_equal(calls, 1L)
  expect_equal(attr(lazy, "dr_input_reference")$release_id, accepted$release_id)
  expect_s3_class(lazy, "tbl_sql")
  expect_true(dr_capabilities(connected)$lazy)
  expect_true(DBI::dbIsValid(lake$con))
  expect_equal(dr_collect(lazy)$id, 1L)
  dr_close_lake(lake)
  calls <- 0L
  eager <- dr_read_source(dr_source_release(config, "orders"))
  expect_equal(calls, 1L)
  expect_equal(
    attr(eager, "dr_input_reference")$release_id,
    accepted$release_id
  )
  expect_s3_class(eager, "tbl_df")
})

test_that("config source provenance survives publication into a different lake", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  original <- dr_open_lake(file.path(root, "source"))
  withr::defer(dr_close_lake(original))
  first <- dr_ingest(data.frame(id = 1L), original, "original", contract = c(id = "integer"))
  config <- original$config
  dr_close_lake(original)
  destination <- dr_open_lake(file.path(root, "destination"))
  withr::defer(dr_close_lake(destination))
  result <- dr_product("orders") |>
    dr_add_source(dr_source_release(config, "original", first$release_id)) |>
    dr_add_contract(c(id = "integer")) |>
    dr_set_target(destination) |>
    dr_run()
  expect_equal(dr_collect(result)$id, 1L)
  edges <- dr_registry(destination, "lineage_edges")
  expect_true(any(
    edges$from_id == "original" &
      edges$from_version == first$release_id &
      edges$to_id == "orders"
  ))
})

test_that("construction and validation create no storage and reject invalid inputs", {
  skip_if_not_installed("duckdb")
  root <- file.path(withr::local_tempdir(), "absent")
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb"
  )
  source <- dr_source_release(config, "orders")
  expect_false(dir.exists(root))
  expect_invisible(dr_check_component(source))
  expect_false(dir.exists(root))
  expect_error(dr_read_source(source), "read-only catalog must already exist")
  expect_false(dir.exists(root))
  expect_error(
    dr_source_release(list(), "orders"),
    "connected lake or dr_lake_config"
  )
  invalid <- config
  invalid$backend <- "unsupported"
  expect_error(dr_source_release(invalid, "orders"), "arg")
  expect_error(dr_source_release(config, "orders", character()), "release_id")
  local_family_bindings(
    need = function(package) {
      stop("Install optional package: duckdb")
    },
    .package = "dataraft.dbt"
  )
  expect_error(
    dr_source_release(config, "orders"),
    "Install optional package: duckdb"
  )
  expect_false(dir.exists(root))
})

test_that("read-only failures preserve existing files and release handles close", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  path <- file.path(root, "empty.db")
  con <- DBI::dbConnect(duckdb::duckdb(), path)
  DBI::dbDisconnect(con, shutdown = TRUE)
  config <- dr_lake_config(
    dr_registry_duckdb(path),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb",
    read_only = TRUE
  )
  checksum <- digest::digest(file = path, algo = "sha256")
  expect_error(
    dr_read_source(dr_source_release(config, "orders")),
    "Registry is missing"
  )
  expect_equal(digest::digest(file = path, algo = "sha256"), checksum)
  expect_false(dir.exists(config$landing))
  expect_false(dir.exists(config$storage$path))
  lake <- dr_open_lake(file.path(root, "initialized"))
  withr::defer(dr_close_lake(lake))
  accepted <- dr_ingest(data.frame(id = 1L), lake, "orders", contract = c(id = "integer"))
  config <- lake$config
  source <- dr_source_release(lake, "orders")
  dr_close_lake(lake)
  expect_error(dr_read_source(source), "connected dr_lake")
  expect_error(
    dr_read_source(dr_source_release(config, "orders", "unknown")),
    class = "dr_no_release"
  )
  config$read_only <- TRUE
  expect_equal(
    dr_read_source(dr_source_release(config, "orders", accepted$release_id))$id,
    1L
  )
  reopened <- dr_connect_lake(config, read_only = FALSE)
  withr::defer(dr_close_lake(reopened))
  expect_true(DBI::dbIsValid(reopened$con))
})

test_that("config sources reuse the same publication lake without a second attachment", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  config <- lake$config
  original <- dr_ingest(data.frame(id = 1L), lake, "original", contract = c(id = "integer"))
  source_config <- config
  source_config$read_only <- TRUE
  source <- dr_source_release(source_config, "original", original$release_id)
  from_open <- dr_product("connected_copy") |>
    dr_add_source(source) |>
    dr_add_contract(c(id = "integer")) |>
    dr_set_target(lake) |>
    dr_run()
  expect_equal(dr_collect(from_open)$id, 1L)
  expect_true(DBI::dbIsValid(lake$con))
  dr_close_lake(lake)
  from_config <- dr_product("config_copy") |>
    dr_add_source(source) |>
    dr_add_contract(c(id = "integer")) |>
    dr_set_target(config) |>
    dr_run()
  expect_equal(dr_collect(from_config)$id, 1L)
  ingested <- dr_ingest(source, config, "raw_copy", contract = c(id = "integer"))
  expect_equal(dr_collect(ingested)$id, 1L)
  expect_equal(
    ingested$inputs$source_version[
      ingested$inputs$source == "original"
    ],
    original$release_id
  )
  reader <- dr_connect_lake(config, read_only = TRUE)
  withr::defer(dr_close_lake(reader))
  edges <- dr_registry(reader, "lineage_edges")
  expect_setequal(
    edges$to_id[edges$from_id == "original"],
    c("connected_copy", "config_copy", "raw_copy")
  )
})

test_that("release-source subclasses keep their custom read method", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  source <- dr_source_release(lake$config, "custom")
  class(source) <- c("custom_release_source", class(source))
  calls <- 0L
  local_adapter_method(
    "dr_read_source",
    "custom_release_source",
    function(source) {
      calls <<- calls + 1L
      data.frame(id = 42L)
    }
  )
  result <- dr_product("custom_export") |>
    dr_add_source(source) |>
    dr_add_contract(c(id = "integer")) |>
    dr_set_target(lake) |>
    dr_run()
  expect_equal(dr_collect(result)$id, 42L)
  expect_equal(calls, 1L)
  result <- dr_ingest(source, lake, "custom_raw", contract = c(id = "integer"))
  expect_equal(dr_collect(result)$id, 42L)
  expect_equal(calls, 2L)
})
