test_that("native and lake targets run the same transformations and quality rules", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(
    file.path(root, "lake"),
    backend = Sys.getenv("DATARAFT_TEST_BACKEND", "duckdb")
  )
  withr::defer(dr_close_lake(lake))
  received <- NULL
  product <- dr_product("orders") |>
    dr_add_source(data.frame(id = 1:2, amount = c(10, 20))) |>
    dr_add_transform(
      function(data) transform(data, amount = amount * 2),
      "double"
    ) |>
    dr_add_contract(c(id = "integer", amount = "numeric")) |>
    dr_add_quality(~ amount > 0) |>
    dr_add_catalog(function(metadata) received <<- metadata)
  native <- dr_run(product)
  persisted <- dr_run(product |> dr_set_target(lake))
  expect_equal(dr_collect(native), dr_collect(persisted))
  expect_equal(persisted$status, "published")
  expect_equal(received$rows, 2)
  expect_equal(DBI::dbIsValid(lake$con), TRUE)
  expect_equal(persisted$quality$status, native$quality$status)
  expect_equal(nrow(persisted$inputs), 1L)
  expect_equal(nrow(persisted$metadata$lineage), 2L)
  expect_equal(file.exists(persisted$inputs$landed_path), TRUE)
  expect_equal(readRDS(persisted$inputs$landed_path)$amount, c(10, 20))
})

test_that("automatic contracts are inferred after transforms and preserve schemas and rules", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  make <- function(data) {
    dr_product("orders") |>
      dr_add_source(data) |>
      dr_add_transform(function(data) data.frame(total = sum(data$amount))) |>
      dr_add_quality(~ total >= 0) |>
      dr_set_target(lake)
  }
  first <- dr_run(make(data.frame(amount = c(10, 20))))
  expect_equal(dr_collect(first)$total, 30)
  second <- dr_run(make(data.frame(amount = c(20, 20))))
  expect_equal(dr_collect(second)$total, 40)
  blocked <- dr_run(make(data.frame(amount = -10)), stop_on_failure = FALSE)
  expect_equal(blocked$status, "blocked")
  expect_equal(dr_read_release(lake, "orders")$total, 40)
  expect_equal(dr_collect(first)$total, 30)
  unguarded <- dr_product("orders") |>
    dr_add_source(data.frame(total = 50)) |>
    dr_set_target(lake)
  expect_equal(dr_run(unguarded, stop_on_failure = FALSE)$status, "error")
  expect_snapshot(
    error = TRUE,
    dr_write_data(lake, data.frame(total = 50), "orders")
  )
})

test_that("file publication archives original bytes and records transform definitions", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  path <- file.path(root, "orders.csv")
  utils::write.csv(data.frame(id = 1:2), path, row.names = FALSE)
  product <- dr_product("orders") |>
    dr_add_source(path) |>
    dr_add_transform(
      function(data) transform(data, amount = id * 10),
      "add_amount"
    )
  result <- dr_publish(product, to = file.path(root, "lake"))
  expect_equal(dr_collect(result)$amount, c(10, 20))
  expect_identical(
    readBin(result$inputs$landed_path, "raw", n = file.info(path)$size),
    readBin(path, "raw", n = file.info(path)$size)
  )
  lake <- dr_open_lake(file.path(root, "lake"), read_only = TRUE)
  withr::defer(dr_close_lake(lake))
  definitions <- dr_registry(lake, "assets")
  expect_equal(
    any(grepl("add_amount", definitions$definition, fixed = TRUE)),
    TRUE
  )
})

test_that("partition targets validate retained partitions and preserve published history", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(
    file.path(root, "lake"),
    backend = Sys.getenv("DATARAFT_TEST_BACKEND", "duckdb")
  )
  withr::defer(dr_close_lake(lake))
  contract <- dr_contract(
    "orders",
    columns = c(id = "integer", month = "character", amount = "numeric"),
    key = c("id", "month")
  )
  make <- function(month, amount) {
    dr_product("orders") |>
      dr_add_source(data.frame(id = 1L, month = month, amount = amount)) |>
      dr_add_contract(contract) |>
      dr_add_quality(~ amount >= 0) |>
      dr_set_target(dr_target_lake(lake, partition_by = "month"))
  }
  first <- dr_run(make("August", 10))
  second <- dr_run(make("September", 20))
  expect_equal(nrow(dr_collect(second)), 2L)
  expect_equal(sum(dr_collect(second)$amount), 30)
  expect_equal(
    dr_run(make("August", -1), stop_on_failure = FALSE)$status,
    "blocked"
  )
  expect_equal(sum(dr_read_release(lake, "orders")$amount), 30)
  expect_equal(nrow(dr_collect(first)), 1L)
})

test_that("explicit contracts and versioned definitions cannot be silently weakened", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  product <- dr_product("orders", version = "1", code_version = "v1") |>
    dr_add_source(data.frame(id = 1L)) |>
    dr_add_contract(c(id = "integer")) |>
    dr_set_target(lake)
  first <- dr_run(product)
  cached <- dr_run(product, cache = TRUE)
  expect_identical(cached$status, "cached")
  expect_false(identical(cached$run_id, first$run_id))
  expect_identical(cached$release_id, first$release_id)
  expect_gt(nrow(cached$quality), 0L)
  expect_setequal(cached$quality$run_id, first$run_id)
  expect_setequal(cached$metadata$quality$run_id, first$run_id)
  changed <- product |>
    dr_add_transform(function(data) transform(data, id = id + 1L))
  expect_equal(dr_run(changed, stop_on_failure = FALSE)$status, "error")
  expect_equal(dr_read_release(lake, "orders")$id, 1L)
  unguarded <- dr_product("orders") |>
    dr_add_source(data.frame(id = 2L)) |>
    dr_set_target(lake)
  expect_equal(dr_run(unguarded, stop_on_failure = FALSE)$status, "error")
  expect_equal(dr_collect(first)$id, 1L)
})

test_that("readonly targets reject a workflow before calling its source", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  dr_close_lake(lake)
  lake <- dr_open_lake(file.path(root, "lake"), read_only = TRUE)
  withr::defer(dr_close_lake(lake))
  calls <- 0
  product <- dr_product("orders") |>
    dr_add_source(function() {
      calls <<- calls + 1
      data.frame(id = 1L)
    }) |>
    dr_set_target(lake)
  expect_snapshot(error = TRUE, dr_run(product))
  expect_equal(calls, 0)
})

test_that("ordinary R quality callbacks see the same values with native and lake execution", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  product <- dr_product("orders") |>
    dr_add_source(data.frame(id = 1:2, amount = c(10, -1))) |>
    dr_add_quality(function(data) all(data$amount >= 0), "positive")
  native <- dr_run(product, stop_on_failure = FALSE)
  stored <- dr_publish(
    product,
    to = file.path(root, "lake"),
    stop_on_failure = FALSE
  )
  expect_equal(native$status, "blocked")
  expect_equal(stored$status, "blocked")
  expect_equal(stored$quality, native$quality, ignore_attr = TRUE)
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  expect_equal(nrow(dr_releases(lake)), 0L)
})

test_that("lake preparation failures have one durable run and archived provenance", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  a <- file.path(root, "a.csv")
  b <- file.path(root, "b.csv")
  utils::write.csv(data.frame(id = 1L), a, row.names = FALSE)
  utils::write.csv(data.frame(id = 2L), b, row.names = FALSE)
  calls <- 0L
  broken_reader <- function(path) {
    calls <<- calls + 1L
    stop("Cannot decode input")
  }
  definition <- dr_product("joined") |>
    dr_add_source(a, "a") |>
    dr_add_source(b, "b", reader = broken_reader) |>
    dr_add_transform(function(data) rbind(data$a, data$b)) |>
    dr_set_target(lake)
  failed <- dr_run(definition, stop_on_failure = FALSE)
  runs <- dr_registry(lake, "runs")
  expect_equal(nrow(runs), 1L)
  expect_identical(runs$run_id, failed$run_id)
  expect_identical(runs$status, "error")
  expect_equal(calls, 1L)
  expect_equal(nrow(failed$inputs), 2L)
  expect_true(all(file.exists(failed$inputs$landed_path)))
  expect_equal(nrow(dr_releases(lake)), 0L)

  joined <- definition |>
    dr_add_source(b, "b", replace = TRUE) |>
    dr_add_transform(function(data) stop("Combining failed"), "fail")
  failed_join <- dr_run(joined, stop_on_failure = FALSE)
  expect_identical(failed_join$status, "error")
  expect_equal(nrow(dr_registry(lake, "runs")), 2L)
  expect_equal(nrow(failed_join$inputs), 2L)

  success <- definition |> dr_add_source(b, "b", replace = TRUE) |> dr_run()
  expect_identical(success$status, "published")
  expect_equal(nrow(dr_registry(lake, "runs")), 3L)
  expect_equal(nrow(success$inputs), 3L)
  expect_equal(dr_collect(success)$id, 1:2)
  expect_false(any(dr_registry(lake, "runs")$status == "running"))
})

test_that("automatic definitions include publication policy while explicit versions stay immutable", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  schema <- dr_contract(
    columns = c(id = "integer", month = "character"),
    key = "id"
  )
  definition <- dr_product("orders", contract = schema) |>
    dr_add_source(data.frame(id = 1L, month = "August")) |>
    dr_set_target(lake)
  first <- dr_run(definition)
  partitioned <- definition |>
    dr_add_source(
      data.frame(id = 2L, month = "September"),
      "source_1",
      replace = TRUE
    ) |>
    dr_set_target(dr_target_lake(lake, partition_by = "month"))
  second <- dr_run(partitioned)
  expect_identical(second$status, "published")
  expect_equal(dr_collect(second)$id, 1:2)
  expect_equal(dr_collect(first)$id, 1L)
  expect_equal(nrow(dr_registry(lake, "runs")), 2L)
  fixed <- dr_product("fixed", contract = schema, version = "1") |>
    dr_add_source(data.frame(id = 1L, month = "August")) |>
    dr_set_target(lake)
  dr_run(fixed)
  changed <- fixed |>
    dr_set_target(dr_target_lake(lake, partition_by = "month"))
  rejected <- dr_run(changed, stop_on_failure = FALSE)
  expect_identical(rejected$status, "error")
  expect_match(conditionMessage(rejected$error), "version bump")
  expect_equal(dr_read_release(lake, "fixed")$id, 1L)
})
