test_that("the minimal workflow survives closing and reopening", {
  skip_if_not_installed("dataraft.catalog")
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root)
  withr::defer(dr_close_lake(lake))
  orders <- data.frame(
    id = 1:2,
    amount = c(10, NA_real_),
    date = as.Date(c("2026-01-01", "2026-01-02"))
  )
  first <- dr_write_data(lake, orders)
  expect_equal(first$status, "published")
  expect_equal(dr_read_release(lake, "orders"), tibble::as_tibble(orders))
  expect_s3_class(dr_read_release(lake, "orders", lazy = TRUE), "tbl_sql")
  expect_equal(dr_freshness(lake)$freshness, "unknown")
  dr_close_lake(lake)
  lake <- dr_open_lake(root)
  expect_equal(lake$config$backend, "duckdb")
  expect_equal(dr_write_data(lake, orders)$status, "cached")
  expect_equal(
    dr_read_release(lake, "orders", release = first$release_id)$id,
    1:2
  )
})

test_that("changed and empty deliveries cannot replace a successful schema", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  orders <- data.frame(id = 1:2)
  dr_write_data(lake, orders)
  for (bad in list(
    data.frame(id = c("a", "b")),
    data.frame(id = 1L, extra = TRUE),
    data.frame(id = integer())
  )) {
    result <- dr_write_data(lake, bad, "orders", stop_on_failure = FALSE)
    expect_equal(result$status, "blocked")
    expect_equal(dr_read_release(lake, "orders")$id, 1:2)
    expect_equal(any(result$quality$status == "failed"), TRUE)
  }
})

test_that("a failed first delivery does not lock the future schema", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  result <- dr_write_data(
    lake,
    data.frame(id = integer()),
    "orders",
    stop_on_failure = FALSE
  )
  expect_equal(result$status, "blocked")
  expect_equal(
    dr_write_data(lake, data.frame(id = "a"), "orders")$status,
    "published"
  )
  expect_equal(dr_read_release(lake, "orders")$id, "a")
})

test_that("writing an older payload makes it current again", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  first <- dr_write_data(lake, data.frame(id = 1L), "orders")
  second <- dr_write_data(lake, data.frame(id = 2L), "orders")
  third <- dr_write_data(lake, data.frame(id = 1L), "orders")
  expect_equal(third$status, "published")
  expect_equal(dr_read_release(lake, "orders")$id, 1L)
  expect_equal(
    dr_read_release(lake, "orders", release = second$release_id)$id,
    2L
  )
  expect_equal(third$release_id == first$release_id, FALSE)
  expect_equal(
    dr_write_data(lake, data.frame(id = 1L), "orders")$status,
    "cached"
  )
})

test_that("file defaults preserve original bytes and support CSV TSV and RDS", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  data <- data.frame(id = 1:2, value = c(10.5, 20.5))
  csv <- file.path(root, "orders.csv")
  tsv <- file.path(root, "tabular.tsv")
  rds <- file.path(root, "snapshot.rds")
  utils::write.csv(data, csv, row.names = FALSE)
  utils::write.table(data, tsv, sep = "\t", row.names = FALSE)
  saveRDS(data, rds)
  for (path in c(csv, tsv, rds)) {
    expect_equal(dr_write_data(lake, path)$status, "published")
    expect_equal(dr_write_data(lake, path)$status, "cached")
    name <- tools::file_path_sans_ext(basename(path))
    expect_equal(dr_read_release(lake, name), tibble::as_tibble(data))
  }
  inputs <- dr_registry(lake, "inputs")
  expect_setequal(
    inputs$original_name,
    c("orders.csv", "tabular.tsv", "snapshot.rds")
  )
  landed <- inputs$landed_path[inputs$original_name == "orders.csv"][[1]]
  expect_equal(
    readBin(landed, "raw", n = file.info(landed)$size),
    readBin(csv, "raw", n = file.info(csv)$size)
  )
})

test_that("explicit contracts can add rules but cannot be silently dropped", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  data <- data.frame(id = 1:2)
  dr_write_data(lake, data, "orders")
  contract <- dr_contract(
    "orders.checked",
    columns = c(id = "integer"),
    key = "id"
  )
  expect_equal(
    dr_write_data(lake, data, "orders", contract)$status,
    "published"
  )
  bad <- dr_write_data(
    lake,
    data.frame(id = c(1L, 1L)),
    "orders",
    contract,
    stop_on_failure = FALSE
  )
  expect_equal(bad$status, "blocked")
  expect_snapshot(error = TRUE, dr_write_data(lake, data, "orders"))
})

test_that("changed rule bindings require new contract versions before re-evaluation", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  allowed <- TRUE
  contract <- dr_contract(
    "checked",
    columns = c(id = "integer"),
    rules = list(dr_quality_rule("external_state", function(data) allowed))
  )
  data <- data.frame(id = 1L)
  expect_equal(
    dr_write_data(lake, data, "orders", contract)$status,
    "published"
  )
  allowed <- FALSE
  expect_error(
    dr_write_data(lake, data, "orders", contract, stop_on_failure = FALSE),
    class = "dr_definition_changed"
  )
  contract$version <- "1.0.1"
  expect_equal(
    dr_write_data(
      lake,
      data,
      "orders",
      contract,
      stop_on_failure = FALSE
    )$status,
    "blocked"
  )
  allowed <- TRUE
  contract$version <- "1.0.2"
  expect_equal(
    dr_write_data(
      lake,
      data,
      "orders",
      contract,
      code_version = "checked-v1"
    )$status,
    "published"
  )
  expect_equal(
    dr_write_data(
      lake,
      data,
      "orders",
      contract,
      code_version = "checked-v1"
    )$status,
    "cached"
  )
  expect_snapshot(
    error = TRUE,
    dr_write_data(lake, data, "orders", contract, cache = TRUE)
  )
})

test_that("custom file readers use archived bytes and changing captured values", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  path <- file.path(root, "input.txt")
  writeLines("original", path)
  value <- 1L
  reader <- function(path) {
    stopifnot(readLines(path) == "original")
    data.frame(id = value)
  }
  expect_equal(dr_write_data(lake, path, reader = reader)$status, "published")
  value <- 2L
  expect_equal(dr_write_data(lake, path, reader = reader)$status, "published")
  expect_equal(dr_read_release(lake, "input")$id, 2L)
})

test_that("reopening refuses an accidental backend switch", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root)
  dr_close_lake(lake)
  expect_snapshot(error = TRUE, dr_open_lake(root, backend = "ducklake"))
})

test_that("existing unmarked catalogs are not adopted implicitly", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  dir.create(file.path(root, "landing"))
  expect_snapshot(error = TRUE, dr_open_lake(root))
})

test_that("arbitrary nonempty folders are left untouched", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  writeLines("existing custom catalog", file.path(root, "custom.db"))
  expect_snapshot(error = TRUE, dr_open_lake(root))
  expect_equal(list.files(root, all.files = TRUE, no.. = TRUE), "custom.db")
  expect_equal(
    readLines(file.path(root, "custom.db")),
    "existing custom catalog"
  )
})

test_that("a blocked first contracted run still requires a contract after reopen", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root)
  withr::defer(dr_close_lake(lake))
  contract <- dr_contract("checked", columns = c(id = "integer"), key = "id")
  result <- dr_write_data(
    lake,
    data.frame(id = c(1L, 1L)),
    "orders",
    contract,
    stop_on_failure = FALSE
  )
  expect_equal(result$status, "blocked")
  dr_close_lake(lake)
  lake <- dr_open_lake(root)
  expect_snapshot(
    error = TRUE,
    dr_write_data(lake, data.frame(id = 1L), "orders")
  )
  expect_equal(nrow(dr_releases(lake, "orders")), 0L)
  expect_equal(
    dr_write_data(lake, data.frame(id = 1L), "orders", contract)$status,
    "published"
  )
})

test_that("a blocked contract upgrade cannot fall back to the automatic schema", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  dr_write_data(lake, data.frame(id = 1L), "orders")
  contract <- dr_contract("checked", columns = c(id = "integer"), key = "id")
  result <- dr_write_data(
    lake,
    data.frame(id = c(1L, 1L)),
    "orders",
    contract,
    stop_on_failure = FALSE
  )
  expect_equal(result$status, "blocked")
  expect_snapshot(
    error = TRUE,
    dr_write_data(lake, data.frame(id = 1L), "orders")
  )
  expect_equal(dr_read_release(lake, "orders")$id, 1L)
})

test_that("a data expression needs a deliberate asset name", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  expect_snapshot(error = TRUE, dr_write_data(lake, data.frame(id = 1L)))
})

test_that("contract drafts keep review explicit with optional metadata", {
  skip_if_not_installed("duckdb")
  draft <- dr_contract_from(data.frame(id = 1:2), "orders")
  contract <- dr_contract_confirm(draft)
  expect_equal(contract$owner, "")
  expect_equal(contract$grain, "")
  expect_equal(contract$columns, list(id = "integer"))
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  expect_snapshot(
    error = TRUE,
    dr_write_data(lake, data.frame(id = 1L), "orders", contract = draft)
  )
})

test_that("the simple entry point also works with DuckLake", {
  skip_if(Sys.getenv("DATARAFT_TEST_DUCKLAKE") != "true")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root, backend = "ducklake")
  withr::defer(dr_close_lake(lake))
  orders <- data.frame(id = 1:2)
  dr_write_data(lake, orders)
  dr_close_lake(lake)
  lake <- dr_open_lake(root)
  expect_equal(lake$config$backend, "ducklake")
  expect_equal(dr_write_data(lake, orders)$status, "cached")
  expect_equal(dr_read_release(lake, "orders")$id, 1:2)
})
