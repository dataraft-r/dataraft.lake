test_that("minimal ingestion publishes an exact raw reference and schema", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  orders <- data.frame(id = 1:2, amount = c(25, 75))
  accepted <- dr_ingest(orders, lake, quality = ~ amount >= 0, contract = dr_contract(columns = c(id = "integer", amount = "numeric"), required = character()))
  expect_s3_class(accepted, "dr_run_result")
  expect_equal(accepted$status, "published")
  expect_equal(accepted$asset, "orders")
  expect_equal(accepted$outputs$database, "lake")
  expect_equal(accepted$outputs$schema, "raw")
  expect_equal(accepted$outputs$asset, accepted$asset)
  expect_equal(accepted$outputs$release_id, accepted$release_id)
  expect_match(accepted$outputs$table, "^candidate_")
  expect_equal(
    accepted$outputs$table,
    resolve_release(lake, "orders", accepted$release_id)$table_name[[1]]
  )
  expect_equal(dr_collect(accepted), tibble::as_tibble(orders))
  expect_equal(accepted$metadata$schema, c(id = "integer", amount = "numeric"))
  expect_equal(accepted$metadata$rows, 2)
  expect_equal(accepted$metadata$contract$required, character())
  expect_setequal(dr_quality(accepted)$stage, c("ingest", "candidate"))
  expect_true(DBI::dbIsValid(lake$con))
})

test_that("native input failures never write raw and keep accepted releases", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  checks <- list(nonnegative = ~ amount >= 0)
  accepted <- dr_ingest(
    data.frame(id = 1L, amount = 10),
    lake,
    "orders",
    quality = checks,
    contract = c(id = "integer", amount = "numeric")
  )
  blocked <- dr_ingest(
    data.frame(id = 2L, amount = -1),
    lake,
    "orders",
    quality = checks,
    stop_on_failure = FALSE,
    contract = c(id = "integer", amount = "numeric")
  )
  expect_equal(blocked$status, "blocked")
  expect_equal(unique(dr_quality(blocked)$stage), "ingest")
  expect_true(any(dr_quality(blocked)$status == "failed"))
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("raw_", blocked$run_id))
  ))
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("candidate_", blocked$run_id))
  ))
  expect_true(all(file.exists(blocked$inputs$landed_path)))
  expect_equal(dr_releases(lake, "orders")$release_id, accepted$release_id)
  expect_equal(dr_collect(accepted)$amount, 10)
  expect_equal(dr_registry(lake, "runs")$status, c("published", "blocked"))
  error <- tryCatch(
    dr_ingest(
      data.frame(id = 3L, amount = -2),
      lake,
      "orders",
      quality = checks
    , contract = c(id = "integer", amount = "numeric")),
    dr_run_failed = identity
  )
  expect_s3_class(error, "dr_run_failed")
  expect_equal(error$result$status, "blocked")
})

test_that("file readers and callbacks run once against retained original bytes", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  path <- file.path(root, "orders.csv")
  utils::write.csv(data.frame(id = 1L, amount = 10), path, row.names = FALSE)
  reads <- checks <- 0L
  observed_path <- NULL
  reader <- function(path) {
    reads <<- reads + 1L
    observed_path <<- path
    utils::read.csv(path)
  }
  check <- function(data) {
    checks <<- checks + 1L
    data$amount >= 0
  }
  accepted <- dr_ingest(path, lake, quality = check, reader = reader, contract = c(id = "integer", amount = "numeric"))
  expect_equal(accepted$asset, "orders")
  expect_equal(reads, 1L)
  expect_equal(checks, 1L)
  expect_equal(observed_path, accepted$inputs$landed_path[[1]])
  utils::write.csv(data.frame(id = 2L, amount = -10), path, row.names = FALSE)
  blocked <- dr_ingest(
    path,
    lake,
    quality = check,
    reader = reader,
    stop_on_failure = FALSE,
    contract = c(id = "integer", amount = "numeric")
  )
  expect_equal(blocked$status, "blocked")
  expect_equal(reads, 2L)
  expect_equal(checks, 2L)
  expect_equal(
    readBin(blocked$inputs$landed_path[[1]], "raw", n = 10000),
    readBin(path, "raw", n = 10000)
  )
  expect_equal(dr_collect(accepted)$amount, 10)
})

test_that("inferred deliveries remain unvalidated and never establish a release schema", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  for (delivery in list(data.frame(id = 1:2), data.frame(id = c("a", "b")))) {
    result <- dr_ingest(delivery, lake, "orders", stop_on_failure = FALSE)
    expect_identical(result$status, "unvalidated")
    expect_true(any(dr_quality(result)$status == "unvalidated"))
    expect_equal(result$diagnostic$data, delivery, ignore_attr = TRUE)
    expect_equal(nrow(dr_releases(lake, "orders")), 0L)
    expect_false(DBI::dbExistsTable(
      lake$con, table_id("raw", paste0("raw_", result$run_id))
    ))
  }
  empty <- dr_ingest(data.frame(id = integer()), lake, "orders",
    stop_on_failure = FALSE)
  expect_identical(empty$status, "blocked")
  declared <- dr_ingest(data.frame(id = "a"), lake, "orders",
    contract = c(id = "character"))
  expect_identical(declared$status, "published")
  expect_equal(dr_collect(declared)$id, "a")
})

test_that("source functions are acquired once and default runs reevaluate captured state", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  calls <- checks <- 0L
  value <- 1L
  allowed <- TRUE
  source <- function() {
    calls <<- calls + 1L
    data.frame(id = value)
  }
  check <- function(data) {
    checks <<- checks + 1L
    allowed
  }
  first <- dr_ingest(source, lake, "orders", quality = check, contract = c(id = "integer"))
  expect_equal(c(calls, checks), c(1L, 1L))
  value <- 2L
  second <- dr_ingest(source, lake, "orders", quality = check, contract = c(id = "integer"))
  expect_equal(c(calls, checks), c(2L, 2L))
  expect_equal(dr_collect(first)$id, 1L)
  expect_equal(dr_collect(second)$id, 2L)
  allowed <- FALSE
  blocked <- dr_ingest(
    source,
    lake,
    "orders",
    quality = check,
    stop_on_failure = FALSE,
    contract = c(id = "integer")
  )
  expect_equal(blocked$status, "blocked")
  expect_equal(c(calls, checks), c(3L, 3L))
  expect_equal(resolve_release(lake, "orders")$release_id, second$release_id)
})

test_that("explicit contracts retain keys and nonnull rules and accept concise types", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  specification <- dr_contract(
    columns = list(id = integer(), amount = double()),
    key = "id"
  )
  accepted <- dr_ingest(
    data.frame(id = 1:2, amount = c(10, 20)),
    lake,
    "orders",
    specification
  )
  candidate <- dr_quality(accepted)
  expect_true(all(
    c("ingest", "candidate") %in%
      candidate$stage[grepl("key|not_null", candidate$rule)]
  ))
  for (bad in list(
    data.frame(id = c(1L, 1L), amount = c(10, 20)),
    data.frame(id = 3L, amount = NA_real_)
  )) {
    result <- dr_ingest(
      bad,
      lake,
      "orders",
      specification,
      stop_on_failure = FALSE
    )
    expect_equal(result$status, "blocked")
    expect_equal(unique(dr_quality(result)$stage), "ingest")
  }
  concise <- dr_ingest(
    data.frame(id = 1L),
    lake,
    "concise",
    contract = c(id = "integer")
  )
  expect_equal(concise$status, "published")
  prototypes <- dr_ingest(
    data.frame(id = 1L),
    lake,
    "prototypes",
    contract = list(id = integer())
  )
  expect_equal(prototypes$status, "published")
})

test_that("warning-only input checks accept data and are not rerun on the candidate", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  calls <- 0L
  advisory <- dr_quality_rule(
    "advisory",
    function(data) {
      calls <<- calls + 1L
      FALSE
    },
    severity = "warning"
  )
  result <- dr_ingest(data.frame(id = 1L), lake, "orders", quality = advisory, contract = c(id = "integer"))
  expect_equal(result$status, "published")
  expect_equal(calls, 1L)
  checks <- dr_quality(result)
  expect_equal(checks$stage[checks$rule == "advisory"], "ingest")
  expect_equal(checks$status[checks$rule == "advisory"], "warning")
})

test_that("config ownership and exact result collection survive later releases", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  config <- dr_lake_config(
    dr_registry_duckdb(file.path(root, "lake.db")),
    dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = "duckdb"
  )
  first <- dr_ingest(data.frame(id = 1L), config, "orders", contract = c(id = "integer"))
  expect_null(first$output_lake)
  expect_equal(first$output_config, config)
  second <- dr_ingest(data.frame(id = 2L), config, "orders", contract = c(id = "integer"))
  expect_equal(dr_collect(first)$id, 1L)
  expect_equal(dr_collect(second)$id, 2L)
  lake <- dr_connect_lake(config)
  withr::defer(dr_close_lake(lake))
  expect_equal(resolve_release(lake, "orders")$release_id, second$release_id)
})

test_that("database source factories open once and close before returning", {
  skip_if_not_installed("dataraft.adapters")
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  calls <- 0L
  source_con <- NULL
  factory <- function() {
    calls <<- calls + 1L
    source_con <<- DBI::dbConnect(duckdb::duckdb(), bigint = "integer64")
    DBI::dbWriteTable(source_con, "delivery", data.frame(id = 1:2))
    source_con
  }
  result <- dr_ingest(
    dr_source_database(factory, table = "delivery"),
    lake,
    "orders",
    contract = c(id = "integer")
  )
  expect_equal(calls, 1L)
  expect_false(DBI::dbIsValid(source_con))
  expect_equal(dr_collect(result)$id, 1:2)
  expect_match(result$inputs$original_name, "rds$")
})

test_that("pointblank builds and checks once before any raw write", {
  skip_if_not_installed("pointblank")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  calls <- 0L
  check <- dr_pointblank_checks("amounts", function(data) {
    calls <<- calls + 1L
    pointblank::create_agent(data) |>
      pointblank::col_vals_gte(columns = "amount", value = 0)
  })
  first <- dr_ingest(data.frame(amount = 10), lake, "orders", quality = check, contract = c(amount = "numeric"))
  expect_equal(first$status, "published")
  expect_equal(calls, 1L)
  blocked <- dr_ingest(
    data.frame(amount = -1),
    lake,
    "orders",
    quality = check,
    stop_on_failure = FALSE,
    contract = c(amount = "numeric")
  )
  expect_equal(blocked$status, "blocked")
  expect_equal(calls, 2L)
  expect_equal(unique(dr_quality(blocked)$stage), "ingest")
  expect_true("pointblank" %in% dr_quality(blocked)$engine)
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("raw_", blocked$run_id))
  ))
  expect_equal(resolve_release(lake, "orders")$release_id, first$release_id)
})

test_that("cache is explicit and returns original checked contract evidence", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  check <- function(data) TRUE
  data <- data.frame(id = 1L)
  expect_error(dr_ingest(data, lake, "orders", cache = TRUE, contract = c(id = "integer")), "code_version")
  first <- dr_ingest(
    data,
    lake,
    "orders",
    quality = check,
    code_version = "v1",
    cache = TRUE,
    contract = c(id = "integer")
  )
  second <- dr_ingest(
    data,
    lake,
    "orders",
    quality = check,
    code_version = "v1",
    cache = TRUE,
    contract = c(id = "integer")
  )
  expect_equal(second$status, "cached")
  expect_equal(second$release_id, first$release_id)
  expect_equal(second$metadata$contract$id, first$metadata$contract$id)
  expect_true(nrow(dr_quality(second)) > 0L)
  expect_equal(dr_collect(second), tibble::as_tibble(data))
})

test_that("reader failure retains one durable run and immutable original", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  path <- file.path(root, "broken.txt")
  writeLines("invalid delivery", path)
  result <- dr_ingest(
    path,
    lake,
    reader = function(path) stop("cannot parse"),
    stop_on_failure = FALSE
  )
  expect_equal(result$status, "error")
  expect_equal(nrow(dr_registry(lake, "runs")), 1L)
  expect_true(file.exists(result$inputs$landed_path[[1]]))
  expect_equal(readLines(result$inputs$landed_path[[1]]), "invalid delivery")
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("raw_", result$run_id))
  ))
})

test_that("pinned release sources retain their exact lineage in ingestion", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  original <- dr_ingest(data.frame(id = 1L), lake, "original", contract = c(id = "integer"))
  dr_ingest(data.frame(id = 2L), lake, "original", contract = c(id = "integer"))
  result <- dr_ingest(
    dr_source_release(lake, "original", original$release_id),
    lake,
    "orders",
    contract = c(id = "integer")
  )
  expect_equal(dr_collect(result)$id, 1L)
  reference <- result$inputs[result$inputs$source == "original", ]
  expect_equal(reference$source_version, original$release_id)
  edges <- dr_registry(lake, "lineage_edges")
  expect_true(any(
    edges$from_id == "original" &
      edges$from_version == original$release_id &
      edges$to_id == "orders"
  ))
})

test_that("single local Parquet input keeps original bytes before checking", {
  skip_if_not_installed("arrow")
  root <- withr::local_tempdir()
  lake <- dr_open_lake(file.path(root, "lake"))
  withr::defer(dr_close_lake(lake))
  path <- file.path(root, "orders.parquet")
  arrow::write_parquet(data.frame(id = 1L, amount = -1), path)
  result <- dr_ingest(
    path,
    lake,
    quality = ~ amount >= 0,
    stop_on_failure = FALSE
  )
  expect_equal(result$status, "blocked")
  expect_equal(result$inputs$original_name, "orders.parquet")
  expect_equal(
    readBin(result$inputs$landed_path[[1]], "raw", n = 10000),
    readBin(path, "raw", n = 10000)
  )
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("raw_", result$run_id))
  ))
})

test_that("ingestion refuses invalid execution options and approved asset reuse", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  data <- data.frame(id = 1L)
  expect_error(dr_ingest(data, lake, business_date = 1:2), "business_date")
  expect_error(dr_ingest(data, lake, cahe = TRUE), "Unknown ingestion option")
  approved <- dr_write_data(lake, data, "orders", contract = c(id = "integer"))
  expect_error(dr_ingest(data, lake, "orders"), "distinct ingestion name")
  expect_equal(resolve_release(lake, "orders")$release_id, approved$release_id)
})

test_that("product ingestion retains its input contract and quality before RAW", {
  skip_if_not_installed("duckdb")
  config <- dr_lake_config(path = file.path(withr::local_tempdir(), "lake"))
  orders <- data.frame(id = 1L, amount = 10)
  specification <- dr_product("orders", orders, code_version = "delivery-v1") |>
    dr_add_contract(dr_contract(
      columns = c(id = "integer", amount = "numeric"),
      key = "id"
    )) |>
    dr_add_quality(~ amount >= 0)
  accepted <- specification |> dr_ingest(to = config)
  expect_identical(accepted$asset, "orders")
  expect_equal(dr_collect(accepted), tibble::as_tibble(orders))
  expect_identical(accepted$metadata$contract$key, "id")
  expect_identical(accepted$metadata$definition$code_version, "delivery-v1")
  bad <- dr_product(
    "orders",
    data.frame(id = 2L, amount = -1),
    code_version = "delivery-v1"
  ) |>
    dr_add_contract(specification$contract) |>
    dr_add_quality(~ amount >= 0)
  rejected <- bad |> dr_ingest(to = config, stop_on_failure = FALSE)
  expect_identical(rejected$status, "blocked")
  expect_identical(unique(dr_quality(rejected)$stage), "ingest")
  lake <- dr_connect_lake(config)
  withr::defer(dr_close_lake(lake))
  expect_false(DBI::dbExistsTable(
    lake$con,
    table_id("raw", paste0("raw_", rejected$run_id))
  ))
  expect_identical(
    resolve_release(lake, "orders")$release_id[[1]],
    accepted$release_id
  )
})

test_that("unsupported product ingestion fails before source or destination I/O", {
  root <- file.path(withr::local_tempdir(), "not-created")
  config <- dr_lake_config(path = root)
  calls <- 0L
  source <- function() {
    calls <<- calls + 1L
    data.frame(id = 1L)
  }
  plain <- dr_product("orders", source)
  transformed <- plain |> dr_add_transform(identity)
  multiple <- plain |> dr_add_source(data.frame(id = 2L))
  nested <- dr_product("nested", plain)
  targeted <- plain |> dr_set_target(config)
  cataloged <- plain |> dr_add_catalog(function(...) invisible(NULL))
  for (invalid in list(dr_product("empty"), transformed, multiple, nested)) {
    expect_error(dr_ingest(invalid, to = config), "one ordinary product source")
  }
  for (invalid in list(targeted, cataloged)) {
    expect_error(
      dr_ingest(invalid, to = config),
      "Remove product targets and catalogs"
    )
  }
  expect_error(
    dr_ingest(plain, to = config, contract = c(id = "integer")),
    "add_contract"
  )
  expect_error(dr_ingest(plain, to = config, reader = readRDS), "add_source")
  expect_error(
    dr_ingest(plain, to = config, name = "different"),
    "own ingestion name"
  )
  expect_equal(calls, 0L)
  expect_false(dir.exists(root))
})

test_that("data-first ingestion has a local default and checks before creating it", {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir()
  withr::local_dir(root)
  orders <- data.frame(id = 1L)
  expect_error(dr_ingest(orders, quality = ~id ~ 1, contract = c(id = "integer")), "one-sided")
  expect_false(dir.exists("dataraft"))
  accepted <- orders |> dr_ingest(contract = c(id = "integer"))
  expect_identical(accepted$asset, "orders")
  expect_identical(accepted$backend, "duckdb")
  expect_equal(dr_collect(accepted)$id, 1L)
  elsewhere <- orders |> dr_ingest(to = "other-lake", contract = c(id = "integer"))
  expect_equal(dr_collect(elsewhere)$id, 1L)
  readonly <- dr_lake_config(path = "not-created", read_only = TRUE)
  expect_error(dr_ingest(orders, to = readonly, contract = c(id = "integer")), "writable destination")
  expect_false(dir.exists("not-created"))
})

test_that("input evidence retains the definition before callback state changes", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  calls <- 0L
  check <- function(data) {
    calls <<- calls + 1L
    data$id > 0
  }
  first <- dr_ingest(data.frame(id = 1L), lake, "checked", quality = check, contract = c(id = "integer"))
  stored <- dr_registry(lake, "assets")
  registered <- stored[
    stored$kind == "contract" &
      stored$id == first$metadata$contract$id &
      stored$version == first$metadata$contract$version,
  ]
  expect_identical(jencode(first$metadata$contract), registered$definition[[1]])
  expect_equal(calls, 1L)
  second <- dr_ingest(data.frame(id = 1L), lake, "checked", quality = check, contract = c(id = "integer"))
  expect_equal(calls, 2L)
  expect_equal(second$status, "published")
  expect_equal(
    identical(
      first$metadata$contract$version,
      second$metadata$contract$version
    ),
    FALSE
  )
})

test_that("dynamic source descriptors cannot enable ingestion caching", {
  skip_if_not_installed("duckdb")
  lake <- dr_open_lake(withr::local_tempdir())
  withr::defer(dr_close_lake(lake))
  state <- new.env(parent = emptyenv())
  state$data <- data.frame(id = 1L)
  source <- function() state$data
  expect_error(
    dr_ingest(source, lake, "dynamic", cache = TRUE, code_version = "v1"),
    class = "dr_dynamic_source_cache"
  )
})
