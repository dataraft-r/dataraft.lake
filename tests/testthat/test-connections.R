test_that("observer lifecycle uses matching identities and shares handles", {
  skip_if_not_installed("duckdb")
  events <- list()
  opened <- NULL
  withr::local_options(
    connectionObserver = list(
      connectionOpened = function(
        type,
        displayName,
        host,
        connectCode,
        disconnect,
        listObjectTypes,
        listObjects,
        listColumns,
        previewObject
      ) {
        opened <<- as.list(environment())
        events[[length(events) + 1L]] <<- list("open", type, host)
      },
      connectionUpdated = function(type, host) {
        events[[length(events) + 1L]] <<- list("update", type, host)
      },
      connectionClosed = function(type, host) {
        events[[length(events) + 1L]] <<- list("close", type, host)
      }
    )
  )
  root <- withr::local_tempdir()
  lake <- dr_open_lake(root)
  withr::defer(dr_close_lake(lake))
  another <- dr_connect_lake(lake$config)
  withr::defer(dr_close_lake(another))
  connection_opened(lake)
  expect_length(events, 1L)
  expect_identical(opened$type, "DataRaft")
  expect_identical(
    opened$listObjectTypes(),
    list(schema = list(contains = list(table = list(contains = "data"))))
  )
  expect_identical(opened$listObjects(), connection_frame())
  dr_refresh_connection(lake)
  expect_identical(events[[2]], list("update", "DataRaft", opened$host))
  dr_close_lake(lake)
  expect_length(events, 2L)
  expect_identical(opened$listObjects(), connection_frame())
  opened$disconnect()
  expect_identical(DBI::dbIsValid(another$con), FALSE)
  expect_identical(events[[3]], list("close", "DataRaft", opened$host))
  dr_close_lake(lake)
  dr_close_lake(another)
  expect_length(events, 3L)
  host <- opened$host
  reopened <- dr_open_lake(root)
  withr::defer(dr_close_lake(reopened))
  expect_identical(opened$host, host)
})

test_that("browser exposes published logical assets and caps SQL previews", {
  opened <- NULL
  updates <- 0L
  withr::local_options(
    connectionObserver = list(
      connectionOpened = function(...) opened <<- list(...),
      connectionUpdated = function(type, host) updates <<- updates + 1L,
      connectionClosed = function(type, host) NULL
    )
  )
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  data <- data.frame(id = seq_len(1200), label = rep("Größe", 1200))
  result <- dr_write_data(f$lake, data, "orders")
  expect_identical(updates, 1L)
  cached <- dr_write_data(f$lake, data, "orders")
  expect_identical(cached$status, "cached")
  expect_identical(cached$release_id, result$release_id)
  expect_identical(updates, 1L)
  exec(f$lake, "CREATE TABLE lake.raw.not_published AS SELECT 1 AS id")
  expect_identical(
    opened$listObjects(),
    connection_frame("validated", "schema")
  )
  expect_identical(
    opened$listObjects(schema = "validated"),
    connection_frame("orders", "table")
  )
  expect_identical(opened$listObjects(schema = "raw"), connection_frame())
  expect_identical(
    opened$listObjects(schema = "validated", table = "orders"),
    connection_frame()
  )
  expect_identical(
    opened$listColumns(schema = "validated", table = "orders"),
    connection_frame(c("id", "label"), c("integer", "character"))
  )
  expect_identical(
    opened$previewObject(rowLimit = 3L, schema = "validated", table = "orders"),
    head(data, 3L)
  )
  expect_equal(
    nrow(opened$previewObject(
      rowLimit = 5000L,
      schema = "validated",
      table = "orders"
    )),
    1000L
  )
  expect_equal(
    nrow(opened$previewObject(
      rowLimit = 0L,
      schema = "validated",
      table = "orders"
    )),
    0L
  )
  expect_identical(
    opened$previewObject(
      rowLimit = Inf,
      schema = "validated",
      table = "orders"
    ),
    data.frame()
  )
  expect_identical(
    opened$previewObject(
      rowLimit = 1L,
      schema = "raw",
      table = "not_published"
    ),
    data.frame()
  )
  expect_identical(
    opened$listColumns(
      schema = "validated",
      table = "orders; DROP TABLE orders"
    ),
    connection_frame()
  )

  queries <- character()
  real_collect <- dplyr::collect
  local_mocked_bindings(
    collect = function(x, ...) {
      queries <<- c(queries, as.character(dbplyr::sql_render(x)))
      real_collect(x, ...)
    },
    .package = "dplyr"
  )
  opened$listColumns(schema = "validated", table = "orders")
  opened$previewObject(rowLimit = 2L, schema = "validated", table = "orders")
  expect_match(queries[[1]], "LIMIT 0")
  expect_match(queries[[2]], "LIMIT 2")
  data$id <- data$id + 10000L
  dr_write_data(f$lake, data, "orders")
  expect_identical(updates, 2L)
  expect_identical(
    opened$listObjects(schema = "validated"),
    connection_frame("orders", "table")
  )
  expect_identical(
    opened$previewObject(rowLimit = 1L, schema = "validated", table = "orders"),
    head(data, 1L)
  )
})

test_that("model publication refreshes only browsable member tables", {
  skip_if_not_installed("dm")
  opened <- NULL
  updates <- 0L
  withr::local_options(
    connectionObserver = list(
      connectionOpened = function(...) opened <<- list(...),
      connectionUpdated = function(type, host) updates <<- updates + 1L,
      connectionClosed = function(type, host) NULL
    )
  )
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  model <- dm::dm(customers = data.frame(id = 1:2)) |>
    dm::dm_add_pk(customers, id)
  result <- dataraft.core::dr_publish(
    dataraft.core::dr_product("portfolio", model),
    to = f$lake
  )
  expect_identical(result$status, "published")
  expect_identical(updates, 1L)
  expect_identical(
    opened$listObjects(schema = "validated"),
    connection_frame("portfolio.customers", "table")
  )
  expect_identical(
    opened$previewObject(
      rowLimit = 1L,
      schema = "validated",
      table = "portfolio.customers"
    ),
    data.frame(id = 1L)
  )
})

test_that("reconnect metadata contains local code or a secret-free hint", {
  skip_if_not_installed("duckdb")
  opened <- NULL
  withr::local_envvar(DATARRAFT_TEST_SECRET = "password=never-copy-this")
  withr::local_options(
    connectionObserver = list(
      connectionOpened = function(...) opened <<- list(...),
      connectionClosed = function(type, host) NULL
    )
  )
  root <- file.path(withr::local_tempdir(), "quote' space")
  lake <- dr_open_lake(root)
  dr_close_lake(lake)
  env <- new.env(parent = globalenv())
  eval(parse(text = opened$connectCode), envir = env)
  withr::defer(dr_close_lake(env$lake))
  expect_identical(
    attr(env$lake$config, "dr_local_path"),
    normalizePath(root, winslash = "/", mustWork = TRUE)
  )
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  expect_match(opened$connectCode, "original lake_config", fixed = TRUE)
  strings <- unlist(opened[vapply(opened, is.character, logical(1))])
  expect_identical(any(grepl("never-copy-this|password=", strings)), FALSE)
  fake_remote <- f$lake
  fake_remote$config$catalog <- dr_registry_postgres("DATARRAFT_TEST_SECRET")
  expect_match(
    connection_code(fake_remote),
    "original lake_config",
    fixed = TRUE
  )
  expect_identical(
    grepl("never-copy-this", connection_code(fake_remote)),
    FALSE
  )
})

test_that("missing and failing IDE observers never fail lake operations", {
  for (observer in list(
    NULL,
    list(connectionOpened = function(...) stop("IDE unavailable"))
  )) {
    withr::local_options(connectionObserver = observer)
    f <- fixture()
    withr::defer(fixture_cleanup(f))
    expect_identical(DBI::dbIsValid(f$lake$con), TRUE)
    expect_identical(dr_refresh_connection(f$lake), f$lake)
    dr_close_lake(f$lake)
  }
  withr::local_options(
    connectionObserver = list(
      connectionOpened = function(...) NULL,
      connectionUpdated = function(...) stop("IDE unavailable"),
      connectionClosed = function(...) stop("IDE unavailable")
    )
  )
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  result <- dr_write_data(f$lake, data.frame(id = 1L), "orders")
  expect_identical(result$status, "published")
  expect_identical(dr_close_lake(f$lake), TRUE)
})
