# An attached file must belong to one DuckDB instance per R process. Reusing
# the driver gives each lake handle its own connection to that instance.
.local_lake_engines <- new.env(parent = emptyenv())

canonical_local_config <- function(config) {
  if (!identical(config$backend, "duckdb")) {
    return(config)
  }
  config$catalog$path <- dataraft.core::dr_internal_absolute_path(
    config$catalog$path
  )
  config$storage$path <- dataraft.core::dr_internal_absolute_path(
    config$storage$path
  )
  config$landing <- dataraft.core::dr_internal_absolute_path(config$landing)
  path <- attr(config, "dr_local_path")
  if (!is.null(path)) {
    attr(config, "dr_local_path") <- dataraft.core::dr_internal_absolute_path(
      path
    )
  }
  config
}

local_lake_connection <- function(config) {
  key <- digest::digest(config$catalog$path, algo = "sha256")
  engine <- .local_lake_engines[[key]]
  existing <- !is.null(engine)
  if (existing && !identical(engine$read_only, config$read_only)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Close the existing lake handles before changing the catalog's read-only mode.",
      "dr_connection_mode_conflict"
    )
  }
  if (!existing) {
    engine <- new.env(parent = emptyenv())
    engine$driver <- suppressMessages(duckdb::duckdb())
    engine$key <- key
    engine$read_only <- config$read_only
    engine$references <- 0L
  }
  con <- tryCatch(
    DBI::dbConnect(engine$driver, bigint = "integer64"),
    error = function(error) {
      if (!existing) {
        tryCatch(duckdb::duckdb_shutdown(engine$driver), error = function(e) {
          NULL
        })
      }
      stop(error)
    }
  )
  engine$references <- engine$references + 1L
  state <- new.env(parent = emptyenv())
  state$engine <- engine
  state$closed <- FALSE
  list(con = con, state = state, attached = existing)
}

close_local_lake_connection <- function(con, state) {
  if (isTRUE(state$closed)) {
    return(invisible(NULL))
  }
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con, shutdown = FALSE)
  }
  state$closed <- TRUE
  engine <- state$engine
  engine$references <- engine$references - 1L
  if (engine$references == 0L) {
    if (identical(.local_lake_engines[[engine$key]], engine)) {
      .local_lake_engines[[engine$key]] <- NULL
    }
    duckdb::duckdb_shutdown(engine$driver)
  }
  invisible(NULL)
}
