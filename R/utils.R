#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name assert_writable

assert_writable <- function(lake) {
  rlang::local_error_call(rlang::caller_env())
  assert_lake(lake)
  if (isTRUE(lake$config$read_only)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "This lake is read-only. Open a writable connection for this operation.",
      "dr_read_only"
    )
  }
  acquire_lake_writer(lake, parent.frame())
}


qident <- function(lake, x) as.character(DBI::dbQuoteIdentifier(lake$con, x))


qlit <- function(lake, x) as.character(DBI::dbQuoteLiteral(lake$con, x))


table_sql <- function(lake, schema, name) {
  rlang::local_error_call(rlang::caller_env())
  paste(qident(lake, c("lake", schema, name)), collapse = ".")
}


table_id <- function(schema, name) {
  rlang::local_error_call(rlang::caller_env())
  DBI::Id(catalog = "lake", schema = schema, table = name)
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name exec

exec <- function(lake, sql, params = NULL) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(params)) {
    DBI::dbExecute(lake$con, sql)
  } else {
    DBI::dbExecute(lake$con, sql, params = params)
  }
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name query

query <- function(lake, sql, params = NULL) {
  rlang::local_error_call(rlang::caller_env())
  tibble::as_tibble(
    if (is.null(params)) {
      DBI::dbGetQuery(lake$con, sql)
    } else {
      DBI::dbGetQuery(lake$con, sql, params = params)
    }
  )
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name meta

meta <- function(lake, name) table_sql(lake, "_dl", name)


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name insert_meta

insert_meta <- function(lake, name, values) {
  rlang::local_error_call(rlang::caller_env())
  assert_writable(lake)
  cols <- paste(qident(lake, names(values)), collapse = ", ")
  marks <- paste(rep("?", length(values)), collapse = ", ")
  exec(
    lake,
    paste0(
      "INSERT INTO ",
      meta(lake, name),
      " (",
      cols,
      ") VALUES (",
      marks,
      ")"
    ),
    unname(values)
  )
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name assert_lake

assert_lake <- function(lake) {
  rlang::local_error_call(rlang::caller_env())
  if (!inherits(lake, "dr_lake") || !DBI::dbIsValid(lake$con)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "A connected dr_lake is required."
    )
  }
}


materialize <- function(lake, data, schema, name) {
  rlang::local_error_call(rlang::caller_env())
  dest <- table_sql(lake, schema, name)
  if (inherits(data, "tbl_sql")) {
    exec(lake, paste0("CREATE TABLE ", dest, " AS ", dbplyr::sql_render(data)))
  } else if (is.data.frame(data)) {
    tmp <- dataraft.core::uid()
    duckdb::duckdb_register(lake$con, tmp, as.data.frame(data))
    on.exit(duckdb::duckdb_unregister(lake$con, tmp), add = TRUE)
    exec(
      lake,
      paste0("CREATE TABLE ", dest, " AS SELECT * FROM ", qident(lake, tmp))
    )
  } else {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Readers and builders must return a data.frame or a lazy SQL table."
    )
  }
  dplyr::tbl(lake$con, table_id(schema, name))
}
