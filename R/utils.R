#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name assert_writable

assert_writable <- function(lake) {
  rlang::local_error_call(rlang::caller_env())
  assert_lake(lake)
  if (isTRUE(lake$config$read_only)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "This lake is read-only. Open a writable connection for this operation.",
      "dr_read_only"
    )
  }
  invisible(NULL)
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
#' @name meta

meta <- function(lake, name) table_sql(lake, "_dl", name)


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name insert_meta

insert_meta <- function(lake, name, values) {
  rlang::local_error_call(rlang::caller_env())
  assert_writable(lake)
  acquire_maintenance_gate(lake, environment())
  if (name %in% c("releases", "runs")) {
    id <- if (identical(name, "releases")) "release_id" else "run_id"
    exists <- query(lake, paste("SELECT COUNT(*) AS n FROM", meta(lake, name),
      "WHERE", qident(lake, id), "= ?"), list(values[[id]]))$n[[1]]
    if (exists > 0) dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake", "Duplicate registry identifier.", "dr_registry_duplicate")
  }
  if (identical(name, "releases")) {
    integrity <- release_content(lake, values$schema_name, values$table_name)
    values$content_hash <- integrity$content_hash
    values$row_count <- integrity$row_count
    # Called within the same publication transaction as the release and lineage.
    # A catalog row serializes only the commit phase, independent of client clocks.
    exec(
      lake,
      paste("UPDATE", meta(lake, "release_counter"), "SET value = value + 1")
    )
    values$release_order <- as.character(query(
      lake,
      paste("SELECT value FROM", meta(lake, "release_counter"))
    )$value[[1]])
  }
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
#' @name assert_lake

assert_lake <- function(lake) {
  rlang::local_error_call(rlang::caller_env())
  if (!inherits(lake, "dr_lake") || !DBI::dbIsValid(lake$con)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "A connected dr_lake is required."
    )
  }
}


materialize <- function(lake, data, schema, name) {
  rlang::local_error_call(rlang::caller_env())
  acquire_maintenance_gate(lake, environment())
  dest <- table_sql(lake, schema, name)
  if (inherits(data, "tbl_sql")) {
    exec(lake, paste0("CREATE TABLE ", dest, " AS ", dbplyr::sql_render(data)))
  } else if (is.data.frame(data)) {
    tmp <- dataraft.core::dr_internal_uid()
    duckdb::duckdb_register(lake$con, tmp, as.data.frame(data))
    on.exit(duckdb::duckdb_unregister(lake$con, tmp), add = TRUE)
    exec(
      lake,
      paste0("CREATE TABLE ", dest, " AS SELECT * FROM ", qident(lake, tmp))
    )
  } else {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Readers and builders must return a data.frame or a lazy SQL table."
    )
  }
  dplyr::tbl(lake$con, table_id(schema, name))
}
