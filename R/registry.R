registry_init <- function(lake) {
  rlang::local_error_call(rlang::caller_env())
  registry_table <- meta(lake, "schema_version")
  if (DBI::dbExistsTable(lake$con, table_id("_dl", "schema_version"))) {
    versions <- query(
      lake,
      paste("SELECT version FROM", registry_table)
    )$version
    if (!identical(versions, 4L)) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Unsupported registry version. Create a new lake with this package version.",
        "dr_registry_version"
      )
    }
    return(invisible(NULL))
  }
  schemas <- list(
    schema_version = "version INTEGER, applied_at VARCHAR",
    assets = "id VARCHAR, version VARCHAR, kind VARCHAR, owner VARCHAR, description VARCHAR, definition VARCHAR, fingerprint VARCHAR, registered_at VARCHAR",
    runs = "run_id VARCHAR, pipeline VARCHAR, asset VARCHAR, status VARCHAR, started_at VARCHAR, finished_at VARCHAR, input_hash VARCHAR, definition_hash VARCHAR, code_version VARCHAR, message VARCHAR, release_id VARCHAR",
    inputs = "run_id VARCHAR, source VARCHAR, source_version VARCHAR, fingerprint VARCHAR, original_name VARCHAR, landed_path VARCHAR, received_at VARCHAR, business_date VARCHAR",
    quality_results = "run_id VARCHAR, contract VARCHAR, rule VARCHAR, status VARCHAR, severity VARCHAR, n_failed DOUBLE, n_total DOUBLE, threshold DOUBLE, message VARCHAR, engine VARCHAR, stage VARCHAR, segment VARCHAR, details VARCHAR",
    releases = "release_id VARCHAR, asset VARCHAR, schema_name VARCHAR, table_name VARCHAR, run_id VARCHAR, published_at VARCHAR, contract VARCHAR, definition_hash VARCHAR, input_hash VARCHAR, quality VARCHAR, business_date VARCHAR, parent_release VARCHAR",
    lineage_edges = "run_id VARCHAR, from_id VARCHAR, from_version VARCHAR, to_id VARCHAR, to_version VARCHAR, relation VARCHAR",
    events = "event_id VARCHAR, run_id VARCHAR, asset VARCHAR, type VARCHAR, recipient VARCHAR, created_at VARCHAR, status VARCHAR, message VARCHAR",
    reports = "id VARCHAR, created_at VARCHAR, manifest VARCHAR",
    run_owners = "run_id VARCHAR, host VARCHAR, pid INTEGER, boot VARCHAR, process_start VARCHAR"
  )
  DBI::dbWithTransaction(lake$con, {
    for (name in names(schemas)) {
      exec(
        lake,
        paste0(
          "CREATE TABLE IF NOT EXISTS ",
          meta(lake, name),
          " (",
          schemas[[name]],
          ")"
        )
      )
    }
    insert_meta(
      lake,
      "schema_version",
      list(version = 4L, applied_at = dataraft.core::now())
    )
  })
}


#' Read framework metadata
#' @param lake Connected lake.
#' @param table Metadata table name.
#' @return A tibble.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dr_lake_config(
#'   dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' lake <- dr_connect_lake(config)
#' dr_registry(lake, "runs")
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_registry <- function(
  lake,
  table = c(
    "assets",
    "runs",
    "inputs",
    "quality_results",
    "releases",
    "lineage_edges",
    "events",
    "reports",
    "schema_version",
    "run_owners"
  )
) {
  assert_lake(lake)
  table <- match.arg(table)
  query(lake, paste("SELECT * FROM", meta(lake, table)))
}


#' Register a versioned definition
#' @param lake Connected lake.
#' @param object A contract, source, product, metric or pipeline definition.
#' @return The definition, invisibly. Reusing a version with changed content
#'   errors.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dr_lake_config(
#'   dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' lake <- dr_connect_lake(config)
#' contract <- dataraft.core::dr_contract(
#'   "orders", "1.0.0", "Analytics", "Order amounts", "One order",
#'   c(order_id = "integer", amount = "numeric"), key = "order_id"
#' )
#' dr_register(lake, contract)
#' dr_registry(lake, "assets")
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_register <- function(lake, object) {
  assert_writable(lake)
  if (inherits(object, "dr_contract")) {
    dataraft.core::assert_contract_ready(object)
  }
  if (is.null(object$id) || is.null(object$version) || is.null(object$kind)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Object is not a registerable definition."
    )
  }
  definition <- object
  definition$config <- NULL
  h <- dataraft.core::fingerprint(definition)
  old <- query(
    lake,
    paste(
      "SELECT fingerprint, definition FROM",
      meta(lake, "assets"),
      "WHERE id = ? AND version = ? AND kind = ?"
    ),
    list(object$id, object$version, object$kind)
  )
  if (nrow(old)) {
    if (any(old$fingerprint != h)) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        paste(
          "Definition changed without a version bump:",
          object$id,
          object$version
        ),
        "dr_definition_changed",
        definition_id = object$id,
        definition_version = object$version
      )
    }
  } else {
    insert_meta(
      lake,
      "assets",
      list(
        id = object$id,
        version = object$version,
        kind = object$kind,
        owner = object$owner %||% "",
        description = object$description %||% "",
        definition = dataraft.core::jencode(definition),
        fingerprint = h,
        registered_at = dataraft.core::now()
      )
    )
  }
  invisible(object)
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name resolve_release

resolve_release <- function(lake, asset, release = NULL) {
  rlang::local_error_call(rlang::caller_env())
  sql <- paste("SELECT * FROM", meta(lake, "releases"), "WHERE asset = ?")
  params <- list(asset)
  if (!is.null(release)) {
    sql <- paste(sql, "AND release_id = ?")
    params <- c(params, list(release))
  }
  rows <- query(
    lake,
    paste(sql, "ORDER BY published_at DESC, release_id DESC LIMIT 1"),
    params
  )
  if (!nrow(rows)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      paste("No published release for", asset),
      "dr_no_release"
    )
  }
  rows
}


#' Query a published immutable data release
#' @param src Connected lake.
#' @param asset Asset id.
#' @param release Release id; NULL selects latest.
#' @param ... Reserved for extensions.
#' @return A lazy dbplyr table.
#' @name dr_tbl
#' @importFrom dplyr tbl
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dr_lake_config(
#'   dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' lake <- dr_connect_lake(config)
#' path <- file.path(root, "orders.csv")
#' utils::write.csv(data.frame(order_id = 1:2, amount = c(25, 75)), path,
#'   row.names = FALSE)
#' source <- dataraft.core::dr_source_file("orders.file", path, reader = utils::read.csv)
#' contract <- dataraft.core::dr_contract(
#'   "orders", "1.0.0", "Analytics", "Order amounts", "One order",
#'   c(order_id = "integer", amount = "numeric"), key = "order_id"
#' )
#' release <- dataraft.core::dr_product("orders", contract = contract, code_version = "v1") |>
#'   dataraft.core::dr_add_source(source) |> dataraft.core::dr_publish(to = lake)
#' dr_tbl(lake, "orders", release$release_id) |> dplyr::collect()
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_tbl <- function(src, ...) dplyr::tbl(src, ...)


#' @rdname dr_tbl
#' @export
tbl.dr_lake <- function(src, asset, release = NULL, ...) {
  rlang::local_error_call(rlang::caller_env())
  rlang::check_dots_empty()
  lake <- src
  r <- resolve_release(lake, dataraft.core::asset_id(asset), release)
  if (startsWith(r$table_name[[1]], "model_")) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Use dr_read_release() to read a complete model, or select a published member table."
    )
  }
  dplyr::tbl(lake$con, table_id(r$schema_name[[1]], r$table_name[[1]]))
}
