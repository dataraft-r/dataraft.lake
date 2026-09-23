#' @export
#' @importFrom dataraft.core dr_connect_backend
dr_connect_backend.dr_config <- function(x, ...) dr_connect_lake(x, ...)

#' @export
#' @importFrom dataraft.core dr_close_backend
dr_close_backend.dr_lake <- function(x, ...) dr_close_lake(x, ...)

#' @export
#' @importFrom dataraft.core dr_resolve_release
dr_resolve_release.dr_lake <- function(x, ...) resolve_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_read_release_data
dr_read_release_data.dr_lake <- function(x, ...) dr_read_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_release_table
dr_release_table.dr_lake <- function(x, ...) dr_tbl(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_release_source
dr_as_release_source.dr_lake <- function(x, ...) dr_source_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_release_source
dr_as_release_source.dr_config <- function(x, ...) dr_source_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_release_source
dr_as_release_source.character <- function(x, ...) dr_source_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_land_source
dr_land_source.dr_lake <- function(x, ...) land_source(x, ...)

#' @export
#' @importFrom dataraft.core dr_read_release_source
dr_read_release_source.dr_release_source <- function(x, ...) {
  read_release_source(x, ...)
}

#' @export
#' @importFrom dataraft.core dr_publish_model_result
dr_publish_model_result.dr_lake_target <- function(x, product, ...) {
  publish_model_result(product, ...)
}

#' @export
#' @importFrom dataraft.core dr_with_release_backend
dr_with_release_backend.dr_model_result <- function(x, ...) {
  with_model_lake(x, ...)
}

#' @export
#' @importFrom dataraft.core dr_read_model_release
dr_read_model_release.dr_lake <- function(x, ...) read_model_release(x, ...)

#' @export
#' @importFrom dataraft.core dr_registry_data
dr_registry_data.dr_lake <- function(x, ...) dr_registry(x, ...)

#' @export
#' @importFrom dataraft.core dr_check_pipeline
dr_check_pipeline.dr_pipeline <- function(x, ...) check_pipeline(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_target
dr_as_target.dr_lake <- function(x, ...) dr_target_lake(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_target
dr_as_target.dr_config <- function(x, ...) dr_target_lake(x, ...)

#' @export
#' @importFrom dataraft.core dr_as_target
dr_as_target.character <- function(x, ...) dr_target_lake(x, ...)

#' @export
#' @importFrom dataraft.core dr_filter_metadata
dr_filter_metadata.dr_lake <- function(
  x,
  table,
  asset = NULL,
  run_id = NULL,
  ...
) {
  lake <- x
  rlang::local_error_call(rlang::caller_env())
  assert_lake(lake)
  filters <- character()
  params <- list()
  if (!is.null(asset)) {
    dataraft.core::dr_internal_asset_id(asset)
    filters <- c(filters, "asset = ?")
    params <- c(params, list(asset))
  }
  if (!is.null(run_id)) {
    dataraft.core::dr_internal_scalar(run_id, "run_id")
    filters <- c(filters, "run_id = ?")
    params <- c(params, list(run_id))
  }
  sql <- paste("SELECT * FROM", meta(lake, table))
  if (length(filters)) {
    sql <- paste(sql, "WHERE", paste(filters, collapse = " AND "))
  }
  query(
    lake,
    sql,
    if (length(params)) params else NULL
  )
}

#' @export
#' @importFrom dataraft.core dr_configure_target
dr_configure_target.dr_lake_target <- function(x, layer, ...) {
  x$layer <- dataraft.core::dr_internal_ident(layer)
  x
}
