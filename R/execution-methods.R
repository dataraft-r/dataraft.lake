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
#' @importFrom dataraft.core dr_land_source
dr_land_source.dr_lake <- function(x, source, ...) land_source(x, source)
#' @export
#' @importFrom dataraft.core dr_output_source
dr_output_source.dr_config <- function(x, asset, release, ...) {
  dr_source_release(x, asset, release)
}
#' @export
#' @importFrom dataraft.core dr_output_source
dr_output_source.dr_lake <- dr_output_source.dr_config
#' @export
#' @importFrom dataraft.core dr_read_output
dr_read_output.dr_config <- function(x, asset, release, ..., connection = NULL) {
  borrowed <- inherits(connection, "dr_lake") && DBI::dbIsValid(connection$con) &&
    identical(x[c("backend", "catalog", "storage")],
      connection$config[c("backend", "catalog", "storage")])
  lake <- if (borrowed) connection else dr_connect_lake(x, read_only = TRUE)
  if (!borrowed) on.exit(dr_close_lake(lake), add = TRUE)
  dr_read_release(lake, asset, release)
}
#' @export
#' @importFrom dataraft.core dr_read_output
dr_read_output.dr_lake <- function(x, asset, release, ...) {
  dr_read_release(x, asset, release)
}
#' @export
#' @importFrom dataraft.core dr_publish_model_result
dr_publish_model_result.dr_lake_target <- function(x, product, result, previous = NULL, ...) {
  if (length(x$partition_by)) {
    dataraft.core::dr_internal_abort(subclass = "dataraft_error_lake",
      "Model products publish complete snapshots without partition replacement.")
  }
  publish_model_result(product, result, previous)
}
#' @export
#' @importFrom dataraft.core dr_read_diagnostic_rows
dr_read_diagnostic_rows.dr_config <- function(x, diagnostic, rule = NULL, limit = 100, ...) {
  lake <- diagnostic$lake
  owned <- !inherits(lake, "dr_lake") || !DBI::dbIsValid(lake$con)
  if (owned) {
    lake <- dr_connect_lake(x, read_only = TRUE)
    on.exit(dr_close_lake(lake), add = TRUE)
  }
  data <- dplyr::tbl(lake$con,
    DBI::Id(catalog = "lake", schema = diagnostic$schema, table = diagnostic$table))
  if (isTRUE(diagnostic$failed_rows)) {
    return(tibble::as_tibble(dplyr::collect(
      if (is.finite(limit)) utils::head(data, limit) else data)))
  }
  dataraft.core::dr_quality_rows(data, rule, diagnostic$contract, limit)
}
#' @export
#' @importFrom dataraft.core dr_check_component
dr_check_component.dr_pipeline <- function(x, ...) {
  check_pipeline(x)
  invisible(x)
}
#' @export
#' @importFrom dataraft.core dr_acquire_write_session
dr_acquire_write_session.dr_lake <- function(x, scope = parent.frame(), ...) {
  acquire_maintenance_gate(x, scope)
  invisible(x)
}
#' @export
#' @importFrom dataraft.core dr_set_target_layer
dr_set_target_layer.dr_lake_target <- function(x, layer, ...) {
  x$layer <- dataraft.core::dr_internal_ident(layer)
  x
}
#' @export
#' @importFrom dataraft.core dr_model
dr_model.dr_lake <- function(
  lake,
  tables,
  primary_keys = list(),
  foreign_keys = list(),
  releases = NULL,
  check = TRUE
) {
  dataraft.core::dr_internal_need("dm")
  if (is.null(names(tables)) || anyDuplicated(names(tables))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_definition",
      "tables must be named uniquely."
    )
  }
  if (!is.null(releases) && !setequal(names(releases), names(tables))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_definition",
      "Pin all model table releases."
    )
  }
  refs <- lapply(names(tables), function(n) {
    resolve_release(
      lake,
      tables[[n]],
      if (is.null(releases)) NULL else releases[[n]]
    )
  })
  names(refs) <- names(tables)
  model <- dm::dm(
    !!!lapply(refs, function(r) {
      dr_tbl(lake, r$asset[[1]], r$release_id[[1]])
    })
  )
  model <- dataraft.core::dr_internal_dm_keys(model, primary_keys, foreign_keys, check)
  attr(model, "dr_releases") <- lapply(refs, function(r) r$release_id[[1]])
  model
}

#' @export
#' @importFrom dataraft.core dr_read_input
dr_read_input.dr_release_source <- function(x, context = NULL, ...) {
  read_release_source(x, context)
}
