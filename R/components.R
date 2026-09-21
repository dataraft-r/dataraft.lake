#' Read a pinned lake release as a product source
#'
#' Execution resolves an omitted `release_id` once and records its immutable
#' identity. Pass a configuration to open an existing lake read-only, collect
#' the selected release into an ordinary tibble, and close the owned connection.
#' This requires enough memory for the release. A connected lake instead returns
#' a lazy table and remains caller-owned; keep it open while using that table.
#' Construction and validation do not open or create a lake. This source connects
#' governed releases to ordinary product composition and consumer exports.
#' When publishing back to the same lake, execution can reuse its open handle;
#' configuration sources still return materialized values without owning it.
#' @param lake Connected lake or [dr_lake_config()] describing an existing lake.
#' @param asset Published asset name.
#' @param release_id Optional immutable release identifier.
#' @returns A source adapter. Reads return a lazy table for a connected lake or
#'   a tibble for a configuration, with the exact release reference attached.
#' @export
#' @examples
#' if (FALSE) {
#'   dataraft.core::dr_product("summary") |>
#'     dataraft.core::dr_add_source(dr_source_release(lake, "orders")) |>
#'     dataraft.core::dr_add_recipe(dataraft.core::dr_recipe() |> dataraft.core::dr_step_transform(function(data) dplyr::summarise(data, rows = dplyr::n()))) |>
#'     dataraft.core::dr_run()
#' }
dr_source_release <- function(lake, asset, release_id = NULL) {
  dataraft.core::asset_id(asset)
  if (!is.null(release_id)) {
    dataraft.core::scalar(release_id, "release_id")
  }
  source <- structure(
    list(lake = lake, asset = asset, release_id = release_id),
    class = "dr_release_source"
  )
  dataraft.core::dr_check_component(source)
  source
}

#' @export
#' @importFrom dataraft.core dr_check_component
dr_check_component.dr_release_source <- function(x, ...) {
  dataraft.core::asset_id(x$asset)
  if (!is.null(x$release_id)) {
    dataraft.core::scalar(x$release_id, "release_id")
  }
  if (inherits(x$lake, "dr_config")) {
    do.call(dr_lake_config, unclass(x$lake))
    dataraft.core::need("duckdb")
  } else if (inherits(x$lake, "dr_lake")) {
    assert_lake(x$lake)
  } else {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Use a connected lake or dr_lake_config() for a release source."
    )
  }
  invisible(x)
}

#' @export
#' @importFrom dataraft.core dr_inspect
dr_inspect.dr_release_source <- function(x, ...) {
  config <- if (inherits(x$lake, "dr_config")) x$lake else x$lake$config
  list(
    type = "lake release",
    asset = x$asset,
    release_id = x$release_id,
    backend = config$backend
  )
}

#' @export
#' @importFrom dataraft.core dr_read_source
dr_read_source.dr_release_source <- function(source, ...) {
  read_release_source(source)
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name read_release_source

read_release_source <- function(source, execution_lake = NULL) {
  rlang::local_error_call(rlang::caller_env())
  dataraft.core::dr_check_component(source)
  materialized <- inherits(source$lake, "dr_config")
  if (
    materialized &&
      inherits(source$output_lake, "dr_lake") &&
      DBI::dbIsValid(source$output_lake$con)
  ) {
    execution_lake <- source$output_lake
  }
  reuse <- materialized &&
    inherits(execution_lake, "dr_lake") &&
    DBI::dbIsValid(execution_lake$con) &&
    identical(
      source$lake[c("backend", "catalog", "storage")],
      execution_lake$config[c("backend", "catalog", "storage")]
    )
  owned <- materialized && !reuse
  lake <- if (reuse) {
    execution_lake
  } else if (owned) {
    dr_connect_lake(source$lake, read_only = TRUE)
  } else {
    source$lake
  }
  if (owned) {
    on.exit(dr_disconnect_lake(lake), add = TRUE)
  }
  ref <- resolve_release(lake, source$asset, source$release_id)
  data <- dplyr::tbl(
    lake$con,
    table_id(ref$schema_name[[1]], ref$table_name[[1]])
  )
  if (materialized) {
    data <- dataraft.core::dr_collect(data)
  }
  attr(data, "dr_input_reference") <- list(
    asset = source$asset,
    release_id = ref$release_id[[1]],
    hash = ref$input_hash[[1]],
    run_id = source$run_id %||% NULL
  )
  data
}
