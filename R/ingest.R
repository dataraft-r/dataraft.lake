#' Land, validate and publish one source file
#'
#' A compact entry point for the standard ingestion pipeline. It performs the
#' same immutable landing and contract gate as explicit pipeline steps. Use
#' `dr_pipeline()` when transformations or partition replacement are needed.
#'
#' @param lake A connected lake or a connection-free [dr_lake_config()].
#' @param source A [dataraft.core::dr_source_file()] definition with a local file and reader.
#' @param contract A [dataraft.core::dr_contract()] defining the publication gate.
#' @param asset Character scalar giving the governed output asset ID.
#' @param version Character scalar giving the ingestion definition version.
#' @param code_version Character scalar identifying all ingestion code and
#'   dependencies. Change both versions when behavior changes.
#' @param layer Character scalar giving the publication schema, `"validated"`
#'   by default. Raw extraction always uses the configured `"raw"` schema.
#' @param input_contract Optional separate contract for an input gate before
#'   writing Raw. Requires a reader returning a data frame. The final candidate
#'   is still validated against `contract`.
#' @inheritParams dr_write_data
#' @param ... Arguments passed to [dataraft.core::dr_run()], such as `business_date` or
#'   `stop_on_failure`. Do not pass another `lake` argument.
#' @returns A `dr_run_result` with `run_id`, `status`, `release_id` and
#'   `quality`.
#'   Connections opened from a config are closed before returning.
#' @seealso `dr_pipeline()`, [dr_tbl()], [dataraft.core::dr_run()]
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
#' release <- pipeline_ingest(lake, source, contract, "orders", code_version = "v1")
#' release
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
#' @noRd
pipeline_ingest <- function(
  lake,
  source,
  contract,
  asset,
  version = "1.0.0",
  code_version,
  layer = "validated",
  input_contract = NULL,
  partition_by = character(),
  ...
) {
  rlang::local_error_call(rlang::caller_env())
  dataraft.core::asset_id(asset)
  pipeline <- dr_pipeline(
    paste0(asset, ".ingest"),
    lake,
    version = version,
    code_version = code_version
  ) |>
    dr_step_land(source) |>
    dr_step_extract()
  if (!is.null(input_contract)) {
    pipeline <- dr_step_precheck(pipeline, input_contract)
  }
  pipeline <- pipeline |>
    dr_step_validate(contract) |>
    dr_step_publish(
      asset,
      layer = layer,
      mode = if (length(partition_by)) "replace_partition" else "replace",
      partition_by = partition_by
    )
  dataraft.core::dr_execute(pipeline, lake = lake, ...)
}
