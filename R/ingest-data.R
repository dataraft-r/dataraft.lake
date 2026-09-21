#' Ingest an R data frame with an immutable delivery snapshot
#'
#' Use after reading Excel or fetching an API response with existing R tools.
#' Stores an RDS snapshot in landing, then uses the standard ingestion pipeline.
#' This records the received R object, not the original response or workbook.
#' Use [dataraft.core::dr_source_file()] to preserve those original files.
#' @param lake Connected lake or [dr_lake_config()].
#' @param data Data frame already in R memory.
#' @param contract Final candidate contract.
#' @param asset Governed output asset ID.
#' @param code_version Version of preparation code and dependencies.
#' @param version Ingestion and source definition version.
#' @param input_contract Optional contract to check before writing Raw.
#' @param ... Arguments forwarded to `pipeline_ingest()`, such as business_date,
#'   layer, notify and stop_on_failure.
#' @returns A dr_run_result. Identical data and definitions can reuse a release.
#'   Connections opened here are closed on exit.
#' @examples
#' root <- tempfile("dataraft-")
#' config <- dr_lake_config(dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb")
#' contract <- dataraft.core::dr_contract("orders", "1", "Analytics", "Orders", "One order",
#'   c(id = "integer"), key = "id")
#' dr_ingest_data(config, data.frame(id = 1:3), contract, "orders",
#'   code_version = "v1", input_contract = contract)
#' unlink(root, recursive = TRUE)
#' @noRd
dr_ingest_data <- function(
  lake,
  data,
  contract,
  asset,
  code_version,
  version = "1.0.0",
  input_contract = NULL,
  ...
) {
  if (!is.data.frame(data)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "data must be a data frame."
    )
  }
  dataraft.core::asset_id(asset)
  with_execution_lake(lake, function(con) {
    assert_writable(con)
    parent <- file.path(con$config$landing, ".dataraft-staging")
    dir.create(parent, recursive = TRUE, showWarnings = FALSE)
    slot <- file.path(parent, asset)
    if (!dir.create(slot, showWarnings = FALSE)) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Staging already exists for this asset. Check for a live or interrupted ingest before removing it."
      )
    }
    on.exit(unlink(slot, recursive = TRUE), add = TRUE)
    writeLines(
      dataraft.core::jencode(writer_identity()),
      file.path(slot, "writer.json")
    )
    path <- file.path(slot, "delivery.rds")
    saveRDS(as.data.frame(data), path, compress = FALSE, version = 3)
    source <- dataraft.core::dr_source_file(
      paste0(asset, ".data"),
      path,
      reader = readRDS,
      version = version,
      owner = contract$owner,
      description = "Snapshot of an R data frame; original transport is external."
    )
    pipeline_ingest(
      con,
      source,
      contract,
      asset,
      code_version = code_version,
      version = version,
      input_contract = input_contract,
      ...
    )
  })
}
