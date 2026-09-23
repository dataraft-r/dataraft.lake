#' Check a delivery before writing its raw table
#'
#' Add after extraction and before transformations. The source file is retained
#' in immutable landing even when the input gate blocks. The reader must return
#' a data frame, ensuring the checked values are the values subsequently written.
#' The mandatory candidate gate remains in place, including after partition
#' composition. A failed input gate is persisted with stage `"ingest"`.
#' @param pipeline Pipeline after `dr_step_extract()`.
#' @param contract Contract for the extracted input, possibly containing
#'   [dataraft.core::dr_pointblank_checks()] rules. It can differ from the final product contract.
#' @returns The updated pipeline specification; no IO is performed.
#' @seealso `pipeline_ingest()`, [dataraft.core::dr_validate()]
#' @examples
#' contract <- dataraft.core::dr_contract("orders", version = "1",
#'   columns = c(id = "integer"), key = "id")
#' pipeline <- dr_pipeline("orders.import", dr_lake_config(backend = "duckdb"),
#'   code_version = "v1") |>
#'   dr_step_land(dataraft.core::dr_source_file("orders.file", "orders.csv", utils::read.csv)) |>
#'   dr_step_extract() |>
#'   dr_step_precheck(contract) |>
#'   dr_step_validate(contract) |>
#'   dr_step_publish("orders")
#' dataraft.core::dr_plan(pipeline)
#' @noRd
dr_step_precheck <- function(pipeline, contract) {
  if (
    !inherits(pipeline, "dr_pipeline") ||
      !identical(names(pipeline$steps), c("land", "extract"))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Add the input gate immediately after extraction, before transforms.",
      "dr_pipeline_invalid"
    )
  }
  if (!inherits(contract, "dr_contract")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "contract must be a contract."
    )
  }
  dataraft.core::dr_internal_assert_contract_ready(contract)
  pipeline$steps$precheck <- contract
  pipeline
}
