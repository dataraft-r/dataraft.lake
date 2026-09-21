#' @export
#' @importFrom dataraft.core dr_validate
dr_validate.dr_pipeline <- function(data, contract = NULL, ...) {
  rlang::check_dots_empty()
  if (!is.null(contract)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "A pipeline already contains its contract."
    )
  }
  check_pipeline(data)
  dataraft.core::dr_internal_need("duckdb")
  dataraft.core::dr_check_component(data$steps$land)
  dataraft.core::dr_internal_assert_contract_ready(data$steps$validate)
  attr(data, "dr_validated") <- TRUE
  data
}
