#' Replace a bounded range of date partitions
#'
#' Uses the existing partition-replacement publication and quality gate.
#' Each date is a separate immutable release. A later failure leaves earlier
#' dates committed; rerunning replaces the same partition without duplicate rows.
#' The caller supplies data and schedules invocation; this function does not
#' implement CDC or start a scheduler.
#' @param lake Connected writable lake.
#' @param product Product with one named primary source.
#' @param from,to Inclusive ISO dates.
#' @param partition_by Date column in the published table.
#' @param source_for_date Function receiving a Date and returning its complete
#'   partition as a data frame.
#' @param source_name Existing source name; inferred if there is exactly one.
#' @return Named list of published run results.
#' @export
 dr_backfill <- function(lake, product, from, to, partition_by, source_for_date, source_name = NULL) {
  assert_writable(lake)
  if (!inherits(product, "dr_product") || !is.function(source_for_date)) {
    dataraft.core::dr_internal_abort("Supply a product and source_for_date function.",
      subclass = "dataraft_error_lake")
  }
  dataraft.core::dr_internal_column_name(partition_by)
  start <- as.Date(from)
  end <- as.Date(to)
  if (length(start) != 1L || length(end) != 1L || is.na(start) || is.na(end) ||
      as.character(start) != as.character(from) || as.character(end) != as.character(to) || start > end) {
    dataraft.core::dr_internal_abort("Supply ordered, inclusive ISO dates.",
      subclass = "dataraft_error_lake")
  }
  source_name <- source_name %||% if (length(product$sources) == 1L) names(product$sources)[[1]] else NULL
  if (is.null(source_name) || !source_name %in% names(product$sources)) {
    dataraft.core::dr_internal_abort("Name one existing primary source.",
      subclass = "dataraft_error_lake")
  }
  if (!is.null(product$target) || length(product$output_ports)) {
    dataraft.core::dr_internal_abort("Backfill configures the partitioned lake target; omit a target on product.",
      subclass = "dataraft_error_lake")
  }
  product <- dataraft.core::dr_set_target(product, dr_target_lake(lake, partition_by = partition_by))
  dates <- seq(start, end, by = "day")
  results <- setNames(vector("list", length(dates)), as.character(dates))
  for (i in seq_along(dates)) {
    rows <- source_for_date(dates[[i]])
    if (!is.data.frame(rows) || !partition_by %in% names(rows) || !nrow(rows) ||
        anyNA(rows[[partition_by]]) ||
        !all(as.character(rows[[partition_by]]) == as.character(dates[[i]]))) {
      dataraft.core::dr_internal_abort("Each delivery must contain one complete, nonempty requested date partition.",
        subclass = "dataraft_error_lake")
    }
    results[[i]] <- dataraft.core::dr_run(product,
      sources = stats::setNames(list(rows), source_name),
      business_date = as.character(dates[[i]]))
  }
  results
}
