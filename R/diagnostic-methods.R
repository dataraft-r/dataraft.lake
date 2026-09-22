#' @export
#' @importFrom dataraft.core dr_metadata_rows
dr_metadata_rows.dr_lake <- function(lake, table, asset = NULL, run_id = NULL, ...) {
  rlang::local_error_call(rlang::caller_env())
  assert_lake(lake)
  table <- match.arg(table, c("assets", "runs", "inputs", "quality_results",
    "releases", "lineage_edges", "events", "reports", "schema_version", "run_owners"))
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
#' @importFrom dataraft.core dr_status
dr_status.dr_lake <- function(x, asset = NULL, ...) {
  runs <- dataraft.core::dr_metadata_rows(x, "runs", asset = asset)
  runs <- runs[order(runs$started_at, decreasing = TRUE), ]
  return(tibble::tibble(
    engine = rep("dataraft", nrow(runs)),
    id = runs$run_id,
    status = runs$status,
    outcome = status_outcome(runs$status),
    success = runs$status %in% c("published", "cached"),
    release_id = runs$release_id,
    asset = runs$asset,
    message = runs$message
  ))
}

#' @export
#' @importFrom dataraft.core dr_quality
dr_quality.dr_lake <- function(x, run_id = NULL, asset = NULL, release = NULL, ...) {
  if (
    is.null(run_id) == is.null(asset) || (!is.null(release) && is.null(asset))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_definition",
      "Supply run_id, or asset with an optional exact release."
    )
  }
  if (!is.null(release)) {
    run_id <- resolve_release(
      x,
      asset,
      release
    )$run_id[[1]]
  } else {
    runs <- dataraft.core::dr_metadata_rows(x, "runs", asset = asset, run_id = run_id)
    if (!nrow(runs)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_definition",
        "No matching run found.",
        "dr_no_run"
      )
    }
    runs <- runs[order(runs$started_at, runs$run_id, decreasing = TRUE), ]
    run_id <- runs$run_id[[1]]
    if (runs$status[[1]] == "cached") {
      run_id <- resolve_release(
        x,
        runs$asset[[1]],
        runs$release_id[[1]]
      )$run_id[[1]]
    }
  }
  out <- dataraft.core::dr_metadata_rows(x, "quality_results", run_id = run_id)
  dataraft.core::dr_quality(out)
}

#' @export
#' @importFrom dataraft.core dr_lineage_edges
dr_lineage_edges.dr_lake <- function(x, ...) {
  dr_registry(x, "lineage_edges")
}


status_outcome <- function(status) {
  rlang::local_error_call(rlang::caller_env())
  out <- rep(NA_character_, length(status))
  out[
    status %in% c("completed", "published", "cached", "success", "pass", "warn")
  ] <- "succeeded"
  out[status %in% c("blocked", "fail", "missing")] <- "blocked"
  out[status %in% c("error", "failed", "runtime error")] <- "failed"
  out[status %in% c("skipped", "skip")] <- "skipped"
  out
}


