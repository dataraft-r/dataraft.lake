# Hash the stored representation, including column types and duplicate rows.
# Row order is deliberately irrelevant. Fetching is bounded, but the sorted
# vector of row digests consumes memory proportional to the number of rows.
release_content <- function(lake, schema, table) {
  result <- DBI::dbSendQuery(lake$con, paste("SELECT * FROM", table_sql(lake, schema, table)))
  on.exit(DBI::dbClearResult(result), add = TRUE)
  columns <- DBI::dbColumnInfo(result)[, c("name", "type"), drop = FALSE]
  blocks <- list()
  repeat {
    chunk <- DBI::dbFetch(result, n = 1000L)
    if (!nrow(chunk)) break
    rows <- vapply(seq_len(nrow(chunk)), function(i) {
      row <- chunk[i, , drop = FALSE]
      rownames(row) <- NULL
      digest::digest(row, algo = "sha256", serialize = TRUE, serializeVersion = 2)
    }, character(1))
    blocks[[length(blocks) + 1L]] <- rows
  }
  hashes <- unlist(blocks, use.names = FALSE)
  if (is.null(hashes)) hashes <- character()
  list(content_hash = paste0("r-row-sha256-v1:", digest::digest(
    list(columns = columns, rows = sort(hashes, method = "radix")),
    algo = "sha256", serialize = TRUE, serializeVersion = 2
  )), row_count = as.double(length(hashes)))
}

#' Verify stored releases against their publication fingerprints
#'
#' Reads each selected physical table and compares its row count and content
#' fingerprint with the registry. Row order does not affect the fingerprint.
#' Existing releases migrated from older registries are `unverified`: migration
#' never invents a trustworthy baseline. Missing tables, duplicate IDs, missing
#' run references and changed content are reported separately.
#'
#' This detects accidental mutation, not an administrator who can rewrite both
#' data and registry. Hashes describe the DBI representation and are not promised
#' portable across driver or R serialization changes. Verification scans all
#' selected rows and retains one digest per row in memory. Use a maintenance
#' window to prevent direct database clients changing tables during verification.
#' @param lake Connected lake.
#' @param release_ids Optional character vector of release IDs; NULL checks all.
#' @return A tibble with release ID, asset, status, expected and observed counts.
#' @export
#' @examples
#' if (FALSE) dr_verify_releases(lake)
dr_verify_releases <- function(lake, release_ids = NULL) {
  assert_lake(lake)
  if (!is.null(release_ids) && (!is.character(release_ids) || anyNA(release_ids))) {
    dataraft.core::dr_internal_abort(subclass = "dataraft_error_lake",
      "release_ids must be character IDs without missing values.")
  }
  releases <- dr_registry(lake, "releases")
  runs <- dr_registry(lake, "runs")
  all_releases <- releases
  if (!is.null(release_ids)) {
    if (any(!release_ids %in% releases$release_id)) {
      dataraft.core::dr_internal_abort(subclass = "dataraft_error_lake",
        "A requested release does not exist.", "dr_no_release")
    }
    releases <- releases[releases$release_id %in% release_ids, ]
  }
  duplicate <- duplicated(releases$release_id) | duplicated(releases$release_id, fromLast = TRUE)
  out <- tibble::tibble(release_id = releases$release_id, asset = releases$asset,
    status = rep("unverified", nrow(releases)), expected_rows = releases$row_count,
    observed_rows = rep(NA_real_, nrow(releases)))
  for (i in seq_len(nrow(releases))) {
    row <- releases[i, ]
    parent <- row$parent_release[[1]]
    parent_ok <- is.na(parent) || !nzchar(parent) ||
      sum(all_releases$release_id == parent & all_releases$asset == row$asset, na.rm = TRUE) == 1L
    run <- runs[runs$run_id == row$run_id & !is.na(runs$run_id), , drop = FALSE]
    run_ok <- nrow(run) == 1L && run$status[[1]] %in% c("published", "cached")
    if (duplicate[[i]] || !parent_ok || !run_ok) {
      out$status[[i]] <- "registry_inconsistent"
      next
    }
    if (!DBI::dbExistsTable(lake$con, table_id(row$schema_name[[1]], row$table_name[[1]]))) {
      out$status[[i]] <- "missing"
      next
    }
    observed <- tryCatch(release_content(lake, row$schema_name[[1]], row$table_name[[1]]), error = function(e) NULL)
    if (is.null(observed)) {
      out$status[[i]] <- "unreadable"
      next
    }
    out$observed_rows[[i]] <- observed$row_count
    if (is.na(row$content_hash[[1]]) || !nzchar(row$content_hash[[1]])) next
    out$status[[i]] <- if (identical(row$content_hash[[1]], observed$content_hash) &&
      isTRUE(as.double(row$row_count[[1]]) == observed$row_count)) "verified" else "changed"
  }
  out
}

registry_migrate_v5 <- function(lake) {
  assert_writable(lake)
  acquire_maintenance_gate(lake, environment(), exclusive = TRUE)
  DBI::dbWithTransaction(lake$con, {
    fields <- DBI::dbListFields(lake$con, table_id("_dl", "releases"))
    if (!"content_hash" %in% fields) exec(lake, paste("ALTER TABLE", meta(lake, "releases"), "ADD COLUMN content_hash VARCHAR"))
    if (!"row_count" %in% fields) exec(lake, paste("ALTER TABLE", meta(lake, "releases"), "ADD COLUMN row_count DOUBLE"))
    exec(lake, paste("UPDATE", meta(lake, "schema_version"), "SET version = 6, applied_at = ?"), list(now()))
  })
  invisible(NULL)
}
