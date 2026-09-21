#' List published release history
#' @param lake Connected lake.
#' @param asset Optional asset ID.
#' @returns A tibble sorted newest first. Historical releases are retained.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_connect_lake(dr_lake_config(dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"))
#' dr_releases(lake)
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_releases <- function(lake, asset = NULL) {
  out <- dataraft.core::dr_internal_metadata_filter(
    lake,
    "releases",
    asset = asset
  )
  out[order(out$published_at, out$release_id, decreasing = TRUE), ]
}
