#' Preview or remove expired unpublished candidate tables
#'
#' Considers only framework-named tables belonging to completed blocked or
#' errored runs. Published release tables, running jobs, landing files, quality
#' evidence and report manifests are always retained. Execute only with one
#' coordinated writer, as required for all registry mutations.
#'
#' Dropping a DuckLake table does not immediately reclaim physical object-store
#' files. This function does not expire DuckLake snapshots or run file cleanup.
#' @param lake Connected lake.
#' @param older_than_days Positive retention period for failed-run tables.
#' @param dry_run Return a plan without removing tables. Defaults to `TRUE`.
#' @param at POSIXct scalar used to evaluate retention.
#' @returns A tibble with schema, table, run, age and action. Executed drops are
#'   enclosed in one catalog transaction and eligibility is checked again.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_connect_lake(dr_lake_config(dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"))
#' dr_cleanup(lake)
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_cleanup <- function(
  lake,
  older_than_days = 30,
  dry_run = TRUE,
  at = Sys.time()
) {
  assert_lake(lake)
  dataraft.core::flag(dry_run, "dry_run")
  if (!dry_run) {
    assert_writable(lake)
  }
  if (
    !is.numeric(older_than_days) ||
      length(older_than_days) != 1L ||
      !is.finite(older_than_days) ||
      older_than_days <= 0
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "older_than_days must be a positive finite number."
    )
  }
  if (
    !inherits(at, "POSIXct") || length(at) != 1L || !is.finite(as.numeric(at))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "at must be one POSIXct value."
    )
  }
  dr_plan <- function() {
    runs <- dr_registry(lake, "runs")
    age <- as.numeric(difftime(
      at,
      as.POSIXct(runs$finished_at, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
      units = "days"
    ))
    eligible <- runs$status %in%
      c("blocked", "error", "missing") &
      is.finite(age) &
      age > older_than_days
    runs$age_days <- age
    runs <- runs[eligible, ]
    tables <- query(
      lake,
      paste(
        "SELECT table_schema, table_name FROM information_schema.tables",
        "WHERE table_catalog = 'lake' AND table_type = 'BASE TABLE'"
      )
    )
    releases <- dr_releases(lake)
    protected <- paste(releases$schema_name, releases$table_name, sep = ".")
    rows <- lapply(seq_len(nrow(runs)), function(i) {
      candidates <- tables[
        tables$table_schema %in%
          lake$config$layers &
          tables$table_name %in%
            paste0(c("raw_", "candidate_"), runs$run_id[[i]]) &
          !paste(tables$table_schema, tables$table_name, sep = ".") %in%
            protected,
      ]
      tibble::tibble(
        schema = candidates$table_schema,
        table = candidates$table_name,
        run_id = rep(runs$run_id[[i]], nrow(candidates)),
        age_days = rep(runs$age_days[[i]], nrow(candidates)),
        action = rep("would_drop", nrow(candidates))
      )
    })
    if (!length(rows)) {
      return(tibble::tibble(
        schema = character(),
        table = character(),
        run_id = character(),
        age_days = double(),
        action = character()
      ))
    }
    dplyr::bind_rows(rows)
  }
  out <- dr_plan()
  if (!dry_run && nrow(out)) {
    DBI::dbWithTransaction(lake$con, {
      current <- dr_plan()
      if (!identical(out, current)) {
        dataraft.core::abort(
          subclass = "dataraft_error_lake",
          "Cleanup eligibility changed; request a fresh plan."
        )
      }
      for (i in seq_len(nrow(out))) {
        exec(
          lake,
          paste("DROP TABLE", table_sql(lake, out$schema[[i]], out$table[[i]]))
        )
      }
    })
    out$action <- "dropped"
  }
  out
}
