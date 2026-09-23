#' Preview or remove expired unpublished candidate tables
#'
#' Considers only framework-named tables belonging to completed successful, blocked or
#' errored runs. Published release tables, running jobs, landing files, quality
#' evidence and report manifests are always retained. Active runs are never eligible. All historical release tables are protected.
#'
#' Dropping a DuckLake table does not immediately reclaim physical object-store
#' files. This function does not expire DuckLake snapshots or run file cleanup.
#' @param lake Connected lake.
#' @param older_than_days Positive retention period for unpublished run tables.
#' @param dry_run Return a plan without removing tables. Defaults to `TRUE`.
#' @param at POSIXct scalar used to evaluate retention.
#' @returns A tibble with schema, table, run, age and action. Executed drops are
#'   enclosed in one catalog transaction and eligibility is checked again.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dataraft.lake::dr_connect_lake(dr_lake_config(dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"))
#' dr_cleanup(lake)
#' dataraft.lake::dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_cleanup <- function(
  lake,
  older_than_days = 30,
  dry_run = TRUE,
  at = Sys.time()
) {
  assert_lake(lake)
  dataraft.core::dr_internal_flag(dry_run, "dry_run")
  if (!dry_run) {
    assert_writable(lake)
    acquire_lake_writer(lake, environment(), "internal:catalog-writer")
  }
  if (
    !is.numeric(older_than_days) ||
      length(older_than_days) != 1L ||
      !is.finite(older_than_days) ||
      older_than_days <= 0
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "older_than_days must be a positive finite number."
    )
  }
  if (
    !inherits(at, "POSIXct") || length(at) != 1L || !is.finite(as.numeric(at))
  ) {
    dataraft.core::dr_internal_abort(
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
      c("published", "cached", "blocked", "error", "missing") &
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
          (tables$table_name %in%
            paste0(c("raw_", "candidate_"), runs$run_id[[i]]) |
            startsWith(
              tables$table_name,
              paste0("candidate_", runs$run_id[[i]], "_")
            )) &
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
        dataraft.core::dr_internal_abort(
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

#' Expire DuckLake snapshots and reclaim scheduled files
#'
#' This is catalog-wide maintenance, including tables outside DataRaft. It does
#' not delete release tables, reports, lineage or landing files. DataRaft release
#' pins refer to live immutable tables, not DuckLake snapshot IDs. External
#' time-travel consumers must retain their own required snapshot horizon.
#'
#' Execution requires an exclusive maintenance window with no other readers or
#' writers, including direct DuckDB clients. `readers_quiescent` is the caller's
#' assertion of that condition; it is not detected across remote sessions.
#' Snapshots are expired first. File cleanup removes only files already scheduled
#' for deletion before the separate retention cutoff, never freshly expired files
#' or untracked orphan files. Physical file deletion is not transactional.
#' @param lake Connected DuckLake lake.
#' @param older_than_days Positive snapshot retention in days.
#' @param file_retention_days Positive grace period after file deletion scheduling.
#' @param dry_run Preview both operations without writes. Defaults to `TRUE`.
#' @param readers_quiescent Whether an exclusive maintenance window is in effect.
#' @returns A list containing DuckLake snapshot and file operation results.
#' @seealso [dr_cleanup()]
#' @export
#' @examples
#' if (FALSE) {
#'   dr_expire_snapshots(lake)
#'   dr_expire_snapshots(lake, dry_run = FALSE, readers_quiescent = TRUE)
#' }
dr_expire_snapshots <- function(
  lake,
  older_than_days = 30,
  file_retention_days = 7,
  dry_run = TRUE,
  readers_quiescent = FALSE
) {
  assert_lake(lake)
  dataraft.core::dr_internal_flag(dry_run, "dry_run")
  dataraft.core::dr_internal_flag(readers_quiescent, "readers_quiescent")
  for (period in list(older_than_days, file_retention_days)) {
    if (
      !is.numeric(period) ||
        length(period) != 1L ||
        !is.finite(period) ||
        period <= 0
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Retention periods must be positive finite numbers.",
        "dr_retention_period"
      )
    }
  }
  if (!identical(lake$config$backend, "ducklake")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Snapshot expiration requires the DuckLake backend.",
      "dr_snapshot_backend"
    )
  }
  if (!dry_run) {
    assert_writable(lake)
    acquire_lake_writer(lake, environment(), "internal:catalog-writer")
    if (
      !readers_quiescent || any(dr_registry(lake, "runs")$status == "running")
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "An exclusive maintenance window with no active readers or writers is required.",
        "dr_maintenance_busy"
      )
    }
    acquire_lake_writer(lake, environment(), "snapshot-maintenance")
  }
  cutoff <- query(
    lake,
    "SELECT CAST(CAST(now() AS TIMESTAMP) AS VARCHAR) AS cutoff"
  )$cutoff[[1]]
  execute <- function(function_name, days) {
    query(
      lake,
      paste0(
        "CALL ",
        function_name,
        "('lake', dry_run => ",
        if (dry_run) "true" else "false",
        ", older_than => CAST(",
        qlit(lake, cutoff),
        " AS TIMESTAMP) - INTERVAL '1 day' * ",
        qlit(lake, days),
        ")"
      )
    )
  }
  list(
    snapshots = execute("ducklake_expire_snapshots", older_than_days),
    files = execute("ducklake_cleanup_old_files", file_retention_days)
  )
}
