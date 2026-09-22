writer_identity <- function() {
  rlang::local_error_call(rlang::caller_env())
  read_one <- function(path) {
    rlang::local_error_call(rlang::caller_env())
    if (file.exists(path)) {
      paste(readLines(path, warn = FALSE), collapse = "")
    } else {
      ""
    }
  }
  list(
    host = unname(Sys.info()[["nodename"]]),
    pid = Sys.getpid(),
    boot = read_one("/proc/sys/kernel/random/boot_id"),
    process_start = process_start(Sys.getpid())
  )
}


process_start <- function(pid) {
  rlang::local_error_call(rlang::caller_env())
  path <- paste0("/proc/", pid, "/stat")
  if (!file.exists(path)) {
    return("")
  }
  # Fields after the command's final parenthesis begin with field 3 (state).
  text <- tryCatch(readLines(path, warn = FALSE), error = function(e) "")
  fields <- strsplit(sub("^.*\\) ", "", text), " ", fixed = TRUE)[[1]]
  if (length(fields) >= 20L) fields[[20L]] else ""
}


writer_state <- function(owner) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(owner) || !length(owner$host)) {
    return("unknown")
  }
  local <- writer_identity()
  if (
    !identical(owner$host, local$host) ||
      !nzchar(local$boot) ||
      !nzchar(owner$boot %||% "")
  ) {
    return("unknown")
  }
  if (!identical(owner$boot, local$boot)) {
    return("stopped")
  }
  current <- process_start(owner$pid)
  if (!nzchar(current)) {
    return(
      if (dir.exists(paste0("/proc/", owner$pid))) "unknown" else "stopped"
    )
  }
  if (!nzchar(owner$process_start %||% "")) {
    return("unknown")
  }
  if (identical(current, owner$process_start)) "alive" else "stopped"
}


#' Recover explicitly selected abandoned runs and staging slots
#'
#' Preview first. A known live writer always blocks recovery. On Linux, the
#' recorded host, boot ID and process start distinguish a dead process from a
#' reused PID. For remote or unsupported hosts, recovery requires the
#' caller to stop the original writer and explicitly set `writer_stopped`.
#' Age alone never proves that a writer has stopped.
#'
#' Selected running jobs become errors, retaining their inputs and quality
#' evidence. Selected data-frame staging slots are removed; immutable landing
#' deliveries and published tables are retained. Use [dr_cleanup()] separately
#' for abandoned candidate tables. Run recovery with one coordinated writer.
#' Database changes are transactional; staging removal happens afterwards and
#' is reported separately, so an incomplete filesystem cleanup can be retried.
#' @param lake Connected lake.
#' @param run_ids Explicit run IDs to recover. Empty previews list interrupted
#'   runs; execution never implicitly selects them.
#' @param staging_assets Explicit asset names whose staging slots to remove.
#' @param dry_run Preview only, the default.
#' @param writer_stopped Confirm that writers with unknown liveness were stopped
#'   externally. Does not override a known live writer.
#' @returns A tibble identifying each selected run or staging slot, its writer
#'   state and planned or completed action.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_open_lake(root)
#' dr_recover(lake)
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_recover <- function(
  lake,
  run_ids = character(),
  staging_assets = character(),
  dry_run = TRUE,
  writer_stopped = FALSE
) {
  assert_lake(lake)
  dataraft.core::dr_internal_flag(dry_run, "dry_run")
  dataraft.core::dr_internal_flag(writer_stopped, "writer_stopped")
  if (!dry_run) {
    assert_writable(lake)
  }
  if (!is.character(run_ids) || anyNA(run_ids) || anyDuplicated(run_ids)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "run_ids must be unique strings."
    )
  }
  invisible(lapply(staging_assets, dataraft.core::dr_internal_asset_id))
  if (anyDuplicated(staging_assets)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "staging_assets must be unique."
    )
  }
  if (!length(run_ids) && !length(staging_assets)) {
    if (!dry_run) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Select run_ids or staging_assets explicitly before recovery."
      )
    }
    run_ids <- dr_interrupted(lake)$run_id
  }
  dr_plan <- function() {
    runs <- dr_registry(lake, "runs")
    owners <- dr_registry(lake, "run_owners")
    rows <- lapply(run_ids, function(id) {
      run <- runs[runs$run_id == id, ]
      if (nrow(run) != 1L || run$status[[1]] != "running") {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          paste("Recovery requires an existing running job:", id)
        )
      }
      owner <- owners[owners$run_id == id, ]
      state <- writer_state(
        if (nrow(owner) == 1L) as.list(owner[1, ]) else NULL
      )
      tibble::tibble(
        kind = "run",
        id = id,
        writer = state,
        action = "would_mark_error"
      )
    })
    for (asset in staging_assets) {
      slots <- staging_slots(lake, asset)
      if (!length(slots)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          paste("Staging slot does not exist:", asset)
        )
      }
      for (name in slots) {
        slot <- file.path(lake$config$landing, ".dataraft-staging", name)
        owner <- tryCatch(
          jdecode(paste(
            readLines(file.path(slot, "writer.json"), warn = FALSE),
            collapse = ""
          )),
          error = function(e) NULL,
          warning = function(w) NULL
        )
        rows[[length(rows) + 1L]] <- tibble::tibble(
          kind = "staging",
          id = name,
          writer = writer_state(owner),
          action = "would_remove_staging"
        )
      }
    }
    if (!length(rows)) {
      return(tibble::tibble(
        kind = character(),
        id = character(),
        writer = character(),
        action = character()
      ))
    }
    dplyr::bind_rows(rows)
  }
  out <- dr_plan()
  if (dry_run || !nrow(out)) {
    return(out)
  }
  if (any(out$writer == "alive")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "A selected writer is still alive. Stop it before recovery."
    )
  }
  if (any(out$writer == "unknown") && !writer_stopped) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Writer liveness is unknown. Stop the original writer and set writer_stopped = TRUE."
    )
  }
  DBI::dbWithTransaction(lake$con, {
    if (!identical(out, dr_plan())) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Recovery eligibility changed; request a fresh plan."
      )
    }
    for (id in run_ids) {
      finish_run(
        lake,
        id,
        "error",
        message = "Abandoned run closed by explicit recovery."
      )
      insert_meta(
        lake,
        "events",
        list(
          event_id = dataraft.core::dr_internal_uid(),
          run_id = id,
          asset = "",
          type = "run_recovered",
          recipient = "",
          created_at = now(),
          status = "recorded",
          message = "Abandoned run closed; published releases retained."
        )
      )
    }
  })
  out$action[out$kind == "run"] <- "marked_error"
  for (i in which(out$kind == "staging")) {
    slot <- file.path(lake$config$landing, ".dataraft-staging", out$id[[i]])
    removed <- unlink(slot, recursive = TRUE) == 0L && !dir.exists(slot)
    out$action[[i]] <- if (removed) {
      "removed_staging"
    } else {
      "staging_removal_failed"
    }
  }
  out
}
