# Accept libpq keyword/value strings without evaluating or logging credentials.
postgres_parameters <- function(value) {
  rlang::local_error_call(rlang::caller_env())
  chars <- strsplit(value, "", fixed = TRUE)[[1]]
  n <- length(chars)
  i <- 1L
  out <- list()
  invalid <- function() {
    rlang::local_error_call(rlang::caller_env())
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Use a libpq keyword=value connection string (or service=name), not a URI."
    )
  }
  whitespace <- function(x) x %in% c(" ", "\t", "\n", "\r")
  while (i <= n) {
    while (i <= n && whitespace(chars[[i]])) {
      i <- i + 1L
    }
    if (i > n) {
      break
    }
    key <- ""
    while (i <= n && grepl("^[A-Za-z0-9_]$", chars[[i]])) {
      key <- paste0(key, chars[[i]])
      i <- i + 1L
    }
    while (i <= n && whitespace(chars[[i]])) {
      i <- i + 1L
    }
    if (!nzchar(key) || i > n || chars[[i]] != "=") {
      invalid()
    }
    i <- i + 1L
    while (i <= n && whitespace(chars[[i]])) {
      i <- i + 1L
    }
    quoted <- i <= n && chars[[i]] == "'"
    if (quoted) {
      i <- i + 1L
    }
    text <- ""
    closed <- !quoted
    while (i <= n) {
      char <- chars[[i]]
      if (quoted && char == "'") {
        i <- i + 1L
        closed <- TRUE
        break
      }
      if (!quoted && whitespace(char)) {
        break
      }
      if (char == "\\") {
        i <- i + 1L
        if (i > n) {
          invalid()
        }
        char <- chars[[i]]
      }
      text <- paste0(text, char)
      i <- i + 1L
    }
    if (!closed || (i <= n && !whitespace(chars[[i]]))) {
      invalid()
    }
    out[[key]] <- text
  }
  if (
    !length(out) ||
      any(
        names(out) %in%
          c("drv", "bigint", "check_interrupts", "timezone", "timezone_out")
      )
  ) {
    invalid()
  }
  out
}


.postgres_writer_states <- new.env(parent = emptyenv())


lake_writer_state <- function(config) {
  rlang::local_error_call(rlang::caller_env())
  if (!identical(config$catalog$type, "postgres")) {
    return(new.env(parent = emptyenv()))
  }
  value <- Sys.getenv(config$catalog$connection_env)
  if (!nzchar(value)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      paste("Set", config$catalog$connection_env)
    )
  }
  parameters <- postgres_parameters(value)
  key <- fingerprint(list(
    pid = Sys.getpid(),
    parameters = parameters[sort(names(parameters))]
  ))
  state <- .postgres_writer_states[[key]]
  if (is.null(state)) {
    state <- new.env(parent = emptyenv())
    .postgres_writer_states[[key]] <- state
  }
  state
}


# Explicit scope: callers acquire only around publication or definition writes.
# Locks are asset-scoped, deterministic, and reentrant within the process.
acquire_lake_writer <- function(lake, frame, asset) {
  rlang::local_error_call(rlang::caller_env())
  if (!identical(lake$config$catalog$type, "postgres")) {
    return(invisible(NULL))
  }
  state <- lake$writer_state
  if (is.null(state)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Reconnect this lake to enable coordinated PostgreSQL writes."
    )
  }
  key <- paste0("asset_", fingerprint(asset))
  if (is.null(state[[key]])) {
    state[[key]] <- new.env(parent = emptyenv())
  }
  state <- state[[key]]
  if (isTRUE(state$held)) {
    if (!DBI::dbIsValid(state$con)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Writer coordination connection was lost. Reconnect and inspect the last release.",
        "dr_writer_lost"
      )
    }
    # Fail before any new mutation if the coordinator died while user code ran.
    tryCatch(DBI::dbGetQuery(state$con, "SELECT 1"), error = function(e) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Writer coordination connection was lost. Reconnect and inspect the last release.",
        "dr_writer_lost"
      )
    })
    return(invisible(NULL))
  }
  dataraft.core::dr_internal_need("RPostgres")
  value <- Sys.getenv(lake$config$catalog$connection_env)
  if (!nzchar(value)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      paste("Set", lake$config$catalog$connection_env)
    )
  }
  parameters <- postgres_parameters(value)
  parameters$connect_timeout <- parameters$connect_timeout %||% "10"
  con <- tryCatch(
    do.call(DBI::dbConnect, c(list(drv = RPostgres::Postgres()), parameters)),
    error = function(e) {
      dataraft.core::dr_internal_abort(
        subclass = c("dataraft_error_backend", "dataraft_error_lake"),
        "PostgreSQL writer coordination failed. Check catalog credentials and connectivity; credentials are omitted."
      )
    }
  )
  acquired <- FALSE
  on.exit(if (!acquired) DBI::dbDisconnect(con), add = TRUE)
  deadline <- Sys.time() + (lake$config$catalog$lock_timeout %||% 30)
  repeat {
    # The server hashes the asset, independent of client credentials or DSN spelling.
    locked <- if (asset %in% c("internal:maintenance-shared", "internal:maintenance-exclusive")) {
      sql <- if (identical(asset, "internal:maintenance-shared")) {
        "SELECT pg_try_advisory_lock_shared(1953981815, 1) AS locked"
      } else {
        "SELECT pg_try_advisory_lock(1953981815, 1) AS locked"
      }
      DBI::dbGetQuery(con, sql)$locked[[1]]
    } else if (identical(asset, "internal:legacy-migration")) {
      DBI::dbGetQuery(
        con,
        "SELECT pg_try_advisory_lock(1953981814, 1) AS locked"
      )$locked[[1]]
    } else {
      DBI::dbGetQuery(
        con,
        "SELECT pg_try_advisory_lock(1953981814, hashtext($1)) AS locked",
        params = list(asset)
      )$locked[[1]]
    }
    if (isTRUE(locked)) {
      break
    }
    if (Sys.time() >= deadline) {
      dataraft.core::dr_internal_abort(
        subclass = c("dataraft_error_backend", "dataraft_error_lake"),
        "Another writer holds this asset. Retry after it finishes.",
        "dr_writer_busy"
      )
    }
    Sys.sleep(0.1)
  }
  state$con <- con
  state$held <- TRUE
  acquired <- TRUE
  withr::defer(
    {
      state$held <- FALSE
      state$con <- NULL
      if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
    },
    envir = frame
  )
  invisible(NULL)
}


check_previous_release <- function(lake, asset, previous) {
  rlang::local_error_call(rlang::caller_env())
  if (is.null(previous)) {
    return(invisible(NULL))
  }
  if (
    !inherits(previous, "dr_run_result") ||
      !previous$status %in% c("published", "cached") ||
      !identical(previous$asset, asset)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "previous must be a successful publication of this product."
    )
  }
  previous_config <- previous$output_config
  if (
    is.null(previous_config) ||
      !identical(previous_config$catalog, lake$config$catalog) ||
      !identical(previous_config$storage, lake$config$storage)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "previous belongs to a different lake configuration."
    )
  }
  current <- resolve_release(lake, asset)$release_id[[1]]
  if (!identical(current, previous$release_id)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "This product has a newer release. Read it and reconcile your correction before publishing again.",
      "dr_publication_conflict",
      expected_release = previous$release_id,
      current_release = current
    )
  }
  invisible(NULL)
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name assert_table_asset

assert_table_asset <- function(lake, asset) {
  rlang::local_error_call(rlang::caller_env())
  prior <- tryCatch(resolve_release(lake, asset), dr_no_release = function(e) {
    NULL
  })
  if (!is.null(prior) && grepl("^(model|member)_", prior$table_name[[1]])) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "This name belongs to a model product. Publish the complete model or choose a different table product name."
    )
  }
  invisible(NULL)
}


create_staging_slot <- function(lake, asset, run) {
  dataraft.core::dr_internal_asset_id(asset)
  dataraft.core::dr_internal_scalar(run, "run")
  if (!grepl("^r[[:alnum:]]+$", run)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Invalid staging run identifier.",
      "dr_staging_invalid"
    )
  }
  parent <- file.path(lake$config$landing, ".dataraft-staging")
  dir.create(parent, recursive = TRUE, showWarnings = FALSE)
  if (file.exists(file.path(parent, asset))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Staging already exists for this asset. Inspect the legacy writer before recovery.",
      "dr_staging_conflict"
    )
  }
  slot <- file.path(parent, paste0(asset, "--", run))
  if (!dir.create(slot, showWarnings = FALSE)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Staging already exists for this run. Inspect its writer before recovery.",
      "dr_staging_conflict"
    )
  }
  slot
}

staging_slots <- function(lake, asset) {
  parent <- file.path(lake$config$landing, ".dataraft-staging")
  candidates <- list.files(parent, full.names = FALSE)
  prefix <- paste0(asset, "--")
  scoped <- startsWith(candidates, prefix) &
    grepl("^r[[:alnum:]]+$", substring(candidates, nchar(prefix) + 1L))
  slots <- candidates[candidates == asset | scoped]
  paths <- file.path(parent, slots)
  # Non-following stat also identifies Windows junctions. Admit only confirmed
  # directories, never links or paths whose type could not be determined.
  types <- fs::file_info(paths, follow = FALSE, fail = FALSE)$type
  sort(slots[!is.na(types) & types == "directory"])
}

# The shared gate spans a run, including raw/candidate creation. Different
# assets remain concurrent; maintenance alone takes the exclusive gate.
acquire_maintenance_gate <- function(lake, frame, exclusive = FALSE) {
  if (!identical(lake$config$catalog$type, "postgres")) return(invisible(NULL))
  state <- lake$writer_state
  exclusive_state <- state[[paste0("asset_", fingerprint("internal:maintenance-exclusive"))]]
  if (!is.null(exclusive_state) && isTRUE(exclusive_state$held)) {
    return(acquire_lake_writer(lake, frame, "internal:maintenance-exclusive"))
  }
  shared_state <- state[[paste0("asset_", fingerprint("internal:maintenance-shared"))]]
  if (exclusive && !is.null(shared_state) && isTRUE(shared_state$held)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Maintenance cannot run inside an active publication.",
      "dr_maintenance_busy"
    )
  }
  acquire_lake_writer(lake, frame, if (exclusive) {
    "internal:maintenance-exclusive"
  } else "internal:maintenance-shared")
}
