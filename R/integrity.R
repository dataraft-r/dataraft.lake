# Hash an unordered multiset of typed rows, retaining duplicate multiplicity.
# Versioned R serialization avoids lossy string/JSON conversion of integer64,
# NaN, timestamps and binary columns. Verification materializes each release.
release_digest <- function(lake, schema, table) {
  data <- DBI::dbReadTable(lake$con, table_id(schema, table))
  hash <- function(x) {
    digest::digest(
      serialize(x, NULL, version = 2L),
      algo = "sha256",
      serialize = FALSE
    )
  }
  rows <- vapply(
    seq_len(nrow(data)),
    function(i) {
      hash(lapply(data, function(column) column[i]))
    },
    character(1)
  )
  list(
    rows = nrow(data),
    hash = hash(list(
      prototype = data[0, , drop = FALSE],
      rows = sort(rows, method = "radix")
    ))
  )
}

#' Verify stored release integrity
#'
#' Checks unique registry identifiers, table existence, row counts and a
#' publication-time content hash. Hashes are insensitive to physical row order
#' and retain duplicate multiplicity. Each release is collected in memory.
#' Older releases without a publication-time hash are reported as unverifiable;
#' verification never blesses their current content as a historical baseline.
#' This detects accidental changes. An administrator able to alter both data and
#' the integrity registry can replace both; hashes are not signed attestations.
#' @param lake Connected lake, including a read-only connection.
#' @param release_ids Optional release IDs. NULL checks all releases.
#' @returns A tibble of release IDs, status and diagnostic messages.
#' @export
dr_verify_releases <- function(lake, release_ids = NULL) {
  assert_lake(lake)
  releases <- dr_releases(lake)
  runs <- dr_registry(lake, "runs")
  integrity <- query(
    lake,
    paste("SELECT * FROM", meta(lake, "release_integrity"))
  )
  if (!is.null(release_ids)) {
    if (
      !is.character(release_ids) ||
        anyNA(release_ids) ||
        any(!release_ids %in% releases$release_id)
    ) {
      dataraft.core::dr_internal_abort(
        "Unknown release IDs.",
        subclass = "dataraft_error_lake"
      )
    }
    releases <- releases[releases$release_id %in% release_ids, , drop = FALSE]
  }
  duplicate_ids <- unique(releases$release_id[duplicated(releases$release_id)])
  rows <- lapply(seq_len(nrow(releases)), function(i) {
    release <- releases[i, ]
    baseline <- integrity[
      which(integrity$release_id == release$release_id),
      ,
      drop = FALSE
    ]
    status <- "verified"
    message <- "Content and registry agree."
    if (
      is.na(release$release_id) ||
        !nzchar(release$release_id) ||
        is.na(release$run_id) ||
        release$release_id %in%
          duplicate_ids ||
        nrow(baseline) > 1L ||
        sum(runs$run_id == release$run_id, na.rm = TRUE) != 1L
    ) {
      status <- "registry_inconsistent"
      message <- "Duplicate identifier or missing/ambiguous originating run."
    } else if (
      !isTRUE(tryCatch(
        DBI::dbExistsTable(
          lake$con,
          table_id(release$schema_name, release$table_name)
        ),
        error = function(e) FALSE
      ))
    ) {
      status <- "missing"
      message <- "Published table is missing."
    } else if (!nrow(baseline)) {
      status <- "unverifiable"
      message <- "No publication-time integrity evidence exists for this release."
    } else if (
      !identical(baseline$algorithm[[1]], "sha256-r-serialize-v2-rows-v1")
    ) {
      status <- "unverifiable"
      message <- "Unsupported integrity algorithm."
    } else {
      actual <- tryCatch(
        release_digest(lake, release$schema_name, release$table_name),
        error = function(e) NULL
      )
      if (is.null(actual)) {
        status <- "unreadable"
        message <- "Published data could not be read."
      } else if (
        is.na(baseline$content_hash[[1]]) ||
          is.na(baseline$row_count[[1]]) ||
          !identical(actual$hash, baseline$content_hash[[1]]) ||
          actual$rows != baseline$row_count[[1]]
      ) {
        status <- "modified"
        message <- "Published data differs from its publication-time fingerprint."
      }
    }
    tibble::tibble(
      release_id = release$release_id,
      asset = release$asset,
      status = status,
      message = message
    )
  })
  if (!length(rows)) {
    return(tibble::tibble(
      release_id = character(),
      asset = character(),
      status = character(),
      message = character()
    ))
  }
  dplyr::bind_rows(rows)
}

registry_migrate_v5 <- function(lake) {
  assert_writable(lake)
  acquire_lake_writer(lake, environment(), "internal:catalog-writer")
  DBI::dbWithTransaction(lake$con, {
    for (table in c("runs", "releases")) {
      key <- if (table == "runs") "run_id" else "release_id"
      duplicates <- query(
        lake,
        paste(
          "SELECT",
          key,
          "FROM",
          meta(lake, table),
          "GROUP BY",
          key,
          "HAVING count(*) > 1 OR",
          key,
          "IS NULL"
        )
      )
      if (nrow(duplicates)) {
        dataraft.core::dr_internal_abort(
          "Registry identifiers are ambiguous; migration stopped without altering history.",
          subclass = "dataraft_error_lake"
        )
      }
    }
    exec(
      lake,
      paste(
        "CREATE TABLE",
        meta(lake, "release_integrity"),
        "(release_id VARCHAR, row_count DOUBLE, content_hash VARCHAR, algorithm VARCHAR)"
      )
    )
    registry_unique_indexes(lake)
    exec(
      lake,
      paste(
        "UPDATE",
        meta(lake, "schema_version"),
        "SET version = 6, applied_at = ?"
      ),
      list(now())
    )
  })
}

registry_unique_indexes <- function(lake) {
  # DuckLake currently cannot enforce UNIQUE constraints. Coordinated writes
  # perform the same identity check in insert_meta(), and verification detects
  # mutations made by external clients that bypass the public API.
  if (!identical(lake$config$backend, "duckdb")) {
    return(invisible(NULL))
  }
  for (table in c("runs", "releases", "release_integrity")) {
    key <- if (table == "runs") "run_id" else "release_id"
    exec(
      lake,
      paste(
        "CREATE UNIQUE INDEX IF NOT EXISTS",
        paste0("dr_unique_", table),
        "ON",
        meta(lake, table),
        paste0("(", key, ")")
      )
    )
  }
}
