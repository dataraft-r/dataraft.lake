#' Check an expected business-date delivery, even when no ingest job ran
#'
#' Compares the latest published stand with an explicit due time and business
#' date. Partitioned releases are checked for the expected date in their current
#' data, so correcting an older partition does not hide a retained delivery.
#' A full replacement does not inherit historical delivery evidence.
#' This function must be called by an existing scheduler; it creates none.
#' Missing deliveries generate deduplicated metadata events. No message is sent
#' unless the caller supplies a notification transport.
#' @param lake Connected lake.
#' @param asset Expected governed asset ID.
#' @param contract Contract identifying the producer and expected freshness.
#' @param business_date Expected date, as `Date` or ISO `YYYY-MM-DD` string.
#' @param due_at,at Due time and evaluation time, both POSIXct scalars.
#' @param notify Optional function receiving an event without source rows.
#'   Requires `record = TRUE` to retain deduplication evidence.
#' @param date_column Optional business-date column to inspect in the current
#'   release. Inferred for a single date partition; otherwise the delivery's
#'   recorded business date is used. Supply explicitly for multi-date full writes.
#' @param record Persist monitoring events. Defaults to `FALSE` on read-only lakes.
#' @returns A tibble with status `pending`, `received` or `missing`. Once due,
#'   the result is also persisted as an operational event.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_connect_lake(dr_lake_config(dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"))
#' contract <- dataraft.core::dr_contract("orders", "1", "Analytics", "Orders", "One order",
#'   c(id = "integer"), key = "id")
#' dr_check_delivery(lake, "orders", contract, as.Date("2026-08-31"),
#'   due_at = as.POSIXct("2026-09-01 09:00:00", tz = "UTC"),
#'   at = as.POSIXct("2026-09-01 10:00:00", tz = "UTC"))
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
dr_check_delivery <- function(
  lake,
  asset,
  contract,
  business_date,
  due_at,
  at = Sys.time(),
  notify = NULL,
  date_column = NULL,
  record = !isTRUE(lake$config$read_only)
) {
  assert_lake(lake)
  dataraft.core::dr_internal_flag(record, "record")
  if (!record && !is.null(notify)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Notifications require record = TRUE so delivery attempts can be deduplicated."
    )
  }
  if (record) {
    assert_writable(lake)
  }
  if (!is.null(date_column)) {
    dataraft.core::dr_internal_column_name(date_column)
  }
  dataraft.core::dr_internal_asset_id(asset)
  if (!inherits(contract, "dr_contract")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "contract must be a contract."
    )
  }
  dataraft.core::dr_internal_assert_contract_ready(contract)
  date <- as.character(business_date)
  if (
    length(date) != 1L ||
      is.na(date) ||
      !grepl("^\\d{4}-\\d{2}-\\d{2}$", date) ||
      is.na(as.Date(date)) ||
      as.character(as.Date(date)) != date
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "business_date must be one valid ISO date."
    )
  }
  valid_time <- function(x) {
    rlang::local_error_call(rlang::caller_env())
    inherits(x, "POSIXct") && length(x) == 1L && is.finite(as.numeric(x))
  }
  if (!valid_time(at) || !valid_time(due_at)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "at and due_at must be POSIXct scalars."
    )
  }
  releases <- dr_releases(lake, asset)
  received <- FALSE
  if (nrow(releases)) {
    ref <- releases[1, ]
    column <- date_column %||% delivery_partition_column(lake, ref)
    if (!is.null(column)) {
      data <- dr_tbl(lake, asset, ref$release_id[[1]])
      if (!column %in% colnames(data)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Delivery date column is missing from the current release."
        )
      }
      type <- dataraft.core::dr_internal_infer_column_types(dplyr::select(
        data,
        dplyr::all_of(column)
      ))[[
        1
      ]]
      if (!type %in% c("Date", "character")) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Delivery date column must contain Date or ISO character values."
        )
      }
      value <- if (type == "Date") as.Date(date) else date
      received <- count_rows(utils::head(
        dplyr::filter(data, !!rlang::sym(column) == !!value),
        1
      )) >
        0
    } else {
      received <- identical(ref$business_date[[1]], date)
    }
  }
  status <- if (received) {
    "received"
  } else if (at < due_at) {
    "pending"
  } else {
    "missing"
  }
  event_id <- NA_character_
  if (status != "pending" && record) {
    dr_register(lake, contract)
    incident <- fingerprint(list(
      asset = asset,
      date = date,
      due_at = format(due_at, tz = "UTC", usetz = TRUE)
    ))
    prior <- query(
      lake,
      paste(
        "SELECT * FROM",
        meta(lake, "events"),
        "WHERE run_id = ? ORDER BY created_at DESC, event_id DESC"
      ),
      list(incident)
    )
    type <- if (received) "delivery_received" else "delivery_overdue"
    delivered <- nrow(prior) &&
      identical(prior$type[[1]], type) &&
      (received || prior$status[[1]] == "delivered")
    event <- list(
      event_id = dataraft.core::dr_internal_uid(),
      run_id = incident,
      asset = asset,
      type = type,
      recipient = contract$producer,
      created_at = now(),
      status = if (delivered) {
        "suppressed"
      } else if (received) {
        "recorded"
      } else {
        "pending"
      },
      message = paste("Expected business date", date, "is", status)
    )
    # Keep the last delivered incident active across suppressed monitoring calls.
    if (
      nrow(prior) &&
        prior$status[[1]] == "suppressed" &&
        prior$type[[1]] == type
    ) {
      event$status <- "suppressed"
    }
    insert_meta(lake, "events", event)
    if (
      status == "missing" && event$status != "suppressed" && !is.null(notify)
    ) {
      event$status <- tryCatch(
        {
          notify(event)
          "delivered"
        },
        error = function(e) "delivery_failed"
      )
      exec(
        lake,
        paste(
          "UPDATE",
          meta(lake, "events"),
          "SET status = ? WHERE event_id = ?"
        ),
        list(event$status, event$event_id)
      )
    }
    event_id <- event$event_id
  }
  tibble::tibble(
    asset = asset,
    business_date = date,
    due_at = due_at,
    checked_at = at,
    status = status,
    release_id = if (received) releases$release_id[[1]] else NA_character_,
    event_id = event_id
  )
}


delivery_partition_column <- function(lake, release) {
  rlang::local_error_call(rlang::caller_env())
  rows <- query(
    lake,
    paste(
      "SELECT definition FROM",
      meta(lake, "assets"),
      "WHERE fingerprint = ? AND kind = 'pipeline'"
    ),
    list(release$definition_hash[[1]])
  )
  if (!nrow(rows)) {
    return(NULL)
  }
  definition <- jdecode(rows$definition[[1]])
  publish <- definition$steps$publish
  if (!identical(publish$mode, "replace_partition")) {
    return(NULL)
  }
  columns <- unlist(publish$partition_by, use.names = FALSE)
  types <- unlist(definition$steps$validate$columns)
  dates <- intersect(columns, names(types)[types == "Date"])
  if (length(dates) == 1L) {
    return(dates[[1]])
  }
  if (length(columns) == 1L && types[[columns]] == "character") {
    return(columns[[1]])
  }
  NULL
}
