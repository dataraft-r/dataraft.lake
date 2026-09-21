# Generated from dataraft.core/inst/standalone/standalone-dataraft.R.
# Private stateless helpers; sync with scripts/sync-standalone.R in the metapackage.
# Copyright (c) 2026 Jan-Hendrik Weinert. MIT license.

`%||%` <- function(x, y) if (is.null(x)) y else x

now <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")

canonical <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  if (is.function(x)) {
    return(list(
      formals = paste(deparse(formals(x)), collapse = "\n"),
      body = paste(deparse(body(x)), collapse = "\n")
    ))
  }
  if (rlang::is_quosure(x)) {
    return(list(
      expression = paste(
        deparse(rlang::get_expr(x), width.cutoff = 500L),
        collapse = "\n"
      ),
      format = 2L
    ))
  }
  if (inherits(x, "formula")) {
    return(list(
      formula = paste(deparse(x, width.cutoff = 500L), collapse = "\n")
    ))
  }
  if (is.list(x)) {
    return(lapply(x, canonical))
  }
  x
}

jencode <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  as.character(jsonlite::toJSON(
    canonical(x),
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    digits = NA
  ))
}

jdecode <- function(x) jsonlite::fromJSON(x, simplifyVector = FALSE)

fingerprint <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  digest::digest(jencode(x), algo = "sha256", serialize = FALSE)
}

count_rows <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  as.numeric(dplyr::collect(dplyr::summarise(
    dplyr::ungroup(x),
    n = dplyr::n()
  ))$n[[1]])
}

null_counts <- function(data, columns) {
  rlang::local_error_call(rlang::caller_env())
  data <- dplyr::ungroup(data)
  if (!length(columns)) {
    return(stats::setNames(numeric(), character()))
  }
  if (!inherits(data, "tbl_sql")) {
    return(vapply(data[columns], function(x) sum(is.na(x)), numeric(1)))
  }
  expressions <- stats::setNames(
    lapply(columns, function(column) {
      rlang::expr(sum(as.integer(is.na(!!rlang::sym(column))), na.rm = TRUE))
    }),
    columns
  )
  result <- dplyr::collect(dplyr::summarise(data, !!!expressions))
  stats::setNames(as.numeric(result[1, ]), columns)
}

report_json <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  as.character(jsonlite::toJSON(
    canonical(x),
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    digits = I(17)
  ))
}

report_fingerprint <- function(x) {
  rlang::local_error_call(rlang::caller_env())
  digest::digest(report_json(x), algo = "sha256", serialize = FALSE)
}

adapter_remote_path <- function(path) {
  rlang::local_error_call(rlang::caller_env())
  grepl("^[[:alpha:]][[:alnum:]+.-]*://", path)
}
