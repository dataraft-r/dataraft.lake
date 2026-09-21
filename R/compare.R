#' Compare two published releases by business key
#'
#' With only an asset name, compares its latest two releases and uses the
#' latest published contract's key. Supply `key` for structural-only assets.
#' Counts and numeric totals run in the database. Only bounded row previews
#' are collected; `limit = Inf` explicitly collects all differences.
#'
#' Keys must be unique and non-missing in both releases. Missing values compare
#' equal. Integer and numeric columns are compatible. A schema change marks
#' every matched row as changed. Numeric totals ignore missing values, whose
#' counts are reported separately. Row order has no significance.
#' @param lake Connected lake, including a read-only connection, or the earlier
#'   published result. With two results, their exact releases are compared and
#'   an owned read-only connection is closed automatically.
#' @param name Published asset name, or the later published result.
#' @param from,to Optional release IDs. Defaults to the previous and latest
#'   releases. When only `to` is given, `from` is its preceding release.
#' @param key Optional business-key column names.
#' @param limit Maximum rows in each added, removed and changed preview.
#' @returns A `dr_comparison` list with release IDs, counts, schema differences,
#'   numeric totals, and row previews. Changed previews contain separate
#'   `before` and `after` tables in the same key order.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_open_lake(root)
#' dr_write_data(lake, data.frame(id = c(1L, 2L), amount = c(10, 20)), "orders")
#' dr_write_data(lake, data.frame(id = c(1L, 3L), amount = c(12, 30)), "orders")
#' difference <- dr_compare(lake, "orders", key = "id")
#' difference
#' difference$changed
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_compare <- function(
  lake,
  name,
  from = NULL,
  to = NULL,
  key = NULL,
  limit = 100
) {
  if (inherits(lake, "dr_run_result")) {
    first <- lake
    second <- name
    if (!inherits(second, "dr_run_result") || !is.null(from) || !is.null(to)) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Supply two published results without from or to release IDs."
      )
    }
    a <- dataraft.core::normalize_result_source(first)
    b <- dataraft.core::normalize_result_source(second)
    if (
      !inherits(a, "dr_release_source") ||
        !inherits(b, "dr_release_source") ||
        !identical(first$asset, second$asset)
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Comparison needs two lake publications of the same product."
      )
    }
    config <- function(source) {
      rlang::local_error_call(rlang::caller_env())
      if (inherits(source$lake, "dr_lake")) source$lake$config else source$lake
    }
    if (
      !identical(
        config(a)[c("backend", "catalog", "storage")],
        config(b)[c("backend", "catalog", "storage")]
      )
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Both comparison results must belong to the same lake."
      )
    }
    connections <- Filter(
      function(con) {
        inherits(con, "dr_lake") && DBI::dbIsValid(con$con)
      },
      list(first$output_lake, second$output_lake, a$lake, b$lake)
    )
    lake <- if (length(connections)) connections[[1L]] else NULL
    if (!inherits(lake, "dr_lake") || !DBI::dbIsValid(lake$con)) {
      lake <- dr_connect_lake(config(a), read_only = TRUE)
      on.exit(dr_close_lake(lake), add = TRUE)
    }
    name <- first$asset
    from <- first$release_id
    to <- second$release_id
  }
  assert_lake(lake)
  dataraft.core::asset_id(name)
  if (
    !is.numeric(limit) ||
      length(limit) != 1L ||
      is.na(limit) ||
      limit < 0 ||
      (is.finite(limit) && limit != floor(limit))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "limit must be a non-negative whole number or Inf."
    )
  }
  history <- dr_releases(lake, name)
  if (!nrow(history)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      paste("No published release for", name)
    )
  }
  to <- to %||% history$release_id[[1]]
  after <- resolve_release(lake, name, to)
  if (is.null(from)) {
    position <- match(to, history$release_id)
    if (position == nrow(history)) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Comparison requires a previous release or an explicit from."
      )
    }
    from <- history$release_id[[position + 1L]]
  }
  before <- resolve_release(lake, name, from)
  if (is.null(key)) {
    parts <- strsplit(after$contract[[1]], "@", fixed = TRUE)[[1]]
    definition <- query(
      lake,
      paste(
        "SELECT definition FROM",
        meta(lake, "assets"),
        "WHERE kind = 'contract' AND id = ? AND version = ?"
      ),
      as.list(parts)
    )
    if (nrow(definition) == 1L) {
      key <- unlist(
        dataraft.core::jdecode(definition$definition[[1]])$key,
        use.names = FALSE
      )
    }
  }
  if (!length(key) || anyDuplicated(key)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Supply key or define a unique key in the published contract."
    )
  }
  invisible(lapply(key, dataraft.core::column_name))
  old <- dr_tbl(lake, name, from)
  new <- dr_tbl(lake, name, to)
  types_before <- dataraft.core::infer_column_types(old)
  types_after <- dataraft.core::infer_column_types(new)
  if (!all(key %in% intersect(names(types_before), names(types_after)))) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Key columns must exist in both releases."
    )
  }
  for (data in list(old, new)) {
    if (
      any(dataraft.core::null_counts(data, key) > 0) ||
        dataraft.core::count_rows(data) !=
          dataraft.core::count_rows(dplyr::distinct(data, !!!rlang::syms(key)))
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Comparison requires unique, non-missing keys in both releases."
      )
    }
  }
  compatible <- function(a, b) {
    rlang::local_error_call(rlang::caller_env())
    identical(a, b) || all(c(a, b) %in% c("integer", "numeric"))
  }
  if (
    !all(vapply(
      key,
      function(column) {
        compatible(types_before[[column]], types_after[[column]])
      },
      logical(1)
    ))
  ) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Key types differ between releases."
    )
  }
  columns <- union(names(types_before), names(types_after))
  schema <- tibble::tibble(
    column = columns,
    before = unname(types_before[columns]),
    after = unname(types_after[columns])
  )
  schema <- schema[
    is.na(schema$before) | is.na(schema$after) | schema$before != schema$after,
  ]
  common <- setdiff(intersect(names(types_before), names(types_after)), key)
  comparable <- common[vapply(
    common,
    function(column) compatible(types_before[[column]], types_after[[column]]),
    logical(1)
  )]
  schema_changed <- !setequal(names(types_before), names(types_after)) ||
    length(comparable) != length(common)
  a <- table_sql(lake, before$schema_name[[1]], before$table_name[[1]])
  b <- table_sql(lake, after$schema_name[[1]], after$table_name[[1]])
  fields <- function(alias, columns) paste0(alias, ".", qident(lake, columns))
  join <- paste(
    paste(fields("a", key), "=", fields("b", key)),
    collapse = " AND "
  )
  changed <- if (schema_changed) {
    "TRUE"
  } else if (!length(comparable)) {
    "FALSE"
  } else {
    paste(
      paste(
        fields("a", comparable),
        "IS DISTINCT FROM",
        fields("b", comparable)
      ),
      collapse = " OR "
    )
  }
  added <- paste(
    b,
    "b WHERE NOT EXISTS (SELECT 1 FROM",
    a,
    "a WHERE",
    join,
    ")"
  )
  removed <- paste(
    a,
    "a WHERE NOT EXISTS (SELECT 1 FROM",
    b,
    "b WHERE",
    join,
    ")"
  )
  matched <- paste(a, "a JOIN", b, "b ON", join)
  modified <- paste(matched, "WHERE", changed)
  count <- function(sql) {
    rlang::local_error_call(rlang::caller_env())
    as.numeric(query(lake, paste("SELECT count(*) AS n FROM", sql))$n[[1]])
  }
  counts <- c(
    added = count(added),
    removed = count(removed),
    changed = count(modified)
  )
  counts <- c(counts, unchanged = count(matched) - counts[["changed"]])
  preview <- function(sql, alias) {
    rlang::local_error_call(rlang::caller_env())
    query(
      lake,
      paste(
        "SELECT",
        paste0(alias, ".*"),
        "FROM",
        sql,
        "ORDER BY",
        paste(fields(alias, key), collapse = ", "),
        if (is.finite(limit)) {
          paste("LIMIT", format(limit, scientific = FALSE))
        } else {
          ""
        }
      )
    )
  }
  numeric <- comparable[vapply(
    comparable,
    function(column) {
      all(
        c(types_before[[column]], types_after[[column]]) %in%
          c("integer", "numeric")
      )
    },
    logical(1)
  )]
  totals <- function(table) {
    rlang::local_error_call(rlang::caller_env())
    if (!length(numeric)) {
      return(list())
    }
    expressions <- unlist(lapply(seq_along(numeric), function(i) {
      column <- qident(lake, numeric[[i]])
      c(
        paste0("coalesce(sum(", column, "), 0) AS total", i),
        paste0("count(*) - count(", column, ") AS missing", i)
      )
    }))
    as.list(query(
      lake,
      paste("SELECT", paste(expressions, collapse = ", "), "FROM", table)
    )[1, ])
  }
  x <- totals(a)
  y <- totals(b)
  number <- function(values, prefix) {
    rlang::local_error_call(rlang::caller_env())
    vapply(
      seq_along(numeric),
      function(i) as.numeric(values[[paste0(prefix, i)]]),
      numeric(1)
    )
  }
  numeric_summary <- tibble::tibble(
    column = numeric,
    before = number(x, "total"),
    after = number(y, "total"),
    difference = number(y, "total") - number(x, "total"),
    missing_before = number(x, "missing"),
    missing_after = number(y, "missing")
  )
  structure(
    list(
      name = name,
      from = from,
      to = to,
      key = key,
      counts = counts,
      schema = schema,
      numeric_summary = numeric_summary,
      added = preview(added, "b"),
      removed = preview(removed, "a"),
      changed = list(
        before = preview(modified, "a"),
        after = preview(modified, "b")
      ),
      limit = limit
    ),
    class = "dr_comparison"
  )
}


#' @export
print.dr_comparison <- function(x, ...) {
  cat("<dr_comparison>", x$name, "\n")
  print(tibble::tibble(change = names(x$counts), rows = unname(x$counts)))
  cat("Row previews are available in $added, $removed and $changed.\n")
  invisible(x)
}
