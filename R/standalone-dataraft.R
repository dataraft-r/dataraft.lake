# Generated from dataraft.core/inst/standalone/standalone-dataraft.R.
# Private stateless helpers; sync with scripts/sync-standalone.R in the metapackage.
# Copyright (c) 2026 Jan-Hendrik Weinert. MIT license.

`%||%` <- function(x, y) if (is.null(x)) y else x

now <- function() format(Sys.time(), "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")

# Only free lexical bindings are included. Captured values are represented by
# hashes so that reference rows and credentials cannot leak into manifests.
canonical_binding_expression <- function(expr) {
  if (missing(expr)) {
    return(quote(expr = ))
  }
  if (!is.call(expr)) {
    return(expr)
  }
  head <- as.character(expr[[1L]])
  if (length(head) == 3L && head[[1L]] %in% c("::", ":::")) {
    head <- head[[3L]]
  }
  if (
    length(head) == 1L && head %in% c("get", "mget", "eval", "evalq", "parse")
  ) {
    rlang::abort(
      "Reflective lookups cannot be fingerprinted; capture explicit immutable values instead.",
      class = c("dataraft_error_fingerprint", "dataraft_error")
    )
  }
  if (
    length(head) == 1L &&
      head %in% c("$", "[[") &&
      identical(expr[[2L]], quote(.env))
  ) {
    name <- expr[[3L]]
    if (head == "$" && is.symbol(name)) {
      name <- as.character(name)
    }
    if (!is.character(name) || length(name) != 1L) {
      rlang::abort(
        "Dynamic environment lookups cannot be fingerprinted.",
        class = c("dataraft_error_fingerprint", "dataraft_error")
      )
    }
    return(as.name(name))
  }
  as.call(lapply(as.list(expr), canonical_binding_expression))
}

canonical_bindings <- function(fun, seen, masked = character()) {
  explicit <- function(expr) {
    if (missing(expr)) {
      return(character())
    }
    if (!is.call(expr)) {
      return(character())
    }
    if (
      length(expr) >= 3L &&
        identical(expr[[2L]], quote(.env)) &&
        as.character(expr[[1L]])[[1L]] %in% c("$", "[[")
    ) {
      return(as.character(expr[[3L]]))
    }
    if (
      length(expr) >= 3L &&
        identical(expr[[1L]], quote(`[[`)) &&
        identical(expr[[2L]], quote(.data)) &&
        is.symbol(expr[[3L]])
    ) {
      return(as.character(expr[[3L]]))
    }
    unlist(lapply(as.list(expr), explicit), use.names = FALSE)
  }
  invisible(canonical_binding_expression(as.call(c(
    list(quote(list)),
    as.list(formals(fun))
  ))))
  analysis <- fun
  body(analysis) <- canonical_binding_expression(body(fun))
  globals <- union(
    setdiff(codetools::findGlobals(analysis, merge = TRUE), masked),
    explicit(body(fun))
  )
  env <- environment(fun)
  out <- list()
  for (name in sort(unique(globals), method = "radix")) {
    where <- env
    while (
      !identical(where, emptyenv()) &&
        !exists(name, envir = where, inherits = FALSE)
    ) {
      where <- parent.env(where)
    }
    # Unbound names can be columns supplied by a data mask at execution.
    if (identical(where, emptyenv())) {
      next
    }
    if (bindingIsActive(name, where)) {
      rlang::abort(
        "Active bindings cannot be fingerprinted.",
        class = c("dataraft_error_fingerprint", "dataraft_error")
      )
    }
    if (
      isNamespace(where) ||
        identical(where, baseenv()) ||
        startsWith(environmentName(where), "package:")
    ) {
      package <- if (identical(where, baseenv())) {
        "base"
      } else {
        sub("^(namespace:|package:)", "", environmentName(where))
      }
      out[[name]] <- list(
        package = package,
        name = name,
        version = as.character(utils::packageVersion(package))
      )
      next
    }
    value <- tryCatch(
      get(name, envir = where, inherits = FALSE),
      error = function(e) {
        rlang::abort(
          "A captured binding could not be resolved for fingerprinting.",
          class = c("dataraft_error_fingerprint", "dataraft_error")
        )
      }
    )
    out[[name]] <- digest::digest(
      list(
        value = canonical(value, seen),
        attributes = if (is.function(value) || inherits(value, "formula")) {
          NULL
        } else {
          attributes(value)
        }
      ),
      algo = "sha256",
      serialize = TRUE
    )
  }
  out
}

canonical <- function(x, seen = list(), masked = character()) {
  rlang::local_error_call(rlang::caller_env())
  if (is.function(x) || inherits(x, "formula")) {
    if (any(vapply(seen, function(value) identical(value, x), logical(1)))) {
      rlang::abort(
        "Cyclic closures cannot be fingerprinted.",
        class = c("dataraft_error_fingerprint", "dataraft_error")
      )
    }
    seen <- c(seen, list(x))
  }
  if (is.function(x)) {
    if (is.primitive(x)) {
      return(list(primitive = deparse(x)))
    }
    return(list(
      formals = paste(deparse(formals(x)), collapse = "\n"),
      body = paste(deparse(body(x)), collapse = "\n"),
      bindings = canonical_bindings(x, seen),
      format = 3L
    ))
  }
  if (inherits(x, "formula")) {
    expr <- if (rlang::is_quosure(x)) rlang::get_expr(x) else x[[length(x)]]
    fun <- rlang::new_function(pairlist(), expr, environment(x))
    return(list(
      expression = paste(deparse(x, width.cutoff = 500L), collapse = "\n"),
      bindings = canonical_bindings(fun, seen, masked),
      format = 3L
    ))
  }
  if (
    is.environment(x) ||
      isS4(x) ||
      typeof(x) %in% c("externalptr", "weakref") ||
      inherits(x, "connection")
  ) {
    rlang::abort(
      "Mutable environments, connections and external pointers cannot be fingerprinted. Use an explicit immutable specification.",
      class = c("dataraft_error_fingerprint", "dataraft_error")
    )
  }
  if (inherits(x, "dr_contract")) {
    masked <- union(masked, names(x$columns))
  }
  if (inherits(x, "dr_product")) {
    masked <- union(
      masked,
      c(
        names(x$contract$columns),
        unlist(
          lapply(x$sources, function(source) {
            if (is.data.frame(source)) names(source) else character()
          }),
          use.names = FALSE
        )
      )
    )
  }
  if (is.list(x)) {
    return(lapply(x, canonical, seen = seen, masked = masked))
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
