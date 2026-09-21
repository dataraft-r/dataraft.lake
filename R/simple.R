#' Open a local lake with sensible defaults
#'
#' Creates or reopens a self-contained local folder. DuckDB is the default and
#' needs no extension download or external service. Use `backend = "ducklake"`
#' for an actual DuckLake. Backend and ordered layers (including named roles)
#' are remembered in `dataraft.json`; reopening never silently switches them.
#' A new lake needs an empty or nonexistent folder. Existing lakes made with
#' custom remote or split-location configuration still open through
#' [dr_connect_lake()]. Use [dr_lake_config()] for a connection-free definition.
#'
#' Read-only opens never update the folder configuration.
#' @param path Local folder, created if needed. Defaults to `"dataraft"` in
#'   the working directory.
#' @param backend Optional `"duckdb"` or `"ducklake"`. DuckLake requires its
#'   DuckDB extension. Defaults to the saved choice, or DuckDB for a new folder.
#' @param read_only Open an existing lake without registry or data writes.
#' @param layers Layer names, optionally named by role. For a new folder the
#'   default is `c("raw", "validated", "products")`. Omit on subsequent opens
#'   to reuse saved layers. Explicit conflicting settings are rejected.
#' @param install_extensions Allow installation of required DuckDB extensions.
#'   Use `FALSE` when the required extensions are already installed.
#' @param lake Connected lake to close.
#' @returns `dr_open_lake()` returns a connected `dr_lake`. `dr_close_lake()` invisibly
#'   returns `TRUE`; it is an alias for [dr_disconnect_lake()].
#' @seealso [dr_write_data()], [dr_read_release()]
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_open_lake(root)
#' dr_write_data(lake, data.frame(id = 1:2), "orders")
#' dr_read_release(lake, "orders")
#' dr_close_lake(lake)
#' lake <- dr_open_lake(root) # reopens the same data
#' dr_read_release(lake, "orders")
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_open_lake <- function(
  path = "dataraft",
  backend = NULL,
  read_only = FALSE,
  layers = NULL,
  install_extensions = TRUE
) {
  if (inherits(path, "dr_config")) {
    if (missing(read_only)) {
      read_only <- path$read_only
    }
    if (!missing(install_extensions)) {
      path$install_extensions <- dataraft.core::dr_internal_flag(
        install_extensions,
        "install_extensions"
      )
    }
    if (!is.null(backend) || !is.null(layers)) {
      dataraft.core::dr_internal_abort(
        "Set backend and layers in the supplied configuration.",
        subclass = "dataraft_error_lake"
      )
    }
    return(dr_connect_lake(path, read_only = read_only))
  }
  dataraft.core::dr_internal_flag(read_only, "read_only")
  path <- dataraft.core::dr_internal_absolute_path(path)
  if (read_only && !file.exists(file.path(path, "dataraft.json"))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "A read-only lake must already exist."
    )
  }
  args <- list(
    path = path,
    read_only = read_only,
    install_extensions = install_extensions
  )
  if (!is.null(backend)) {
    args$backend <- backend
  }
  if (!is.null(layers)) {
    args$layers <- layers
  }
  dr_connect_lake(do.call(dr_lake_config, args))
}


#' @rdname dr_open_lake
#' @export
dr_close_lake <- function(lake) dr_disconnect_lake(lake)


#' Write data with optional configuration
#'
#' Starts the normal landing, validation and publication workflow. Without a
#' contract, the first successful write establishes a structural schema. Later
#' writes must match that schema. Automatic numeric columns accept integers
#' and decimals; explicit integer contracts remain strict. Missing values are allowed; empty tables are
#' blocked. No keys, business rules, owners or freshness deadlines are guessed.
#'
#' Supply `contract` whenever you need business checks or an intentional schema
#' change. Once an execution attempt uses an explicit contract, subsequent writes
#' must supply one too, even if that attempt was blocked. This prevents
#' accidentally dropping its rules. Draft contracts
#' still require [dataraft.core::dr_contract_confirm()].
#'
#' Data frames are archived as RDS snapshots. File inputs preserve their original
#' bytes before parsing. CSV, TSV and RDS have native readers; Excel uses
#' optional readxl. CSV/TSV use
#' base R type inference; use `reader` for specific parsing requirements. The
#' first file may be parsed twice to establish and validate its schema. Readers
#' must be deterministic and must not modify their input.
#'
#' Definition versions are derived automatically. Use [dataraft.core::dr_product()] and [dataraft.core::dr_run()]
#' when composing transformations or controlling definition versions. Built-in
#' readers and structural checks can reuse the current release. Custom readers
#' or rules run again by default because captured values and external state
#' cannot be fingerprinted reliably. Supply `code_version` to enable reuse and
#' update it whenever code, dependencies or captured values change.
#' @param lake Connected lake or [dr_lake_config()].
#' @param data Data frame, path to a local file, or a zero-argument function
#'   returning a data frame. A source function is called once per write before
#'   cache lookup; its returned data is archived as RDS. Use it to connect
#'   existing API or database clients. It requires `name` and does not archive
#'   the original transport response.
#' @param name Optional asset name. Defaults to the data frame's variable name
#'   or the file name without its extension. Expressions need an explicit name.
#'   Names start with a letter and use letters, digits, underscores or dots.
#' @param contract Optional publication contract. Omit for structural checks.
#' @param reader Optional file reader taking a path and returning a data frame.
#' @param code_version Optional code and dependency version for cache reuse.
#' @param input_contract Optional separate gate before writing Raw.
#' @param cache Optional reuse policy. Defaults to safe reuse for built-in
#'   readers and structural checks; custom callbacks need `code_version` to
#'   enable reuse. `FALSE` always runs validation again.
#' @param partition_by Optional column names identifying complete partitions
#'   to replace. Omit to replace the whole asset. Every row of each supplied
#'   partition replaces that partition; other partitions remain published.
#' @param ... Publication options, such as `business_date`,
#'   `notify`, `layer` and `stop_on_failure`.
#' @returns A `dr_run_result` with status, release ID and quality results.
#'   Successful results retain their exact release and connection configuration
#'   for [dataraft.core::dr_collect()], [dataraft.core::dr_product()], [dataraft.core::dr_step_lookup()] and [dataraft.metrics::dr_measure()], including
#'   after the original connection closes. Caller-owned connections remain open.
#'   [dataraft.core::dr_quality()] explains a failure.
#' @seealso [dr_open_lake()], [dr_read_release()], [dataraft.core::dr_product()], [dataraft.core::dr_run()]
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_open_lake(root)
#' orders <- data.frame(id = 1:3, amount = c(25, 75, 50))
#' dr_write_data(lake, orders)
#' dr_read_release(lake, "orders")
#' contract <- dataraft.core::dr_contract("orders.checked",
#'   columns = c(id = "integer", amount = "numeric"), key = "id")
#' dr_write_data(lake, orders, contract = contract)
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_write_data <- function(
  lake,
  data,
  name = NULL,
  contract = NULL,
  reader = NULL,
  code_version = NULL,
  input_contract = NULL,
  cache = NULL,
  partition_by = character(),
  ...
) {
  invisible(lapply(partition_by, dataraft.core::dr_internal_column_name))
  expression <- substitute(data)
  owned <- inherits(lake, "dr_config")
  if (is.function(data)) {
    if (is.null(name)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Supply name when writing from a source function."
      )
    }
    fetch <- data
    return(with_execution_lake(lake, function(con) {
      assert_writable(con)
      received <- fetch()
      if (!is.data.frame(received)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "A source function must return a data frame."
        )
      }
      result <- dr_write_data(
        con,
        received,
        name,
        contract = contract,
        code_version = code_version,
        input_contract = input_contract,
        cache = cache,
        partition_by = partition_by,
        ...
      )
      if (owned) {
        result$output_lake <- NULL
      }
      result
    }))
  }
  file_input <- is.character(data) && length(data) == 1L && !is.na(data)
  if (!is.data.frame(data) && !file_input) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "data must be a data frame or a local file path."
    )
  }
  if (is.null(name)) {
    name <- if (file_input) {
      tools::file_path_sans_ext(basename(data))
    } else if (is.symbol(expression)) {
      as.character(expression)
    } else {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Supply name when writing a data frame expression, for example name = 'orders'."
      )
    }
  }
  if (
    !is.character(name) ||
      length(name) != 1L ||
      is.na(name) ||
      !grepl("^[A-Za-z][A-Za-z0-9_.]*$", name)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Supply name starting with a letter and using letters, digits, underscores or dots."
    )
  }
  custom_reader <- !is.null(reader)
  if (custom_reader && (!file_input || !is.function(reader))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "reader must be a function and is only used with a file path."
    )
  }
  if (file_input && is.null(reader)) {
    reader <- dataraft.core::dr_internal_simple_reader(data)
  }
  for (value in list(contract, input_contract)) {
    if (!is.null(value)) {
      if (!inherits(value, "dr_contract")) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Use dr_contract() for contracts."
        )
      }
      dataraft.core::dr_internal_assert_contract_ready(value)
    }
  }
  if (!is.null(code_version)) {
    dataraft.core::dr_internal_scalar(code_version, "code_version")
  }
  with_execution_lake(lake, function(con) {
    assert_writable(con)
    assert_table_asset(con, name)
    if (is.null(contract)) {
      contract <- published_schema(con, name)
    }
    source <- NULL
    if (file_input) {
      source <- dataraft.core::dr_source_file(
        paste0(name, ".file"),
        data,
        reader
      )
      landed <- land_source(con, source)
      source$original_path <- source$path
      source$path <- landed$path
      if (is.null(contract)) {
        prototype <- reader(landed$path)
        if (
          !identical(
            digest::digest(
              file = landed$path,
              algo = "sha256",
              serialize = FALSE
            ),
            landed$hash
          )
        ) {
          dataraft.core::dr_internal_abort(
            subclass = "dataraft_error_lake",
            "Reader modified immutable landing input."
          )
        }
        contract <- dataraft.core::dr_internal_automatic_schema(
          name,
          dataraft.core::dr_internal_automatic_types(dataraft.core::dr_internal_infer_column_types(
            prototype
          ))
        )
      }
    } else if (is.null(contract)) {
      contract <- dataraft.core::dr_internal_automatic_schema(
        name,
        dataraft.core::dr_internal_automatic_types(dataraft.core::dr_internal_infer_column_types(
          data
        ))
      )
    }
    callbacks <- custom_reader ||
      length(contract$rules) > 0L ||
      length(input_contract$rules) > 0L
    cache <- cache %||% (!callbacks || !is.null(code_version))
    dataraft.core::dr_internal_flag(cache, "cache")
    if (cache && callbacks && is.null(code_version)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Supply code_version to cache custom readers or rules, or leave cache unset."
      )
    }
    runtime <- list(
      package = as.character(utils::packageVersion("dataraft.lake")),
      R = as.character(getRversion()),
      duckdb = as.character(utils::packageVersion("duckdb"))
    )
    code_version <- code_version %||%
      paste0("auto-", fingerprint(runtime))
    version <- paste0(
      "auto-",
      fingerprint(list(
        contract = contract,
        input_contract = input_contract,
        source = source,
        code_version = code_version,
        partition_by = partition_by
      ))
    )
    result <- if (file_input) {
      source$version <- version
      pipeline_ingest(
        con,
        source,
        contract,
        name,
        version = version,
        code_version = code_version,
        input_contract = input_contract,
        cache = if (cache) "current" else FALSE,
        partition_by = partition_by,
        ...
      )
    } else {
      dr_ingest_data(
        con,
        data,
        contract,
        name,
        code_version = code_version,
        version = version,
        input_contract = input_contract,
        cache = if (cache) "current" else FALSE,
        partition_by = partition_by,
        ...
      )
    }
    if (result$status %in% c("published", "cached")) {
      release <- resolve_release(con, name, result$release_id)
      result$asset <- name
      result$output_config <- con$config
      if (!owned) {
        result$output_lake <- con
      }
      result$outputs <- list(
        type = "lake release",
        database = "lake",
        schema = release$schema_name[[1]],
        table = release$table_name[[1]],
        asset = name,
        release_id = result$release_id
      )
    }
    result
  })
}


published_schema <- function(lake, name) {
  rlang::local_error_call(rlang::caller_env())
  definitions <- query(
    lake,
    paste(
      "SELECT DISTINCT a.definition FROM",
      meta(lake, "assets"),
      "a JOIN",
      meta(lake, "runs"),
      "r ON a.fingerprint = r.definition_hash",
      "AND a.id = r.pipeline WHERE a.kind = 'pipeline' AND r.asset = ?"
    ),
    list(name)
  )
  for (definition in definitions$definition) {
    contract <- jdecode(definition)$steps$validate
    if (isTRUE(contract$automatic_schema) && length(contract$rules)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset has explicit quality rules. Use its composed product to keep those checks active."
      )
    }
    if (!is.null(contract) && !isTRUE(contract$automatic_schema)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset uses an explicit contract. Supply contract to keep its checks active."
      )
    }
  }
  release <- tryCatch(resolve_release(lake, name), dr_no_release = function(e) {
    NULL
  })
  if (is.null(release)) {
    return(NULL)
  }
  reference <- strsplit(release$contract[[1]], "@", fixed = TRUE)[[1]]
  record <- query(
    lake,
    paste(
      "SELECT definition, fingerprint FROM",
      meta(lake, "assets"),
      "WHERE kind = 'contract' AND id = ? AND version = ?"
    ),
    as.list(reference)
  )
  if (nrow(record) != 1L) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Published contract metadata is missing."
    )
  }
  definition <- jdecode(record$definition[[1]])
  if (!isTRUE(definition$automatic_schema)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "This asset uses an explicit contract. Supply contract to keep its checks active."
    )
  }
  contract <- dataraft.core::dr_internal_automatic_schema(
    name,
    unlist(definition$columns, use.names = TRUE)
  )
  if (!identical(fingerprint(contract), record$fingerprint[[1]])) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Automatic schema metadata does not match its registered definition."
    )
  }
  dataraft.core::dr_internal_automatic_schema(
    name,
    dataraft.core::dr_internal_automatic_types(unlist(contract$columns))
  )
}


#' Read a published asset
#'
#' Returns the latest successfully published data as a tibble. Failed writes
#' leave that release intact. Set `lazy = TRUE` to filter or aggregate in the
#' database before collecting large data; keep the connection open while using
#' a lazy table. Use `release` to read an exact historical version.
#' @param lake Connected lake.
#' @param name Published asset name.
#' @param release Optional release ID. Defaults to the latest release.
#' @param lazy Return a lazy database table instead of collecting all rows.
#' @returns A tibble, a lazy `tbl_sql` when `lazy = TRUE`, or a dm for a
#'   model product. Models restore all member tables from the same manifest.
#' @seealso [dr_write_data()], [dr_tbl()], [dr_releases()]
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-")
#' lake <- dr_open_lake(root)
#' dr_write_data(lake, data.frame(id = 1:3), "orders")
#' dr_read_release(lake, "orders")
#' dr_read_release(lake, "orders", lazy = TRUE) |>
#'   dplyr::filter(id > 1) |>
#'   dplyr::collect()
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_read_release <- function(lake, name, release = NULL, lazy = FALSE) {
  dataraft.core::dr_internal_flag(lazy, "lazy")
  ref <- resolve_release(lake, name, release)
  if (startsWith(ref$table_name[[1]], "model_")) {
    if (lazy) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Model reads return a dm of collected tables; select a member for lazy queries."
      )
    }
    return(read_model_release(lake, name, ref$release_id[[1]]))
  }
  data <- dr_tbl(lake, name, release)
  if (lazy) data else dplyr::collect(data)
}
