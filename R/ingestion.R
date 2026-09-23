#' Check a delivery and publish an accepted raw release
#'
#' Reads a delivery once, keeps its original file in immutable landing, and
#' evaluates its contract and quality checks before writing any raw table.
#' Rejected input leaves previous accepted releases available. A successful
#' result points to an immutable physical table in the `raw` schema, suitable
#' for a dbt source binding. Landing is evidence of receipt, not acceptance.
#'
#' Without a contract, the first accepted delivery establishes a structural
#' schema. Subsequent deliveries must match it. Required values, keys, owners
#' and freshness deadlines are optional and are never guessed. Additional
#' `quality` checks accept the same functions, formulas, named lists and
#' pointblank adapters as [dataraft.core::dr_add_quality()]. Business checks run at the input
#' gate; the stored candidate also undergoes structural validation.
#'
#' Ordinary tables, dataset directories and non-file adapters are archived as
#' RDS snapshots. Single local files retain their original bytes. A snapshot
#' records the received R values, not an API's original transport response.
#' Ingestion collects lazy adapters into memory for this gate and snapshot.
#' For data exceeding available memory, use a lazy product and database target.
#' Connections opened from a configuration are closed before returning;
#' [dataraft.core::dr_collect()] can reopen the exact accepted release when needed.
#' @param x Data frame, lazy table, local file path, zero-argument function,
#'   source adapter or product. A product must have one ordinary source and no
#'   transformations, lookups, target or catalogs. Its contract, quality rules,
#'   name and code version are retained. Use [dataraft.core::dr_publish()] for transformed products.
#' @param to Local lake folder, connected lake or [dr_lake_config()] with a `raw`
#'   layer. Defaults to a local `"dataraft"` folder, using the same configuration
#'   and backend marker as [dr_open_lake()].
#' @param name Optional asset name. Defaults to a file basename, source asset
#'   or ID, or the data variable name; expressions use `"data"`.
#' @param contract Optional contract, named type vector or prototype list.
#' @param quality Optional input checks accepted by [dataraft.core::dr_add_quality()].
#' @param reader Optional file reader. CSV, TSV, RDS and Excel have defaults.
#' @param execution Optional [dataraft.core::dr_execution_config()] defaults, overriding defaults
#'   stored on a product. Its layer must be
#'   `NULL` or `"raw"`; an explicit `to` overrides its destination.
#' @param ... Named execution options: `stop_on_failure` (default `TRUE`),
#'   `business_date`, `notify`, `code_version`, and `cache` (default `FALSE`).
#'   Reusing a cached raw release requires an explicit `code_version`; live
#'   reference checks always require `cache = FALSE`.
#' @returns A `dr_run_result`. Accepted results contain `asset`, `release_id`,
#'   `output_config`, and an `outputs` list with exact `database`, `schema`
#'   and `table` identifiers. Failures can be returned with
#'   `stop_on_failure = FALSE`; otherwise the error contains `condition$result`.
#' @seealso [dataraft.adapters::dr_source_database()], [dataraft.core::dr_pointblank_checks()], [dataraft.core::dr_quality()], [dataraft.core::dr_collect()]
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("raw-ingestion-")
#' config <- dr_lake_config(path = root)
#' orders <- data.frame(id = 1:2, amount = c(25, 75))
#' accepted <- orders |> dr_ingest(to = config, quality = ~ amount >= 0)
#' dataraft.core::dr_collect(accepted)
#' accepted$outputs
#' unlink(root, recursive = TRUE)
dr_ingest <- function(
  x,
  to = NULL,
  name = NULL,
  contract = NULL,
  quality = NULL,
  reader = NULL,
  execution = NULL,
  ...
) {
  expression <- substitute(x)
  execution <- dataraft.core::dr_internal_product_execution(x, execution)
  if (!is.null(execution$layer) && execution$layer != "raw") {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Ingestion requires execution layer = 'raw' or NULL."
    )
  }
  if (is.null(to)) {
    to <- execution$to %||% "dataraft"
    if (inherits(to, "dr_lake_target")) {
      if (length(to$partition_by)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Ingestion does not accept partitioned targets."
        )
      }
      to <- to$destination
    }
  }
  options <- ingestion_options(list(...))
  if (inherits(x, "dr_product")) {
    if (
      length(x$sources) != 1L ||
        inherits(x$sources[[1L]], "dr_product") ||
        length(x$transforms)
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Ingestion accepts one ordinary product source without transformations or lookups. Use dr_publish() for a transformed product."
      )
    }
    if (!is.null(x$target) || length(x$catalogs)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Ingestion uses to as its RAW destination. Remove product targets and catalogs before ingesting."
      )
    }
    if (!is.null(reader) || !is.null(contract)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Configure a product's reader and contract with dr_add_source() and dr_add_contract() before ingestion."
      )
    }
    if (!is.null(name) && !identical(name, x$id)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "A product supplies its own ingestion name. Omit name or use the product id."
      )
    }
    definition <- x
    name <- x$id
    options$code_version <- options$code_version %||% x$code_version
    definition$code_version <- options$code_version
  } else {
    name <- name %||% ingestion_name(x, expression)
    dataraft.core::dr_internal_asset_id(name)
    definition <- dataraft.core::dr_product(
      name,
      code_version = options$code_version
    ) |>
      dataraft.core::dr_add_source(x, reader = reader)
    if (!is.null(contract)) {
      definition <- dataraft.core::dr_add_contract(definition, contract)
    }
  }
  if (!is.null(quality)) {
    definition <- dataraft.core::dr_add_quality(definition, quality)
  }
  quality_defaults <- execution
  if (!is.null(quality_defaults)) {
    quality_defaults[c("to", "layer")] <- list(NULL, NULL)
    definition <- dataraft.core::dr_internal_apply_execution_defaults(
      definition,
      quality_defaults
    )
  }
  dataraft.core::dr_validate(definition)
  if (is.character(to)) {
    to <- dr_lake_config(path = to)
  }
  if (!inherits(to, c("dr_lake", "dr_config"))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "to must be a local folder, connected lake or dr_lake_config()."
    )
  }
  config <- if (inherits(to, "dr_config")) to else to$config
  if (isTRUE(config$read_only)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Ingestion requires a writable destination."
    )
  }
  if (!"raw" %in% config$layers) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Ingestion requires a configured raw layer."
    )
  }
  rules <- c(definition$contract$rules, definition$quality)
  if (options$cache && is.null(options$code_version)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Supply code_version before enabling ingestion cache, or use cache = FALSE."
    )
  }
  if (
    options$cache &&
      any(vapply(
        rules,
        function(rule) {
          isTRUE(rule$dynamic_reference) || isTRUE(rule$volatile)
        },
        logical(1)
      ))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Volatile or live reference checks require cache = FALSE."
    )
  }
  owned <- inherits(to, "dr_config")
  with_execution_lake(to, function(con) {
    assert_writable(con)
    acquire_lake_writer(con, environment(), "internal:catalog-writer")
    if (!"raw" %in% con$config$layers) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Ingestion requires a configured raw layer."
      )
    }
    previous <- tryCatch(
      resolve_release(con, name),
      dr_no_release = function(e) NULL
    )
    if (!is.null(previous) && previous$schema_name[[1]] != "raw") {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset already publishes outside raw. Use a distinct ingestion name."
      )
    }
    if (!is.null(definition$contract)) {
      dr_register(con, definition$contract)
    }
    description <- dataraft.core::dr_inspect(definition)
    if (
      options$cache &&
        any(vapply(
          description$sources,
          function(source) identical(source$fingerprintable, FALSE),
          logical(1)
        ))
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Dynamic source state cannot be fingerprinted; use cache = FALSE.",
        "dr_dynamic_source_cache"
      )
    }
    description$status <- NULL
    description$plan <- NULL
    description$sources <- lapply(description$sources, function(source) {
      source$rows <- NULL
      source
    })
    code <- options$code_version %||%
      paste0("automatic-", utils::packageVersion("dataraft.lake"))
    version <- paste0(
      "auto-",
      fingerprint(list(ingestion = description, code = code))
    )
    run_id <- new_run(
      con,
      paste0(name, ".ingest"),
      name,
      fingerprint(description),
      code
    )
    state <- new.env(parent = emptyenv())
    state$contract <- NULL
    state$contract_definition <- NULL
    state$reference <- NULL
    result <- tryCatch(
      {
        source <- definition$sources[[1]]
        if (
          inherits(source, "dr_parquet_source") &&
            !adapter_remote_path(source$path) &&
            !dir.exists(source$path)
        ) {
          parquet <- source
          source <- dataraft.core::dr_source_file(
            paste0(name, ".delivery"),
            parquet$path,
            reader = function(path) {
              parquet$path <- path
              dataraft.core::dr_collect(dataraft.core::dr_read_source(parquet))
            }
          )
        }
        if (!inherits(source, "dr_source")) {
          received <- dataraft.core::dr_internal_table_result(
            if (identical(class(source), "dr_release_source")) {
              read_release_source(source, con)
            } else {
              dataraft.core::dr_read_source(source)
            },
            "The ingestion source"
          )
          state$reference <- attr(received, "dr_input_reference")
          received <- dataraft.core::dr_collect(received)
          slot <- create_staging_slot(con, name, run_id)
          on.exit(unlink(slot, recursive = TRUE), add = TRUE)
          writeLines(
            jencode(writer_identity()),
            file.path(slot, "writer.json")
          )
          path <- file.path(slot, "delivery.rds")
          saveRDS(as.data.frame(received), path, compress = FALSE, version = 3)
          source <- dataraft.core::dr_source_file(
            paste0(name, ".delivery"),
            path,
            readRDS
          )
          attr(source, "dr_definition_path") <- file.path(
            con$config$landing,
            ".dataraft-staging",
            name,
            "delivery.rds"
          )
        }
        source$version <- version
        placeholder <- definition$contract %||%
          structure(
            list(
              id = paste0(name, ".schema"),
              version = "inferred",
              kind = "contract",
              owner = "",
              producer = "",
              columns = NULL,
              rules = definition$quality,
              automatic_schema = TRUE
            ),
            class = "dr_contract"
          )
        pipeline <- dr_pipeline(
          paste0(name, ".ingest"),
          con,
          version = version,
          code_version = code
        ) |>
          dr_step_land(source) |>
          dr_step_extract(into = "raw") |>
          dr_step_precheck(placeholder) |>
          dr_step_validate(placeholder) |>
          dr_step_publish(name, layer = "raw")
        pipeline$infer_input_contract <- TRUE
        pipeline$infer_contract <- TRUE
        pipeline$input_rules_only <- TRUE
        pipeline$ingestion <- description
        attr(pipeline, "dr_run_id") <- run_id
        if (!is.null(state$reference$asset)) {
          reference <- state$reference
          attr(pipeline, "dr_product_inputs") <- list(list(
            source = reference$asset,
            source_version = reference$release_id %||% reference$run_id,
            fingerprint = reference$hash %||%
              reference$release_id %||%
              reference$run_id,
            original_name = "",
            landed_path = "",
            received_at = now()
          ))
        }
        attr(pipeline, "dr_resolve_input_contract") <- function(data) {
          state$contract <- if (is.null(definition$contract)) {
            resolve_product_contract(con, definition, data)
          } else {
            dataraft.core::dr_internal_product_contract(definition, data)
          }
          state$contract_definition <- canonical(state$contract)
          state$contract
        }
        attr(pipeline, "dr_resolve_contract") <- function(data) state$contract
        dataraft.core::dr_run(
          pipeline,
          con,
          business_date = options$business_date,
          notify = options$notify,
          cache = if (options$cache) "current" else FALSE,
          stop_on_failure = FALSE
        )
      },
      error = function(e) {
        finish_run(
          con,
          run_id,
          "error",
          "Input acquisition or validation failed."
        )
        out <- dataraft.core::dr_internal_run_result(run_id, "error")
        out$error <- e
        out
      }
    )
    row <- dataraft.core::dr_internal_metadata_filter(
      con,
      "runs",
      run_id = result$run_id
    )
    result$asset <- name
    result$started_at <- row$started_at[[1]]
    result$finished_at <- row$finished_at[[1]]
    result$backend <- con$config$backend
    result$inputs <- dataraft.core::dr_internal_metadata_filter(
      con,
      "inputs",
      run_id = result$run_id
    )
    result$output_config <- con$config
    if (!owned) {
      result$output_lake <- con
    }
    result$metadata <- list(
      product = name,
      run_id = result$run_id,
      status = result$status,
      contract = state$contract_definition,
      definition = description,
      backend = con$config$backend
    )
    if (result$status %in% c("published", "cached")) {
      release <- resolve_release(con, name, result$release_id)
      result$outputs <- list(
        type = "lake release",
        database = "lake",
        schema = release$schema_name[[1]],
        table = release$table_name[[1]],
        asset = name,
        release_id = result$release_id
      )
      if (is.null(result$metadata$contract)) {
        reference <- strsplit(release$contract[[1]], "@", fixed = TRUE)[[1]]
        stored <- query(
          con,
          paste(
            "SELECT definition FROM",
            meta(con, "assets"),
            "WHERE kind = 'contract' AND id = ? AND version = ?"
          ),
          as.list(reference)
        )
        if (nrow(stored) != 1L) {
          dataraft.core::dr_internal_abort(
            subclass = "dataraft_error_lake",
            "Published contract metadata is missing."
          )
        }
        result$metadata$contract <- jdecode(stored$definition[[
          1
        ]])
      }
      if (is.null(result$quality)) {
        result$quality <- dataraft.core::dr_quality(con, run_id = result$run_id)
      }
      result$metadata$schema <- dataraft.core::dr_internal_infer_column_types(dr_tbl(
        con,
        name,
        result$release_id
      ))
      result$metadata$rows <- count_rows(dr_tbl(
        con,
        name,
        result$release_id
      ))
    }
    result$lifecycle <- tibble::tibble(
      state = c("defined", "validated", "running", result$status),
      at = c(
        NA_character_,
        result$started_at,
        result$started_at,
        result$finished_at
      )
    )
    if (
      options$stop_on_failure && !result$status %in% c("published", "cached")
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        paste0(
          "Ingestion `",
          name,
          "` ended with ",
          result$status,
          ". Inspect condition$result for input evidence."
        ),
        "dr_run_failed",
        result = result,
        parent = result$error
      )
    }
    result
  })
}


ingestion_options <- function(options) {
  rlang::local_error_call(rlang::caller_env())
  defaults <- list(
    stop_on_failure = TRUE,
    business_date = NA_character_,
    notify = NULL,
    code_version = NULL,
    cache = FALSE
  )
  if (
    length(options) &&
      (is.null(names(options)) ||
        anyNA(names(options)) ||
        any(!nzchar(names(options))) ||
        anyDuplicated(names(options)))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Ingestion options must be named uniquely."
    )
  }
  unknown <- setdiff(names(options), names(defaults))
  if (length(unknown)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      paste("Unknown ingestion option:", paste(unknown, collapse = ", "))
    )
  }
  defaults[names(options)] <- options
  dataraft.core::dr_internal_flag(defaults$stop_on_failure, "stop_on_failure")
  dataraft.core::dr_internal_flag(defaults$cache, "cache")
  if (
    length(defaults$business_date) != 1L ||
      !inherits(defaults$business_date, c("Date", "character"))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "business_date must be one Date or character value."
    )
  }
  if (!is.null(defaults$code_version)) {
    dataraft.core::dr_internal_scalar(defaults$code_version, "code_version")
  }
  if (!is.null(defaults$notify) && !is.function(defaults$notify)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "notify must be a function."
    )
  }
  defaults
}


ingestion_name <- function(data, expression) {
  rlang::local_error_call(rlang::caller_env())
  value <- if (is.character(data) && length(data) == 1L && !is.na(data)) {
    tools::file_path_sans_ext(basename(data))
  } else if (inherits(data, "dr_source")) {
    tools::file_path_sans_ext(basename(data$path))
  } else if (inherits(data, "dr_release_source")) {
    data$asset
  } else if (is.symbol(expression)) {
    as.character(expression)
  } else {
    "data"
  }
  value <- make.names(value)
  if (!grepl("^[A-Za-z]", value)) {
    value <- paste0("data", value)
  }
  value
}
