#' Choose governed lake storage for a composed product
#'
#' A folder path opens local DuckDB storage. Pass [dr_lake_config()] for DuckLake,
#' S3 or PostgreSQL catalog configuration, or reuse an open lake. Connections
#' supplied by the caller remain caller-owned. The adapter compiles to the
#' existing immutable landing, candidate and publication transaction.
#' @param destination Folder path, connected lake, or lake configuration.
#' @param partition_by Optional columns identifying complete partitions to
#'   replace. Retained partitions are included in the final quality gate.
#' @param layer Publication layer in the lake configuration.
#' @returns A target accepted by [dataraft.core::dr_set_target()] or [dataraft.core::dr_publish()].
#' @export
#' @examples
#' dataraft.core::dr_product("orders") |>
#'   dataraft.core::dr_add_source(data.frame(id = 1:2)) |>
#'   dataraft.core::dr_set_target(dr_target_lake("reporting-lake"))
dr_target_lake <- function(
  destination = "dataraft",
  partition_by = character(),
  layer = "validated"
) {
  if (is.character(destination)) {
    destination <- dataraft.core::dr_internal_absolute_path(destination)
  }
  if (
    !is.character(destination) &&
      !inherits(destination, c("dr_lake", "dr_config"))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "destination must be a folder, dr_lake_config() or an open lake."
    )
  }
  invisible(lapply(partition_by, dataraft.core::dr_internal_column_name))
  if (anyDuplicated(partition_by)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Partition columns must be unique."
    )
  }
  dataraft.core::dr_internal_ident(layer)
  structure(
    list(destination = destination, partition_by = partition_by, layer = layer),
    class = "dr_lake_target"
  )
}


#' @export
#' @importFrom dataraft.core dr_inspect
dr_inspect.dr_lake_target <- function(x, ...) {
  config <- if (inherits(x$destination, "dr_lake")) {
    x$destination$config
  } else {
    x$destination
  }
  list(
    type = if (inherits(config, "dr_config")) config$backend else "local lake",
    layer = x$layer,
    partition_by = x$partition_by
  )
}

#' @export
#' @importFrom dataraft.core dr_check_component
dr_check_component.dr_lake_target <- function(x, ...) {
  dataraft.core::dr_internal_need("duckdb")
  if (utils::packageVersion("duckdb") < "1.5.5") {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Lake storage requires duckdb >= 1.5.5."
    )
  }
  if (inherits(x$destination, "dr_lake")) {
    assert_writable(x$destination)
  }
  config <- if (inherits(x$destination, "dr_lake")) {
    x$destination$config
  } else {
    x$destination
  }
  if (inherits(config, "dr_config")) {
    if (isTRUE(config$read_only)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "The publication target is read-only.",
        "dr_read_only"
      )
    }
    if (!all(c("raw", x$layer) %in% config$layers)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "The target configuration must include raw and the publication layer."
      )
    }
  }
  invisible(x)
}


#' @export
#' @noRd
#' @importFrom dataraft.core dr_execute_target
dr_execute_target.dr_lake_target <- function(
  target,
  product,
  business_date = NA_character_,
  notify = NULL,
  cache = FALSE,
  previous = NULL,
  ...
) {
  rlang::check_dots_empty()
  dataraft.core::dr_internal_flag(cache, "cache")
  if (cache && is.null(product$code_version)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Supply code_version in dr_product() before enabling cache; callbacks are re-evaluated by default."
    )
  }
  rules <- c(product$contract$rules, product$quality)
  if (
    cache &&
      any(vapply(
        rules,
        function(rule) isTRUE(rule$dynamic_reference),
        logical(1)
      ))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Live reference checks cannot reuse cached releases. Use cache = FALSE."
    )
  }
  lake <- target$destination
  own <- !inherits(lake, "dr_lake")
  if (own) {
    lake <- if (inherits(lake, "dr_config")) {
      dr_connect_lake(lake)
    } else {
      dr_open_lake(lake)
    }
    on.exit(dr_close_lake(lake), add = TRUE)
  }
  assert_writable(lake)
  check_previous_release(lake, product$id, previous)
  assert_table_asset(lake, product$id)
  definition <- dataraft.core::dr_inspect(product)
  if (
    cache &&
      any(vapply(
        definition$sources,
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
  definition$status <- NULL
  definition$target <- NULL
  definition$publication <- list(
    layer = target$layer,
    partition_by = target$partition_by
  )
  definition$sources <- lapply(definition$sources, function(x) {
    x$rows <- NULL
    x
  })
  version <- if (product$automatic_version) {
    paste0("auto-", fingerprint(definition))
  } else {
    product$version
  }
  code <- product$code_version %||% "unversioned-no-cache"
  record <- c(list(kind = "composed_product"), definition)
  record$version <- version
  dr_register(lake, record)
  if (!is.null(product$contract)) {
    dr_register(lake, product$contract)
  }
  run_id <- new_run(
    lake,
    paste0(product$id, ".compose"),
    product$id,
    fingerprint(record),
    code
  )
  acquired <- NULL
  extra_inputs <- list()
  record_input <- function(name, input) {
    rlang::local_error_call(rlang::caller_env())
    extra_inputs[[name]] <<- input
    insert_meta(
      lake,
      "inputs",
      c(
        list(run_id = run_id),
        input,
        list(business_date = as.character(business_date))
      )
    )
  }
  column_lineage <- list(
    complete = FALSE,
    fields = list(),
    reason = "Deferred or multiple lake inputs"
  )
  result <- tryCatch(
    {
      single <- length(product$sources) == 1L
      source <- if (single) product$sources[[1]] else NULL
      transforms <- product$transforms
      transform_metadata <- list()
      auxiliary <- any(vapply(
        transforms,
        function(step) {
          length(dataraft.core::dr_internal_component_sources(step)) > 0L
        },
        logical(1)
      ))
      if (!single || !inherits(source, "dr_source") || auxiliary) {
        acquired <- dataraft.core::dr_internal_read_product_sources(
          product,
          lake = lake,
          on_input = record_input
        )
        data <- acquired$data
        if (single && !auxiliary && !length(target$partition_by)) {
          recipe <- dataraft.core::dr_recipe()
          recipe$steps <- transforms
          column_lineage <- dataraft.core::dr_column_lineage(
            recipe,
            colnames(data)
          )
        }
        # Multiple inputs must be combined before they enter one publication.
        # The lake adapter is an explicit materialization boundary; native and
        # database targets preserve lazy tables through their transformations.
        if (!single || auxiliary) {
          for (name in names(transforms)) {
            data <- dataraft.core::dr_internal_apply_product_transform(
              transforms[[name]],
              data,
              name,
              sources = acquired$transform_sources[[name]]
            )
            details <- attr(data, "dr_transform_metadata")
            if (!is.null(details)) {
              transform_metadata[[name]] <- details
            }
            attr(data, "dr_transform_metadata") <- NULL
          }
          data <- dataraft.core::dr_internal_table_result(
            data,
            "The final transformation"
          )
          transforms <- list()
        }
        data <- dataraft.core::dr_collect(dataraft.core::dr_internal_table_result(
          data,
          "The source"
        ))
        parent <- file.path(lake$config$landing, ".dataraft-staging")
        dir.create(parent, recursive = TRUE, showWarnings = FALSE)
        slot <- file.path(parent, product$id)
        if (!dir.create(slot, showWarnings = FALSE)) {
          dataraft.core::dr_internal_abort(
            subclass = "dataraft_error_lake",
            "Staging already exists for this asset. Check for a live or interrupted ingest before removing it."
          )
        }
        on.exit(unlink(slot, recursive = TRUE), add = TRUE)
        writeLines(
          jencode(writer_identity()),
          file.path(slot, "writer.json")
        )
        path <- file.path(slot, "delivery.rds")
        saveRDS(as.data.frame(data), path, compress = FALSE, version = 3)
        source <- dataraft.core::dr_source_file(
          paste0(product$id, ".source"),
          path,
          readRDS,
          version = version
        )
      } else {
        source$version <- version
      }
      contract <- if (is.null(product$contract)) {
        structure(
          list(
            id = paste0(product$id, ".schema"),
            version = "inferred",
            kind = "contract",
            owner = "",
            producer = "",
            columns = NULL,
            rules = product$quality,
            automatic_schema = TRUE
          ),
          class = "dr_contract"
        )
      } else {
        dataraft.core::dr_internal_combine_quality(
          dataraft.core::dr_internal_effective_product_contract(product),
          product$quality
        )
      }
      pipeline <- dr_pipeline(
        paste0(product$id, ".compose"),
        lake,
        version = version,
        code_version = code
      ) |>
        dr_step_land(source) |>
        dr_step_extract()
      for (name in names(transforms)) {
        step <- local({
          implementation <- transforms[[name]]
          label <- name
          inputs <- acquired$transform_sources[[name]] %||% list()
          function(data) {
            out <- dataraft.core::dr_internal_apply_product_transform(
              implementation,
              dataraft.core::dr_collect(data),
              label,
              sources = inputs
            )
            details <- attr(out, "dr_transform_metadata")
            if (!is.null(details)) {
              transform_metadata[[label]] <<- details
            }
            attr(out, "dr_transform_metadata") <- NULL
            out
          }
        })
        pipeline <- pipeline_step_transform(pipeline, step, name)
      }
      pipeline <- pipeline |>
        dr_step_validate(contract) |>
        dr_step_publish(
          product$id,
          mode = if (length(target$partition_by)) {
            "replace_partition"
          } else {
            "replace"
          },
          partition_by = target$partition_by,
          layer = target$layer
        )
      attr(pipeline, "dr_previous_release") <- previous
      attr(pipeline, "dr_product_inputs") <- extra_inputs
      attr(pipeline, "dr_run_id") <- run_id
      attr(pipeline, "dr_inputs_recorded") <- TRUE
      pipeline$composition <- definition
      pipeline$infer_contract <- is.null(product$contract)
      # Runtime-only closure; excluded from persisted specification and fingerprints.
      attr(pipeline, "dr_resolve_contract") <- function(data) {
        resolve_product_contract(lake, product, data)
      }
      dataraft.core::dr_run(
        pipeline,
        lake,
        business_date = business_date,
        notify = notify,
        cache = if (cache) "current" else FALSE,
        stop_on_failure = FALSE
      )
    },
    error = function(e) {
      status <- if (inherits(e, "dr_missing_delivery")) "missing" else "error"
      finish_run(
        lake,
        run_id,
        status,
        "Product preparation or execution failed."
      )
      emit_event(
        lake,
        run_id,
        product$id,
        "run_error",
        product$contract$producer %||% "",
        "Product execution failed; inspect source availability and transformations.",
        notify
      )
      result <- dataraft.core::dr_internal_run_result(run_id, status)
      result$error <- e
      result
    }
  )
  run <- dr_registry(lake, "runs")
  run <- run[run$run_id == result$run_id, ]
  result$started_at <- run$started_at[[1]]
  result$finished_at <- run$finished_at[[1]]
  result$backend <- lake$config$backend
  result$output_config <- lake$config
  if (!own) {
    result$output_lake <- lake
  }
  result$asset <- product$id
  result$inputs <- dataraft.core::dr_internal_metadata_filter(
    lake,
    "inputs",
    run_id = result$run_id
  )
  if (!is.null(acquired)) {
    result$source_inputs <- acquired$inputs
  }
  if (result$status %in% c("published", "cached")) {
    if (is.null(result$quality)) {
      result$quality <- dataraft.core::dr_quality(lake, run_id = result$run_id)
    }
    table <- dr_tbl(lake, product$id, result$release_id)
    result$outputs <- list(asset = product$id, release_id = result$release_id)
    ref <- resolve_release(lake, product$id, result$release_id)
    result$outputs$schema <- ref$schema_name[[1]]
    result$outputs$table <- ref$table_name[[1]]
    if (identical(lake$config$backend, "duckdb")) {
      result$outputs$dataset <- list(
        namespace = paste0("duckdb://", lake$config$catalog$path),
        name = paste(
          "lake",
          ref$schema_name[[1]],
          ref$table_name[[1]],
          sep = "."
        )
      )
    }
    result$metadata <- list(
      column_lineage = column_lineage,
      transformations = transform_metadata,
      schema = dataraft.core::dr_internal_infer_column_types(table),
      rows = count_rows(table),
      lineage = dataraft.core::dr_internal_metadata_filter(
        lake,
        "lineage_edges",
        run_id = result$run_id
      )
    )
  }
  result
}


resolve_product_contract <- function(lake, product, data) {
  rlang::local_error_call(rlang::caller_env())
  definitions <- query(
    lake,
    paste(
      "SELECT DISTINCT a.definition FROM",
      meta(lake, "assets"),
      "a JOIN",
      meta(lake, "runs"),
      "r ON a.fingerprint = r.definition_hash AND a.id = r.pipeline",
      "WHERE a.kind = 'pipeline' AND r.asset = ?"
    ),
    list(product$id)
  )
  for (json in definitions$definition) {
    definition <- jdecode(json)
    prior <- definition$steps$validate
    if (!isTRUE(prior$automatic_schema)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset uses an explicit contract. Add it with dr_add_contract() to keep its checks active."
      )
    }
    old_rules <- vapply(prior$rules, `[[`, character(1), "name")
    current_rules <- vapply(product$quality, `[[`, character(1), "name")
    if (!all(old_rules %in% current_rules)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset has quality rules. Keep them in dr_add_quality() or use an explicit, versioned contract change."
      )
    }
  }
  release <- tryCatch(
    resolve_release(lake, product$id),
    dr_no_release = function(e) NULL
  )
  columns <- dataraft.core::dr_internal_automatic_types(dataraft.core::dr_internal_infer_column_types(
    data
  ))
  if (!is.null(release)) {
    ref <- strsplit(release$contract[[1]], "@", fixed = TRUE)[[1]]
    previous <- query(
      lake,
      paste(
        "SELECT definition, fingerprint FROM",
        meta(lake, "assets"),
        "WHERE kind = 'contract' AND id = ? AND version = ?"
      ),
      as.list(ref)
    )
    if (nrow(previous) != 1L) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Published contract metadata is missing."
      )
    }
    saved <- jdecode(previous$definition[[1]])
    if (!identical(fingerprint(saved), previous$fingerprint[[1]])) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Published contract metadata does not match its registered definition."
      )
    }
    if (!isTRUE(saved$automatic_schema)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This asset uses an explicit contract. Add it with dr_add_contract()."
      )
    }
    columns <- dataraft.core::dr_internal_automatic_types(unlist(
      saved$columns,
      use.names = TRUE
    ))
  }
  dataraft.core::dr_internal_combine_quality(
    dataraft.core::dr_internal_automatic_schema(product$id, columns),
    product$quality
  )
}
