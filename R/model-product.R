#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name with_model_lake

with_model_lake <- function(x, fn) {
  rlang::local_error_call(rlang::caller_env())
  lake <- x$output_lake
  if (is.null(lake) || !DBI::dbIsValid(lake$con)) {
    lake <- dr_connect_lake(x$output_config, read_only = TRUE)
    on.exit(dr_close_lake(lake), add = TRUE)
  }
  fn(lake)
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name publish_model_result

publish_model_result <- function(x, result, previous) {
  rlang::local_error_call(rlang::caller_env())
  target <- x$target
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
  check_previous_release(lake, x$id, previous)
  prior <- tryCatch(resolve_release(lake, x$id), dr_no_release = function(e) {
    NULL
  })
  if (!is.null(prior) && !startsWith(prior$table_name[[1]], "model_")) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "This name already publishes a table. Choose a distinct model product name."
    )
  }
  tables <- as.list(result$data)
  layer <- target$layer
  if (!layer %in% lake$config$layers) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Model target layer is missing from the lake."
    )
  }
  definition <- dataraft.core::dr_inspect(x)
  definition$target <- NULL
  definition$version <- if (x$automatic_version) {
    paste0("auto-", dataraft.core::fingerprint(definition))
  } else {
    x$version
  }
  dr_register(lake, definition)
  run <- new_run(
    lake,
    x$id,
    x$id,
    dataraft.core::fingerprint(definition),
    x$code_version %||% "unversioned-no-cache"
  )
  release <- dataraft.core::uid()
  members <- list()
  success <- FALSE
  on.exit(
    if (!success) {
      finish_run(
        lake,
        run,
        "error",
        "Model publication failed; prior model remains available."
      )
    },
    add = TRUE,
    after = FALSE
  )
  DBI::dbWithTransaction(lake$con, {
    for (name in names(tables)) {
      member <- x$sources[[name]]
      asset <- paste(x$id, name, sep = ".")
      old <- tryCatch(
        resolve_release(lake, asset),
        dr_no_release = function(e) NULL
      )
      if (!is.null(old)) {
        origin <- dataraft.core::metadata_filter(
          lake,
          "runs",
          run_id = old$run_id[[1]]
        )
        if (nrow(origin) != 1L || origin$pipeline[[1]] != x$id) {
          dataraft.core::abort(
            subclass = "dataraft_error_lake",
            paste("Model member name belongs to another product:", asset)
          )
        }
      }
      member_release <- dataraft.core::uid()
      table <- paste0("member_", member_release)
      contract <- member$contract %||%
        dataraft.core::automatic_schema(
          asset,
          dataraft.core::automatic_types(dataraft.core::infer_column_types(tables[[
            name
          ]]))
        )
      if (is.null(member$contract) && !is.null(old)) {
        ref <- strsplit(old$contract[[1]], "@", fixed = TRUE)[[1]]
        saved <- query(
          lake,
          paste(
            "SELECT definition FROM",
            meta(lake, "assets"),
            "WHERE kind = 'contract' AND id = ? AND version = ?"
          ),
          as.list(ref)
        )
        if (nrow(saved) != 1L) {
          dataraft.core::abort(
            subclass = "dataraft_error_lake",
            "Published member contract is missing."
          )
        }
        prior_contract <- dataraft.core::jdecode(saved$definition[[1]])
        if (!isTRUE(prior_contract$automatic_schema)) {
          dataraft.core::abort(
            subclass = "dataraft_error_lake",
            paste(
              "Keep the explicit contract for model table",
              name,
              "in contracts or its replacement product."
            )
          )
        }
        contract <- dataraft.core::automatic_schema(
          asset,
          dataraft.core::automatic_types(unlist(
            prior_contract$columns,
            use.names = TRUE
          ))
        )
        if (
          !dataraft.core::quality_ok(dataraft.core::dr_validate(
            tables[[name]],
            contract
          ))
        ) {
          dataraft.core::abort(
            subclass = "dataraft_error_lake",
            paste(
              "Model table",
              name,
              "changed its established schema. Supply an explicit reviewed contract."
            )
          )
        }
      }
      dr_register(lake, contract)
      DBI::dbWriteTable(
        lake$con,
        table_id(layer, table),
        as.data.frame(tables[[name]])
      )
      insert_meta(
        lake,
        "releases",
        list(
          release_id = member_release,
          asset = asset,
          schema_name = layer,
          table_name = table,
          run_id = run,
          published_at = dataraft.core::now(),
          contract = paste(contract$id, contract$version, sep = "@"),
          definition_hash = dataraft.core::fingerprint(definition),
          input_hash = dataraft.core::fingerprint(tables[[name]]),
          quality = "passed",
          business_date = NA_character_,
          parent_release = if (is.null(old)) {
            NA_character_
          } else {
            old$release_id[[1]]
          }
        )
      )
      members[[name]] <- list(asset = asset, release_id = member_release)
      insert_meta(
        lake,
        "lineage_edges",
        list(
          run_id = run,
          from_id = asset,
          from_version = member_release,
          to_id = x$id,
          to_version = release,
          relation = "model_member"
        )
      )
    }
    manifest <- list(
      format = 1L,
      members = members,
      primary_keys = x$primary_keys,
      foreign_keys = x$foreign_keys
    )
    table <- paste0("model_", release)
    DBI::dbWriteTable(
      lake$con,
      table_id(layer, table),
      data.frame(manifest = dataraft.core::jencode(manifest))
    )
    insert_meta(
      lake,
      "releases",
      list(
        release_id = release,
        asset = x$id,
        schema_name = layer,
        table_name = table,
        run_id = run,
        published_at = dataraft.core::now(),
        contract = "",
        definition_hash = dataraft.core::fingerprint(definition),
        input_hash = dataraft.core::fingerprint(tables),
        quality = "passed",
        business_date = NA_character_,
        parent_release = if (is.null(prior)) {
          NA_character_
        } else {
          prior$release_id[[1]]
        }
      )
    )
    for (i in seq_len(nrow(result$quality))) {
      row <- as.list(result$quality[
        i,
        intersect(
          names(result$quality),
          DBI::dbListFields(lake$con, table_id("_dl", "quality_results"))
        ),
        drop = FALSE
      ])
      insert_meta(
        lake,
        "quality_results",
        c(
          list(run_id = run, contract = x$id),
          row[setdiff(names(row), c("run_id", "contract"))]
        )
      )
    }
    finish_run(lake, run, "published", release = release)
  })
  success <- TRUE
  result$run_id <- run
  result$release_id <- release
  result$status <- "published"
  result$output_config <- lake$config
  if (!own) {
    result$output_lake <- lake
  }
  result$data <- NULL
  result$members <- lapply(members, function(ref) {
    out <- dataraft.core::run_result(run, "published", ref$release_id)
    out$asset <- ref$asset
    out$output_config <- lake$config
    if (!own) {
      out$output_lake <- lake
    }
    out
  })
  result
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name read_model_release

read_model_release <- function(lake, asset, release = NULL) {
  rlang::local_error_call(rlang::caller_env())
  dataraft.core::need("dm")
  ref <- resolve_release(lake, asset, release)
  if (!startsWith(ref$table_name[[1]], "model_")) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "This release is not a model product."
    )
  }
  manifest <- dataraft.core::jdecode(query(
    lake,
    paste(
      "SELECT manifest FROM",
      table_sql(lake, ref$schema_name[[1]], ref$table_name[[1]])
    )
  )$manifest[[1]])
  if (!identical(manifest$format, 1L)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Unsupported model manifest format."
    )
  }
  tables <- lapply(manifest$members, function(x) {
    dr_read_release(lake, x$asset, x$release_id)
  })
  pk <- lapply(manifest$primary_keys, unlist, use.names = FALSE)
  fk <- lapply(manifest$foreign_keys, function(x) {
    x$columns <- unlist(x$columns, use.names = FALSE)
    x$ref_columns <- unlist(x$ref_columns, use.names = FALSE)
    x
  })
  dataraft.core::dm_keys(dm::dm(!!!tables), pk, fk, FALSE)
}
