# The observer is optional: neither its absence nor its failures affect storage.
.lake_connections <- new.env(parent = emptyenv())
.lake_connections$entries <- new.env(parent = emptyenv())
.lake_connections$next_id <- 0L

connection_try <- function(code, fallback = NULL) {
  tryCatch(suppressWarnings(code), error = function(e) fallback)
}

connection_frame <- function(name = character(), type = character()) {
  data.frame(name = unname(name), type = unname(type), stringsAsFactors = FALSE)
}

connection_lake <- function(entry) {
  for (lake in rev(entry$handles)) {
    if (isTRUE(connection_try(DBI::dbIsValid(lake$con), FALSE))) return(lake)
  }
  stop("This DataRaft connection is closed.", call. = FALSE)
}

connection_releases <- function(lake) {
  # Only published tables and model members, never staging or model manifests.
  releases <- dr_releases(lake)
  releases <- releases[!duplicated(releases$asset), , drop = FALSE]
  releases[!startsWith(releases$table_name, "model_"), , drop = FALSE]
}

connection_table <- function(entry, schema, table) {
  lake <- connection_lake(entry)
  releases <- connection_releases(lake)
  selected <- releases[
    releases$schema_name == schema & releases$asset == table,
  ]
  if (nrow(selected) != 1L) {
    stop("Select a published table.", call. = FALSE)
  }
  dr_tbl(lake, selected$asset[[1]], release = selected$release_id[[1]])
}

connection_code <- function(lake) {
  path <- attr(lake$config, "dr_local_path")
  reproducible <- !is.null(path) &&
    file.exists(file.path(path, "dataraft.json")) &&
    isTRUE(connection_try(
      identical(
        lake$config,
        dr_lake_config(
          path = path,
          read_only = lake$config$read_only,
          install_extensions = lake$config$install_extensions
        )
      ),
      FALSE
    ))
  if (reproducible) {
    return(paste0(
      "lake <- dataraft.lake::dr_open_lake(",
      paste(deparse(path), collapse = "\n"),
      ", install_extensions = ",
      if (lake$config$install_extensions) "TRUE" else "FALSE",
      ", read_only = ",
      if (lake$config$read_only) "TRUE" else "FALSE",
      ")"
    ))
  }
  paste(
    "# Recreate your original lake_config in `config` and restore its credential environment.",
    "# Then run: lake <- dataraft.lake::dr_connect_lake(config)",
    sep = "\n"
  )
}

connection_opened <- function(lake) {
  state <- lake$connection_state
  if (isTRUE(state$attempted)) {
    return(invisible(NULL))
  }
  state$attempted <- TRUE
  observer <- getOption("connectionObserver")
  if (!is.list(observer) || !is.function(observer$connectionOpened)) {
    return(invisible(NULL))
  }
  # Hash configuration, never resolved credentials; persisted metadata is opaque.
  host <- paste0("dataraft:", digest::digest(lake$config, algo = "sha256"))
  entries <- .lake_connections$entries
  entry <- entries[[host]]
  .lake_connections$next_id <- .lake_connections$next_id + 1L
  id <- as.character(.lake_connections$next_id)
  state$host <- host
  state$id <- id
  if (!is.null(entry)) {
    entry$handles[[id]] <- lake
    return(invisible(NULL))
  }
  entry <- new.env(parent = emptyenv())
  entry$host <- host
  entry$observer <- observer
  entry$handles <- stats::setNames(list(lake), id)
  entries[[host]] <- entry
  opened <- connection_try(
    {
      observer$connectionOpened(
        type = "DataRaft",
        displayName = paste("DataRaft", lake$config$backend),
        host = host,
        connectCode = connection_code(lake),
        disconnect = function() {
          for (handle in entry$handles) {
            dr_disconnect_lake(handle)
          }
          invisible(NULL)
        },
        listObjectTypes = function() {
          list(schema = list(contains = list(table = list(contains = "data"))))
        },
        listObjects = function(schema = NULL, table = NULL) {
          connection_try(
            {
              releases <- connection_releases(connection_lake(entry))
              if (!is.null(table)) {
                return(connection_frame())
              }
              if (is.null(schema)) {
                names <- sort(unique(releases$schema_name))
                connection_frame(names, rep("schema", length(names)))
              } else {
                names <- sort(releases$asset[releases$schema_name == schema])
                connection_frame(names, rep("table", length(names)))
              }
            },
            connection_frame()
          )
        },
        listColumns = function(schema, table) {
          connection_try(
            {
              data <- dplyr::collect(utils::head(
                connection_table(entry, schema, table),
                0L
              ))
              connection_frame(
                names(data),
                vapply(
                  data,
                  function(x) paste(class(x), collapse = "/"),
                  character(1)
                )
              )
            },
            connection_frame()
          )
        },
        previewObject = function(rowLimit = 100L, schema, table) {
          connection_try(
            {
              if (
                !is.numeric(rowLimit) ||
                  length(rowLimit) != 1L ||
                  !is.finite(rowLimit) ||
                  rowLimit < 0
              ) {
                stop("Use a finite non-negative row limit.", call. = FALSE)
              }
              limit <- as.integer(min(rowLimit, 1000L))
              as.data.frame(dplyr::collect(utils::head(
                connection_table(entry, schema, table),
                limit
              )))
            },
            data.frame()
          )
        }
      )
      TRUE
    },
    FALSE
  )
  if (!isTRUE(opened)) {
    entries[[host]] <- NULL
  }
  invisible(NULL)
}

connection_closed <- function(lake) {
  state <- lake$connection_state
  if (is.null(state$host) || isTRUE(state$closed)) {
    return(invisible(NULL))
  }
  state$closed <- TRUE
  entry <- .lake_connections$entries[[state$host]]
  if (is.null(entry)) {
    return(invisible(NULL))
  }
  entry$handles[[state$id]] <- NULL
  if (!length(entry$handles)) {
    .lake_connections$entries[[state$host]] <- NULL
    connection_try(entry$observer$connectionClosed(
      type = "DataRaft",
      host = entry$host
    ))
  }
  invisible(NULL)
}

#' Refresh a lake in the IDE Connections pane
#'
#' Lakes opened by [dr_connect_lake()] or [dr_open_lake()] announce themselves
#' to the optional RStudio/Positron Connections observer. The browser shows only
#' the latest published tables and model members, grouped by release schema.
#' Raw input tables, unpublished candidates and model manifests are omitted.
#' Column discovery queries zero rows; previews return at most 1,000 rows.
#'
#' Publications through this handle refresh the pane automatically. Call this
#' function after another writer publishes; there is no polling. Observer
#' failures never fail storage operations. Multiple handles with the same
#' configuration share one pane entry; disconnecting that entry closes them all.
#' The entry closes when its final handle is disconnected.
#' If the observer becomes available only after opening the lake, reopen the
#' lake to register its connection.
#'
#' Reconnect code is provided only for self-contained local lake folders with a
#' saved configuration. Other connections display a code hint requiring the
#' original configuration and credential environment. Resolved credentials and
#' remote connection strings are never included in the persisted IDE metadata.
#' @param lake A connected lake.
#' @return The lake, invisibly. Without an IDE observer this is a no-op.
#' @export
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-ide-")
#' lake <- dr_open_lake(root)
#' dr_refresh_connection(lake)
#' dr_close_lake(lake)
#' unlink(root, recursive = TRUE)
dr_refresh_connection <- function(lake) {
  connection_try({
    state <- lake$connection_state
    if (!is.null(state$host) && !isTRUE(state$closed)) {
      entry <- .lake_connections$entries[[state$host]]
      if (!is.null(entry)) {
        entry$observer$connectionUpdated(type = "DataRaft", host = entry$host)
      }
    }
  })
  invisible(lake)
}
