#' Define metadata and storage configuration
#' @param path Local path.
#' @param connection_env Environment variable containing a PostgreSQL libpq
#'   string.
#' @param lock_timeout Seconds to wait for another PostgreSQL writer. Writes are
#'   coordinated per catalog database using optional RPostgres. All writers must
#'   use this protocol; direct SQL and older clients are not coordinated.
#' @param bucket S3 bucket.
#' @param prefix Prefix within the bucket.
#' @param endpoint S3 endpoint, including https://.
#' @param region AWS region.
#' @return A serializable configuration object containing no credentials.
#' @export
#' @examples
#' dr_registry_duckdb(file.path(tempdir(), "lake.db"))
#' dr_storage_local(file.path(tempdir(), "data"))
#' dr_registry_postgres("DUCKLAKE_PG_CONNECTION")
dr_registry_duckdb <- function(path) {
  structure(
    list(
      type = "duckdb",
      path = dataraft.core::dr_internal_absolute_path(path)
    ),
    class = "dr_catalog_spec"
  )
}

#' @rdname dr_registry_duckdb
#' @export
dr_registry_postgres <- function(
  connection_env = "DUCKLAKE_PG_CONNECTION",
  lock_timeout = 30
) {
  if (
    !is.numeric(lock_timeout) ||
      length(lock_timeout) != 1L ||
      !is.finite(lock_timeout) ||
      lock_timeout < 0
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "lock_timeout must be a non-negative number of seconds."
    )
  }
  structure(
    list(
      type = "postgres",
      lock_timeout = lock_timeout,
      connection_env = dataraft.core::dr_internal_scalar(
        connection_env,
        "connection_env"
      )
    ),
    class = "dr_catalog_spec"
  )
}

#' @rdname dr_registry_duckdb
#' @export
dr_storage_local <- function(path) {
  structure(
    list(type = "local", path = dataraft.core::dr_internal_absolute_path(path)),
    class = "dr_storage_spec"
  )
}

#' @rdname dr_registry_duckdb
#' @export
dr_storage_s3 <- function(
  bucket,
  prefix = "dataloom/",
  endpoint,
  region = "eu-central-1"
) {
  dataraft.core::dr_internal_scalar(bucket, "bucket")
  dataraft.core::dr_internal_scalar(endpoint, "endpoint")
  if (!grepl("^https?://[^/]+/?$", endpoint)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "endpoint must contain a scheme and host, without a path."
    )
  }
  prefix <- gsub("^/+|/+$", "", prefix)
  structure(
    list(
      type = "s3",
      bucket = bucket,
      prefix = prefix,
      endpoint = sub("/$", "", endpoint),
      region = region
    ),
    class = "dr_storage_spec"
  )
}


#' Set up or reconnect to a data lake
#' @param catalog Metadata configuration.
#' @param storage Data file storage configuration.
#' @param layers Schema names.
#' @param landing Local immutable landing directory (also used as staging for
#'   S3).
#' @param backend DuckLake, or local DuckDB for offline development.
#' @param install_extensions Allow DuckDB to install required extensions.
#' @param read_only Attach existing storage read-only and skip schema creation
#'   writes. Only the current registry schema is supported.
#' @param config A configuration from a previously connected lake.
#' @param path Optional self-contained local folder, as in [dr_open_lake()].
#'   Supply this instead of `catalog`, `storage` and `landing`. New folders
#'   default to DuckDB; use `backend = "ducklake"` for DuckLake. Saved settings
#'   are reused when reopening.
#' @return A connected lake handle. Close it with dr_disconnect_lake().

#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dr_lake_config(
#'   dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' lake <- dr_connect_lake(config)
#' lake
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
#' @keywords internal
#' @noRd
dr_setup_lake <- function(
  catalog = dr_registry_duckdb("metadata.ducklake"),
  storage = dr_storage_local("data"),
  layers = c("raw", "validated", "products"),
  landing = "landing",
  backend = c("ducklake", "duckdb"),
  install_extensions = TRUE,
  read_only = FALSE,
  path = NULL
) {
  if (!is.null(path)) {
    if (!missing(catalog) || !missing(storage) || !missing(landing)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Supply path or explicit catalog, storage and landing settings, not both."
      )
    }
    args <- list(
      path = path,
      install_extensions = install_extensions,
      read_only = read_only
    )
    if (!missing(backend)) {
      args$backend <- backend
    }
    if (!missing(layers)) {
      args$layers <- layers
    }
    return(dr_connect_lake(do.call(dr_lake_config, args)))
  }
  dr_connect_lake(dr_lake_config(
    catalog,
    storage,
    layers,
    landing,
    backend,
    install_extensions,
    read_only
  ))
}


#' Define a lake without opening a connection
#'
#' This constructor validates configuration and resolves paths, but does not
#' create directories, install extensions, connect to databases or read secrets.
#' With `path`, it may read an existing `dataraft.json` to preserve the saved
#' backend and ordered layers, including named layer roles. It never creates
#' or writes files. A new shorthand lake defaults to
#' DuckDB; other calls retain the usual DuckLake default. The local layout and
#' backend marker are shared with [dr_open_lake()]. Unknown non-empty folders are
#' refused rather than interpreted as a new lake.
#' Pass its result to [dr_connect_lake()] or [dr_target_lake()] when ready to execute.
#' @param catalog Metadata configuration.
#' @param storage Data file storage configuration.
#' @param layers Ordered schema names.
#' @param landing Local immutable landing directory and S3 staging folder.
#' @param backend `"ducklake"` or local `"duckdb"`.
#' @param install_extensions Allow DuckDB to install required extensions.
#' @param read_only Attach existing storage read-only and skip creation writes.
#' @param path Optional local lake folder. Derives `metadata.duckdb`, `data`
#'   and `landing` within that folder. Supply either `path` or explicit
#'   `catalog`, `storage` and `landing`, not both. An explicit backend must
#'   agree with a folder's saved backend. Explicit layers must match its saved
#'   layers; omit them when reopening. Default layers are unchanged;
#'   select `layers = c("raw", "staging", "core", "marts")` when needed.
#' @return A connection-free `lake_config` specification.
#' @export
#' @examples
#' config <- dr_lake_config(backend = "duckdb")
#' print(config)
#' local <- dr_lake_config(path = file.path(tempdir(), "my-data-lake"))
#' print(local)
dr_lake_config <- function(
  catalog = dr_registry_duckdb("metadata.ducklake"),
  storage = dr_storage_local("data"),
  layers = c("raw", "validated", "products"),
  landing = "landing",
  backend = c("ducklake", "duckdb"),
  install_extensions = TRUE,
  read_only = FALSE,
  path = NULL
) {
  layers_missing <- missing(layers)
  dataraft.core::dr_internal_flag(read_only, "read_only")
  if (!is.null(path)) {
    if (!missing(catalog) || !missing(storage) || !missing(landing)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Supply path or explicit catalog, storage and landing settings, not both."
      )
    }
    path <- dataraft.core::dr_internal_absolute_path(path)
    local <- local_lake_settings(
      path,
      if (missing(backend)) NULL else match.arg(backend),
      if (missing(layers)) NULL else layers
    )
    backend <- local$backend
    layers <- local$layers
    catalog <- dr_registry_duckdb(file.path(path, "metadata.duckdb"))
    storage <- dr_storage_local(file.path(path, "data"))
    landing <- file.path(path, "landing")
  } else {
    backend <- match.arg(backend)
  }
  if (
    !inherits(catalog, "dr_catalog_spec") ||
      !inherits(storage, "dr_storage_spec")
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Use catalog and storage constructors."
    )
  }
  invisible(lapply(layers, dataraft.core::dr_internal_ident))
  if (length(layers) == 0L || anyDuplicated(layers)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "layers must be non-empty and unique."
    )
  }
  if (
    backend == "duckdb" && (catalog$type != "duckdb" || storage$type != "local")
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "The DuckDB backend only supports local storage and catalog."
    )
  }
  out <- structure(
    list(
      catalog = catalog,
      storage = storage,
      layers = layers,
      landing = dataraft.core::dr_internal_absolute_path(landing),
      backend = backend,
      install_extensions = install_extensions,
      read_only = read_only
    ),
    class = "dr_config"
  )
  if (!is.null(path)) {
    attr(out, "dr_local_path") <- path
  }
  out
}


local_lake_settings <- function(path, backend = NULL, layers = NULL) {
  rlang::local_error_call(rlang::caller_env())
  if (file.exists(path) && !dir.exists(path)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "The local lake path is a file. Choose a folder."
    )
  }
  manifest <- file.path(path, "dataraft.json")
  if (file.exists(manifest)) {
    saved <- tryCatch(
      jdecode(paste(
        readLines(manifest, warn = FALSE),
        collapse = "\n"
      )),
      error = function(e) NULL
    )
    if (
      !is.list(saved) ||
        !is.character(saved$backend) ||
        length(saved$backend) != 1L ||
        is.na(saved$backend) ||
        !saved$backend %in% c("duckdb", "ducklake")
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Invalid dataraft.json. Restore the folder's original configuration."
      )
    }
    if (!identical(saved$format, 2L)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Unsupported local configuration format. Create a new lake with this package version."
      )
    }
    if (!is.null(backend) && !identical(backend, saved$backend)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This folder uses a different backend. Reopen without backend or choose a new folder."
      )
    }
    valid <- function(x) {
      rlang::local_error_call(rlang::caller_env())
      is.list(x) &&
        length(x) > 0L &&
        all(vapply(
          x,
          function(value) {
            is.character(value) && length(value) == 1L && !is.na(value)
          },
          logical(1)
        ))
    }
    if (!valid(saved$layers)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Invalid dataraft.json layers. Restore the folder's original configuration."
      )
    }
    saved_layers <- unlist(saved$layers, use.names = FALSE)
    if (
      anyDuplicated(saved_layers) ||
        any(!grepl("^[A-Za-z][A-Za-z0-9_]*$", saved_layers))
    ) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Invalid dataraft.json layers. Restore the folder's original configuration."
      )
    }
    if (!is.null(saved$layer_names)) {
      if (
        !valid(saved$layer_names) ||
          length(saved$layer_names) != length(saved_layers)
      ) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Invalid dataraft.json layer names. Restore the folder's original configuration."
        )
      }
      names(saved_layers) <- unlist(saved$layer_names, use.names = FALSE)
    }
    if (!is.null(layers) && !identical(layers, saved_layers)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "This folder has different saved layers. Omit layers to reuse its configuration, or choose a new folder."
      )
    }
    layers <- saved_layers
    return(list(
      backend = saved$backend,
      layers = layers
    ))
  }
  if (
    dir.exists(path) &&
      length(list.files(path, all.files = TRUE, no.. = TRUE))
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "This folder is not empty and has no dataraft.json. Use its original dr_lake_config() or choose an empty folder."
    )
  }
  list(
    backend = backend %||% "duckdb",
    layers = layers %||% c("raw", "validated", "products")
  )
}


save_local_lake_settings <- function(config) {
  rlang::local_error_call(rlang::caller_env())
  path <- attr(config, "dr_local_path")
  if (is.null(path)) {
    return(invisible(NULL))
  }
  manifest <- file.path(path, "dataraft.json")
  if (file.exists(manifest)) {
    local_lake_settings(path, config$backend, config$layers)
    return(invisible(NULL))
  }
  temporary <- tempfile(".local-config-", tmpdir = path)
  on.exit(unlink(temporary), add = TRUE)
  writeLines(
    jencode(list(
      format = 2L,
      backend = config$backend,
      layers = unname(as.list(config$layers)),
      layer_names = if (is.null(names(config$layers))) {
        NULL
      } else {
        as.list(names(config$layers))
      }
    )),
    temporary
  )
  if (!suppressWarnings(file.rename(temporary, manifest))) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Unable to save dataraft.json for the local lake."
    )
  }
  invisible(NULL)
}


#' Connect a configured lake
#' @param config A lake configuration.
#' @param read_only Whether to open read-only.
#' @returns A connected lake handle.

#' @keywords internal
#' @export
dr_connect_lake <- function(config, read_only = config$read_only) {
  dataraft.core::dr_internal_need("duckdb")
  dataraft.core::dr_internal_need("bit64", "Lake storage with 64-bit integers")
  if (!inherits(config, "dr_config")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Use dr_lake_config() to describe this lake."
    )
  }
  dataraft.core::dr_internal_flag(read_only, "read_only")
  config$read_only <- read_only
  local_path <- attr(config, "dr_local_path")
  if (!is.null(local_path)) {
    local_lake_settings(local_path, config$backend, config$layers)
  }
  if (
    read_only &&
      config$catalog$type == "duckdb" &&
      !file.exists(config$catalog$path)
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "A read-only catalog must already exist."
    )
  }
  if (!is.null(local_path) && !read_only) {
    dir.create(local_path, recursive = TRUE, showWarnings = FALSE)
    if (!dir.exists(local_path)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Unable to create the local lake folder."
      )
    }
    save_local_lake_settings(config)
  }
  con <- DBI::dbConnect(
    suppressMessages(duckdb::duckdb()),
    dbdir = ":memory:",
    bigint = "integer64"
  )
  ok <- FALSE
  on.exit(if (!ok) DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  lake <- structure(
    list(
      con = con,
      config = config,
      writer_state = lake_writer_state(config)
    ),
    class = "dr_lake"
  )
  if (!read_only) {
    assert_writable(lake)
  }
  cat <- config$catalog
  st <- config$storage
  if (!read_only) {
    dir.create(config$landing, recursive = TRUE, showWarnings = FALSE)
    if (cat$type == "duckdb") {
      dir.create(dirname(cat$path), recursive = TRUE, showWarnings = FALSE)
    }
    if (st$type == "local") {
      dir.create(st$path, recursive = TRUE, showWarnings = FALSE)
    }
  }
  if (config$backend == "duckdb") {
    exec(
      lake,
      paste(
        "ATTACH",
        qlit(lake, cat$path),
        "AS lake",
        if (read_only) "(READ_ONLY)" else ""
      )
    )
  } else {
    extensions <- c(
      "ducklake",
      if (cat$type == "postgres") "postgres",
      if (st$type == "s3") "httpfs"
    )
    for (ext in extensions) {
      if (isTRUE(config$install_extensions)) {
        exec(lake, paste("INSTALL", ext))
      }
      exec(lake, paste("LOAD", ext))
    }
    if (st$type == "s3") {
      # Resolve credentials at execution time and never save them to the registry.
      key <- Sys.getenv("AWS_ACCESS_KEY_ID")
      secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
      if (!nzchar(key) || !nzchar(secret)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
        )
      }
      parts <- c(
        "TYPE S3",
        paste("KEY_ID", qlit(lake, key)),
        paste("SECRET", qlit(lake, secret)),
        paste("REGION", qlit(lake, st$region)),
        paste("ENDPOINT", qlit(lake, sub("^https?://", "", st$endpoint))),
        "URL_STYLE 'path'",
        paste(
          "USE_SSL",
          if (startsWith(st$endpoint, "https://")) "true" else "false"
        ),
        paste("SCOPE", qlit(lake, paste0("s3://", st$bucket, "/")))
      )
      token <- Sys.getenv("AWS_SESSION_TOKEN")
      if (nzchar(token)) {
        parts <- c(parts, paste("SESSION_TOKEN", qlit(lake, token)))
      }
      tryCatch(
        exec(
          lake,
          paste0("CREATE SECRET dr_s3 (", paste(parts, collapse = ", "), ")")
        ),
        error = function(e) {
          dataraft.core::dr_internal_abort(
            subclass = c("dataraft_error_backend", "dataraft_error_lake"),
            "S3 credential configuration failed; check endpoint and environment variables."
          )
        }
      )
      data_path <- paste0(
        "s3://",
        st$bucket,
        "/",
        if (nzchar(st$prefix)) paste0(st$prefix, "/"),
        "tables/"
      )
    } else {
      data_path <- paste0(st$path, "/")
    }
    uri <- if (cat$type == "postgres") {
      value <- Sys.getenv(cat$connection_env)
      if (!nzchar(value)) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          paste("Set", cat$connection_env)
        )
      }
      paste0("ducklake:postgres:", value)
    } else {
      paste0("ducklake:", cat$path)
    }
    tryCatch(
      exec(
        lake,
        paste(
          "ATTACH",
          qlit(lake, uri),
          "AS lake (DATA_PATH",
          qlit(lake, data_path),
          if (read_only) ", READ_ONLY)" else ")"
        )
      ),
      error = function(e) {
        dataraft.core::dr_internal_abort(
          subclass = c("dataraft_error_backend", "dataraft_error_lake"),
          "DuckLake attach failed. Check extension, catalog connectivity and storage access. Credentials are omitted."
        )
      }
    )
  }
  if (!read_only) {
    for (s in c(config$layers, "_dl")) {
      exec(
        lake,
        paste(
          "CREATE SCHEMA IF NOT EXISTS",
          paste(qident(lake, c("lake", s)), collapse = ".")
        )
      )
    }
    registry_init(lake)
  } else {
    versions <- tryCatch(
      dr_registry(lake, "schema_version")$version,
      error = function(e) {
        dataraft.core::dr_internal_abort(
          subclass = "dataraft_error_lake",
          "Registry is missing. Open with a writable connection first."
        )
      }
    )
    if (!identical(versions, 4L)) {
      dataraft.core::dr_internal_abort(
        subclass = "dataraft_error_lake",
        "Unsupported registry version. Create a new lake with this package version."
      )
    }
  }
  query(lake, "SELECT 1 AS connection_test")
  ok <- TRUE
  lake
}


#' Close a lake connection
#' @param lake Connected lake.
#' @return Invisibly TRUE.

#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#' root <- tempfile("dataraft-example-")
#' config <- dr_lake_config(
#'   dr_registry_duckdb(file.path(root, "lake.db")),
#'   dr_storage_local(file.path(root, "data")),
#'   landing = file.path(root, "landing"), backend = "duckdb"
#' )
#' lake <- dr_connect_lake(config)
#' dr_disconnect_lake(lake)
#' unlink(root, recursive = TRUE)
#' @keywords internal
#' @export
dr_disconnect_lake <- function(lake) {
  if (DBI::dbIsValid(lake$con)) {
    DBI::dbDisconnect(lake$con, shutdown = TRUE)
  }
  invisible(TRUE)
}

#' @export
print.dr_lake <- function(x, ...) {
  cat(
    "<dataraft>",
    x$config$backend,
    "| layers:",
    paste(x$config$layers, collapse = ", "),
    "\n"
  )
  invisible(x)
}
