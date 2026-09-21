#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @export
#' @name land_source

land_source <- function(lake, source) {
  rlang::local_error_call(rlang::caller_env())
  if (!file.exists(source$path) || dir.exists(source$path)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Source file is missing.",
      "dr_missing_delivery"
    )
  }
  # Copy first, then hash the bytes that will actually be parsed.
  tmp <- tempfile("incoming-", tmpdir = lake$config$landing)
  on.exit(unlink(tmp), add = TRUE)
  if (!file.copy(source$path, tmp, overwrite = FALSE)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Unable to stage source file."
    )
  }
  hash <- digest::digest(file = tmp, algo = "sha256", serialize = FALSE)
  directory <- file.path(lake$config$landing, source$id, hash)
  dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  target <- file.path(directory, basename(source$path))
  if (file.exists(target)) {
    if (
      !identical(
        digest::digest(file = target, algo = "sha256", serialize = FALSE),
        hash
      )
    ) {
      dataraft.core::abort(
        subclass = "dataraft_error_lake",
        "Landing integrity check failed."
      )
    }
  } else if (!file.rename(tmp, target)) {
    dataraft.core::abort(
      subclass = "dataraft_error_lake",
      "Unable to commit landed source file."
    )
  }
  st <- lake$config$storage
  uri <- target
  if (st$type == "s3") {
    dataraft.core::need("paws.storage")
    client <- paws.storage::s3(
      config = list(
        region = st$region,
        endpoint = st$endpoint,
        s3_force_path_style = TRUE
      )
    )
    key <- paste(
      c(
        if (nzchar(st$prefix)) st$prefix,
        "landing",
        source$id,
        hash,
        basename(target)
      ),
      collapse = "/"
    )
    # Content-addressed key plus a conditional create protects the original.
    tryCatch(
      client$put_object(
        Bucket = st$bucket,
        Key = key,
        Body = target,
        IfNoneMatch = "*"
      ),
      error = function(e) {
        if (!grepl("PreconditionFailed|412", conditionMessage(e))) {
          dataraft.core::abort(
            subclass = "dataraft_error_lake",
            "S3 landing upload failed."
          )
        }
      }
    )
    uri <- paste0("s3://", st$bucket, "/", key)
  }
  list(
    path = target,
    uri = uri,
    hash = hash,
    received_at = dataraft.core::now()
  )
}
