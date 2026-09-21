#' Add an explicit transformation before validation
#'
#' Transforms are applied in order after raw extraction and before composing
#' the candidate and validating its contract. Each receives a lazy table or the
#' prior transform's data frame. No collection is performed by this step.
#' The original file and raw table remain intact. Change the pipeline version
#' and code_version whenever transformation behavior changes.
#' @param pipeline Pipeline specification after extraction and before
#'   validation.
#' @param transform Function of one data argument, returning a lazy table or
#'   data frame.
#' @param id Unique step identifier within this pipeline.
#' @return An updated pipeline specification.
#' @examples
#' pipeline <- dr_pipeline("orders.import", dr_lake_config(backend = "duckdb"),
#'   code_version = "v1") |>
#'   dr_step_land(dataraft.core::dr_source_file("orders.file", "orders.csv", utils::read.csv)) |>
#'   dr_step_extract() |>
#'   pipeline_step_transform(function(data) dplyr::filter(data, amount > 0), "positive")
#' dataraft.core::dr_plan(pipeline)
#' @noRd
pipeline_step_transform <- function(pipeline, transform, id) {
  rlang::local_error_call(rlang::caller_env())
  if (!inherits(pipeline, "dr_pipeline")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Use dr_pipeline() first."
    )
  }
  dataraft.core::dr_internal_scalar(id, "id")
  if (!is.function(transform)) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "transform must be a function."
    )
  }
  if (
    !identical(
      setdiff(names(pipeline$steps), c("transform", "precheck")),
      c("land", "extract")
    )
  ) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Add transforms after extraction and before validation.",
      "dr_pipeline_invalid"
    )
  }
  previous <- pipeline$steps$transform
  if (id %in% vapply(previous, `[[`, character(1), "id")) {
    dataraft.core::dr_internal_abort(
      subclass = "dataraft_error_lake",
      "Transform ids must be unique."
    )
  }
  pipeline$steps$transform <- c(
    previous,
    list(list(id = id, transform = transform))
  )
  pipeline
}

#' @export
#' @noRd
#' @importFrom dataraft.core dr_execute
dr_execute.dr_pipeline <- function(object, lake = NULL, ...) {
  with_execution_lake(
    lake,
    function(con) dataraft.core::dr_run(object, con, ...),
    allow_null = TRUE
  )
}


#' Extension implementation helper
#'
#' Internal implementation interface for the DataRaft package family.
#' @usage NULL
#' @keywords internal
#' @name with_execution_lake

with_execution_lake <- function(lake, fn, allow_null = FALSE) {
  rlang::local_error_call(rlang::caller_env())
  if (inherits(lake, "dr_config")) {
    lake <- dr_connect_lake(lake)
    on.exit(dr_disconnect_lake(lake), add = TRUE)
  }
  if (is.null(lake) && allow_null) {
    return(fn(NULL))
  }
  assert_lake(lake)
  fn(lake)
}


#' @export
print.dr_config <- function(x, ...) {
  cat(
    "<lake_config>",
    x$backend,
    "| catalog:",
    x$catalog$type,
    "| storage:",
    x$storage$type,
    "\n"
  )
  cat("Layers:", paste(x$layers, collapse = ", "), "\nNo connection opened.\n")
  invisible(x)
}

#' @export
print.dr_pipeline <- function(x, ...) {
  cat("<dr_pipeline>", x$id, "@", x$version, "| code:", x$code_version, "\n")
  plan <- dataraft.core::dr_plan(x)
  print(plan)
  cat(
    if (isTRUE(attr(plan, "complete"))) {
      "Ready for execution.\n"
    } else {
      "Incomplete: add the remaining mandatory steps or configure layers.\n"
    }
  )
  invisible(x)
}
