#' @export
#' @importFrom dataraft.core dr_capabilities
dr_capabilities.dr_release_source <- function(x, ...) {
  dataraft.core::dr_component_capabilities(
    read = TRUE,
    write = FALSE,
    lazy = inherits(x$lake, "dr_lake"),
    transactions = TRUE,
    partition = FALSE,
    immutable = TRUE
  )
}

#' @export
#' @importFrom dataraft.core dr_capabilities
dr_capabilities.dr_lake_target <- function(x, ...) {
  dataraft.core::dr_component_capabilities(
    read = FALSE,
    write = TRUE,
    lazy = FALSE,
    transactions = TRUE,
    partition = TRUE,
    immutable = TRUE
  )
}

#' @export
#' @importFrom dataraft.core dr_capabilities
dr_capabilities.dr_lake <- function(x, ...) {
  dataraft.core::dr_component_capabilities(
    read = TRUE,
    write = !isTRUE(x$config$read_only),
    lazy = TRUE,
    transactions = TRUE,
    partition = TRUE,
    immutable = TRUE
  )
}
