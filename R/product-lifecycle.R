#' Read persisted product lifecycle state
#' @param lake Connected lake.
#' @param product Product ID or DataRaft product.
#' @return State string. Unregistered products are draft.
#' @export
 dr_product_state <- function(lake, product) {
  assert_lake(lake)
  id <- if (inherits(product, "dr_product")) product$id else product
  dataraft.core::dr_internal_asset_id(id)
  if (!DBI::dbExistsTable(lake$con, table_id("_dl", "product_transitions"))) return("draft")
  history <- query(lake, paste("SELECT to_state FROM", meta(lake, "product_transitions"),
    "WHERE asset = ? ORDER BY sequence DESC LIMIT 1"), list(id))
  if (!nrow(history)) "draft" else history$to_state[[1]]
}

#' Move a registered product to its next lifecycle state
#'
#' Transitions are durable and auditable in the lake registry. Supply a
#' successful `dr_run(product, write = FALSE)` result when validating a draft.
#' Policies configured through `options(dataraft.policies = ...)` are checked
#' on activation. Existing products without a transition remain drafts until
#' explicitly enrolled; publication is only blocked for retired products.
#' @param lake Writable lake registry.
#' @param product DataRaft product.
#' @param to Next state.
#' @param validation Successful run result for draft validation.
#' @param actor Identifier of the person or service requesting the transition.
#' @return New state invisibly.
#' @export
 dr_promote <- function(lake, product, to, validation = NULL, actor = Sys.info()[["user"]]) {
  assert_writable(lake)
  if (!inherits(product, "dr_product")) {
    dataraft.core::dr_internal_abort("Supply a product.", subclass = "dataraft_error_lake")
  }
  if (!is.character(actor) || length(actor) != 1L || is.na(actor) || !nzchar(actor)) {
    dataraft.core::dr_internal_abort("Supply an actor.", subclass = "dataraft_error_lake")
  }
  allowed <- list(draft = "validated", validated = "active",
    active = "deprecated", deprecated = "retired")
  acquire_lake_writer(lake, environment(), paste0("state:", product$id))
  if (!DBI::dbExistsTable(lake$con, table_id("_dl", "product_transitions"))) {
    exec(lake, paste("CREATE TABLE IF NOT EXISTS", meta(lake, "product_transitions"),
      "(sequence BIGINT, asset VARCHAR, version VARCHAR, from_state VARCHAR, to_state VARCHAR, actor VARCHAR, changed_at VARCHAR, validation_run VARCHAR, policies VARCHAR)"))
  }
  DBI::dbWithTransaction(lake$con, {
    current <- dr_product_state(lake, product)
    if (length(to) != 1L || is.na(to) || !identical(to, allowed[[current]])) {
      dataraft.core::dr_internal_abort(paste0("Cannot transition from ", current, " to requested state."),
        subclass = "dataraft_error_lake")
    }
    if (identical(to, "validated") &&
        (!inherits(validation, "dr_run_result") || !validation$status %in% c("completed", "published") ||
          !identical(validation$asset, product$id) ||
          !identical(validation$validation_status, "passed"))) {
      dataraft.core::dr_internal_abort("Validation requires a successful dr_run() result.",
        subclass = "dataraft_error_lake")
    }
    policies <- if (identical(to, "active")) {
      dataraft.core::dr_check_policies(product, event = "activate")
    } else NULL
    if (!is.null(policies) && any(policies$decision == "block")) {
      dataraft.core::dr_internal_abort("Activation blocked by organization policy.",
        subclass = "dataraft_error_lake")
    }
    sequence <- query(lake, paste("SELECT COALESCE(MAX(sequence), 0) AS n FROM", meta(lake, "product_transitions")))$n[[1]] + 1
    exec(lake, paste("INSERT INTO", meta(lake, "product_transitions"),
      "(sequence, asset, version, from_state, to_state, actor, changed_at, validation_run, policies) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
      list(sequence, product$id, product$version, current, to, actor, now(),
        if (is.null(validation)) "" else validation$run_id,
        jsonlite::toJSON(policies, dataframe = "rows", auto_unbox = TRUE)))
  })
  invisible(to)
}

#' @rdname dr_promote
#' @export
 dr_deprecate <- function(lake, product, actor = Sys.info()[["user"]]) dr_promote(lake, product, "deprecated", actor = actor)

#' @rdname dr_promote
#' @export
 dr_retire <- function(lake, product, actor = Sys.info()[["user"]]) dr_promote(lake, product, "retired", actor = actor)

#' Inspect lifecycle transitions
#' @param lake Connected lake.
#' @param product Optional product ID.
#' @return Recorded transition table.
#' @export
 dr_product_transitions <- function(lake, product = NULL) {
  assert_lake(lake)
  if (!DBI::dbExistsTable(lake$con, table_id("_dl", "product_transitions"))) return(tibble::tibble())
  if (is.null(product)) return(query(lake, paste("SELECT * FROM", meta(lake, "product_transitions"))))
  dataraft.core::dr_internal_asset_id(product)
  query(lake, paste("SELECT * FROM", meta(lake, "product_transitions"), "WHERE asset = ? ORDER BY sequence"), list(product))
}
