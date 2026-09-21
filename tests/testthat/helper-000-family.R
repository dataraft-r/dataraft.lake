# Test bindings for this package; unavailable optional packages are not loaded.
family_owners <- c(
  "dr_target_database" = "dataraft.adapters",
  "dr_capabilities" = "dataraft.core",
  "dr_freshness" = "dataraft.catalog",
  "dr_read_source" = "dataraft.core",
  "dr_source_database" = "dataraft.adapters",
  "dr_check_component" = "dataraft.core",
  "dr_source_release" = "dataraft.lake",
  "dr_add_source" = "dataraft.core",
  "dr_add_transform" = "dataraft.core",
  "dr_add_contract" = "dataraft.core",
  "dr_add_quality" = "dataraft.core",
  "dr_set_target" = "dataraft.core",
  "dr_add_catalog" = "dataraft.core",
  "dr_inspect" = "dataraft.core",
  "dr_contract_from" = "dataraft.core",
  "dr_contract_confirm" = "dataraft.core",
  "dr_contract" = "dataraft.core",
  "dr_quality_rule" = "dataraft.core",
  "dr_quality_counts" = "dataraft.core",
  "dr_pointblank_checks" = "dataraft.core",
  "dr_validate" = "dataraft.core",
  "quality_ok" = "dataraft.core",
  "dr_dbt_publish" = "dataraft.dbt",
  "dbt_read_artifacts" = "dataraft.dbt",
  "dr_check_delivery" = "dataraft.lake",
  "dr_quality" = "dataraft.core",
  "dr_releases" = "dataraft.lake",
  "dr_publish" = "dataraft.core",
  "dr_collect" = "dataraft.core",
  "dr_ingest_data" = "dataraft.lake",
  "pipeline_ingest" = "dataraft.lake",
  "dr_ingest" = "dataraft.lake",
  "dr_add_lookup" = "dataraft.core",
  "dr_cleanup" = "dataraft.lake",
  "dr_metric" = "dataraft.metrics",
  "dr_measure" = "dataraft.metrics",
  "dr_pipeline" = "dataraft.lake",
  "dr_step_land" = "dataraft.lake",
  "dr_step_extract" = "dataraft.lake",
  "dr_step_validate" = "dataraft.lake",
  "dr_step_publish" = "dataraft.lake",
  "new_run" = "dataraft.lake",
  "compose_candidate" = "dataraft.lake",
  "publish_candidate" = "dataraft.lake",
  "dr_run" = "dataraft.core",
  "dr_product" = "dataraft.core",
  "registry_init" = "dataraft.lake",
  "dr_registry" = "dataraft.lake",
  "dr_register" = "dataraft.lake",
  "resolve_release" = "dataraft.lake",
  "dr_tbl" = "dataraft.lake",
  "dr_registry_duckdb" = "dataraft.lake",
  "dr_storage_local" = "dataraft.lake",
  "dr_setup_lake" = "dataraft.lake",
  "dr_lake_config" = "dataraft.lake",
  "dr_connect_lake" = "dataraft.lake",
  "dr_disconnect_lake" = "dataraft.lake",
  "postgres_parameters" = "dataraft.lake",
  "dr_open_lake" = "dataraft.lake",
  "dr_close_lake" = "dataraft.lake",
  "dr_write_data" = "dataraft.lake",
  "dr_read_release" = "dataraft.lake",
  "dr_source_file" = "dataraft.core",
  "dr_target_lake" = "dataraft.lake",
  "need" = "dataraft.core",
  "scalar" = "dataraft.core",
  "canonical" = "dataraft.core",
  "table_id" = "dataraft.lake",
  "materialize" = "dataraft.lake"
)
for (name in names(family_owners)) {
  owner <- family_owners[[name]]
  if (requireNamespace(owner, quietly = TRUE)) {
    assign(name, get(name, asNamespace(owner), inherits = FALSE))
  }
}
local_family_bindings <- function(..., .package = NULL, .env = parent.frame()) {
  bindings <- list(...)
  if (
    !is.null(.package) && !.package %in% c("dataraft", unique(family_owners))
  ) {
    return(do.call(
      testthat::local_mocked_bindings,
      c(bindings, list(.package = .package, .env = .env))
    ))
  }
  owners <- unname(family_owners[names(bindings)])
  if (anyNA(owners)) {
    stop("Unknown mocked family binding")
  }
  for (owner in unique(owners)) {
    do.call(
      testthat::local_mocked_bindings,
      c(bindings[owners == owner], list(.package = owner, .env = .env))
    )
  }
}
