fixture <- function(backend = Sys.getenv("DATARAFT_TEST_BACKEND", "duckdb")) {
  testthat::skip_if_not_installed("duckdb")
  testthat::skip_if_not_installed("bit64")
  root <- tempfile("dataraft-test-")
  dir.create(root)
  lake <- getFromNamespace("dr_setup_lake", "dataraft.lake")(
    dataraft.lake::dr_registry_duckdb(file.path(root, "meta.duckdb")),
    dataraft.lake::dr_storage_local(file.path(root, "data")),
    landing = file.path(root, "landing"),
    backend = backend
  )
  path <- file.path(root, "input.csv")
  good <- data.frame(
    id = c("a", "b"),
    company = c("Alpha", "Beta"),
    date = as.Date(c("2026-08-31", "2026-08-31")),
    reserve = c(100, 200)
  )
  write <- function(data = good) utils::write.csv(data, path, row.names = FALSE)
  write()
  reader <- function(path) {
    x <- utils::read.csv(
      path,
      colClasses = c("character", "character", "Date", "numeric")
    )
    x
  }
  contract <- dataraft.core::dr_contract(
    "risk.contract",
    version = "1.0.0",
    owner = "Risk",
    description = "Validated reserves",
    grain = "One contract at a date",
    columns = c(
      id = "character",
      company = "character",
      date = "Date",
      reserve = "numeric"
    ),
    key = c("id", "date"),
    max_age_hours = 48,
    rules = list(dataraft.core::dr_quality_rule("nonnegative", function(x) {
      counts <- dplyr::collect(dplyr::summarise(
        x,
        n = dplyr::n(),
        failed = sum(as.integer(reserve < 0), na.rm = TRUE)
      ))
      dataraft.core::dr_quality_counts(counts$failed, counts$n)
    }))
  )
  pipeline <- getFromNamespace("dr_pipeline", "dataraft.lake")(
    "risk.import",
    lake,
    code_version = "test-code-v1"
  ) |>
    getFromNamespace(
      "dr_step_land",
      "dataraft.lake"
    )(dataraft.core::dr_source_file("risk.source", path, reader = reader)) |>
    getFromNamespace("dr_step_extract", "dataraft.lake")() |>
    getFromNamespace("dr_step_validate", "dataraft.lake")(contract) |>
    getFromNamespace("dr_step_publish", "dataraft.lake")("risk.validated")
  list(
    root = root,
    lake = lake,
    path = path,
    good = good,
    write = write,
    contract = contract,
    pipeline = pipeline
  )
}
fixture_cleanup <- function(f) {
  dataraft.lake::dr_disconnect_lake(f$lake)
  unlink(f$root, recursive = TRUE)
}
