# Runs from a component checkout; sibling sources are available under family/.
root <- normalizePath(".", winslash = "/")
component <- read.dcf("DESCRIPTION", fields = "Package")[[1L]]
paths <- c(
  stats::setNames(root, component),
  stats::setNames(
    file.path(
      root,
      "family",
      paste0(
        "dataraft.",
        c("core", "lake", "adapters", "metrics", "dbt", "catalog")
      )
    ),
    paste0(
      "dataraft.",
      c("core", "lake", "adapters", "metrics", "dbt", "catalog")
    )
  )
)
paths <- paths[!duplicated(names(paths))]
installed <- character()
install <- function(name) {
  if (name %in% installed) {
    return(invisible(NULL))
  }
  path <- paths[[name]]
  imports <- read.dcf(file.path(path, "DESCRIPTION"), fields = "Imports")[[1L]]
  dependencies <- trimws(sub("\\s*\\(.*", "", strsplit(imports, ",")[[1L]]))
  for (dependency in intersect(dependencies, names(paths))) {
    install(dependency)
  }
  cran <- setdiff(dependencies, c(names(paths), rownames(installed.packages())))
  if (length(cran)) {
    install.packages(cran)
  }
  status <- system2(
    file.path(R.home("bin"), "R"),
    c("CMD", "INSTALL", shQuote(path))
  )
  if (status != 0L) {
    stop("Cannot install ", name)
  }
  installed <<- c(installed, name)
}
install(component)
install.packages(c("testthat", "pkgload"))
# Optional storage and sibling packages must truly be absent in this job.
for (name in setdiff(names(paths), installed)) {
  stopifnot(!requireNamespace(name, quietly = TRUE))
}
stopifnot(
  !requireNamespace("duckdb", quietly = TRUE),
  !requireNamespace("arrow", quietly = TRUE)
)
testthat::test_local(stop_on_failure = TRUE)
