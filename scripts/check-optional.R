# Full integration jobs must have every declared optional dependency available.
description <- read.dcf("DESCRIPTION", fields = "Suggests")[[1L]]
packages <- trimws(sub("[(].*", "", strsplit(description, ",")[[1L]]))
available <- vapply(packages, requireNamespace, logical(1), quietly = TRUE)
dir.create("check", showWarnings = FALSE)
utils::write.csv(
  data.frame(package = packages, installed = available),
  "check/optional-dependencies.csv",
  row.names = FALSE
)
if (any(!available)) {
  stop(
    "Full CI cannot skip missing optional packages: ",
    paste(packages[!available], collapse = ", ")
  )
}
