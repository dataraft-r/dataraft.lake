library(testthat)
library(dataraft.lake)
results <- test_check("dataraft.lake", stop_on_failure = FALSE)
summary <- as.data.frame(results)
# Keep the denominator explicit: assertions and test blocks are different units.
utils::write.csv(
  summary[setdiff(names(summary), "result")],
  "test-summary.csv",
  row.names = FALSE
)
skips <- lapply(seq_along(results), function(i) {
  block <- results[[i]]
  skipped <- Filter(function(x) inherits(x, "expectation_skip"), block$results)
  if (!length(skipped)) {
    return(NULL)
  }
  data.frame(
    file = block$file,
    test = block$test,
    reason = vapply(skipped, conditionMessage, character(1))
  )
})
skips <- do.call(rbind, skips)
if (is.null(skips)) {
  skips <- data.frame(
    file = character(),
    test = character(),
    reason = character()
  )
}
utils::write.csv(skips, "test-skips.csv", row.names = FALSE)
cat(sprintf(
  "\ndataraft.lake: %d test blocks, %d passed assertions, %d skipped blocks\n",
  nrow(summary),
  sum(summary$passed),
  sum(summary$skipped)
))
if (!nrow(summary) || any(summary$failed > 0L | summary$error)) {
  stop("Package tests failed or returned no results")
}
if (
  identical(Sys.getenv("DATARAFT_REQUIRE_ALL_TESTS"), "true") &&
    any(summary$skipped)
) {
  print(skips)
  stop("Full CI requires every test block to run; see test-skips.csv")
}
