test_that("README first example runs", {
  skip_if_not_installed("duckdb")
  readme <- test_path("..", "..", "README.md")
  if (!file.exists(readme)) {
    readme <- file.path(Sys.getenv("GITHUB_WORKSPACE"), "README.md")
  }
  if (!file.exists(readme)) skip("README source is unavailable here")
  description <- file.path(dirname(readme), "DESCRIPTION")
  if (!file.exists(description) ||
      read.dcf(description, fields = "Package")[1L] != "dataraft.lake") {
    skip("This checkout does not contain the dataraft.lake README")
  }
  lines <- readLines(readme, warn = FALSE)
  heading <- match("## Try it", lines)
  expect_false(is.na(heading))
  opening <- which(lines == "```r" & seq_along(lines) > heading)[1L]
  closing <- which(lines == "```" & seq_along(lines) > opening)[1L]
  expect_false(is.na(opening))
  expect_false(is.na(closing))
  code <- paste(lines[seq.int(opening + 1L, closing - 1L)], collapse = "\n")
  expect_no_error(eval(parse(text = code), envir = new.env(parent = globalenv())))
})
