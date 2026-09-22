test_that("unsupported local formats are rejected without rewriting configuration", {
  root <- withr::local_tempdir()
  marker <- file.path(root, "dataraft.json")
  content <- '{"format":1,"backend":"duckdb"}'
  writeLines(content, marker)
  expect_snapshot(error = TRUE, dr_lake_config(path = root))
  expect_identical(readLines(marker), content)
})

test_that("corrupt current layer settings are rejected without rewriting them", {
  root <- withr::local_tempdir()
  marker <- file.path(root, "dataraft.json")
  content <- '{"format":2,"backend":"duckdb","layers":["raw","raw"]}'
  writeLines(content, marker)
  expect_snapshot(error = TRUE, dr_lake_config(path = root))
  expect_identical(readLines(marker), content)
})

test_that("S3 credential chains remain connection-free configurations", {
  withr::local_envvar(AWS_ACCESS_KEY_ID = NA, AWS_SECRET_ACCESS_KEY = NA)
  storage <- dr_storage_s3(
    "bucket",
    endpoint = "https://s3.example.org",
    credential_provider = "credential_chain",
    credential_chain = "env;web_identity;instance"
  )
  expect_identical(storage$credential_provider, "credential_chain")
  expect_identical(storage$credential_chain, "env;web_identity;instance")
  expect_equal(
    grepl("KEY|SECRET|TOKEN", paste(names(storage), collapse = " ")),
    FALSE
  )
  expect_error(
    dr_storage_s3(
      "bucket",
      endpoint = "https://s3.example.org",
      credential_chain = "env"
    ),
    class = "dataraft_error_lake"
  )
})
