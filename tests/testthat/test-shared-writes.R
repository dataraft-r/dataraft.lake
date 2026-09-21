test_that("libpq keyword values preserve quoting without exposing secrets", {
  expect_equal(
    postgres_parameters(
      "host=localhost dbname='team lake' user=test password='a\\'b\\\\c'"
    ),
    list(
      host = "localhost",
      dbname = "team lake",
      user = "test",
      password = "a'b\\c"
    )
  )
  expect_equal(
    postgres_parameters("service=team connect_timeout=10"),
    list(service = "team", connect_timeout = "10")
  )
  for (invalid in c(
    "postgres://secret@host/db",
    "password='secret",
    "drv=secret",
    "password='secret'x"
  )) {
    error <- tryCatch(postgres_parameters(invalid), error = identity)
    expect_s3_class(error, "dataraft_error")
    expect_equal(grepl("secret", conditionMessage(error)), FALSE)
  }
})
