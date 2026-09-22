test_that("verification detects edits, missing data and broken registry references", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  published <- dr_run(f$pipeline, f$lake)
  expect_equal(dr_verify_releases(f$lake)$status, "verified")
  release <- dr_releases(f$lake)[1, ]
  table <- table_sql(f$lake, release$schema_name[[1]], release$table_name[[1]])
  exec(f$lake, paste("UPDATE", table, "SET reserve = reserve + 1"))
  expect_equal(dr_verify_releases(f$lake)$status, "changed")
  exec(f$lake, paste("DROP TABLE", table))
  expect_equal(dr_verify_releases(f$lake)$status, "missing")
  exec(f$lake, "UPDATE lake._dl.releases SET run_id = 'missing-run'")
  expect_equal(dr_verify_releases(f$lake)$status, "registry_inconsistent")
  expect_error(dr_verify_releases(f$lake, "absent"), class = "dr_no_release")
})

test_that("content hashes ignore row order but retain duplicate multiplicity", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  materialize(f$lake, f$good, "raw", "hash_a")
  materialize(f$lake, f$good[2:1, ], "raw", "hash_b")
  materialize(f$lake, rbind(f$good, f$good[1, ]), "raw", "hash_c")
  a <- release_content(f$lake, "raw", "hash_a")
  expect_identical(a, release_content(f$lake, "raw", "hash_b"))
  expect_false(identical(a$content_hash, release_content(f$lake, "raw", "hash_c")$content_hash))
})

test_that("v5 migration retains evidence without backfilling trusted hashes", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  dr_run(f$pipeline, f$lake)
  evidence <- dr_registry(f$lake, "quality_results")
  exec(f$lake, "ALTER TABLE lake._dl.releases DROP COLUMN content_hash")
  exec(f$lake, "ALTER TABLE lake._dl.releases DROP COLUMN row_count")
  exec(f$lake, "UPDATE lake._dl.schema_version SET version = 5")
  registry_init(f$lake)
  expect_identical(dr_registry(f$lake, "quality_results"), evidence)
  expect_identical(dr_registry(f$lake, "schema_version")$version, 6L)
  expect_equal(dr_verify_releases(f$lake)$status, "unverified")
})

test_that("framework inserts reject duplicate run and release identifiers", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  result <- dr_run(f$pipeline, f$lake)
  expect_error(insert_meta(f$lake, "runs", list(run_id = result$run_id)), class = "dr_registry_duplicate")
  expect_error(insert_meta(f$lake, "releases", list(release_id = result$release_id)), class = "dr_registry_duplicate")
})

test_that("hashes are independent of fetch chunk boundaries", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  data <- data.frame(id = seq_len(1003), text = rep(c("é", NA_character_, ""), length.out = 1003))
  materialize(f$lake, data, "raw", "chunk_forward")
  materialize(f$lake, data[1003:1, ], "raw", "chunk_reverse")
  expect_identical(release_content(f$lake, "raw", "chunk_forward"),
    release_content(f$lake, "raw", "chunk_reverse"))
})

test_that("verification detects duplicate releases and missing parent references", {
  f <- fixture()
  withr::defer(fixture_cleanup(f))
  dr_run(f$pipeline, f$lake)
  exec(f$lake, "UPDATE lake._dl.releases SET parent_release = 'missing-parent'")
  expect_equal(dr_verify_releases(f$lake)$status, "registry_inconsistent")
  exec(f$lake, "UPDATE lake._dl.releases SET parent_release = NULL")
  exec(f$lake, "INSERT INTO lake._dl.releases SELECT * FROM lake._dl.releases")
  expect_equal(dr_verify_releases(f$lake)$status, rep("registry_inconsistent", 2))
})
