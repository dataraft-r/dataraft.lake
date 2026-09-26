# dataraft.lake

**Keep checked data releases and their evidence in a lake.**

Use this package when you need to publish a version, inspect history or reopen an earlier result. It adds storage and release management to products defined in `dataraft.core`. Local DuckDB and optional DuckLake configurations have different requirements; the example uses DuckDB.

[`dataraft` overview](https://github.com/dataraft-r/dataraft) · [Lake reference](https://dataraft-r.github.io/dataraft/packages/dataraft.lake/)

## Try it

Requires the optional `duckdb` package.

```r
library(dataraft.lake)

lake <- dr_open_lake(file.path(tempdir(), "orders-lake"))
orders <- dataraft.core::dr_product(
  "orders", data.frame(id = 1L, amount = 25)
) |>
  dataraft.core::dr_add_contract(c(id = "integer", amount = "numeric")) |>
  dataraft.core::dr_add_quality(~ amount >= 0) |>
  dataraft.core::dr_set_target(dr_target_lake(lake))

result <- dataraft.core::dr_run(orders)
result$status
dr_close_lake(lake)
```

The product is checked before its lake target is published. Close the lake when finished. For a first workflow without DuckDB, start with [core](https://github.com/dataraft-r/dataraft.core) or the [RDS adapter](https://github.com/dataraft-r/dataraft.adapters#try-it).

Install the development package with `pak::pak("dataraft-r/dataraft.lake")`. Read the [lake documentation](https://dataraft-r.github.io/dataraft/packages/dataraft.lake/) before using shared writers or DuckLake storage.

## Further details

## IDE Connections

Opening a lake in RStudio or Positron adds a Connections entry when the IDE
provides its connection observer. Browse the latest published tables and model
members by schema; unpublished raw data and candidates stay out of this browser.
Column discovery reads zero rows and previews read at most 1,000 rows.

Publication refreshes the entry. Use `dr_refresh_connection(lake)` after an
external writer publishes. Closing the final handle removes the active entry;
the pane's Disconnect action closes all handles for that configuration. Local
self-contained folders have executable reopen code. Other configurations show a
hint to recreate your original configuration and credential environment; the
package never saves resolved credentials in IDE metadata. Outside an IDE these
hooks are no-ops, and observer errors do not interrupt storage operations.


## Registry ordering and maintenance

The current registry uses schema v6. Release order is assigned inside the
publication transaction, independently of writer clocks. Registry versions from
earlier development builds are rejected without modification. Create a new lake
with the current package; there is no automatic migration path.

PostgreSQL publication coordination is asset-scoped. Readers and transforms do
not hold a writer lock. A short shared commit gate protects the catalog counter against overlapping
PostgreSQL publication transactions. It does not cover reading or transforming
inputs. Transaction conflicts from non-cooperating clients are surfaced and the
caller can retry the run. Local
DuckDB uses native transaction coordination and remains a single-process writer.
All cooperating clients must use the same current protocol.

`dr_cleanup()` defaults to a preview and removes only expired unpublished
scratch tables from terminal runs, including successful runs. Every historical
release table and its evidence remains protected, so this is not release-history
expiry. For DuckLake, `dr_expire_snapshots()` separately previews snapshot expiry
and delayed cleanup of files scheduled for deletion. Execute only during an
exclusive maintenance window; external snapshot/time-travel users must agree on
the retention horizon. Freshly expired files receive a grace period before later
cleanup. There is no untracked orphan-file deletion.

For S3 role-based credentials, use
`dr_storage_s3(..., credential_provider = "credential_chain")`; an optional
`credential_chain = "env;web_identity;instance"` configures provider order. This
loads DuckDB's AWS extension. Credentials are resolved at execution time and are
not serialized into configuration or evidence.
