# dataraft.lake

Publish checked releases to DuckDB or DuckLake. Use `dr_open_lake()` and `dr_close_lake()` for resource ownership, and `dr_target_lake()` to bind a workflow destination. DuckDB is required for execution.

This is an independently installable DataRaft component. The `dataraft`
metapackage provides the shared introduction and re-exports the family API.
See `help(package = "dataraft.lake")` for the component reference.

Install the development version:

```r
install.packages("pak")
pak::pak("dataraft-r/dataraft.lake")
```

[Get started with DataRaft](https://github.com/dataraft-r/dataraft).

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

Schema v5 assigns release order inside the publication transaction, independently
of writer clocks. Opening v4 writable migrates metadata in place and retains all
release IDs, reports and lineage. The migration warns that the previous
clock/hash order is preserved; it cannot reconstruct past clock drift. Back up
catalogs before upgrading and use an exclusive upgrade window. Stop all older
clients before reopening for production; migration also acquires the legacy
PostgreSQL writer lock while copying release order. Read-only clients require a
migrated catalog.

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

## Release verification and maintenance

Registry v6 adds a row count and content fingerprint to each new publication,
inside its publication transaction. `dr_verify_releases(lake)` scans stored data
and reports `verified`, `changed`, `missing`, `unreadable`, `unverified` or
`registry_inconsistent`. Migration from v4/v5 preserves releases, reports and
quality evidence. Old releases without a publication fingerprint remain
`unverified`; reopening does not manufacture a trusted baseline.

Fingerprints include column names/types and the multiset of stored rows, so
reordering rows does not change a release. Verification fetches 1,000 rows at a
time and retains one digest per row for sorting. It detects accidental edits,
not an administrator able to rewrite both data and registry. The fingerprint
format depends on the R/DBI representation; portability across driver/type or
serialization changes is not guaranteed. Run verification without external
writers to ensure a consistent view.

Framework inserts reject duplicate run/release IDs and verification detects
broken run/parent references. This is not a database constraint against direct
SQL. [DuckLake does not support UNIQUE or PRIMARY KEY constraints](https://ducklake.select/docs/stable/duckdb/advanced_features/constraints),
so DataRaft does not install nonportable DDL that would fail on that backend.

For PostgreSQL catalogs, every publication holds a shared maintenance gate,
including staging and candidate creation; maintenance takes its exclusive side.
Different assets can still prepare concurrently. `dr_cleanup()`, `dr_recover()`
and snapshot expiry therefore cannot race with cooperating DataRaft publishers.
Direct database clients do not participate. Local DuckDB catalogs still require
one application writer. `readers_quiescent` remains the caller's assertion about
external readers, not a remote-reader detector.

Failed publication commits remove their unpublished candidate when the
connection remains usable. Blocked quality runs retain diagnostic tables.
Process crashes and lost connections can leave tables; `dr_cleanup()` removes
expired `raw_<run>`, `candidate_<run>` and `candidate_<run>_clean` tables while
protecting all published tables. With the plain DuckDB backend, table data lives
in the registry `.db` file; `dr_storage_local()` configures DuckLake object files,
not a second data directory for plain DuckDB tables.
