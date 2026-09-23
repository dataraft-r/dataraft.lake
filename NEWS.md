# dataraft.lake 0.1.0.9006

* Use the current product API throughout code, examples and tests; development compatibility wrappers are removed.
* Catalog integrations are provided exclusively by dataraft.adapters.

# dataraft.lake 0.1.0.9005
* Add persisted product lifecycle transitions and prevent retired lake releases.
* `dr_backfill()` replaces bounded date partitions with existing publication gates.

* Adapter capabilities no longer declare partition; partition_by publication is unchanged.
* Schema-evolution tests preserve old releases and require a new contract version.

# dataraft.lake 0.1.0.9004

* Migrate registry to v6 with release content hashes, identity checks and dr_verify_releases().
* Coordinate PostgreSQL publication and maintenance under a shared catalog writer lock.
* Clean quarantine candidates and failed publication tables; preserve committed releases and test interrupted-run recovery.
* Propagate unvalidated and volatile evidence into table and model release metadata.

* Use the umbrella CI manifest as the single immutable family dependency lock.

# dataraft.lake 0.1.0.9000

* Full CI now rejects skipped test blocks and records per-test summaries and
  explicit skip reasons as check artifacts. DuckLake integration is enabled in
  the component full check.

* `dr_connect_lake()` integrates with the RStudio/Positron Connections observer to browse published assets, inspect columns and preview bounded data. `dr_refresh_connection()` refreshes external publications; connection and observer lifecycles are isolated from storage operations.

* Recovery excludes symbolic links and Windows directory junctions using non-following filesystem metadata; paths with unknown types are retained.

* Partition native output quarantine rules before release publication and preserve rejected rows on the local result.

* Keep stateless helpers private and prefix shared implementation interfaces with `dr_internal_`. Move component tests into their owning repository; add minimal and downstream CI.

* Classify known connection and writer-lock failures as backend errors while retaining lake classes. Move local configuration regression tests into this package.

* Initial independent DataRaft package.

* Release ordering now uses a transactional catalog counter, independent of writer clocks. Registry v4 migrates in place while preserving reports, lineage and release IDs; legacy timestamp ordering is retained with an explicit warning.
* Publication locks are acquired explicitly per asset on PostgreSQL only during publication, not during readers or transforms; a short shared commit gate protects the counter from concurrent publication conflicts. DuckDB relies on native single-process transaction coordination.
* `dr_cleanup()` includes unpublished scratch tables from successful runs and protects every historical release table.
* `dr_expire_snapshots()` previews DuckLake snapshot expiry and scheduled file cleanup; execution requires an exclusive maintenance window and preserves live release tables.
* `dr_storage_s3()` supports the AWS credential chain, including web identity and instance roles, without embedding credentials in configuration.
* `dr_ingest()` retains the pre-execution contract definition when input callbacks change lexical state. The structural publication gate reuses the input gate's registered definition; changes between runs still require new versions.
* Caching rejects source factories whose mutable state cannot be fingerprinted.
* Concurrent runs of the same asset now use separate staging directories. Stable source definitions retain cache identity, and recovery recognizes both legacy asset slots and individual run slots while protecting live writers.
