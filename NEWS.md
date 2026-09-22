# dataraft.lake 0.1.0.9001

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

* `dr_cleanup()` also removes expired unpublished quarantine candidates, while
  leaving release tables and blocked-run evidence protected.
* `dr_cleanup()`, `dr_recover()` and `dr_expire_snapshots()` coordinate with
  PostgreSQL publishers through an exclusive maintenance gate.
* `dr_verify_releases()` checks stored content, counts and registry references.
  Registry v6 preserves older history as unverified instead of backfilling hashes.
* Failed publication commits clean up unpublished candidates when possible;
  crash leftovers remain eligible for explicit retention cleanup.

* Diagnostic providers now implement public S3 methods; status, quality and lineage no longer require reverse calls from core into extension packages.

* `dr_write_data()` and `dr_ingest()` require a declared contract for publication.
  Inferred schema evidence is `unvalidated`, never accepted as proof of validity.
  Declared volatile rules cannot reuse cached releases or pass publication gates.
