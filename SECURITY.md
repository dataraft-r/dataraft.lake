# Security policy

DataRaft is experimental. Use the current development branch for fixes; older
versions have no promised security-support window. No response-time SLA is offered.

Report a vulnerability privately through the repository's Security tab if private
reporting is enabled, or email the maintainer address in DESCRIPTION. Include the
package version, affected integration, a minimal synthetic reproducer and impact.
Do not post credentials, personal data, live connection strings or sensitive rows
in a public issue. Ordinary reproducible bugs belong in Issues.

Connection factories and R transformations execute trusted project code. RDS is
for trusted files only. Checksums detect changes, not malicious replacement.
YAML/ODCS import does not evaluate expressions; supported predicates use an
allowlist. The host backend owns authentication, encryption, authorization,
retention, object locking and deletion. See the guarantees vignette.
