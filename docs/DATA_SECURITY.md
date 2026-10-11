# Data Security

The reconciliation engine handles API credentials, vendor market data, and—when
real blotters replace the synthetic examples—commercially sensitive trade data.
Security therefore applies to secrets, data access, retention, and operator
output, not only to network encryption.

## Implemented Local Controls

- `project.env` is excluded from Git and must be owner-readable/writable only.
- Provider credentials are loaded only when a live provider call is made. Their
  values are never included in exceptions or dashboard queries.
- DuckDB files are changed to mode `0600` after database initialization on POSIX
  systems. Raw, normalized, and reconciliation Parquet archives receive the same
  owner-only permissions when written.
- The dashboard connects to DuckDB with `read_only=True` and queries curated run,
  result, and exception tables. It cannot update or resolve exceptions.
- Raw market-data staging is excluded from the dashboard to reduce unnecessary
  exposure.

The corresponding implementation lives in `reconciliation_engine/security.py`,
the provider ingestion package, `reconciliation_engine/storage/duckdb.py`, and
`reconciliation_engine/dashboard.py`. Automated tests verify private-file
enforcement and read-only analytics loading.

These controls protect a single-user local MVP. A shared or deployed service
also needs the controls below.

## Production Control Plan

| Area | Required control | Evidence |
| --- | --- | --- |
| Secrets | Store keys in a secrets manager; rotate on exposure and on schedule | Rotation log and secret-access audit |
| Transport | TLS for provider APIs, alerts, dashboard access, and service-to-service traffic | TLS configuration and certificate monitoring |
| Storage | Encrypt databases, Parquet archives, backups, and persistent volumes | Encryption configuration and restore test |
| Access | Service identities and role-based access for operator, investigator, and administrator roles | Quarterly access review |
| Data minimization | Keep only fields needed for reconciliation; mask counterparties and account identifiers in views and alerts | Approved field inventory |
| Retention | Apply explicit retention to raw payloads, results, exceptions, logs, and backups | Automated deletion report |
| Audit | Record run creation, rule changes, exception state changes, exports, and administrative access | Immutable audit event store |
| Integrity | Hash immutable raw payloads and retain rule/config version with every result | Hash verification and lineage query |
| Recovery | Back up encrypted data and test point-in-time restore | Recovery drill result |

## Analytics and Display Rules

Operational analytics are appropriate because they help an operator decide what
to investigate. Useful measures are break rate by run, largest mismatches,
repeat offenders, open-exception age, SLA breaches, and patterns by source,
instrument, and rule. Avoid showing API credentials, complete raw payloads,
counterparty names, account identifiers, or unrestricted trade-level exports.

Dashboard access should use the same role and masking rules as exports. Counts
and trends may be broadly visible, while row-level evidence should be limited to
people assigned to investigate the exception.

### Data classification for the dashboard

| Data class | Examples | Dashboard rule |
| --- | --- | --- |
| Secret | API keys, tokens, recovery codes | Never query, render, log, or export |
| Restricted trade data | Counterparty, account, notional, internal trade ID | Mask and restrict to assigned investigators |
| Control evidence | Source values, deltas, rule, run ID | Show through curated role-based views |
| Aggregate operations data | Counts, rates, age buckets, SLA totals | Suitable for broader operations monitoring |

Refreshing the dashboard must not write access logs into the reconciliation
database. In a deployed service, send access/export events to a separate,
append-only audit sink so the read-only analytics role remains intact.

Recovery codes belong in an approved password or secrets manager. They must not
be treated as project documentation, even when the local filename is ignored by
Git.

## Incident Response

If a credential may have been exposed, revoke or rotate it at the provider,
replace the local value, review provider access logs, and check Git history and
build logs. Changing the local file alone does not invalidate a copied key.

After rotation, verify the old key is rejected, the new key can perform only the
required read operations, and no plaintext secret appears in shell history,
logs, screenshots, alerts, or committed files.
