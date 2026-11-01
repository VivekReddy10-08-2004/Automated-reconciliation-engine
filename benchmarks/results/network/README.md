# Network benchmark results

- `full/` contains the committed ADR 0007 evidence produced with the published
  workload sizes.
- `quick/` is the default destination for smoke-test suite runs.
- `manual/` is the default destination for individually executed workloads.

Every suite directory contains `environment.json`, which records the selected
profile, workload settings, Python runtime, platform, and CPU count. Compare
runs only when their environment and settings are compatible.
