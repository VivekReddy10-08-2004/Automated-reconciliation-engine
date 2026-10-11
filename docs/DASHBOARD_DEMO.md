# Dashboard Degraded-Mode Demonstrations

Task 20 keeps the DuckDB and ZeroMQ read paths independent. These terminal
captures demonstrate the three required operating modes with synthetic,
non-sensitive data.

## Both Panels Available

![Both batch and real-time panels available](dashboard_demos/both-available.svg)

## Batch Data Unavailable

![Real-time panel remains available when batch data is unavailable](dashboard_demos/batch-unavailable.svg)

## Real-Time Feed Unavailable

![Batch panel remains available when the real-time feed is unavailable](dashboard_demos/realtime-unavailable.svg)

For a live demonstration, start the anomaly scorer, the dashboard with
`--refresh 0.5`, and then the simulated publisher. The dashboard never writes
to DuckDB and creates only a ZeroMQ `SUB` socket.
