#!/usr/bin/env node
/* Dependency-free authenticated API load test. */
const fs = require("fs");
const { performance } = require("perf_hooks");

const baseUrl = process.env.RECONCILIATION_API_URL || "http://127.0.0.1:8000";
const token = process.env.RECONCILIATION_EXPORT_API_KEY;
const requests = Number(process.env.LOAD_TEST_REQUESTS || 200);
const levels = (process.env.LOAD_TEST_CONCURRENCY || "1,10,25,50").split(",").map(Number);
if (!token) throw new Error("RECONCILIATION_EXPORT_API_KEY is required");

function percentile(values, fraction) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

async function run(concurrency) {
  let next = 0;
  const latencies = [];
  let errors = 0;
  const started = performance.now();
  async function worker() {
    while (next < requests) {
      next += 1;
      const requestStarted = performance.now();
      try {
        const response = await fetch(`${baseUrl}/api/v1/reconciliations?limit=100`, {headers: {Authorization: `Bearer ${token}`}});
        if (!response.ok) errors += 1;
        await response.arrayBuffer();
      } catch (_) { errors += 1; }
      latencies.push(performance.now() - requestStarted);
    }
  }
  await Promise.all(Array.from({length: concurrency}, worker));
  const seconds = (performance.now() - started) / 1000;
  return {concurrency, requests, requests_per_second: requests / seconds, p50_ms: percentile(latencies, .50), p95_ms: percentile(latencies, .95), p99_ms: percentile(latencies, .99), error_rate: errors / requests};
}

(async () => {
  const results = [];
  for (const level of levels) results.push(await run(level));
  fs.mkdirSync("benchmarks/results", {recursive: true});
  const header = Object.keys(results[0]);
  fs.writeFileSync("benchmarks/results/api_load_results.csv", header.join(",") + "\n" + results.map(row => header.map(key => Number.isFinite(row[key]) ? row[key].toFixed(3) : row[key]).join(",")).join("\n") + "\n");
  console.table(results);
  if (results.some(row => row.error_rate > 0.01)) process.exitCode = 1;
})();
