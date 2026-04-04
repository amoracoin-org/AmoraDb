'use strict';

const AmoraDB = require('./npm/index.js');
const { performance } = require('perf_hooks');

/**
 * Real-world Benchmark for AmoraDB (Native Addon)
 *
 * Measures end-to-end throughput and latency from Node.js into the native addon:
 * - Load (insert N unique keys)
 * - Read hits (existing keys)
 * - Read misses (non-existent keys)
 * - Updates (overwrite existing keys)
 *
 * Notes:
 * - Uses random-looking keys to avoid best-case cache patterns.
 * - Uses a fixed-size value to keep results comparable between runs.
 */

const CONFIG = {
  CAP: 262144,
  KEY_COUNT: 200_000,
  VALUE_SIZE: 1024,
  LAT_SAMPLE: 20_000,
};

function randomStr(len) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

function fmt(n) {
  return n.toLocaleString('en-US');
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

function measurePhase(name, opCount, fn, latSample) {
  const lat = [];
  const t0 = performance.now();
  for (let i = 0; i < opCount; i++) {
    const s0 = performance.now();
    fn(i);
    const s1 = performance.now();
    if (i < latSample) lat.push(s1 - s0);
  }
  const t1 = performance.now();
  lat.sort((a, b) => a - b);
  const sec = (t1 - t0) / 1000;
  const ops = opCount / sec;
  const p50 = percentile(lat, 0.50) * 1000;
  const p99 = percentile(lat, 0.99) * 1000;
  console.log(`\n${name}`);
  console.log(`  ops:        ${fmt(opCount)}`);
  console.log(`  throughput: ${fmt(ops.toFixed(0))} ops/s`);
  console.log(`  latency:    P50 ${p50.toFixed(2)}µs | P99 ${p99.toFixed(2)}µs (sample ${fmt(lat.length)})`);
  return { ops_s: ops, p50_us: p50, p99_us: p99 };
}

async function runBenchmark(name, opts) {
  console.log(`\n🚀 Scenario: ${name}`);

  const db = AmoraDB.open({ cap: opts.cap });

  const keys = new Array(opts.keyCount);
  for (let i = 0; i < opts.keyCount; i++) {
    keys[i] = `k:${i.toString(16)}:${randomStr(16)}`;
  }
  const missingKeys = new Array(opts.keyCount);
  for (let i = 0; i < opts.keyCount; i++) {
    missingKeys[i] = `m:${i.toString(16)}:${randomStr(16)}`;
  }
  const value = 'X'.repeat(opts.valueSize);

  console.log(`  cap:        ${fmt(opts.cap)}`);
  console.log(`  keys:       ${fmt(opts.keyCount)}`);
  console.log(`  valueSize:  ${fmt(opts.valueSize)} bytes`);
  console.log(`  latSample:  ${fmt(opts.latSample)}`);

  console.log('\nWarmup...');
  for (let i = 0; i < Math.min(10_000, opts.keyCount); i++) db.set(keys[i], value);

  const load = measurePhase('LOAD (insert unique keys)', opts.keyCount, (i) => {
    db.set(keys[i], value);
  }, opts.latSample);

  const sanityKey = keys[Math.floor(opts.keyCount / 2)];
  const sanityVal = db.get(sanityKey);
  if (sanityVal !== value) {
    throw new Error('Sanity check failed: get() did not return the expected value.');
  }

  const readHit = measurePhase('READ HIT (existing keys)', opts.keyCount, (i) => {
    const r = db.get(keys[i]);
    if (i < 5 && r !== value) throw new Error('Unexpected value during READ HIT phase.');
  }, opts.latSample);

  const readMiss = measurePhase('READ MISS (non-existent keys)', opts.keyCount, (i) => {
    const r = db.get(missingKeys[i]);
    if (i < 5 && r !== null) throw new Error('Expected null during READ MISS phase.');
  }, opts.latSample);

  const update = measurePhase('UPDATE (overwrite existing keys)', opts.keyCount, (i) => {
    db.set(keys[i], value);
  }, opts.latSample);

  const s = db.stats();
  console.log('\nStats');
  console.log(`  count:   ${fmt(s.count)}`);
  console.log(`  hits:    ${fmt(s.hits)}`);
  console.log(`  misses:  ${fmt(s.misses)}`);

  return { load, readHit, readMiss, update };
}

async function main() {
  console.log('=== AMORADB NATIVE ADDON BENCHMARK ===');
  
  await runBenchmark('In-Memory (Native Addon)', {
    cap: CONFIG.CAP,
    keyCount: CONFIG.KEY_COUNT,
    valueSize: CONFIG.VALUE_SIZE,
    latSample: CONFIG.LAT_SAMPLE,
  });

  console.log('\n✅ Benchmark completed.');
}

main().catch(console.error);
