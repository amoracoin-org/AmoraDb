'use strict';

const AmoraDB = require('./npm/index.js');
const { performance } = require('perf_hooks');

/**
 * Real-world Benchmark for AmoraDB (Native C Addon)
 * 
 * Tests:
 * 1. JavaScript <-> Native C bridge (real overhead)
 * 2. Variable-sized data (32B keys, 1KB values)
 * 3. Random access patterns
 * 4. Latency measurement (P50, P99)
 */

const CONFIG = {
  NUM_OPS: 500_000,
  KEY_SIZE: 32,      // bytes
  VALUE_SIZE: 1024,  // bytes (1KB)
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

async function runBenchmark(name, opts) {
  console.log(`\n🚀 Scenario: ${name}`);

  const db = AmoraDB.open({
    cap: 262144 // 256K entries initial
  });

  const keys = [];
  for (let i = 0; i < 10000; i++) keys.push(randomStr(CONFIG.KEY_SIZE));
  const value = 'X'.repeat(CONFIG.VALUE_SIZE);

  // --- WRITE ---
  let start = performance.now();
  let latencies = [];

  for (let i = 0; i < CONFIG.NUM_OPS; i++) {
    const k = keys[i % keys.length] + i;
    const t0 = performance.now();
    db.set(k, value);
    const t1 = performance.now();
    if (i < 10000) latencies.push(t1 - t0); // Sample first 10k for latency
    
    if (i % 50000 === 0 && i > 0) {
      process.stdout.write('.');
    }
  }
  let end = performance.now();
  let totalTime = (end - start) / 1000;
  let throughput = CONFIG.NUM_OPS / totalTime;

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.50)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log(`\n   WRITE:`);
  console.log(`     Throughput: ${fmt(throughput.toFixed(0))} ops/s`);
  console.log(`     Latency:    P50: ${(p50 * 1000).toFixed(2)}µs | P99: ${(p99 * 1000).toFixed(2)}µs`);

  // --- READ ---
  start = performance.now();
  latencies = [];
  for (let i = 0; i < CONFIG.NUM_OPS; i++) {
    const k = keys[i % keys.length] + i;
    const t0 = performance.now();
    db.get(k);
    const t1 = performance.now();
    if (i < 10000) latencies.push(t1 - t0);
    
    if (i % 50000 === 0 && i > 0) process.stdout.write('.');
  }
  end = performance.now();
  totalTime = (end - start) / 1000;
  throughput = CONFIG.NUM_OPS / totalTime;

  latencies.sort((a, b) => a - b);
  const rp50 = latencies[Math.floor(latencies.length * 0.50)];
  const rp99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log(`\n   READ:`);
  console.log(`     Throughput: ${fmt(throughput.toFixed(0))} ops/s`);
  console.log(`     Latency:    P50: ${(rp50 * 1000).toFixed(2)}µs | P99: ${(rp99 * 1000).toFixed(2)}µs`);
}

async function main() {
  console.log('=== AMORADB NATIVE C BENCHMARK ===');
  
  // Pure In-Memory
  await runBenchmark('Pure In-Memory (Native C)', {});

  console.log('\n✅ Benchmark completed.');
}

main().catch(console.error);
