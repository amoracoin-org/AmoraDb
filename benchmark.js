'use strict';

const AmoraDB = require('./amora.js');
const { performance } = require('perf_hooks');
const fs = require('fs');

/**
 * Real-world Benchmark for AmoraDB
 * 
 * Unlike the internal C benchmark, this test utilizes:
 * 1. The JavaScript <-> WebAssembly bridge (real overhead).
 * 2. Variable-sized data (32B keys, 1KB values).
 * 3. Random access patterns (Random keys).
 * 4. Real multi-threading via Worker threads.
 * 5. Latency measurement (P50, P99).
 */

const CONFIG = {
  NUM_OPS: 500_000,
  KEY_SIZE: 32,      // bytes
  VALUE_SIZE: 1024,  // bytes (1KB)
  THREADS: [1, 4, 8],
  WAL_SYNC: [false, true]
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
  console.log(`   Config: ${opts.threads} threads | WAL: ${opts.walPath ? (opts.walSync ? 'SYNC' : 'ASYNC') : 'OFF'}`);

  const db = AmoraDB.open(null, {
    threads: opts.threads,
    cap: 262144, // 256K entries initial
    walPath: opts.walPath,
    walSync: opts.walSync
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

  console.log(`\n   WRITE (Sync):`);
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

  // --- BUFFERED WRITE (Parallel) ---
  if (opts.threads > 1) {
    start = performance.now();
    for (let i = 0; i < CONFIG.NUM_OPS; i++) {
      const k = keys[i % keys.length] + i + '_buf';
      db.setBuffered(k, value);
      if (i % 10000 === 0 && i > 0) {
        await db.flush();
      }
    }
    await db.flush();
    end = performance.now();
    totalTime = (end - start) / 1000;
    throughput = CONFIG.NUM_OPS / totalTime;
    console.log(`   WRITE (Buffered/Parallel):`);
    console.log(`     Throughput: ${fmt(throughput.toFixed(0))} ops/s`);
  }

  await db.close();
  if (opts.walPath && fs.existsSync(opts.walPath)) fs.unlinkSync(opts.walPath);
}

async function main() {
  console.log('=== AMORADB REAL-WORLD BENCHMARK ===');
  
  // 1. Pure In-Memory (Baseline)
  await runBenchmark('Pure In-Memory', { threads: 1, walPath: null });

  // 2. With Workers (Parallelism)
  await runBenchmark('Multi-threaded (Workers)', { threads: 4, walPath: null });

  // 3. With Persistence (WAL ASYNC)
  await runBenchmark('Persistence (WAL ASYNC)', { threads: 1, walPath: './bench_async.wal', walSync: false });

  // 4. With Persistence (WAL SYNC - Fsync)
  // Note: This will be MUCH slower depending on your disk/SSD
  await runBenchmark('Persistence (WAL SYNC)', { threads: 1, walPath: './bench_sync.wal', walSync: true });

  console.log('\n✅ Benchmark completed.');
}

main().catch(console.error);
