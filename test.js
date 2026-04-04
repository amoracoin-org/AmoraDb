'use strict';

const path = require('path');
const AmoraDB = require('./npm/index.js');

const ok  = (label, val) => console.log(`  ${label}`.padEnd(36), val);
const sep = ()           => console.log();
const hdr = (t)          => console.log(`\n► ${t}`);

let _failed = 0;
const check = (label, condition) => {
  if (condition) ok(label, '✅');
  else { ok(label, '❌'); _failed++; }
};

const BOX_W = 64;
const box = (lines) => {
  console.log('  ┌' + '─'.repeat(BOX_W) + '┐');
  for (const l of lines) {
    const s = '  │ ' + l;
    console.log(s.padEnd(BOX_W + 4) + '│');
  }
  console.log('  └' + '─'.repeat(BOX_W) + '┘');
};

const M   = (n, ms) => ((n / ms) * 1000 / 1_000_000).toFixed(2) + 'M ops/s';
const FMT = (ms)    => ms < 1 ? (ms * 1000).toFixed(0) + 'µs' : ms.toFixed(1) + 'ms';

console.log('\n╔════════════════════════════════════════════════════════════╗');
console.log('║   AmoraDB v2.0 — Native C Addon                            ║');
console.log('║   Pure C · NAPI · 64 Shards · Spinlocks · SIMD · Native   ║');
console.log('╚════════════════════════════════════════════════════════════╝\n');

let db;
try {
  db = AmoraDB.open({
    cap: 65536,
  });

  const s = db.stats();
  console.log(`► Open ✅  (${s.shards} shards · Native C addon)`);
  console.log(`  count: ${s.count} · capacity: ${s.capacity}\n`);
} catch (e) {
  console.error('► Open ❌', e.message);
  process.exit(1);
}

hdr('Sanity');
try {
  check('heartbeat()', db.heartbeat() === true);
} catch (e) {
  console.error('CRASH:', e.message);
  process.exit(1);
}

sep();

hdr('Core API');
try {
  check('set()', db.set('user:1', 'alice') === true);
  check('get() hit', db.get('user:1') === 'alice');
  check('has() hit', db.has('user:1') === true);
  check('get() miss', db.get('missing:key') === null);

  check('update()', db.set('user:1', 'alice2') === true);
  check('get() updated', db.get('user:1') === 'alice2');

  check('delete() hit', db.delete('user:1') === true);
  check('has() after delete', db.has('user:1') === false);
  check('get() after delete', db.get('user:1') === null);
} catch (e) {
  console.error('CRASH:', e.message);
  process.exit(1);
}

sep();

hdr('Large values (up to 1MB)');
try {
  const bigVal = 'X'.repeat(512 * 1024);
  check('set 512KB', db.set('big:key', bigVal) === true);
  const got = db.get('big:key');
  check('get 512KB', got && got.length === bigVal.length);
  check('delete 512KB', db.delete('big:key') === true);
} catch (e) {
  console.error('CRASH large value:', e.message);
  process.exit(1);
}

sep();

hdr('Size validation (safety)');
try {
  const tooLongKeyOk = db.set('x'.repeat(5000), 'val');
  check('Reject key > 4KB', tooLongKeyOk === false);

  const emptyKeyOk = db.set('', 'val');
  check('Reject empty key', emptyKeyOk === false);

  const tooBigValOk = db.set('toobig', 'Y'.repeat((1 << 20) + 1));
  check('Reject value > 1MB', tooBigValOk === false);
} catch (e) {
  console.error('CRASH:', e.message);
  process.exit(1);
}

sep();

hdr('UPDATE (slot reuse)');
try {
  db.set('config:version', '4');
  check('update then get', db.get('config:version') === '4');
  for (let i = 0; i < 1000; i++) db.set('config:version', String(i));
  check('after 1000 updates', db.get('config:version') === '999');
} catch (e) {
  console.error('CRASH:', e.message);
  process.exit(1);
}

sep();

hdr('Stats');
try {
  const s = db.stats();
  check('stats.count is number', typeof s.count === 'number');
  check('stats.capacity is number', typeof s.capacity === 'number');
  check('stats.shards == 64', s.shards === 64);
} catch (e) {
  console.error('CRASH:', e.message);
  process.exit(1);
}

sep();

hdr('Stress: 50,000 keys');
try {
  const { performance } = require('perf_hooks');
  const dbStress = AmoraDB.open({ cap: 131072 });
  const N = 50_000;

  const t0 = performance.now();
  for (let i = 0; i < N; i++)
    dbStress.set(`stress:key:${i}`, `value:${i}`);
  const t1 = performance.now();

  const ms  = t1 - t0;
  const cnt = dbStress.count();
  console.log(`  inserted:  ${cnt.toLocaleString()} keys`);
  console.log(`  time:      ${ms.toFixed(0)}ms`);
  console.log(`  throughput: ${M(N, ms)}`);
  check('count == inserted', cnt === N);

  const t2 = performance.now();
  let hits = 0;
  for (let i = 0; i < N; i++)
    if (dbStress.get(`stress:key:${i}`) !== null) hits++;
  const t3 = performance.now();
  console.log(`  reads:      ${hits.toLocaleString()} hits | ${M(N, t3 - t2)} read`);
  check('reads all hit', hits === N);
} catch (e) {
  console.error('CRASH stress:', e.message);
  process.exit(1);
}

sep();

if (_failed > 0) {
  console.error(`\n❌ Tests failed: ${_failed}`);
  process.exit(1);
}

console.log('\n✅ All tests passed!\n');
process.exit(0);
