'use strict';

const path = require('path');

// Try to load native addon
let native = null;
try {
  native = require('./build/Release/amoradb.node');
} catch (e) {
  // Try alternative paths
  const altPaths = [
    './build/Debug/amoradb.node',
  ];
  for (const p of altPaths) {
    try {
      native = require(p);
      break;
    } catch (_) {}
  }
}

if (!native) {
  throw new Error('AmoraDB: Native addon not found. Reinstall and ensure build toolchain is available.');
}

const DEC = new TextDecoder();

class AmoraDB {
  constructor() {
    this._native = native;
  }

  static open(opts = {}) {
    const db = new AmoraDB();
    const cap = opts.cap || 65536;
    if (!db._native.init(cap)) {
      throw new Error('AmoraDB: init failed');
    }
    return db;
  }

  set(key, value) {
    const val = typeof value === 'string' ? value : String(value);
    return this._native.set(key, val);
  }

  get(key) {
    const result = this._native.get(key);
    return result === null ? null : result;
  }

  has(key) {
    return this._native.has(key);
  }

  delete(key) {
    return this._native.delete(key);
  }

  count() {
    return this._native.count();
  }

  capacity() {
    return this._native.capacity();
  }

  hits() {
    return this._native.hits();
  }

  misses() {
    return this._native.misses();
  }

  ops() {
    return this._native.ops();
  }

  stats() {
    return this._native.stats();
  }

  reset(cap) {
    return this._native.reset(cap || 65536);
  }

  heartbeat() {
    return this._native.heartbeat();
  }

  bench(n) {
    n = n || 1000000;
    return this._native.bench(n);
  }
}

module.exports = AmoraDB;
