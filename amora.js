'use strict';



const fs   = require('fs');

const path = require('path');

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');



const DEC = new TextDecoder();

const ENC = new TextEncoder();


const CMD_BUF_SIZE      = 1 << 19;

const AUTO_FLUSH_SIZE   = (CMD_BUF_SIZE * 9) / 10 | 0;

const MAX_THREADS       = 8;

const WORKER_TIMEOUT_MS = 60_000;

const WAL_PERSIST_EVERY = 128;


let _MAX_KEY_SIZE   = 4096;

let _MAX_VALUE_SIZE = 1 << 20;



const SC_SIZE  = 16384;

const SC_MASK  = SC_SIZE - 1;

const _scKey   = new Array(SC_SIZE).fill(null);

const _scEnc   = new Array(SC_SIZE).fill(null);

const _scAge   = new Uint32Array(SC_SIZE);

let   _scTick  = 0;

let   _lastKey = null, _lastBytes = null;


function fnv1a(s) {

  let h = 0x811c9dc5;

  for (let i = 0, len = s.length; i < len; i++) {

    h ^= s.charCodeAt(i);

    h  = Math.imul(h, 0x01000193);

  }

  return h >>> 0;

}


function enc(s) {

  if (s === _lastKey) return _lastBytes;

  const slot = fnv1a(s) & SC_MASK;

  if (_scKey[slot] === s) {

    _lastKey = s; _lastBytes = _scEnc[slot];

    _scAge[slot] = ++_scTick;

    return _lastBytes;

  }

  const b = ENC.encode(s);

  _scKey[slot] = s; _scEnc[slot] = b;

  _scAge[slot] = ++_scTick;

  _lastKey = s; _lastBytes = b;

  return b;

}




const _encBuf = new Uint8Array(MAX_THREADS > 0 ? 65536 : 4096);


function writeStr(dst, off, s) {


  const len = s.length;

  let isAscii = true;

  for (let i = 0; i < len; i++) {

    const c = s.charCodeAt(i);

    if (c >= 128) { isAscii = false; break; }

    dst[off++] = c;

  }

  if (isAscii) return off;



  off -= len;

  const result = ENC.encodeInto(s, dst.subarray(off));

  return off + result.written;

}


function strByteLen(s) {


  let bytes = 0;

  for (let i = 0, len = s.length; i < len; i++) {

    const c = s.charCodeAt(i);

    if (c < 0x80) bytes += 1;

    else if (c < 0x800) bytes += 2;

    else if (c >= 0xD800 && c <= 0xDBFF) { bytes += 4; i++; }

    else bytes += 3;

  }

  return bytes;

}


function isBytes(x) {

  return x != null && (Buffer.isBuffer(x) || x instanceof Uint8Array);

}


function validateKey(kb) {

  if (!kb || kb.length === 0) throw new RangeError('AmoraDB: chave vazia');

  if (kb.length > _MAX_KEY_SIZE) throw new RangeError(`AmoraDB: chave muito longa (${kb.length} > ${_MAX_KEY_SIZE})`);

}


function validateValue(vb) {

  if (vb && vb.length > _MAX_VALUE_SIZE) throw new RangeError(`AmoraDB: valor muito longo (${vb.length} > ${_MAX_VALUE_SIZE})`);

}



function writeSet(dst, off, key, val) {

  const kl = strByteLen(key), vl = strByteLen(val);

  dst[off]     = 1;

  dst[off + 1] = kl & 0xFF; dst[off + 2] = (kl >> 8) & 0xFF;

  dst[off + 3] = vl & 0xFF; dst[off + 4] = (vl >> 8) & 0xFF;

  off += 5;

  off = writeStr(dst, off, key);

  off = writeStr(dst, off, val);

  return off;

}


function writeDel(dst, off, key) {

  const kl = strByteLen(key);

  dst[off]     = 2;

  dst[off + 1] = kl & 0xFF; dst[off + 2] = (kl >> 8) & 0xFF;

  dst[off + 3] = 0; dst[off + 4] = 0;

  off += 5;

  off = writeStr(dst, off, key);

  return off;

}


function writeKeyOnly(dst, off, key) {

  const kl = strByteLen(key);

  dst[off]     = kl & 0xFF; dst[off + 1] = (kl >> 8) & 0xFF;

  off += 2;

  off = writeStr(dst, off, key);

  return off;

}


function writeSetBytes(dst, off, kb, vb) {

  const kl = kb.length >>> 0, vl = vb.length >>> 0;

  dst[off]     = 1;

  dst[off + 1] = kl & 0xFF; dst[off + 2] = (kl >> 8) & 0xFF;

  dst[off + 3] = vl & 0xFF; dst[off + 4] = (vl >> 8) & 0xFF;

  off += 5;

  dst.set(kb, off); off += kl;

  dst.set(vb, off); off += vl;

  return off;

}


function writeDelBytes(dst, off, kb) {

  const kl = kb.length >>> 0;

  dst[off]     = 2;

  dst[off + 1] = kl & 0xFF; dst[off + 2] = (kl >> 8) & 0xFF;

  dst[off + 3] = 0; dst[off + 4] = 0;

  off += 5;

  dst.set(kb, off); off += kl;

  return off;

}


function writeKeyOnlyBytes(dst, off, kb) {

  const kl = kb.length >>> 0;

  dst[off]     = kl & 0xFF; dst[off + 1] = (kl >> 8) & 0xFF;

  off += 2;

  dst.set(kb, off); off += kl;

  return off;

}



let _hrtimeBase = null;

function makeNowFn() {

  if (typeof process !== 'undefined' && process.hrtime && process.hrtime.bigint) {

    _hrtimeBase = process.hrtime.bigint();

    return function jsNow() {

      return Number(process.hrtime.bigint() - _hrtimeBase) / 1_000_000;

    };

  }

  try {

    const { performance: _perf } = require('perf_hooks');

    return function jsNow() { return _perf.now(); };

  } catch (_) {

    return function jsNow() { return Date.now(); };

  }

}



function makeWalHandlers(walPath, walSync) {

  if (!walPath) {

    return { flush: () => {}, load: () => 0 };

  }


  let _walFd = null;


  return {

    flush(ptr, len, memBuffer) {
      const buf = Buffer.from(new Uint8Array(memBuffer, ptr, len));
      let fd = null;
      try {
        fd = fs.openSync(walPath, 'w');
        fs.writeSync(fd, buf);
        if (walSync !== false) fs.fsyncSync(fd);
        fs.closeSync(fd); fd = null;
      } catch (err) {
        if (fd != null) { try { fs.closeSync(fd); } catch (_) {} }
        throw err;
      }
    },


    load(ptr, maxLen, memBuffer) {

      if (!fs.existsSync(walPath)) return 0;

      let data;

      try { data = fs.readFileSync(walPath); }

      catch (_) { return 0; }

      const n = Math.min(data.length, maxLen);

      new Uint8Array(memBuffer, ptr, n).set(data.subarray(0, n));

      return n;

    },

  };

}



if (!isMainThread && workerData && workerData.__amoraWorker) {

  const { wasmBytes, sharedMem, cmdBufOffset, scratchOffset, tid } = workerData;

  const now = makeNowFn();



  const sharedMemory = new WebAssembly.Memory({

    initial: sharedMem.byteLength >> 16,

    maximum: 65536,

    shared: true,

  });



  const mod  = new WebAssembly.Module(wasmBytes);

  const inst = new WebAssembly.Instance(mod, {

    env: {

      memory:       sharedMemory,

      js_now:       now,

      js_wal_flush: () => {},

      js_wal_load:  () => 0,

    }

  });


  const e = inst.exports;

  if (e.db_set_worker_mode) e.db_set_worker_mode();



  const cmdView = new Uint8Array(sharedMem, cmdBufOffset, CMD_BUF_SIZE);


  parentPort.on('message', (msg) => {

    try {

      if (msg.type === 'exec') {


        const wasmU8 = new Uint8Array(sharedMemory.buffer);

        wasmU8.set(new Uint8Array(sharedMem, cmdBufOffset, msg.cmdOff), cmdBufOffset);

        const n = e.db_exec_cmdbuf_thread(cmdBufOffset, msg.cmdOff, msg.scratchOff);

        parentPort.postMessage({ type: 'done', n });

      } else if (msg.type === 'mget') {

        const wasmU8 = new Uint8Array(sharedMemory.buffer);

        wasmU8.set(new Uint8Array(sharedMem, cmdBufOffset, msg.cmdOff), cmdBufOffset);

        const n = e.db_mget_cmdbuf(cmdBufOffset, msg.cmdOff, 8192);


        const mgetOff = e.db_mget_buf_off ? e.db_mget_buf_off() : 0;

        if (mgetOff) {

          const resultSize = Math.min(n * 8 + 4, 65536);

          new Uint8Array(sharedMem, mgetOff, resultSize).set(

            new Uint8Array(sharedMemory.buffer, mgetOff, resultSize)

          );

        }

        parentPort.postMessage({ type: 'done', n });

      } else {

        parentPort.postMessage({ type: 'done', n: 0 });

      }

    } catch (err) {

      parentPort.postMessage({ type: 'error', message: err.message });

    }

  });


  parentPort.postMessage({ type: 'ready' });

}



class WorkerPool {

  constructor(wasmBytes, sharedMem, n) {

    this._workers = [];

    this._ready   = [];

    this._queue   = [];

    this._n       = n;

    this._robin   = 0;


    for (let i = 0; i < n; i++) {

      const cmdBufOffset = sharedMem.byteLength - CMD_BUF_SIZE * (i + 1);

      const scratchOff   = 8192 * 16 + 64 + i * 8192;


      const w = new Worker(__filename, {

        workerData: {

          __amoraWorker: true,

          wasmBytes, sharedMem,

          tid: i, cmdBufOffset,

          scratchOffset: scratchOff,

        }

      });


      w._tid        = i;

      w._cmdBufOff  = cmdBufOffset;

      w._scratchOff = scratchOff;

      w._busy       = false;

      w._cmdBuf     = new Uint8Array(sharedMem, cmdBufOffset, CMD_BUF_SIZE);

      w._timeoutId  = null;

      w._pending    = null;


      w.on('message', (msg) => {

        if (w._timeoutId) { clearTimeout(w._timeoutId); w._timeoutId = null; }

        if (msg.type === 'ready') {

          this._ready.push(w);

          this._drain();

        } else if (msg.type === 'done') {

          w._busy = false;

          if (w._pending) {

            const { resolve } = w._pending;

            w._pending = null;

            this._ready.push(w);

            this._drain();

            resolve(msg.n);

          }

        } else if (msg.type === 'error') {

          w._busy = false;

          if (w._pending) {

            const { reject } = w._pending;

            w._pending = null;

            this._ready.push(w);

            this._drain();

            reject(new Error('Worker: ' + msg.message));

          }

        }

      });


      w.on('error', (err) => {

        if (w._timeoutId) { clearTimeout(w._timeoutId); w._timeoutId = null; }

        if (w._pending) { w._pending.reject(err); w._pending = null; }

        w._busy = false;


      });


      this._workers.push(w);

    }

  }


  _drain() {

    while (this._ready.length > 0 && this._queue.length > 0) {

      const w = this._ready.shift(), task = this._queue.shift();

      this._dispatch_to(w, task);

    }

  }


  _dispatch_to(w, task) {

    w._busy = true;

    w._pending = task;

    w._timeoutId = setTimeout(() => {

      if (w._pending) {

        const { reject } = w._pending;

        w._pending = null;

        w._busy = false;

        this._ready.push(w);

        this._drain();

        reject(new Error('AmoraDB: worker timeout'));

      }

    }, WORKER_TIMEOUT_MS);

    w.postMessage(task.msg);

  }


  dispatch(worker, msg) {

    return new Promise((resolve, reject) => {

      const task = { msg, resolve, reject };

      const idx = this._ready.indexOf(worker);

      if (!worker._busy && idx !== -1) {

        this._ready.splice(idx, 1);

        this._dispatch_to(worker, task);

      } else {

        this._queue.push(task);

      }

    });

  }



  getNextWorker() {

    const w = this._workers[this._robin % this._n];

    this._robin = (this._robin + 1) % this._n;

    return w;

  }


  getWorker(idx) { return this._workers[idx % this._n]; }


  async terminate() {

    await Promise.all(this._workers.map(w => {

      if (w._timeoutId) clearTimeout(w._timeoutId);

      return w.terminate();

    }));

  }

}



class AmoraDB {

  constructor(inst, mem, walHandlers, opts) {

    this._e           = inst.exports;

    this._mem         = mem;

    this._walHandlers = walHandlers;

    this._walPath     = (opts && opts.walPath) || null;

    this._walSync     = opts && opts.walSync !== false;

    this._inBatch     = 0;

    this._autoFlush   = true;

    this._viewsDirty  = false;

    this._buf         = null;

    this._scratchPtr     = 0;

    this._mgetBufPtr     = 0;

    this._kbufPtr        = 0;

    this._vbufPtr        = 0;

    this._cmdBufPtr      = 0;

    this._scratchVlenOff = 0;

    this._scratchVoffOff = 4;

    this._dv     = null;

    this._u8raw  = null;

    this._u8kbuf = null;

    this._u8vbuf = null;

    this._u8cmd  = null;

    this._cmdOff       = 0;

    this._walPendingOps = 0;

    this._pool      = null;

    this._wasmBytes = null;

    this._nThreads  = 0;


    this._tempAllocs = [];

  }


  static open(wasmPath, opts = {}) {

    let bytes;

    if (isBytes(wasmPath)) {

      bytes = wasmPath;

    } else {

      let p = wasmPath || opts.wasmPath || null;

      if (!p) {

        const dir = __dirname;

        const cands = [

          path.join(dir, 'amora_core_mt_simd.wasm'),

          path.join(dir, 'amora_core_simd.wasm'),

          path.join(dir, 'amora_core_mt.wasm'),

          path.join(dir, 'amora_core.wasm'),

        ];

        for (const c of cands) { if (fs.existsSync(c)) { p = c; break; } }

        if (!p) throw new Error('AmoraDB: nenhum arquivo .wasm encontrado');

      }

      bytes = fs.readFileSync(p);

    }


    const nThreads  = Math.min(opts.threads || 1, MAX_THREADS);

    const useShared = nThreads > 1;

    const extraPages = useShared ? nThreads * 8 : 0;

    const initPages  = (opts.initialPages || 4096) + extraPages;

    const maxPages   = opts.maxPages || 65536;


    let mem;

    try {

      mem = new WebAssembly.Memory({ initial: initPages, maximum: maxPages, shared: true });

    } catch (_) {

      mem = new WebAssembly.Memory({ initial: initPages, maximum: maxPages });

    }


    const now      = makeNowFn();

    const walPath  = opts.walPath || null;

    const walSync  = opts.walSync !== false;

    const handlers = makeWalHandlers(walPath, walSync);


    const mod  = new WebAssembly.Module(bytes);

    const inst = new WebAssembly.Instance(mod, {

      env: {

        memory:  mem,

        js_now:  now,

        js_wal_flush: (ptr, len) => {

          handlers.flush(ptr, len, mem.buffer);

        },

        js_wal_load: (ptr, maxLen) => {

          return handlers.load(ptr, maxLen, mem.buffer);

        },

      }

    });


    const db = new AmoraDB(inst, mem, handlers, opts);

    db._autoFlush = opts.autoFlush !== false;

    db._wasmBytes = bytes;

    db._nThreads  = nThreads;


    if (!db._e.db_init(opts.cap || 65536))

      throw new Error('AmoraDB: db_init falhou — memória insuficiente');


    db._setup();


    if (opts.maxMemoryBytes && db._e.db_set_memory_limit) {

      db._e.db_set_memory_limit(opts.maxMemoryBytes >>> 0);

    }


    if (useShared && mem.buffer instanceof SharedArrayBuffer)

      db._pool = new WorkerPool(bytes, mem.buffer, nThreads);


    if (walPath && fs.existsSync(walPath)) {

      const replayed = db._e.db_restore();

      if (typeof replayed === 'number' && replayed < 0) {

        console.warn(`AmoraDB: replay WAL completou com ${-replayed} registros corrompidos ignorados`);

      }

      try { fs.truncateSync(walPath, 0); } catch (_) {}

    }


    return db;

  }


  _setup() {

    const e = this._e;

    if (e.db_max_key_size)   _MAX_KEY_SIZE   = e.db_max_key_size()   >>> 0;

    if (e.db_max_value_size) _MAX_VALUE_SIZE = e.db_max_value_size() >>> 0;

    this._scratchPtr  = e.db_scratch_off() >>> 0;

    this._kbufPtr     = e.db_kbuf()        >>> 0;

    this._vbufPtr     = e.db_vbuf()        >>> 0;

    this._mgetBufPtr  = e.db_mget_buf_off ? (e.db_mget_buf_off() >>> 0) : this._scratchPtr + 4;

    this._cmdBufPtr   = e.mem_alloc(CMD_BUF_SIZE) >>> 0;

    if (!this._cmdBufPtr) throw new Error('AmoraDB: falha ao alocar CmdBuf');

    if (e.db_scratch_vlen_off) this._scratchVlenOff = e.db_scratch_vlen_off() >>> 0;

    if (e.db_scratch_voff_off) this._scratchVoffOff = e.db_scratch_voff_off() >>> 0;

    this._rebuildViews();

  }


  _rebuildViews() {

    const buf = this._mem.buffer;

    this._buf        = buf;

    this._viewsDirty = false;

    this._dv     = new DataView(buf);

    this._u8raw  = new Uint8Array(buf);

    this._u8kbuf = new Uint8Array(buf, this._kbufPtr, _MAX_KEY_SIZE);

    this._u8vbuf = new Uint8Array(buf, this._vbufPtr, Math.min(_MAX_VALUE_SIZE + 1, buf.byteLength - this._vbufPtr));

    this._u8cmd  = new Uint8Array(buf, this._cmdBufPtr, CMD_BUF_SIZE);

  }


  _checkBuf() {

    if (this._viewsDirty || this._buf !== this._mem.buffer)

      this._rebuildViews();

  }


  _maybeInvalidate() {

    if (this._mem.buffer !== this._buf) this._viewsDirty = true;

  }



  flush() {

    const off = this._cmdOff;

    if (off === 0) {

      if (this._walPath && !this._inBatch && this._walPendingOps > 0) {

        this._e.db_persist();

        this._walPendingOps = 0;

      }

      return;

    }

    this._checkBuf();

    this._e.db_exec_cmdbuf(this._cmdBufPtr, off);

    this._maybeInvalidate();

    this._checkBuf();

    this._cmdOff = 0;

    if (this._walPath && !this._inBatch) {

      this._e.db_persist();

      this._walPendingOps = 0;

    }

  }



  set(key, value) {

    const val = typeof value === 'string' ? value : String(value);

    if (this._cmdOff > 0) this.flush();

    const kb = enc(key), vb = enc(val);

    validateKey(kb); validateValue(vb);

    this._checkBuf();

    this._u8kbuf.set(kb);

    this._u8vbuf.set(vb);

    const ok = this._e.db_set_inplace(kb.length, vb.length) === 1;

    this._maybeInvalidate();

    this._checkBuf();

    if (ok && this._walPath && !this._inBatch) {

      if (++this._walPendingOps >= WAL_PERSIST_EVERY) {

        this._e.db_persist();

        this._walPendingOps = 0;

      }

    }

    return ok;

  }


  setBuffered(key, value) {

    const val = typeof value === 'string' ? value : String(value);

    const kl  = strByteLen(key), vl = strByteLen(val);

    if (kl > _MAX_KEY_SIZE)   throw new RangeError('AmoraDB: chave muito longa');

    if (vl > _MAX_VALUE_SIZE) throw new RangeError('AmoraDB: valor muito longo');

    this._checkBuf();

    if (this._cmdOff + 5 + kl + vl > AUTO_FLUSH_SIZE) {

      this._e.db_exec_cmdbuf(this._cmdBufPtr, this._cmdOff);

      this._maybeInvalidate();

      this._checkBuf();

      this._cmdOff = 0;

      if (this._walPath && !this._inBatch) this._e.db_persist();

    }

    this._cmdOff = writeSet(this._u8cmd, this._cmdOff, key, val);

  }


  setBytes(keyBytes, valueBytes) {

    if (!isBytes(keyBytes) || !isBytes(valueBytes))

      throw new TypeError('setBytes: esperado Uint8Array/Buffer');

    validateKey(keyBytes); validateValue(valueBytes);

    if (this._cmdOff > 0) this.flush();

    this._checkBuf();

    this._u8kbuf.set(keyBytes);

    this._u8vbuf.set(valueBytes);

    const ok = this._e.db_set_inplace(keyBytes.length >>> 0, valueBytes.length >>> 0) === 1;

    this._maybeInvalidate();

    this._checkBuf();

    if (ok && this._walPath && !this._inBatch) {

      if (++this._walPendingOps >= WAL_PERSIST_EVERY) {

        this._e.db_persist();

        this._walPendingOps = 0;

      }

    }

    return ok;

  }


  setSync(key, value) { return this.set(key, value); }



  get(key) {

    if (this._cmdOff > 0) this.flush();

    const kb = enc(key);

    validateKey(kb);

    this._checkBuf();

    this._u8kbuf.set(kb);

    const found = this._e.db_get_inplace(kb.length);

    if (!found) return null;

    const sp   = this._scratchPtr;

    const vlen = this._dv.getUint32(sp + this._scratchVlenOff, true);

    const voff = this._dv.getUint32(sp + this._scratchVoffOff, true);

    if (!voff || !vlen) return null;

    return DEC.decode(this._u8raw.subarray(voff, voff + vlen));

  }


  getBytes(keyBytes) {

    if (!isBytes(keyBytes)) throw new TypeError('getBytes: esperado Uint8Array/Buffer');

    validateKey(keyBytes);

    if (this._cmdOff > 0) this.flush();

    this._checkBuf();

    this._u8kbuf.set(keyBytes);

    const found = this._e.db_get_inplace(keyBytes.length >>> 0);

    if (!found) return null;

    const sp   = this._scratchPtr;

    const vlen = this._dv.getUint32(sp + this._scratchVlenOff, true);

    const voff = this._dv.getUint32(sp + this._scratchVoffOff, true);

    if (!voff || !vlen) return null;

    return Buffer.from(this._u8raw.slice(voff, voff + vlen));

  }



  has(key) {

    if (this._cmdOff > 0) this.flush();

    const kb = enc(key);

    validateKey(kb);

    this._checkBuf();

    this._u8kbuf.set(kb);

    return this._e.db_has_inplace(kb.length) === 1;

  }


  hasBytes(keyBytes) {

    if (!isBytes(keyBytes)) throw new TypeError('hasBytes: esperado Uint8Array/Buffer');

    validateKey(keyBytes);

    if (this._cmdOff > 0) this.flush();

    this._checkBuf();

    this._u8kbuf.set(keyBytes);

    return this._e.db_has_inplace(keyBytes.length >>> 0) === 1;

  }


  hasPrefix(prefix) {

    if (this._cmdOff > 0) this.flush();

    const pb = enc(prefix);

    validateKey(pb);

    this._checkBuf();

    this._u8kbuf.set(pb);

    return this._e.db_has_prefix(this._kbufPtr, pb.length) === 1;

  }


  countPrefix(prefix) {

    if (this._cmdOff > 0) this.flush();

    const pb = enc(prefix);

    validateKey(pb);

    this._checkBuf();

    this._u8kbuf.set(pb);

    return this._e.db_count_prefix(this._kbufPtr, pb.length) >>> 0;

  }



  delete(key) {

    if (this._cmdOff > 0) this.flush();

    const kb = enc(key);

    validateKey(kb);

    this._checkBuf();

    this._u8kbuf.set(kb);

    const ok = this._e.db_delete_inplace(kb.length) === 1;

    this._maybeInvalidate();

    this._checkBuf();

    if (ok && this._walPath && !this._inBatch) {

      if (++this._walPendingOps >= WAL_PERSIST_EVERY) {

        this._e.db_persist();

        this._walPendingOps = 0;

      }

    }

    return ok;

  }


  deleteBytes(keyBytes) {

    if (!isBytes(keyBytes)) throw new TypeError('deleteBytes: esperado Uint8Array/Buffer');

    validateKey(keyBytes);

    if (this._cmdOff > 0) this.flush();

    this._checkBuf();

    this._u8kbuf.set(keyBytes);

    const ok = this._e.db_delete_inplace(keyBytes.length >>> 0) === 1;

    this._maybeInvalidate();

    this._checkBuf();

    if (ok && this._walPath && !this._inBatch) {

      if (++this._walPendingOps >= WAL_PERSIST_EVERY) {

        this._e.db_persist();

        this._walPendingOps = 0;

      }

    }

    return ok;

  }


  deleteSync(key) { return this.delete(key); }



  mget(keys) {

    if (this._cmdOff > 0) this.flush();

    const e = this._e, cmd = this._u8cmd;

    let off = 0;

    this._checkBuf();

    for (let i = 0; i < keys.length; i++) {

      const kl = strByteLen(keys[i]);

      if (kl > _MAX_KEY_SIZE) throw new RangeError('mget: chave muito longa');

      if (off + 2 + kl > CMD_BUF_SIZE) break;

      off = writeKeyOnly(cmd, off, keys[i]);

    }

    const n = e.db_mget_cmdbuf(this._cmdBufPtr, off, keys.length);

    this._maybeInvalidate();

    this._checkBuf();

    const out = new Array(n);

    const dv  = this._dv, mp = this._mgetBufPtr + 4, raw = this._u8raw;

    for (let i = 0; i < n; i++) {

      const base = mp + i * 8;

      const vptr = dv.getUint32(base,     true);

      const vlen = dv.getUint32(base + 4, true);

      out[i] = vptr ? DEC.decode(raw.subarray(vptr, vptr + vlen)) : null;

    }

    return out;

  }


  mgetBytes(keysBytes) {

    if (this._cmdOff > 0) this.flush();

    const e = this._e, cmd = this._u8cmd;

    let off = 0;

    this._checkBuf();

    for (let i = 0; i < keysBytes.length; i++) {

      const kb = keysBytes[i];

      if (!isBytes(kb)) throw new TypeError('mgetBytes: esperado array de Uint8Array/Buffer');

      validateKey(kb);

      const kl = kb.length >>> 0;

      if (off + 2 + kl > CMD_BUF_SIZE) break;

      off = writeKeyOnlyBytes(cmd, off, kb);

    }

    const n = e.db_mget_cmdbuf(this._cmdBufPtr, off, keysBytes.length);

    this._maybeInvalidate();

    this._checkBuf();

    const out = new Array(n), dv = this._dv, mp = this._mgetBufPtr + 4, raw = this._u8raw;

    for (let i = 0; i < n; i++) {

      const base = mp + i * 8;

      const vptr = dv.getUint32(base,     true);

      const vlen = dv.getUint32(base + 4, true);

      out[i] = vptr ? Buffer.from(raw.slice(vptr, vptr + vlen)) : null;

    }

    return out;

  }



  async mgetAsync(keys) {

    if (!this._pool) return this.mget(keys);

    if (this._cmdOff > 0) this.flush();

    const w = this._pool.getNextWorker(), cmd = w._cmdBuf;

    let off = 0;

    for (let i = 0; i < keys.length; i++) {

      const kl = strByteLen(keys[i]);

      if (kl > _MAX_KEY_SIZE) throw new RangeError('mgetAsync: chave muito longa');

      if (off + 2 + kl > CMD_BUF_SIZE) break;

      off = writeKeyOnly(cmd, off, keys[i]);

    }

    await this._pool.dispatch(w, { type: 'mget', cmdOff: off });

    this._checkBuf();

    const dv = this._dv, mp = this._mgetBufPtr + 4;

    const raw = this._u8raw;

    const n   = dv.getUint32(this._mgetBufPtr, true), out = new Array(n);

    for (let i = 0; i < n; i++) {

      const base = mp + i * 8;

      const vptr = dv.getUint32(base,     true);

      const vlen = dv.getUint32(base + 4, true);

      out[i] = vptr ? DEC.decode(raw.subarray(vptr, vptr + vlen)) : null;

    }

    return out;

  }



  async setParallel(entries) {

    if (!this._pool || entries.length < 100) {

      for (const [k, v] of entries) this.set(k, v);

      return;

    }

    if (this._cmdOff > 0) this.flush();

    const n = this._pool._n, sz = Math.ceil(entries.length / n), ops = [];

    for (let t = 0; t < n; t++) {

      const slice = entries.slice(t * sz, (t + 1) * sz);

      if (slice.length === 0) break;

      const w = this._pool.getWorker(t), cmd = w._cmdBuf;

      let off = 0;

      for (const [k, v] of slice) {

        const val = typeof v === 'string' ? v : String(v);

        const kl  = strByteLen(k), vl = strByteLen(val);

        if (kl > _MAX_KEY_SIZE || vl > _MAX_VALUE_SIZE) continue;

        if (off + 5 + kl + vl > AUTO_FLUSH_SIZE) break;

        off = writeSet(cmd, off, k, val);

      }

      ops.push(this._pool.dispatch(w, { type: 'exec', cmdOff: off, scratchOff: w._scratchOff }));

    }

    await Promise.all(ops);

    this._maybeInvalidate();

    this._checkBuf();

  }



  batch(ops) {

    if (this._cmdOff > 0) this.flush();

    const e = this._e, cmd = this._u8cmd;

    e.db_batch_begin();

    this._inBatch = 1;

    try {

      let off = 0;

      for (const op of ops) {

        const val = op.op === 'set'

          ? (typeof op.value === 'string' ? op.value : String(op.value))

          : null;

        const kl = strByteLen(op.key);

        const vl = val ? strByteLen(val) : 0;

        if (kl > _MAX_KEY_SIZE)   throw new RangeError(`batch: chave muito longa: ${op.key}`);

        if (vl > _MAX_VALUE_SIZE) throw new RangeError('batch: valor muito longo');

        const need = 5 + kl + vl;

        if (off + need > AUTO_FLUSH_SIZE) {

          e.db_batch_exec_cmdbuf(this._cmdBufPtr, off);

          this._maybeInvalidate();

          this._checkBuf();

          off = 0;

        }

        off = op.op === 'set'

          ? writeSet(cmd, off, op.key, val)

          : writeDel(cmd, off, op.key);

      }

      if (off > 0) {

        e.db_batch_exec_cmdbuf(this._cmdBufPtr, off);

        this._maybeInvalidate();

        this._checkBuf();

      }

      e.db_batch_commit();

      this._inBatch = 0;

      if (this._walPath) e.db_persist();

    } catch (err) {

      this._inBatch = 0;

      e.db_batch_rollback();

      this._maybeInvalidate();

      this._checkBuf();

      throw err;

    }

  }



  scan(prefix) {

    if (this._cmdOff > 0) this.flush();

    const pb = enc(prefix);

    validateKey(pb);

    this._checkBuf();

    this._u8kbuf.set(pb);

    return this._readScan(this._e.db_scan_prefix(this._kbufPtr, pb.length));

  }


  range(from, to) {

    if (this._cmdOff > 0) this.flush();

    const fb = enc(from), tb = enc(to);

    validateKey(fb); validateKey(tb);

    this._checkBuf();

    const raw = this._u8raw;

    const fp  = this._e.mem_alloc(fb.length) >>> 0;

    const tp  = this._e.mem_alloc(tb.length) >>> 0;

    if (!fp || !tp) throw new Error('AmoraDB: range: falha ao alocar buffers');

    raw.set(fb, fp); raw.set(tb, tp);

    const result = this._readScan(this._e.db_scan_range(fp, fb.length, tp, tb.length));

    return result;

  }


  _readScan(cnt) {

    const dv  = this._dv, sp = this._scratchPtr, raw = this._u8raw;

    const RESULTS_OFF = 16;

    const out = new Array(cnt);

    for (let i = 0; i < cnt; i++) {

      const base = sp + RESULTS_OFF + i * 16;

      const ko = dv.getUint32(base,      true);

      const kl = dv.getUint32(base +  4, true);

      const vo = dv.getUint32(base +  8, true);

      const vl = dv.getUint32(base + 12, true);

      out[i] = {

        key:   ko ? DEC.decode(raw.subarray(ko, ko + kl)) : '',

        value: vo ? DEC.decode(raw.subarray(vo, vo + vl)) : '',

      };

    }

    return out;

  }



  export(maxBytes) {

    if (this._cmdOff > 0) this.flush();

    maxBytes = maxBytes || 128 * 1024 * 1024;

    this._checkBuf();

    const snapPtr = this._e.mem_alloc(maxBytes) >>> 0;

    if (!snapPtr) throw new Error('AmoraDB: export: sem memória');

    const written = this._e.db_export_snapshot(snapPtr, maxBytes) >>> 0;

    if (!written) throw new Error('AmoraDB: export falhou');

    this._maybeInvalidate();

    this._checkBuf();

    return Buffer.from(this._u8raw.slice(snapPtr, snapPtr + written));

  }


  import(buf) {

    if (!isBytes(buf)) throw new TypeError('import: esperado Buffer/Uint8Array');

    if (this._cmdOff > 0) this.flush();

    this._checkBuf();

    const snapPtr = this._e.mem_alloc(buf.length) >>> 0;

    if (!snapPtr) throw new Error('AmoraDB: import: sem memória');

    this._u8raw.set(buf, snapPtr);

    const imported = this._e.db_import_snapshot(snapPtr, buf.length) >>> 0;

    this._maybeInvalidate();

    this._checkBuf();

    return imported;

  }



  gc() {

    if (this._cmdOff > 0) this.flush();

    return this._e.db_gc();

  }


  autoCompact() {

    if (this.fragmentation() > 25) return this.gc();

    return 0;

  }


  fragmentation() {

    return this._e.db_fragmentation_pct ? this._e.db_fragmentation_pct() >>> 0 : 0;

  }


  heartbeat() {

    return this._e.db_heartbeat ? this._e.db_heartbeat() === 1 : true;

  }



  stats() {

    if (this._cmdOff > 0) this.flush();

    const e = this._e;

    const count   = e.db_count();

    const cap     = e.db_capacity();

    const hits    = Number(e.db_hits());

    const misses  = Number(e.db_misses());

    const ops     = Number(e.db_ops ? e.db_ops() : 0);

    const deleted = e.db_deleted_count ? e.db_deleted_count() : 0;

    const write_errors = e.db_write_errors ? Number(e.db_write_errors()) : 0;

    const wal_errors   = e.db_wal_errors   ? Number(e.db_wal_errors())   : 0;

    const compactions  = e.db_compactions  ? Number(e.db_compactions())  : 0;

    const frag_pct     = e.db_fragmentation_pct ? e.db_fragmentation_pct() : 0;

    return {

      count,

      capacity:        cap,

      deleted,

      shards:          e.db_shard_count ? e.db_shard_count() : 64,

      threads:         this._nThreads,

      inline_key_max:  e.db_inline_key_max ? e.db_inline_key_max() : 22,

      max_key_size:    _MAX_KEY_SIZE,

      max_value_size:  _MAX_VALUE_SIZE,

      load:            ((count / Math.max(cap, 1)) * 100).toFixed(1) + '%',

      fragmentation:   frag_pct + '%',

      hit_rate:        (hits + misses) > 0

                         ? ((hits / (hits + misses)) * 100).toFixed(1) + '%'

                         : '0.0%',

      total_ops:       ops,

      write_errors,

      wal_errors,

      compactions,

      arena_kb:        (e.db_arena_used()    / 1024).toFixed(1),

      wal_kb:          (e.db_wal_size()      / 1024).toFixed(1),

      wasm_mb:         (this._buf.byteLength / 1024 / 1024).toFixed(1),

      mem_shared:      this._buf instanceof SharedArrayBuffer,

      in_batch:        !!(e.db_wact && e.db_wact()),

      pending:         this._cmdOff,

      cmd_buf_kb:      (CMD_BUF_SIZE / 1024).toFixed(0),

      cache_slots:     SC_SIZE,

      wal_sync:        this._walSync,

    };

  }


  persist()  { this.flush(); this._e.db_persist(); }

  restore()  { return this._e.db_restore(); }

  reset(cap) { this._e.db_init(cap || 65536); this._setup(); }


  async close() {

    this.flush();

    if (this._walPath) this._e.db_persist();

    if (this._pool) await this._pool.terminate();

  }



  bench(n) {

    if (this._cmdOff > 0) this.flush();

    n = n || 1_000_000;

    const ok = this._e.db_bench(n);

    this._maybeInvalidate();

    this._checkBuf();

    if (!ok) {

      return {

        ops: n, error: 'shard_init_failed',

        write_ms: 'N/A', read_ms: 'N/A', delete_ms: 'N/A', scan_ms: 'N/A',

        write_ops_s: 'N/A', read_ops_s: 'N/A', delete_ops_s: 'N/A',

      };

    }

    const ptr = this._e.db_bench_ptr ? (this._e.db_bench_ptr() >>> 0) : 0;

    if (!ptr) return { ops: n, error: 'no_bench_ptr' };

    const dv   = this._dv;

    const wms  = dv.getFloat64(ptr,      true);

    const rms  = dv.getFloat64(ptr +  8, true);

    const dms  = dv.getFloat64(ptr + 16, true);

    const sms  = dv.getFloat64(ptr + 24, true);

    if (wms < 0) {

      return {

        ops: n, error: 'shard_init_failed',

        write_ms: 'N/A', read_ms: 'N/A', delete_ms: 'N/A', scan_ms: 'N/A',

        write_ops_s: 'N/A', read_ops_s: 'N/A', delete_ops_s: 'N/A',

      };

    }

    const fmt = x => {

      if (!isFinite(x) || x <= 0) return '< 1K';

      const m = x / 1e6;

      return m >= 1 ? m.toFixed(2) + 'M' : (x / 1e3).toFixed(1) + 'K';

    };

    const safeOps = (ops, ms) => {

      if (!isFinite(ms) || ms <= 0) return '> 1B';

      return fmt(ops / (ms / 1000));

    };

    return {

      ops:          n,

      write_ms:     wms < 1 ? (wms * 1000).toFixed(0) + 'µs' : wms.toFixed(1) + 'ms',

      read_ms:      rms < 1 ? (rms * 1000).toFixed(0) + 'µs' : rms.toFixed(1) + 'ms',

      delete_ms:    dms < 1 ? (dms * 1000).toFixed(0) + 'µs' : dms.toFixed(1) + 'ms',

      scan_ms:      sms.toFixed(2) + 'ms',

      write_ops_s:  safeOps(n,     wms),

      read_ops_s:   safeOps(n,     rms),

      delete_ops_s: safeOps(n / 2, dms),

    };

  }

}


module.exports = AmoraDB;

