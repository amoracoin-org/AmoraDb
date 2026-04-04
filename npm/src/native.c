// AmoraDB Native Binding - Pure C with NAPI
// Uses only node_api.h (built into Node.js)

#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef _WIN32
  #include <windows.h>
  #include <intrin.h>
  #define BENCH_NOW() ((double)GetTickCount64())
#else
  #include <sys/time.h>
  #include <sched.h>
  #define BENCH_NOW() ({ struct timeval tv; gettimeofday(&tv, NULL); tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0; })
#endif

#if defined(__SSE2__) || defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
  #include <emmintrin.h>
  #define AMORA_SSE2 1
#else
  #define AMORA_SSE2 0
#endif

// ============================================================
// CONFIG
// ============================================================
#define MAX_KEY_SIZE 4096
#define MAX_VALUE_SIZE (1 << 20)
#define AMORA_INLINE_KEY_MAX 22
#define N_SHARDS 64
#define GROUP_SIZE 16
#define CTRL_EMPTY 0x80
#define CTRL_DELETED 0xFE

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int32_t i32;
typedef double f64;
typedef uintptr_t uptr;

// ============================================================
// MEMORY
// ============================================================
typedef struct Chunk {
    u8* mem;
    size_t size;
    size_t used;
    struct Chunk* next;
} Chunk;

static Chunk* _chunk_head = NULL;
static Chunk* _chunk_cur  = NULL;

static void* mem_alloc(size_t sz) {
    sz = (sz + 15) & ~15;
    if (!_chunk_cur || _chunk_cur->used + sz > _chunk_cur->size) {
        size_t base = _chunk_cur ? (_chunk_cur->size * 2) : (128u * 1024u * 1024u);
        size_t new_size = base;
        if (new_size < sz) new_size = sz;
        Chunk* c = (Chunk*)malloc(sizeof(Chunk));
        if (!c) return NULL;
        c->mem = (u8*)malloc(new_size);
        if (!c->mem) { free(c); return NULL; }
        c->size = new_size;
        c->used = 0;
        c->next = NULL;
        if (!_chunk_head) _chunk_head = c;
        if (_chunk_cur) _chunk_cur->next = c;
        _chunk_cur = c;
    }
    void* ptr = _chunk_cur->mem + _chunk_cur->used;
    _chunk_cur->used += sz;
    return ptr;
}

static void mem_free_all(void) {
    Chunk* c = _chunk_head;
    while (c) {
        Chunk* nx = c->next;
        if (c->mem) free(c->mem);
        free(c);
        c = nx;
    }
    _chunk_head = NULL;
    _chunk_cur  = NULL;
}

#define MP(off) ((u8*)(uptr)(off))

// ============================================================
// HASH
// ============================================================
static const u64 RS = 0x9e3779b97f4a7c15ULL;
static const u64 RM1 = 0xa0761d6478bd642fULL;
static const u64 RM2 = 0xe7037ed1a0b428dbULL;

static u64 mix64(u64 a, u64 b) {
    u32 al = (u32)a, ah = (u32)(a >> 32);
    u32 bl = (u32)b, bh = (u32)(b >> 32);
    u64 ll = (u64)al * bl, lh = (u64)al * bh, hl = (u64)ah * bl, hh = (u64)ah * bh;
    u64 mid = lh + hl + (ll >> 32);
    return (hh + (mid >> 32)) ^ ((ll & 0xFFFFFFFFULL) | (mid << 32));
}

static u32 r4(const u8* p) { u32 v; memcpy(&v, p, 4); return v; }
static u64 r8(const u8* p) { u64 v; memcpy(&v, p, 8); return v; }

static u32 hash32(const u8* p, u32 len) {
    u64 h = RS ^ (u64)len;

    u32 i = len;
    if (i <= 8u) {
        u64 a = 0;
        if (i >= 4u) {
            a = ((u64)r4(p) << 32) | (u64)r4(p + i - 4u);
        } else if (i > 0u) {
            a = ((u64)p[0] << 16) | ((u64)p[i >> 1] << 8) | (u64)p[i - 1u];
        }
        h = mix64(h ^ RM1, a ^ RM2);
    } else if (i <= 16u) {
        h = mix64(r8(p) ^ RM1, r8(p + i - 8u) ^ h);
    } else {
        const u8* q = p;
        while (i > 16u) {
            h = mix64(r8(q) ^ RM1, r8(q + 8u) ^ h);
            q += 16u;
            i -= 16u;
        }
        h = mix64(r8(q + i - 16u) ^ RM1, r8(q + i - 8u) ^ h);
    }

    u32 r = (u32)(h ^ (h >> 32));
    return r ? r : 1u;
}

#define H2(h) ((h) & 0x7F)
#define H1(h) ((h) >> 7)
#define HSHARD(h) (((h) >> 26) & 63)

// ============================================================
// BLOOM FILTER
// ============================================================
#define BLOOM_BITS (2 * 1024 * 1024)

static inline void bloom_set(u8* bm, u32 h) {
    u32 a = (h * 0x9e3779b9u) & (BLOOM_BITS - 1);
    u32 b = ((h >> 16) * 0x517cc1b7u) & (BLOOM_BITS - 1);
    bm[a >> 3] |= (1u << (a & 7));
    bm[b >> 3] |= (1u << (b & 7));
}

static inline int bloom_test(const u8* bm, u32 h) {
    u32 a = (h * 0x9e3779b9u) & (BLOOM_BITS - 1);
    u32 b = ((h >> 16) * 0x517cc1b7u) & (BLOOM_BITS - 1);
    return (bm[a >> 3] >> (a & 7) & 1) & (bm[b >> 3] >> (b & 7) & 1);
}

// ============================================================
// SLOT
// ============================================================
#pragma pack(push, 1)
typedef struct {
    u32 hash;
    uptr koff;
    uptr voff;
    u16 klen;
    u32 kcap;
    u32 vlen;
    u32 vcap;
    u8 ikey[AMORA_INLINE_KEY_MAX];
} Slot;
#pragma pack(pop)

#define SLOT_SZ (sizeof(Slot))

// ============================================================
// SHARD
// ============================================================
#ifdef _WIN32
  typedef LONG spinlock_t;
  #define SPIN_INIT(x) (*(x) = 0)
  #define SPIN_LOCK(x) while (InterlockedExchange((volatile LONG*)(x), 1) == 1) Sleep(0)
  #define SPIN_UNLOCK(x) InterlockedExchange((volatile LONG*)(x), 0)
#else
  typedef int spinlock_t;
  #define SPIN_INIT(x) (*(x) = 0)
  #define SPIN_LOCK(x) while (__sync_lock_test_and_set(x, 1)) sched_yield()
  #define SPIN_UNLOCK(x) __sync_lock_release(x)
#endif

typedef struct {
    spinlock_t lock;
    uptr ctrl_off, slot_off, bloom_off;
    u32 cap, count, deleted;
    volatile u64 hits, misses;
} Shard;

static Shard _shards[N_SHARDS];

static inline u8* sh_ctrl(Shard* sh) { return MP(sh->ctrl_off); }
static inline Slot* sh_slot(Shard* sh, u32 i) { return (Slot*)(MP(sh->slot_off) + (uptr)i * (uptr)SLOT_SZ); }
static inline u8* sh_bloom(Shard* sh) { return MP(sh->bloom_off); }
static inline void shard_lock(Shard* sh) { SPIN_LOCK(&sh->lock); }
static inline void shard_unlock(Shard* sh) { SPIN_UNLOCK(&sh->lock); }

// ============================================================
// SLAB
// ============================================================
#define SLAB_CLASSES 20
static void* _slab_heads[SLAB_CLASSES];
static spinlock_t _slab_lock = 0;

static u32 slab_class(u32 sz) {
    if (sz <= 16) return 0;
    u32 bits = 0, s = sz - 1;
    if (s >= (1<<16)) { bits += 16; s >>= 16; }
    if (s >= (1<<8)) { bits += 8; s >>= 8; }
    if (s >= (1<<4)) { bits += 4; s >>= 4; }
    if (s >= (1<<2)) { bits += 2; s >>= 2; }
    if (s >= (1<<1)) bits += 1;
    return bits > 3 ? bits - 3 : 0;
}

static u32 slab_size(u32 cls) { return 16u << cls; }

static uptr blk_alloc(u32 sz, u32* cap) {
    if (sz == 0) sz = 16;
    u32 cls = slab_class(sz);
    *cap = slab_size(cls);
    SPIN_LOCK(&_slab_lock);
    if (_slab_heads[cls]) {
        void* head = _slab_heads[cls];
        _slab_heads[cls] = *(void**)head;
        SPIN_UNLOCK(&_slab_lock);
        return (uptr)head;
    }
    SPIN_UNLOCK(&_slab_lock);
    return (uptr)mem_alloc(*cap);
}

// ============================================================
// COPY
// ============================================================
static inline void xcp(u8* d, const u8* s, u32 n) {
    u32 i = 0;
#if AMORA_SSE2
    for (; i + 16u <= n; i += 16u) {
        __m128i v = _mm_loadu_si128((const __m128i*)(s + i));
        _mm_storeu_si128((__m128i*)(d + i), v);
    }
#endif
    for (; i + 8u <= n; i += 8u) {
        u64 v;
        memcpy(&v, s + i, 8);
        memcpy(d + i, &v, 8);
    }
    for (; i < n; i++) d[i] = s[i];
}

static inline void xzero(u8* d, u32 n) {
    u32 i = 0;
#if AMORA_SSE2
    __m128i z = _mm_setzero_si128();
    for (; i + 16u <= n; i += 16u) _mm_storeu_si128((__m128i*)(d + i), z);
#endif
    for (; i < n; i++) d[i] = 0;
}

// ============================================================
// SHARD INIT
// ============================================================
static int shard_init(Shard* sh, u32 cap) {
    if (cap < GROUP_SIZE) cap = GROUP_SIZE;
    while (cap & (cap - 1)) cap++;
    
    uptr co = (uptr)mem_alloc(cap + GROUP_SIZE);
    uptr so = (uptr)mem_alloc((uptr)cap * (uptr)SLOT_SZ);
    uptr bo = (uptr)mem_alloc(BLOOM_BITS / 8);
    
    if (!co || !so || !bo) return 0;
    
    u8* ctrl = MP(co);
    for (u32 i = 0; i < cap + GROUP_SIZE; i++) ctrl[i] = CTRL_EMPTY;
    
    xzero(MP(so), cap * SLOT_SZ);
    xzero(MP(bo), BLOOM_BITS / 8);
    
    sh->ctrl_off = co; sh->slot_off = so; sh->bloom_off = bo;
    sh->cap = cap; sh->count = 0; sh->deleted = 0;
    sh->hits = 0; sh->misses = 0;
    SPIN_INIT(&sh->lock);
    
    return 1;
}

static inline u8* slot_key(const Slot* s) {
    return s->koff ? MP(s->koff) : (u8*)s->ikey;
}

static inline void amora_inc_u64(volatile u64* p) {
#ifdef _WIN32
    InterlockedIncrement64((volatile LONG64*)p);
#else
    __sync_add_and_fetch(p, 1);
#endif
}

// ============================================================
// SHARD FIND
// ============================================================
static Slot* shard_find(Shard* sh, const u8* k, u32 kl, u32 fh) {
    if (!bloom_test(sh_bloom(sh), fh)) {
        amora_inc_u64(&sh->misses);
        return NULL;
    }
    
    u8 fp = H2(fh);
    u32 idx = H1(fh) & (sh->cap - 1);
    u32 mask = sh->cap - 1;
    u8* ctrl = sh_ctrl(sh);
    
    for (u32 probe = 0; probe < sh->cap; probe += GROUP_SIZE) {
        for (u32 i = 0; i < GROUP_SIZE; i++) {
            u32 si = (idx + i) & mask;
            if (ctrl[si] == CTRL_EMPTY) {
                amora_inc_u64(&sh->misses);
                return NULL;
            }
            if (ctrl[si] != fp) continue;
            
            Slot* s = sh_slot(sh, si);
            if (s->hash == fh && s->klen == kl && memcmp(slot_key(s), k, kl) == 0) {
                amora_inc_u64(&sh->hits);
                return s;
            }
        }
        idx = (idx + GROUP_SIZE) & mask;
    }
    
    amora_inc_u64(&sh->misses);
    return NULL;
}

// ============================================================
// SHARD SET
// ============================================================
static int shard_set(Shard* sh, const u8* k, u32 kl, const u8* v, u32 vl, u32 fh) {
    if (!kl || kl > MAX_KEY_SIZE || vl > MAX_VALUE_SIZE) return 0;
    
    u8 fp = H2(fh);
    u32 idx = H1(fh) & (sh->cap - 1);
    u32 mask = sh->cap - 1;
    u8* ctrl = sh_ctrl(sh);
    i32 first_del = -1;
    u32 first_empty = 0;
    
    for (u32 probe = 0; probe < sh->cap; probe += GROUP_SIZE) {
        for (u32 i = 0; i < GROUP_SIZE; i++) {
            u32 si = (idx + i) & mask;
            u8 c = ctrl[si];
            
            if (c == CTRL_EMPTY) { first_empty = si; goto do_insert; }
            if (c == CTRL_DELETED) { if (first_del < 0) first_del = si; continue; }
            if (c != fp) continue;
            
            Slot* s = sh_slot(sh, si);
            if (s->hash == fh && s->klen == kl && memcmp(slot_key(s), k, kl) == 0) {
                if (vl + 1u <= s->vcap) {
                    xcp(MP(s->voff), v, vl);
                    MP(s->voff)[vl] = 0;
                } else {
                    u32 ncap = 0;
                    uptr vo = blk_alloc(vl + 1u, &ncap);
                    if (!vo) return 0;
                    xcp(MP(vo), v, vl);
                    MP(vo)[vl] = 0;
                    s->voff = vo; s->vlen = vl; s->vcap = ncap;
                }
                s->vlen = vl;
                return 1;
            }
        }
        idx = (idx + GROUP_SIZE) & mask;
    }
    return 0;

do_insert: {
    uptr ko = 0;
    u32  kcap = 0;
    uptr vo = 0;
    u32  vcap = 0;
    
    if (AMORA_INLINE_KEY_MAX == 0 || kl > AMORA_INLINE_KEY_MAX) {
        ko = blk_alloc(kl, &kcap);
        if (!ko) return 0;
        xcp(MP(ko), k, kl);
    }
    
    vo = blk_alloc(vl + 1u, &vcap);
    if (!vo) { if (ko) {} return 0; }
    xcp(MP(vo), v, vl);
    MP(vo)[vl] = 0;
    
    u32 target = (first_del >= 0) ? (u32)first_del : first_empty;
    if (first_del >= 0) sh->deleted--;
    
    ctrl[target] = fp;
    Slot* s = sh_slot(sh, target);
    s->hash = fh; s->koff = ko; s->voff = vo;
    s->klen = kl; s->vlen = vl; s->kcap = kcap; s->vcap = vcap;
    
    if (!ko) memcpy(s->ikey, k, kl < AMORA_INLINE_KEY_MAX ? kl : AMORA_INLINE_KEY_MAX);
    
    bloom_set(sh_bloom(sh), fh);
    sh->count++;
    return 1;
    }
}

// ============================================================
// SHARD DELETE
// ============================================================
static int shard_delete(Shard* sh, const u8* k, u32 kl, u32 fh) {
    if (!kl || !bloom_test(sh_bloom(sh), fh)) return 0;
    
    u8 fp = H2(fh);
    u32 idx = H1(fh) & (sh->cap - 1);
    u32 mask = sh->cap - 1;
    u8* ctrl = sh_ctrl(sh);
    
    for (u32 probe = 0; probe < sh->cap; probe += GROUP_SIZE) {
        for (u32 i = 0; i < GROUP_SIZE; i++) {
            u32 si = (idx + i) & mask;
            if (ctrl[si] == CTRL_EMPTY) return 0;
            if (ctrl[si] != fp) continue;
            
            Slot* s = sh_slot(sh, si);
            if (s->hash == fh && s->klen == kl && memcmp(slot_key(s), k, kl) == 0) {
                ctrl[si] = CTRL_DELETED;
                xzero((u8*)s, SLOT_SZ);
                sh->count--; sh->deleted++;
                return 1;
            }
        }
        idx = (idx + GROUP_SIZE) & mask;
    }
    return 0;
}

// ============================================================
// GLOBAL STATE
// ============================================================
static volatile u64 _ops_total = 0;

// ============================================================
// DB INIT
// ============================================================
static int db_init(u32 cap) {
    mem_free_all();
    u32 c = 1024;
    if (cap > c) { c = 1; while (c < cap) c <<= 1; }
    for (int i = 0; i < SLAB_CLASSES; i++) _slab_heads[i] = NULL;
    SPIN_INIT(&_slab_lock);
    for (int i = 0; i < N_SHARDS; i++) {
        if (!shard_init(&_shards[i], c)) return 0;
    }
    return 1;
}

static u32 db_count(void) { u32 t = 0; for (int i = 0; i < N_SHARDS; i++) t += _shards[i].count; return t; }
static u32 db_capacity(void) { u32 t = 0; for (int i = 0; i < N_SHARDS; i++) t += _shards[i].cap; return t; }
static u64 db_hits(void) { u64 t = 0; for (int i = 0; i < N_SHARDS; i++) t += _shards[i].hits; return t; }
static u64 db_misses(void) { u64 t = 0; for (int i = 0; i < N_SHARDS; i++) t += _shards[i].misses; return t; }

// ============================================================
// NAPI HELPER
// ============================================================
static napi_value make_bool(napi_env env, bool val) {
    napi_value result;
    napi_get_boolean(env, val, &result);
    return result;
}

static napi_value make_uint32(napi_env env, u32 val) {
    napi_value result;
    napi_create_uint32(env, val, &result);
    return result;
}

static napi_value make_double(napi_env env, double val) {
    napi_value result;
    napi_create_double(env, val, &result);
    return result;
}

static napi_value make_string(napi_env env, const char* str) {
    napi_value result;
    napi_create_string_utf8(env, str, NAPI_AUTO_LENGTH, &result);
    return result;
}

static napi_value make_null(napi_env env) {
    napi_value result;
    napi_get_null(env, &result);
    return result;
}

static int get_string_checked(napi_env env, napi_value val, char* buf, size_t max, size_t* out_len) {
    size_t slen = 0;
    napi_get_value_string_utf8(env, val, NULL, 0, &slen);
    if (slen > max) return 0;
    napi_get_value_string_utf8(env, val, buf, slen + 1, &slen);
    buf[slen] = 0;
    *out_len = slen;
    return 1;
}

// ============================================================
// NAPI FUNCTIONS
// ============================================================
static napi_value native_init(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    u32 cap = 65536;
    if (argc > 0) napi_get_value_uint32(env, argv[0], &cap);
    return make_bool(env, db_init(cap) == 1);
}

static napi_value native_set(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    
    char kbuf[MAX_KEY_SIZE + 1];
    char vbuf[MAX_VALUE_SIZE + 1];
    size_t klen = 0, vlen = 0;
    if (!get_string_checked(env, argv[0], kbuf, MAX_KEY_SIZE, &klen)) return make_bool(env, false);
    if (!get_string_checked(env, argv[1], vbuf, MAX_VALUE_SIZE, &vlen)) return make_bool(env, false);
    
    if (klen == 0 || klen > MAX_KEY_SIZE) return make_bool(env, false);
    if (vlen > MAX_VALUE_SIZE) return make_bool(env, false);
    
    u32 fh = hash32((u8*)kbuf, (u32)klen);
    u32 si = HSHARD(fh);
    
    shard_lock(&_shards[si]);
    int ok = shard_set(&_shards[si], (u8*)kbuf, (u32)klen, (u8*)vbuf, (u32)vlen, fh);
    shard_unlock(&_shards[si]);
    
    if (ok) amora_inc_u64(&_ops_total);
    return make_bool(env, ok == 1);
}

static napi_value native_get(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    
    char kbuf[MAX_KEY_SIZE + 1];
    size_t klen = 0;
    if (!get_string_checked(env, argv[0], kbuf, MAX_KEY_SIZE, &klen)) return make_null(env);
    
    if (klen == 0 || klen > MAX_KEY_SIZE) return make_null(env);
    
    u32 fh = hash32((u8*)kbuf, (u32)klen);
    u32 si = HSHARD(fh);
    
    Slot* s;
    shard_lock(&_shards[si]);
    s = shard_find(&_shards[si], (u8*)kbuf, (u32)klen, fh);
    shard_unlock(&_shards[si]);
    
    if (!s) return make_null(env);
    return make_string(env, (char*)MP(s->voff));
}

static napi_value native_has(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    
    char kbuf[MAX_KEY_SIZE + 1];
    size_t klen = 0;
    if (!get_string_checked(env, argv[0], kbuf, MAX_KEY_SIZE, &klen)) return make_bool(env, false);

    if (klen == 0 || klen > MAX_KEY_SIZE) return make_bool(env, false);
    
    u32 fh = hash32((u8*)kbuf, (u32)klen);
    u32 si = HSHARD(fh);
    
    Slot* s;
    shard_lock(&_shards[si]);
    s = shard_find(&_shards[si], (u8*)kbuf, (u32)klen, fh);
    shard_unlock(&_shards[si]);
    
    return make_bool(env, s != NULL);
}

static napi_value native_delete(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    
    char kbuf[MAX_KEY_SIZE + 1];
    size_t klen = 0;
    if (!get_string_checked(env, argv[0], kbuf, MAX_KEY_SIZE, &klen)) return make_bool(env, false);

    if (klen == 0 || klen > MAX_KEY_SIZE) return make_bool(env, false);
    
    u32 fh = hash32((u8*)kbuf, (u32)klen);
    u32 si = HSHARD(fh);
    
    shard_lock(&_shards[si]);
    int ok = shard_delete(&_shards[si], (u8*)kbuf, (u32)klen, fh);
    shard_unlock(&_shards[si]);
    
    return make_bool(env, ok == 1);
}

static napi_value native_count(napi_env env, napi_callback_info info) { return make_uint32(env, db_count()); }
static napi_value native_capacity(napi_env env, napi_callback_info info) { return make_uint32(env, db_capacity()); }
static napi_value native_hits(napi_env env, napi_callback_info info) { return make_uint32(env, (u32)db_hits()); }
static napi_value native_misses(napi_env env, napi_callback_info info) { return make_uint32(env, (u32)db_misses()); }
static napi_value native_ops(napi_env env, napi_callback_info info) { return make_uint32(env, (u32)_ops_total); }

static napi_value native_reset(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    u32 cap = 65536;
    if (argc > 0) napi_get_value_uint32(env, argv[0], &cap);
    return make_bool(env, db_init(cap) == 1);
}

static napi_value native_heartbeat(napi_env env, napi_callback_info info) { return make_bool(env, true); }

static napi_value native_stats(napi_env env, napi_callback_info info) {
    napi_value result;
    napi_create_object(env, &result);
    
    napi_value val;
    napi_create_uint32(env, db_count(), &val); napi_set_named_property(env, result, "count", val);
    napi_create_uint32(env, db_capacity(), &val); napi_set_named_property(env, result, "capacity", val);
    napi_create_uint32(env, (u32)db_hits(), &val); napi_set_named_property(env, result, "hits", val);
    napi_create_uint32(env, (u32)db_misses(), &val); napi_set_named_property(env, result, "misses", val);
    napi_create_uint32(env, (u32)_ops_total, &val); napi_set_named_property(env, result, "total_ops", val);
    napi_create_uint32(env, N_SHARDS, &val); napi_set_named_property(env, result, "shards", val);
    
    return result;
}

// ============================================================
// BENCHMARK
// ============================================================
static double _bench_times[4];

static napi_value native_bench(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    
    u32 n = 1000000;
    if (argc > 0) napi_get_value_uint32(env, argv[0], &n);
    
    // Save state
    u32 saved_caps[N_SHARDS];
    for (int i = 0; i < N_SHARDS; i++) saved_caps[i] = _shards[i].cap;
    
    // Init fresh
    for (int i = 0; i < N_SHARDS; i++) shard_init(&_shards[i], 131072);
    
    // Write benchmark
    u8 keybuf[32], valbuf[32];
    double t = BENCH_NOW();
    for (u32 i = 0; i < n; i++) {
        *(u64*)keybuf = i; *(u64*)(keybuf + 8) = i ^ 0xABCD;
        *(u64*)valbuf = i * 7; *(u64*)(valbuf + 8) = i * 13;
        u32 fh = hash32(keybuf, 24);
        u32 si = HSHARD(fh);
        shard_lock(&_shards[si]);
        shard_set(&_shards[si], keybuf, 24, valbuf, 24, fh);
        shard_unlock(&_shards[si]);
    }
    _bench_times[0] = BENCH_NOW() - t;
    
    // Read benchmark
    t = BENCH_NOW();
    for (u32 i = 0; i < n; i++) {
        *(u64*)keybuf = i; *(u64*)(keybuf + 8) = i ^ 0xABCD;
        u32 fh = hash32(keybuf, 24);
        u32 si = HSHARD(fh);
        shard_lock(&_shards[si]);
        shard_find(&_shards[si], keybuf, 24, fh);
        shard_unlock(&_shards[si]);
    }
    _bench_times[1] = BENCH_NOW() - t;
    
    // Delete benchmark
    t = BENCH_NOW();
    for (u32 i = 0; i < n / 2; i++) {
        *(u64*)keybuf = i; *(u64*)(keybuf + 8) = i ^ 0xABCD;
        u32 fh = hash32(keybuf, 24);
        u32 si = HSHARD(fh);
        shard_lock(&_shards[si]);
        shard_delete(&_shards[si], keybuf, 24, fh);
        shard_unlock(&_shards[si]);
    }
    _bench_times[2] = BENCH_NOW() - t;
    
    // Scan benchmark
    t = BENCH_NOW();
    for (int si = 0; si < N_SHARDS; si++) {
        shard_lock(&_shards[si]);
        for (u32 i = 0; i < _shards[si].cap; i++) {
            if (sh_ctrl(&_shards[si])[i] != CTRL_EMPTY) {}
        }
        shard_unlock(&_shards[si]);
    }
    _bench_times[3] = BENCH_NOW() - t;
    
    // Restore
    for (int i = 0; i < N_SHARDS; i++) shard_init(&_shards[i], saved_caps[i]);
    
    // Result object
    napi_value result;
    napi_create_object(env, &result);
    
    napi_value val;
    napi_create_double(env, _bench_times[0], &val); napi_set_named_property(env, result, "write_ms", val);
    napi_create_double(env, _bench_times[1], &val); napi_set_named_property(env, result, "read_ms", val);
    napi_create_double(env, _bench_times[2], &val); napi_set_named_property(env, result, "delete_ms", val);
    napi_create_double(env, _bench_times[3], &val); napi_set_named_property(env, result, "scan_ms", val);
    napi_create_double(env, n / (_bench_times[0] / 1000.0), &val); napi_set_named_property(env, result, "write_ops_s", val);
    napi_create_double(env, n / (_bench_times[1] / 1000.0), &val); napi_set_named_property(env, result, "read_ops_s", val);
    napi_create_double(env, (n/2) / (_bench_times[2] / 1000.0), &val); napi_set_named_property(env, result, "delete_ops_s", val);
    
    return result;
}

// ============================================================
// MODULE
// ============================================================
#define DECLARE_METHOD(name, func) { name, 0, func, 0, 0, 0, napi_default, 0 }

napi_value Init(napi_env env, napi_value exports) {
    napi_property_descriptor desc[] = {
        DECLARE_METHOD("init", native_init),
        DECLARE_METHOD("set", native_set),
        DECLARE_METHOD("get", native_get),
        DECLARE_METHOD("has", native_has),
        DECLARE_METHOD("delete", native_delete),
        DECLARE_METHOD("count", native_count),
        DECLARE_METHOD("capacity", native_capacity),
        DECLARE_METHOD("hits", native_hits),
        DECLARE_METHOD("misses", native_misses),
        DECLARE_METHOD("ops", native_ops),
        DECLARE_METHOD("stats", native_stats),
        DECLARE_METHOD("bench", native_bench),
        DECLARE_METHOD("reset", native_reset),
        DECLARE_METHOD("heartbeat", native_heartbeat),
    };
    napi_define_properties(env, exports, 14, desc);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
