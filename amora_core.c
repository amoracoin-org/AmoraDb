


#if defined(__wasm_simd128__)

#  include <wasm_simd128.h>

#  define AMORA_SIMD 1

#else

#  define AMORA_SIMD 0

#endif


#if defined(__wasm_threads__) || defined(__wasm_shared_memory__)

#  include <stdatomic.h>

#  include <stdint.h>

#  define AMORA_ATOMICS 1

#else

#  include <stdint.h>

#  define AMORA_ATOMICS 0

#endif



typedef unsigned char      u8;

typedef signed char        i8;

typedef unsigned short     u16;

typedef unsigned int       u32;

typedef unsigned long long u64;

typedef signed int         i32;

typedef signed long long   i64;

typedef double             f64;

typedef uintptr_t          uptr;


#if AMORA_ATOMICS

typedef _Atomic(u32) au32;

typedef _Atomic(u64) au64;

#else

typedef u32 au32;

typedef u64 au64;

#endif


#ifndef NULL

#  define NULL ((void*)0)

#endif



#define INLINE      __attribute__((always_inline)) static inline

#define NOINLINE    __attribute__((noinline))

#define EXPORT      __attribute__((visibility("default")))

#define LIKELY(x)   __builtin_expect(!!(x), 1)

#define UNLIKELY(x) __builtin_expect(!!(x), 0)


#define PREFETCH_R(p) __builtin_prefetch((p), 0, 3)

#define PREFETCH_W(p) __builtin_prefetch((p), 1, 1)



#define MAX_KEY_SIZE      4096u

#define MAX_VALUE_SIZE    (1u << 20)


#define CTRL_EMPTY        0x80u

#define CTRL_DELETED      0xFEu

#define GROUP_SIZE        16u

#define LOAD_NUM          7u

#define LOAD_DEN          8u



#define N_SHARDS_SHIFT    6u

#define N_SHARDS          (1u << N_SHARDS_SHIFT)

#define SHARD_INIT        1024u


#define TOMB_RATIO_PCT    25u


#ifndef AMORA_INLINE_KEY_MAX

#  define AMORA_INLINE_KEY_MAX 22u

#endif



#define BLOOM_BITS_SHIFT  21u

#define BLOOM_BITS        (1u << BLOOM_BITS_SHIFT)

#define BLOOM_MASK        (BLOOM_BITS - 1u)

#define BLOOM_BYTES       (BLOOM_BITS >> 3)



#define WAL_MAGIC         0x414D5257u

#define WAL_VERSION       20u

#define WSET              0x01u

#define WDEL              0x02u

#define WCMT              0xFFu

#define WAL_MAX           (32u * 1024u * 1024u)

#define WAL_HEADER_SZ     8u


#define KBUF_SZ           4096u

#define VBUF_SZ           (MAX_VALUE_SIZE + 1u)



#define SL_LEVELS         16u

#define SL_MAX_NODES      (1u << 20)


#define MAX_THREADS       8u

#define SPIN_LIMIT        64u


#define SCRATCH_PER_THREAD   8192u

#define SCRATCH_VLEN_OFF     0u

#define SCRATCH_VOFF_OFF     4u

#define SCRATCH_COUNT_OFF    8u

#define SCRATCH_RESULTS_OFF  16u

#define SCRATCH_MAX_RESULTS  4096u


#define MGET_BUF_ENTRIES  8192u

#define MGET_BUF_SZ       (MGET_BUF_ENTRIES * 8u)


#define SNAP_MAGIC        0x504D4153u

#define SNAP_VERSION      20u



#define SLAB_CLASSES      32u

#define SLAB_MIN_SIZE     16u

#define SLAB_MAX_SIZE     (SLAB_MIN_SIZE << (SLAB_CLASSES - 1u))



static const u32 _crc32c_tab[256] = {

    0x00000000,0xF26B8303,0xE13B70F7,0x1350F3F4,0xC79A971F,0x35F1141C,0x26A1E7E8,0xD4CA64EB,

    0x8AD958CF,0x78B2DBCC,0x6BE22838,0x9989AB3B,0x4D43CFD0,0xBF284CD3,0xAC78BF27,0x5E133C24,

    0x105EC76F,0xE235446C,0xF165B798,0x030E349B,0xD7C45070,0x25AFD373,0x36FF2087,0xC494A384,

    0x9A879FA0,0x68EC1CA3,0x7BBCEF57,0x89D76C54,0x5D1D08BF,0xAF768BBC,0xBC267848,0x4E4DFB4B,

    0x20BD8EDE,0xD2D60DDD,0xC186FE29,0x33ED7D2A,0xE72719C1,0x154C9AC2,0x064C6936,0xF4270A35,

    0xAA340611,0x585F8512,0x4B0F76E6,0xB964F5E5,0x6DAE910E,0x9FC5120D,0x8C95E1F9,0x7EFE62FA,

    0x30B399B1,0xC2D81AB2,0xD188E946,0x23E36A45,0xF7290EAE,0x05428DAD,0x16127E59,0xE479FD5A,

    0xBA6AC17E,0x4801427D,0x5B51B189,0xA93A328A,0x7DF05661,0x8F9BD562,0x9CCB2696,0x6EA0A595,

    0x417B1DBC,0xB3109EBF,0xA0406D4B,0x5228EE48,0x86E68AA3,0x748D09A0,0x67DDFA54,0x95B67957,

    0xCBA54573,0x39CEC670,0x2A9E3584,0xD8F5B687,0x0C3FD26C,0xFE54516F,0xED04A29B,0x1F6F2198,

    0x5122DAD3,0xA34959D0,0xB019AA24,0x42722927,0x96B84DCC,0x64D3CECF,0x77833D3B,0x85E8BE38,

    0xDBFB821C,0x2990011F,0x3AC0F2EB,0xC8AB71E8,0x1C611503,0xEE0A9600,0xFD5A65F4,0x0F31E6F7,

    0x61C19362,0x93AA1061,0x80FAE395,0x72916096,0xA65B047D,0x5430877E,0x4760748A,0xB50BF789,

    0xEB18CBAD,0x197348AE,0x0A23BB5A,0xF8483859,0x2C825CB2,0xDEE9DFB1,0xCDB92C45,0x3FD2AF46,

    0x719F540D,0x83F4D70E,0x90A424FA,0x62CFA7F9,0xB605C312,0x446E4011,0x573EB3E5,0xA55530E6,

    0xFB460CC2,0x092D8FC1,0x1A7D7C35,0xE816FF36,0x3CDC9BDD,0xCEB718DE,0xDDE7EB2A,0x2F8C6829,

    0x82F63B78,0x70ADBD7B,0x63FD4E8F,0x91969D8C,0x4558F967,0xB7337A64,0xA4638990,0x56080A93,

    0x081B36B7,0xFA70B5B4,0xE9204640,0x1B4BC543,0xCF81A1A8,0x3DEA22AB,0x2EBAD15F,0xDCD1525C,

    0x929CA917,0x60F72A14,0x73A7D9E0,0x81CC5AE3,0x55063E08,0xA76DBD0B,0xB43D4EFF,0x4656CDFC,

    0x1845F1D8,0xEA2E72DB,0xF97E812F,0x0B15022C,0xDFDF66C7,0x2DB4E5C4,0x3EE41630,0xCC8F9533,

    0xA27FE0A6,0x501463A5,0x43449051,0xB12F1352,0x65E577B9,0x978EF4BA,0x84DE074E,0x76B5844D,

    0x28A6B869,0xDACD3B6A,0xC99DC89E,0x3BF64B9D,0xEF3C2F76,0x1D57AC75,0x0E075F81,0xFC6CDC82,

    0xB22127C9,0x404AA4CA,0x531A573E,0xA171D43D,0x75BBB0D6,0x87D033D5,0x9480C021,0x66EB4322,

    0x38F87F06,0xCA93FC05,0xD9C30FF1,0x2BA88CF2,0xFF62E819,0x0D096B1A,0x1E5998EE,0xEC321BED,

    0xC3E9A3C4,0x31822087,0x22D2D383,0xD0B95080,0x0473346B,0xF618B768,0xE548449C,0x1723C79F,

    0x4930FBBB,0xBB5B78B8,0xA80B8B4C,0x5A60084F,0x8EAA6CA4,0x7CC1EFA7,0x6F911C53,0x9DFA9F50,

    0xD3B7641B,0x21DCE718,0x328C14EC,0xC0E797EF,0x142DF304,0xE6467007,0xF51683F3,0x077D00F0,

    0x596E3CD4,0xAB05BFD7,0xB8554C23,0x4A3ECF20,0x9EF4ABCB,0x6C9F28C8,0x7FCFDB3C,0x8DA4583F,

    0xE354AD7A,0x113F2E79,0x026FDD8D,0xF0045E8E,0x24CE3A65,0xD6A5B966,0xC5F54A92,0x379EC991,

    0x698DF5B5,0x9BE676B6,0x88B68542,0x7ADD0641,0xAE1762AA,0x5C7CE1A9,0x4F2C125D,0xBD47915E,

    0xF30A6A15,0x01611916,0x1231BAE2,0xE05A39E1,0x34905D0A,0xC6FBDE09,0xD5AB2DFD,0x27C0AEFE,

    0x79D392DA,0x8BB811D9,0x98E8E22D,0x6A83612E,0xBE4905C5,0x4C2286C6,0x5F727532,0xAD19F631

};


static u32 crc32c(const u8 *data, u32 len) {

    u32 crc = 0xFFFFFFFFu;

    u32 i = 0;


    for (; i + 4u <= len; i += 4u) {

        crc = _crc32c_tab[(crc ^ data[i])     & 0xFFu] ^ (crc >> 8);

        crc = _crc32c_tab[(crc ^ data[i+1u])  & 0xFFu] ^ (crc >> 8);

        crc = _crc32c_tab[(crc ^ data[i+2u])  & 0xFFu] ^ (crc >> 8);

        crc = _crc32c_tab[(crc ^ data[i+3u])  & 0xFFu] ^ (crc >> 8);

    }

    for (; i < len; i++)

        crc = _crc32c_tab[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);

    return crc ^ 0xFFFFFFFFu;

}



#if AMORA_ATOMICS

INLINE u32  au32_load_relaxed(const au32 *p)         { return atomic_load_explicit(p, memory_order_relaxed); }

INLINE u32  au32_load_acquire(const au32 *p)         { return atomic_load_explicit(p, memory_order_acquire); }

INLINE void au32_store_relaxed(au32 *p, u32 v)       { atomic_store_explicit(p, v, memory_order_relaxed); }

INLINE void au32_store_release(au32 *p, u32 v)       { atomic_store_explicit(p, v, memory_order_release); }

INLINE u32  au32_fetch_add_relaxed(au32 *p, u32 v)   { return atomic_fetch_add_explicit(p, v, memory_order_relaxed); }

INLINE i32  au32_cas_weak_acq_rel(au32 *p, u32 *e, u32 d) {

    return atomic_compare_exchange_weak_explicit(p, e, d, memory_order_acq_rel, memory_order_relaxed);

}

INLINE i32  au32_cas_strong_acq_rel(au32 *p, u32 *e, u32 d) {

    return atomic_compare_exchange_strong_explicit(p, e, d, memory_order_acq_rel, memory_order_relaxed);

}

INLINE u64  au64_load_relaxed(const au64 *p)         { return atomic_load_explicit(p, memory_order_relaxed); }

INLINE void au64_store_relaxed(au64 *p, u64 v)       { atomic_store_explicit(p, v, memory_order_relaxed); }

INLINE u64  au64_fetch_add_relaxed(au64 *p, u64 v)   { return atomic_fetch_add_explicit(p, v, memory_order_relaxed); }

#else

INLINE u32  au32_load_relaxed(const au32 *p)         { return *p; }

INLINE u32  au32_load_acquire(const au32 *p)         { return *p; }

INLINE void au32_store_relaxed(au32 *p, u32 v)       { *p = v; }

INLINE void au32_store_release(au32 *p, u32 v)       { *p = v; }

INLINE u32  au32_fetch_add_relaxed(au32 *p, u32 v)   { u32 o = *p; *p = o + v; return o; }

INLINE i32  au32_cas_weak_acq_rel(au32 *p, u32 *e, u32 d) {

    u32 c = *p; if (c == *e) { *p = d; return 1; } *e = c; return 0;

}

INLINE i32  au32_cas_strong_acq_rel(au32 *p, u32 *e, u32 d) {

    u32 c = *p; if (c == *e) { *p = d; return 1; } *e = c; return 0;

}

INLINE u64  au64_load_relaxed(const au64 *p)         { return *p; }

INLINE void au64_store_relaxed(au64 *p, u64 v)       { *p = v; }

INLINE u64  au64_fetch_add_relaxed(au64 *p, u64 v)   { u64 o = *p; *p = o + v; return o; }

#endif



extern u8 __heap_base;

static au32 _abump_atomic;

static au32 _memory_limit;


INLINE u32 mem_grow(u32 p)  { return (u32)__builtin_wasm_memory_grow(0, (u64)p); }

INLINE u32 mem_bytes(void)  { return (u32)(__builtin_wasm_memory_size(0) << 16); }

INLINE u8* MP(u32 off)      { return (u8*)(uptr)off; }


INLINE u32 aalloc(u32 sz) {

    sz = (sz + 15u) & ~15u;

    if (UNLIKELY(sz == 0)) return 0;

    u32 lim = au32_load_relaxed(&_memory_limit);

    u32 cur, nxt;

    do {

        cur = au32_load_relaxed(&_abump_atomic);

        if (UNLIKELY(cur == 0)) {

            u32 base = ((u32)(uptr)&__heap_base + 63u) & ~63u;

            if (base < 65536u) base = 65536u;

            au32_cas_strong_acq_rel(&_abump_atomic, &cur, base);

            cur = au32_load_relaxed(&_abump_atomic);

        }

        if (UNLIKELY(sz > 0xFFFFFFFFu - cur)) return 0;

        nxt = cur + sz;

        if (UNLIKELY(lim > 0 && nxt > lim)) return 0;

        if (UNLIKELY(nxt > mem_bytes())) {

            u32 needed = nxt - mem_bytes();

            u32 pages  = ((needed + 65535u) >> 16) + 16u;

            if (UNLIKELY(mem_grow(pages) == (u32)-1)) return 0;

        }

    } while (!au32_cas_weak_acq_rel(&_abump_atomic, &cur, nxt));

    return cur;

}



#define SLAB_NUM_CLASSES 20u

static u32  _slab_head[SLAB_NUM_CLASSES];

static au32 _slab_lock;


INLINE void slab_lock_acquire(void) {

#if AMORA_ATOMICS

    u32 z = 0;

    while (!au32_cas_weak_acq_rel(&_slab_lock, &z, 1u)) z = 0;

#endif

}

INLINE void slab_lock_release(void) {

#if AMORA_ATOMICS

    au32_store_release(&_slab_lock, 0u);

#endif

}


INLINE u32 slab_class(u32 sz) {

    if (sz <= 16u) return 0;


    u32 s = sz - 1u;

    u32 bits = 0;

    if (s >= (1u<<16)) { bits += 16; s >>= 16; }

    if (s >= (1u<<8))  { bits += 8;  s >>= 8; }

    if (s >= (1u<<4))  { bits += 4;  s >>= 4; }

    if (s >= (1u<<2))  { bits += 2;  s >>= 2; }

    if (s >= (1u<<1))  { bits += 1; }

    u32 cls = bits > 3 ? bits - 3 : 0;

    return cls < SLAB_NUM_CLASSES ? cls : SLAB_NUM_CLASSES - 1u;

}


INLINE u32 slab_class_size(u32 cls) {

    return 16u << cls;

}


INLINE u32 blk_alloc(u32 sz, u32 *out_cap) {

    if (sz == 0) sz = 16u;

    u32 cls = slab_class(sz);

    u32 cap = slab_class_size(cls);


    slab_lock_acquire();

    u32 head = _slab_head[cls];

    if (head) {

        _slab_head[cls] = *(u32*)MP(head);

        slab_lock_release();

        *out_cap = cap;

        return head;

    }

    slab_lock_release();


    u32 off = aalloc(cap);

    if (UNLIKELY(!off)) { *out_cap = 0; return 0; }

    *out_cap = cap;

    return off;

}


INLINE void blk_free(u32 off, u32 cap) {

    if (!off || !cap) return;

    u32 cls = slab_class(cap);

    if (slab_class_size(cls) != cap) return;


    slab_lock_acquire();

    *(u32*)MP(off) = _slab_head[cls];

    _slab_head[cls] = off;

    slab_lock_release();

}



INLINE void xcp(u8 *d, const u8 *s, u32 n) {

    u32 i = 0;

#if AMORA_SIMD

    for (; i + 16u <= n; i += 16u)

        wasm_v128_store(d + i, wasm_v128_load(s + i));

#endif

    for (; i + 8u <= n; i += 8u) {

        u64 v; __builtin_memcpy(&v, s + i, 8); __builtin_memcpy(d + i, &v, 8);

    }

    for (; i < n; i++) d[i] = s[i];

}


INLINE void xzero(u8 *d, u32 n) {

    u32 i = 0;

#if AMORA_SIMD

    v128_t z = wasm_i64x2_const(0, 0);

    for (; i + 16u <= n; i += 16u) wasm_v128_store(d + i, z);

#endif

    for (; i + 8u <= n; i += 8u) { u64 z2 = 0; __builtin_memcpy(d + i, &z2, 8); }

    for (; i < n; i++) d[i] = 0;

}


INLINE i32 xeq(const u8 *a, const u8 *b, u32 n) {

    u32 i = 0;

#if AMORA_SIMD

    for (; i + 16u <= n; i += 16u) {

        if (!wasm_i8x16_all_true(wasm_i8x16_eq(

                wasm_v128_load(a + i), wasm_v128_load(b + i))))

            return 0;

    }

#endif

    for (; i + 8u <= n; i += 8u) {

        u64 av, bv;

        __builtin_memcpy(&av, a + i, 8);

        __builtin_memcpy(&bv, b + i, 8);

        if (av != bv) return 0;

    }

    for (; i < n; i++) if (a[i] != b[i]) return 0;

    return 1;

}


INLINE i32 xcmp(const u8 *a, u32 al, const u8 *b, u32 bl) {

    u32 mn = al < bl ? al : bl;

    u32 i = 0;

    for (; i + 8u <= mn; i += 8u) {

        u64 av, bv;

        __builtin_memcpy(&av, a + i, 8);

        __builtin_memcpy(&bv, b + i, 8);

        if (av != bv) {

            u64 x = av ^ bv;

            u32 bi = (__builtin_ctzll(x) >> 3) & 7u;

            return a[i + bi] < b[i + bi] ? -1 : 1;

        }

    }

    for (; i < mn; i++) {

        if (a[i] < b[i]) return -1;

        if (a[i] > b[i]) return 1;

    }

    return (al < bl) ? -1 : (al > bl) ? 1 : 0;

}



#define RAPID_SEED 0x9e3779b97f4a7c15ULL

#define RAPID_M1   0xa0761d6478bd642fULL

#define RAPID_M2   0xe7037ed1a0b428dbULL


INLINE u64 rapid_mix(u64 a, u64 b) {

    u32 al = (u32)a, ah = (u32)(a >> 32), bl = (u32)b, bh = (u32)(b >> 32);

    u64 ll = (u64)al * bl, lh = (u64)al * bh, hl = (u64)ah * bl, hh = (u64)ah * bh;

    u64 mid = lh + hl + (ll >> 32);

    return (hh + (mid >> 32)) ^ ((ll & 0xFFFFFFFFULL) | (mid << 32));

}


INLINE u32 rapid_r4(const u8 *p) { u32 v; __builtin_memcpy(&v, p, 4); return v; }

INLINE u64 rapid_r8(const u8 *p) { u64 v; __builtin_memcpy(&v, p, 8); return v; }


static u32 rapidhash32(const u8 *p, u32 len) {

    u64 h = RAPID_SEED ^ (u64)len;

    u32 i = len;

    if (LIKELY(i <= 8u)) {

        u64 a = 0;

        if (LIKELY(i >= 4u)) {

            a = ((u64)rapid_r4(p) << 32) | rapid_r4(p + i - 4u);

        } else if (i > 0u) {

            a = ((u64)p[0] << 16) | ((u64)p[i >> 1] << 8) | p[i - 1];

        }

        h = rapid_mix(h ^ RAPID_M1, a ^ RAPID_M2);

    } else if (LIKELY(i <= 16u)) {

        h = rapid_mix(rapid_r8(p) ^ RAPID_M1, rapid_r8(p + i - 8u) ^ h);

    } else {

        const u8 *q = p;

        while (LIKELY(i > 16u)) {

            h = rapid_mix(rapid_r8(q) ^ RAPID_M1, rapid_r8(q + 8u) ^ h);

            q += 16u; i -= 16u;

        }

        h = rapid_mix(rapid_r8(q + i - 16u) ^ RAPID_M1, rapid_r8(q + i - 8u) ^ h);

    }

    u32 r = (u32)(h ^ (h >> 32));

    return r ? r : 1u;

}



INLINE u8  h2(u32 h)     { return (u8)(h & 0x7Fu); }

INLINE u32 h1(u32 h)     { return h >> 7; }

INLINE u32 hshard(u32 h) { return (h >> (32u - N_SHARDS_SHIFT)) & (N_SHARDS - 1u); }



INLINE void bloom_set(u8 *bm, u32 h) {

    u32 a = (h * 0x9e3779b9u) & BLOOM_MASK;

    u32 b = ((h >> 16) * 0x517cc1b7u) & BLOOM_MASK;

    u32 c = ((h ^ (h >> 11)) * 0xb5026f5cu) & BLOOM_MASK;

    u32 d = ((h * 0xc4ceb9feu) ^ (h >> 8)) & BLOOM_MASK;

    u32 e = ((h * 0x85ebca6bu) ^ (h >> 13)) & BLOOM_MASK;

    u32 f = ((h >> 5) * 0xcc9e2d51u) & BLOOM_MASK;

    bm[a >> 3] |= (u8)(1u << (a & 7u));

    bm[b >> 3] |= (u8)(1u << (b & 7u));

    bm[c >> 3] |= (u8)(1u << (c & 7u));

    bm[d >> 3] |= (u8)(1u << (d & 7u));

    bm[e >> 3] |= (u8)(1u << (e & 7u));

    bm[f >> 3] |= (u8)(1u << (f & 7u));

}


INLINE i32 bloom_test(const u8 *bm, u32 h) {

    u32 a = (h * 0x9e3779b9u) & BLOOM_MASK;

    u32 b = ((h >> 16) * 0x517cc1b7u) & BLOOM_MASK;

    u32 c = ((h ^ (h >> 11)) * 0xb5026f5cu) & BLOOM_MASK;

    u32 d = ((h * 0xc4ceb9feu) ^ (h >> 8)) & BLOOM_MASK;

    u32 e = ((h * 0x85ebca6bu) ^ (h >> 13)) & BLOOM_MASK;

    u32 f = ((h >> 5) * 0xcc9e2d51u) & BLOOM_MASK;

    return (bm[a >> 3] >> (a & 7u) & 1u) & (bm[b >> 3] >> (b & 7u) & 1u) &

           (bm[c >> 3] >> (c & 7u) & 1u) & (bm[d >> 3] >> (d & 7u) & 1u) &

           (bm[e >> 3] >> (e & 7u) & 1u) & (bm[f >> 3] >> (f & 7u) & 1u);

}



typedef struct __attribute__((packed)) {

    u32 koff, voff;

    u16 klen;

    u32 vlen;

    u32 next[SL_LEVELS];

} SLNode;


static u32 _sl_base  = 0;

static u32 _sl_count = 0;

static u32 _sl_rand  = 0xdeadbeef;


INLINE u32 sl_rnd(void) {

    _sl_rand ^= _sl_rand << 13;

    _sl_rand ^= _sl_rand >> 7;

    _sl_rand ^= _sl_rand << 17;

    return _sl_rand;

}


INLINE u32 sl_lvl(void) {

    u32 l = 1, r = sl_rnd();

    while (l < SL_LEVELS && (r & 3u) == 0) { l++; r >>= 2; }

    return l;

}


INLINE SLNode* sl_node(u32 i) {

    return (SLNode*)MP(_sl_base + (u32)sizeof(SLNode) * i);

}


static i32 sl_init(void) {

    u32 sz = (u32)sizeof(SLNode) * (SL_MAX_NODES + 1u);

    _sl_base = aalloc(sz);

    if (!_sl_base) return 0;

    xzero(MP(_sl_base), sz);

    _sl_count = 0;

    return 1;

}


static void sl_insert(u32 koff, u16 klen, u32 voff, u32 vlen) {

    if (_sl_count >= SL_MAX_NODES) return;

    u32 update[SL_LEVELS];

    SLNode *head = sl_node(0), *cur = head;

    for (i32 i = (i32)SL_LEVELS - 1; i >= 0; i--) {

        while (cur->next[i]) {

            SLNode *nx = sl_node(cur->next[i]);

            if (xcmp(MP(nx->koff), nx->klen, MP(koff), klen) < 0) cur = nx;

            else break;

        }

        update[i] = (u32)((u8*)cur - MP(_sl_base)) / (u32)sizeof(SLNode);

    }

    u32 lvl = sl_lvl();

    _sl_count++;

    SLNode *nn = sl_node(_sl_count);

    nn->koff = koff; nn->klen = klen; nn->voff = voff; nn->vlen = vlen;

    for (u32 i = 0; i < SL_LEVELS; i++) nn->next[i] = 0;

    for (u32 i = 0; i < lvl; i++) {

        SLNode *upd = sl_node(update[i]);

        nn->next[i] = upd->next[i];

        upd->next[i] = _sl_count;

    }

}



typedef struct __attribute__((packed)) {

    u32 hash;

    u32 koff;

    u32 voff;

    u16 klen;

    u32 kcap;

    u32 vlen;

    u32 vcap;

#if AMORA_INLINE_KEY_MAX > 0

    u8  ikey[AMORA_INLINE_KEY_MAX];

#endif

} Slot;


#define SLOT_SZ ((u32)sizeof(Slot))


typedef struct {

    au32 lock;

    u32  ctrl_off, slot_off, bloom_off;

    u32  cap, count, deleted;

    au64 hits, misses;

    u8   _pad[CACHE_LINE - 48u];

} __attribute__((aligned(64))) Shard;


_Static_assert(sizeof(Shard) == CACHE_LINE, "sizeof(Shard) != CACHE_LINE");


static au64   _ops_total;

static Shard  _shards[N_SHARDS];

static au64   _write_errors;

static au64   _wal_errors;

static au64   _compactions;


INLINE u8*   sh_ctrl(const Shard *sh)        { return MP(sh->ctrl_off); }

INLINE Slot* sh_slot(const Shard *sh, u32 i) { return (Slot*)MP(sh->slot_off + i * SLOT_SZ); }

INLINE u8*   sh_bloom(const Shard *sh)       { return MP(sh->bloom_off); }


INLINE const u8* slot_key(const Slot *s) {

#if AMORA_INLINE_KEY_MAX > 0

    return s->koff ? MP(s->koff) : s->ikey;

#else

    return MP(s->koff);

#endif

}


INLINE u32 slot_key_wasm_off(const Slot *s) {

#if AMORA_INLINE_KEY_MAX > 0

    return s->koff ? s->koff : (u32)(uptr)s->ikey;

#else

    return s->koff;

#endif

}



#if AMORA_ATOMICS

static _Atomic(u32) _lock_can_wait = 0;

#endif


EXPORT void db_set_worker_mode(void) {

#if AMORA_ATOMICS

    atomic_store_explicit(&_lock_can_wait, 1u, memory_order_relaxed);

#endif

}


INLINE void shard_lock(Shard *sh) {

#if AMORA_ATOMICS

    u32 spins = 0, zero = 0;

    while (!au32_cas_weak_acq_rel(&sh->lock, &zero, 1u)) {

        zero = 0;


        u32 limit = 1u << (spins < 5u ? spins : 5u);

        for (volatile u32 b = 0; b < limit; b++) {}

        if (++spins >= SPIN_LIMIT) {

            spins = 0;

            if (atomic_load_explicit(&_lock_can_wait, memory_order_relaxed))

                __builtin_wasm_memory_atomic_wait32((int*)&sh->lock, 1, -1LL);

        }

    }

#else

    (void)sh;

#endif

}


INLINE void shard_unlock(Shard *sh) {

#if AMORA_ATOMICS

    au32_store_release(&sh->lock, 0u);

    __builtin_wasm_memory_atomic_notify((int*)&sh->lock, 1u);

#else

    (void)sh;

#endif

}


INLINE i32 shard_trylock(Shard *sh) {

#if AMORA_ATOMICS

    u32 zero = 0;

    return au32_cas_strong_acq_rel(&sh->lock, &zero, 1u);

#else

    (void)sh; return 1;

#endif

}



INLINE u32 simd_match_byte_aligned(const u8 *ctrl, u32 idx, u8 target) {

#if AMORA_SIMD

    return (u32)wasm_i8x16_bitmask(wasm_i8x16_eq(

        wasm_v128_load(ctrl + idx), wasm_i8x16_splat((i8)target)));

#else

    u32 m = 0;

    for (u32 i = 0; i < 16u; i++) m |= (u32)(ctrl[idx + i] == target) << i;

    return m;

#endif

}


INLINE u32 simd_match_empty_aligned(const u8 *ctrl, u32 idx) {

    return simd_match_byte_aligned(ctrl, idx, CTRL_EMPTY);

}


INLINE u32 simd_match_empty_or_deleted(const u8 *ctrl, u32 idx) {

#if AMORA_SIMD


    v128_t v = wasm_v128_load(ctrl + idx);

    v128_t mask = wasm_i8x16_splat((i8)0x80);

    return (u32)wasm_i8x16_bitmask(wasm_v128_and(v, mask));

#else

    u32 m = 0;

    for (u32 i = 0; i < 16u; i++) m |= (u32)(ctrl[idx + i] >= 0x80u) << i;

    return m;

#endif

}



static i32 shard_rehash(Shard *sh, u32 nc);


INLINE Slot* shard_find(Shard *sh, const u8 *k, u32 kl, u32 fh) {

    if (UNLIKELY(!bloom_test(sh_bloom(sh), fh))) {

        au64_fetch_add_relaxed(&sh->misses, 1);

        return NULL;

    }

    u8  fp   = h2(fh);

    u32 idx  = h1(fh) & (sh->cap - 1u);

    u32 mask = sh->cap - 1u;

    u8 *ctrl = sh_ctrl(sh);

    u32 probe_count = 0;

    u32 max_probes = sh->cap;


    while (probe_count < max_probes) {

        u32 m = simd_match_byte_aligned(ctrl, idx, fp);

        while (m) {

            u32 bit = __builtin_ctz(m);

            u32 si  = (idx + bit) & mask;

            Slot *s = sh_slot(sh, si);

            PREFETCH_R(slot_key(s));

            if (LIKELY(s->hash == fh && s->klen == (u16)kl)) {

                if (LIKELY(xeq(slot_key(s), k, kl))) {

                    au64_fetch_add_relaxed(&sh->hits, 1);

                    return s;

                }

            }

            m &= m - 1u;

        }

        u32 em = simd_match_empty_aligned(ctrl, idx);

        if (LIKELY(em)) { au64_fetch_add_relaxed(&sh->misses, 1); return NULL; }

        idx = (idx + GROUP_SIZE) & mask;

        probe_count += GROUP_SIZE;

    }

    au64_fetch_add_relaxed(&sh->misses, 1);

    return NULL;

}


static i32 shard_init(Shard *sh, u32 cap) {

    if (UNLIKELY(cap < GROUP_SIZE)) cap = GROUP_SIZE;


    u32 c = 1; while (c < cap) c <<= 1; cap = c;


    u32 co = aalloc(cap + GROUP_SIZE); if (!co) return 0;

    u32 so = aalloc(cap * SLOT_SZ);   if (!so) return 0;

    u32 bo = aalloc(BLOOM_BYTES);      if (!bo) return 0;


    u8 *ctrl = MP(co);

    u32 i = 0;

#if AMORA_SIMD

    v128_t e = wasm_i8x16_splat((i8)(u8)CTRL_EMPTY);

    for (; i + 16u <= cap + GROUP_SIZE; i += 16u) wasm_v128_store(ctrl + i, e);

#endif

    for (; i < cap + GROUP_SIZE; i++) ctrl[i] = CTRL_EMPTY;


    xzero(MP(so), cap * SLOT_SZ);

    xzero(MP(bo), BLOOM_BYTES);


    sh->ctrl_off = co; sh->slot_off = so; sh->bloom_off = bo;

    sh->cap = cap; sh->count = 0; sh->deleted = 0;

    au32_store_relaxed(&sh->lock, 0);

    return 1;

}


INLINE i32 shard_needs_tombstone_rehash(const Shard *sh) {

    return (sh->deleted * 100u) > (sh->cap * TOMB_RATIO_PCT);

}



static void shard_rebuild_bloom(Shard *sh) {

    u8 *bm = sh_bloom(sh);

    xzero(bm, BLOOM_BYTES);

    u8 *ctrl = sh_ctrl(sh);

    for (u32 i = 0; i < sh->cap; i++) {

        if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

        Slot *s = sh_slot(sh, i);

        bloom_set(bm, s->hash);

    }

}


INLINE i32 shard_set(Shard *sh, const u8 *k, u32 kl, const u8 *v, u32 vl, u32 fh) {

    if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE))   { au64_fetch_add_relaxed(&_write_errors, 1); return 0; }

    if (UNLIKELY((u32)vl > MAX_VALUE_SIZE))             { au64_fetch_add_relaxed(&_write_errors, 1); return 0; }



    if (UNLIKELY((sh->count + sh->deleted + 1u) * LOAD_DEN >= sh->cap * LOAD_NUM)) {

        if (UNLIKELY(!shard_rehash(sh, sh->cap * 2u))) {

            au64_fetch_add_relaxed(&_write_errors, 1); return 0;

        }

    }

    if (UNLIKELY(shard_needs_tombstone_rehash(sh))) {

        if (UNLIKELY(!shard_rehash(sh, sh->cap))) {

            au64_fetch_add_relaxed(&_write_errors, 1); return 0;

        }

    }


    u8  fp    = h2(fh);

    u32 idx   = h1(fh) & (sh->cap - 1u);

    u32 mask  = sh->cap - 1u;

    u8 *ctrl  = sh_ctrl(sh);

    i32 first_del = -1;

    u32 probe = idx, first_empty = 0;

    u32 probe_count = 0;


    while (probe_count < sh->cap) {

        u32 m = simd_match_byte_aligned(ctrl, probe, fp);

        while (m) {

            u32 bit = __builtin_ctz(m);

            u32 si  = (probe + bit) & mask;

            Slot *s = sh_slot(sh, si);

            if (LIKELY(s->hash == fh && s->klen == (u16)kl && xeq(slot_key(s), k, kl))) {


                if (LIKELY(vl <= s->vcap)) {

                    xcp(MP(s->voff), v, vl);

                    s->vlen = vl;

                } else {

                    u32 ncap = 0;

                    u32 vo = blk_alloc(vl, &ncap);

                    if (UNLIKELY(!vo)) { au64_fetch_add_relaxed(&_write_errors, 1); return 0; }

                    xcp(MP(vo), v, vl);

                    blk_free(s->voff, s->vcap);

                    s->voff = vo; s->vlen = vl; s->vcap = ncap;

                }

                au64_fetch_add_relaxed(&_ops_total, 1);

                return 1;

            }

            m &= m - 1u;

        }

        if (first_del < 0) {

            u32 dm = simd_match_byte_aligned(ctrl, probe, CTRL_DELETED);

            if (dm) first_del = (i32)((probe + __builtin_ctz(dm)) & mask);

        }

        u32 em = simd_match_empty_aligned(ctrl, probe);

        if (LIKELY(em)) { first_empty = (probe + __builtin_ctz(em)) & mask; break; }

        probe = (probe + GROUP_SIZE) & mask;

        probe_count += GROUP_SIZE;

    }



    u32 ko = 0, vo = 0, kcap = 0, vcap = 0;


    if (AMORA_INLINE_KEY_MAX == 0 || kl > AMORA_INLINE_KEY_MAX) {

        ko = blk_alloc(kl, &kcap);

        if (UNLIKELY(!ko)) { au64_fetch_add_relaxed(&_write_errors, 1); return 0; }

        xcp(MP(ko), k, kl);

    }


    vo = blk_alloc(vl > 0 ? vl : 1u, &vcap);

    if (UNLIKELY(!vo)) {

        if (ko) blk_free(ko, kcap);

        au64_fetch_add_relaxed(&_write_errors, 1); return 0;

    }

    if (vl > 0) xcp(MP(vo), v, vl);


    u32 target = (first_del >= 0) ? (u32)first_del : first_empty;

    if (first_del >= 0) sh->deleted--;


    ctrl[target] = fp;

    if (target < GROUP_SIZE) ctrl[target + sh->cap] = fp;


    Slot *s  = sh_slot(sh, target);

    s->hash  = fh;

    s->koff  = ko;

    s->voff  = vo;

    s->klen  = (u16)kl;

    s->vlen  = vl;

    s->kcap  = kcap;

    s->vcap  = vcap;


#if AMORA_INLINE_KEY_MAX > 0

    if (!ko) xcp(s->ikey, k, kl);

#endif


    bloom_set(sh_bloom(sh), fh);

    sh->count++;

    au64_fetch_add_relaxed(&_ops_total, 1);

    return 1;

}


INLINE i32 shard_delete(Shard *sh, const u8 *k, u32 kl, u32 fh) {

    if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE)) return 0;

    if (UNLIKELY(!bloom_test(sh_bloom(sh), fh))) return 0;


    u8  fp   = h2(fh);

    u32 idx  = h1(fh) & (sh->cap - 1u);

    u32 mask = sh->cap - 1u;

    u8 *ctrl = sh_ctrl(sh);

    u32 probe_count = 0;


    while (probe_count < sh->cap) {

        u32 m = simd_match_byte_aligned(ctrl, idx, fp);

        while (m) {

            u32 bit = __builtin_ctz(m);

            u32 si  = (idx + bit) & mask;

            Slot *s = sh_slot(sh, si);

            if (LIKELY(s->hash == fh && s->klen == (u16)kl && xeq(slot_key(s), k, kl))) {

                ctrl[si] = CTRL_DELETED;

                if (si < GROUP_SIZE) ctrl[si + sh->cap] = CTRL_DELETED;

                blk_free(s->koff, s->kcap);

                blk_free(s->voff, s->vcap);

                xzero((u8*)s, SLOT_SZ);

                sh->count--;

                sh->deleted++;

                au64_fetch_add_relaxed(&_ops_total, 1);

                return 1;

            }

            m &= m - 1u;

        }

        u32 em = simd_match_empty_aligned(ctrl, idx);

        if (LIKELY(em)) return 0;

        idx = (idx + GROUP_SIZE) & mask;

        probe_count += GROUP_SIZE;

    }

    return 0;

}


INLINE i32 shard_set_adopt(Shard *sh, const Slot *src) {

    u32 fh = src->hash;

    u8  fp = h2(fh);

    u32 idx = h1(fh) & (sh->cap - 1u);

    u32 mask = sh->cap - 1u;

    u8 *ctrl = sh_ctrl(sh);


    while (1) {

        u32 em = simd_match_empty_or_deleted(ctrl, idx);

        if (em) {

            u32 target = (idx + __builtin_ctz(em)) & mask;

            ctrl[target] = fp;

            if (target < GROUP_SIZE) ctrl[target + sh->cap] = fp;

            *sh_slot(sh, target) = *src;

            bloom_set(sh_bloom(sh), fh);

            sh->count++;

            return 1;

        }

        idx = (idx + GROUP_SIZE) & mask;

    }

}


static i32 shard_rehash(Shard *sh, u32 nc) {

    u32 c = 1; while (c < nc) c <<= 1; nc = c;


    Shard old = *sh;

    if (!shard_init(sh, nc)) { *sh = old; return 0; }


    au64_store_relaxed(&sh->hits,   au64_load_relaxed(&old.hits));

    au64_store_relaxed(&sh->misses, au64_load_relaxed(&old.misses));


    u8 *oc = MP(old.ctrl_off);

    for (u32 i = 0; i < old.cap; i++) {

        if (oc[i] == CTRL_EMPTY || oc[i] == CTRL_DELETED) continue;

        shard_set_adopt(sh, (Slot*)MP(old.slot_off + i * SLOT_SZ));

    }

    au64_fetch_add_relaxed(&_compactions, 1);

    return 1;

}


EXPORT u32 db_gc(void) {

    u32 cleaned = 0;

    for (u32 i = 0; i < N_SHARDS; i++) {

        Shard *sh = &_shards[i];

        shard_lock(sh);

        if (shard_needs_tombstone_rehash(sh)) {

            shard_rehash(sh, sh->cap);

            cleaned++;

        } else if (sh->deleted > 0) {


            shard_rebuild_bloom(sh);

        }

        shard_unlock(sh);

    }

    return cleaned;

}



static u32 _wal_off = 0, _wbump = 0, _wact = 0, _wstart = 0;

static u32 _scratch_base = 0;

static u32 _mget_buf_off = 0;

static u32 _kbuf_off     = 0;

static u32 _vbuf_off     = 0;


#define _scratch_off (_scratch_base)


extern void js_wal_flush(u32 ptr, u32 len);

extern u32  js_wal_load(u32 ptr, u32 max_len);

extern f64  js_now(void);


static void wadd(u8 op, const u8 *k, u32 kl, const u8 *v, u32 vl) {

    if (UNLIKELY(_wact == 2)) return;

    u32 need = 4u + 1u + 2u + 2u + kl + vl;

    if (UNLIKELY(need > WAL_MAX)) return;

    if (UNLIKELY(_wbump + need > WAL_MAX)) {

        js_wal_flush(_wal_off, _wbump);

        _wbump = 0;

        u8 *wal = MP(_wal_off);

        *(u32*)(wal + 0) = WAL_MAGIC;

        *(u32*)(wal + 4) = WAL_VERSION;

        _wbump = WAL_HEADER_SZ;

    }

    u8 *wal = MP(_wal_off);

    u32 rec_start = _wbump + 4u;

    u32 off = rec_start;

    wal[off++] = op;

    wal[off++] = (u8)(kl & 0xFFu);

    wal[off++] = (u8)((kl >> 8) & 0xFFu);

    wal[off++] = (u8)(vl & 0xFFu);

    wal[off++] = (u8)((vl >> 8) & 0xFFu);

    xcp(wal + off, k, kl); off += kl;

    if (vl) { xcp(wal + off, v, vl); off += vl; }

    u32 crc = crc32c(wal + rec_start, off - rec_start);

    wal[_wbump + 0] = (u8)(crc & 0xFFu);

    wal[_wbump + 1] = (u8)((crc >> 8) & 0xFFu);

    wal[_wbump + 2] = (u8)((crc >> 16) & 0xFFu);

    wal[_wbump + 3] = (u8)((crc >> 24) & 0xFFu);

    _wbump = off;

}


INLINE void wpu8_commit(void) {

    u32 need = 4u + 1u;

    if (UNLIKELY(_wbump + need > WAL_MAX)) {

        js_wal_flush(_wal_off, _wbump);

        _wbump = WAL_HEADER_SZ;

    }

    u8 *wal = MP(_wal_off);

    u32 rec_start = _wbump + 4u;

    wal[rec_start] = WCMT;

    u32 crc = crc32c(wal + rec_start, 1u);

    wal[_wbump + 0] = (u8)(crc & 0xFFu);

    wal[_wbump + 1] = (u8)((crc >> 8) & 0xFFu);

    wal[_wbump + 2] = (u8)((crc >> 16) & 0xFFu);

    wal[_wbump + 3] = (u8)((crc >> 24) & 0xFFu);

    _wbump = rec_start + 1u;

}


static i32 db_wal_replay(void);


EXPORT void db_persist(void) {

    if (_wbump > WAL_HEADER_SZ) js_wal_flush(_wal_off, _wbump);

}


EXPORT i32 db_restore(void) {

    u32 len = js_wal_load(_wal_off, WAL_MAX);

    if (!len) return 0;

    _wbump = len;

    return db_wal_replay();

}



EXPORT i32 db_init(u32 cap) {

    u32 base = ((u32)(uptr)&__heap_base + 63u) & ~63u;

    if (base < 65536u) base = 65536u;

    au32_store_relaxed(&_abump_atomic, base);

    au32_store_relaxed(&_memory_limit, 0u);

    au64_store_relaxed(&_ops_total, 0);

    au64_store_relaxed(&_write_errors, 0);

    au64_store_relaxed(&_wal_errors, 0);

    au64_store_relaxed(&_compactions, 0);


    if (!cap) cap = SHARD_INIT;

    u32 c = 1; while (c < cap) c <<= 1;


    _wal_off = aalloc(WAL_MAX);

    if (!_wal_off) return 0;

    *(u32*)(MP(_wal_off) + 0) = WAL_MAGIC;

    *(u32*)(MP(_wal_off) + 4) = WAL_VERSION;

    _wbump = WAL_HEADER_SZ;

    _wact = 0;


    _scratch_base = aalloc(SCRATCH_PER_THREAD * MAX_THREADS);

    if (!_scratch_base) return 0;


    _mget_buf_off = aalloc(MGET_BUF_SZ + 4u);

    if (!_mget_buf_off) return 0;


    _kbuf_off = aalloc(KBUF_SZ * MAX_THREADS);

    if (!_kbuf_off) return 0;


    _vbuf_off = aalloc(VBUF_SZ * MAX_THREADS);

    if (!_vbuf_off) return 0;


    if (!sl_init()) return 0;


    for (u32 i = 0; i < SLAB_NUM_CLASSES; i++) _slab_head[i] = 0;

    au32_store_relaxed(&_slab_lock, 0u);


    for (u32 i = 0; i < N_SHARDS; i++) {

        if (!shard_init(&_shards[i], c)) return 0;

    }

    return 1;

}


EXPORT void db_set_memory_limit(u32 max_bytes) {

    au32_store_relaxed(&_memory_limit, max_bytes);

}



EXPORT u32 db_kbuf(void)               { return _kbuf_off; }

EXPORT u32 db_vbuf(void)               { return _vbuf_off; }

EXPORT u32 db_scratch_off(void)        { return _scratch_base; }

EXPORT u32 db_scratch_vlen_off(void)   { return SCRATCH_VLEN_OFF; }

EXPORT u32 db_scratch_voff_off(void)   { return SCRATCH_VOFF_OFF; }

EXPORT u32 db_mget_buf_off(void)       { return _mget_buf_off; }

EXPORT u32 mem_alloc(u32 sz)           { return aalloc(sz); }

EXPORT u32 db_kbuf_thread(u32 tid)     { return _kbuf_off + tid * KBUF_SZ; }

EXPORT u32 db_vbuf_thread(u32 tid)     { return _vbuf_off + tid * VBUF_SZ; }



EXPORT i32 db_set(u32 kptr, u32 kl, u32 vptr, u32 vl) {

    if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) return 0;

    u8 *k = MP(kptr), *v = MP(vptr);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    wadd(WSET, k, kl, v, vl);

    shard_lock(&_shards[si]);

    i32 r = shard_set(&_shards[si], k, kl, v, vl, fh);

    shard_unlock(&_shards[si]);

    return r;

}


EXPORT i32 db_get(u32 kptr, u32 kl) {

    if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE)) return 0;

    u8 *k = MP(kptr);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    shard_lock(&_shards[si]);

    Slot *s = shard_find(&_shards[si], k, kl, fh);

    u32 *scr = (u32*)MP(_scratch_base + SCRATCH_VLEN_OFF);

    if (!s) { scr[0] = 0; scr[1] = 0; shard_unlock(&_shards[si]); return 0; }

    scr[0] = s->vlen;

    scr[1] = s->voff;

    shard_unlock(&_shards[si]);

    return 1;

}


EXPORT i32 db_delete(u32 kptr, u32 kl) {

    if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE)) return 0;

    u8 *k = MP(kptr);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    wadd(WDEL, k, kl, NULL, 0);

    shard_lock(&_shards[si]);

    i32 r = shard_delete(&_shards[si], k, kl, fh);

    shard_unlock(&_shards[si]);

    return r;

}


EXPORT i32 db_set_inplace(u32 kl, u32 vl) {

    if (UNLIKELY(kl == 0 || kl > KBUF_SZ || vl > VBUF_SZ)) return 0;

    u8 *k = MP(_kbuf_off), *v = MP(_vbuf_off);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    wadd(WSET, k, kl, v, vl);

    shard_lock(&_shards[si]);

    i32 r = shard_set(&_shards[si], k, kl, v, vl, fh);

    shard_unlock(&_shards[si]);

    return r;

}


EXPORT i32 db_set_inplace_thread(u32 kl, u32 vl, u32 tid) {

    if (UNLIKELY(tid >= MAX_THREADS || kl == 0 || kl > KBUF_SZ || vl > VBUF_SZ)) return 0;

    u8 *k = MP(_kbuf_off + tid * KBUF_SZ), *v = MP(_vbuf_off + tid * VBUF_SZ);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    shard_lock(&_shards[si]);

    i32 r = shard_set(&_shards[si], k, kl, v, vl, fh);

    shard_unlock(&_shards[si]);

    return r;

}


EXPORT i32 db_get_inplace(u32 kl) {

    if (UNLIKELY(kl == 0 || kl > KBUF_SZ)) return 0;

    u8 *k = MP(_kbuf_off);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    shard_lock(&_shards[si]);

    Slot *s = shard_find(&_shards[si], k, kl, fh);

    u32 *scr = (u32*)MP(_scratch_base + SCRATCH_VLEN_OFF);

    if (!s) { scr[0] = 0; scr[1] = 0; shard_unlock(&_shards[si]); return 0; }

    scr[0] = s->vlen;

    scr[1] = s->voff;

    shard_unlock(&_shards[si]);

    return 1;

}


EXPORT i32 db_get_inplace_thread(u32 kl, u32 tid) {

    if (UNLIKELY(tid >= MAX_THREADS || kl == 0 || kl > KBUF_SZ)) return 0;

    u8 *kbuf = MP(_kbuf_off + tid * KBUF_SZ);

    u32 fh = rapidhash32(kbuf, kl), si = hshard(fh);

    shard_lock(&_shards[si]);

    Slot *s = shard_find(&_shards[si], kbuf, kl, fh);

    u32 *scr = (u32*)MP(_scratch_base + tid * SCRATCH_PER_THREAD + SCRATCH_VLEN_OFF);

    if (!s) { scr[0] = 0; scr[1] = 0; shard_unlock(&_shards[si]); return 0; }

    scr[0] = s->vlen;

    scr[1] = s->voff;

    shard_unlock(&_shards[si]);

    return 1;

}


EXPORT i32 db_has_inplace(u32 kl) {

    if (UNLIKELY(kl == 0 || kl > KBUF_SZ)) return 0;

    u8 *k = MP(_kbuf_off);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    shard_lock(&_shards[si]);

    i32 r = shard_find(&_shards[si], k, kl, fh) ? 1 : 0;

    shard_unlock(&_shards[si]);

    return r;

}


EXPORT i32 db_delete_inplace(u32 kl) {

    if (UNLIKELY(kl == 0 || kl > KBUF_SZ)) return 0;

    u8 *k = MP(_kbuf_off);

    u32 fh = rapidhash32(k, kl), si = hshard(fh);

    wadd(WDEL, k, kl, NULL, 0);

    shard_lock(&_shards[si]);

    i32 r = shard_delete(&_shards[si], k, kl, fh);

    shard_unlock(&_shards[si]);

    return r;

}



EXPORT i32 db_has_prefix(u32 pfx_ptr, u32 pl) {

    if (UNLIKELY(pl == 0 || pl > MAX_KEY_SIZE)) return 0;

    u8 *pfx = MP(pfx_ptr);

    for (u32 si = 0; si < N_SHARDS; si++) {

        Shard *sh = &_shards[si];

        shard_lock(sh);

        u8 *ctrl = sh_ctrl(sh);

        for (u32 i = 0; i < sh->cap; i++) {

            if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

            Slot *s = sh_slot(sh, i);

            if (i + 2u < sh->cap) PREFETCH_R(sh_slot(sh, i + 2u));

            if (s->klen >= (u16)pl && xeq(slot_key(s), pfx, pl)) {

                shard_unlock(sh);

                return 1;

            }

        }

        shard_unlock(sh);

    }

    return 0;

}


EXPORT u32 db_count_prefix(u32 pfx_ptr, u32 pl) {

    if (UNLIKELY(pl == 0 || pl > MAX_KEY_SIZE)) return 0;

    u8 *pfx = MP(pfx_ptr);

    u32 cnt = 0;

    for (u32 si = 0; si < N_SHARDS; si++) {

        Shard *sh = &_shards[si];

        shard_lock(sh);

        u8 *ctrl = sh_ctrl(sh);

        for (u32 i = 0; i < sh->cap; i++) {

            if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

            Slot *s = sh_slot(sh, i);

            if (i + 2u < sh->cap) PREFETCH_R(sh_slot(sh, i + 2u));

            if (s->klen >= (u16)pl && xeq(slot_key(s), pfx, pl)) cnt++;

        }

        shard_unlock(sh);

    }

    return cnt;

}



EXPORT u32 db_exec_cmdbuf(u32 buf_ptr, u32 total_bytes) {

    if (UNLIKELY(!buf_ptr || total_bytes == 0)) return 0;

    u8 *buf = MP(buf_ptr);

    u32 off = 0, n = 0;

    u8 *res = MP(_scratch_base);

    while (off + 5u <= total_bytes && n < 65536u) {

        u8  op = buf[off++];

        u16 kl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        u16 vl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) {

            off += kl + vl; continue;

        }

        if (UNLIKELY(off + kl + vl > total_bytes)) break;

        u8 *k = buf + off; off += kl;

        u8 *v = buf + off; off += vl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);

        u8 ok = 0;

        if (op == WSET) {

            wadd(WSET, k, kl, v, vl);

            shard_lock(&_shards[si]);

            ok = (u8)shard_set(&_shards[si], k, kl, v, vl, fh);

            shard_unlock(&_shards[si]);

        } else if (op == WDEL) {

            wadd(WDEL, k, kl, NULL, 0);

            shard_lock(&_shards[si]);

            ok = (u8)shard_delete(&_shards[si], k, kl, fh);

            shard_unlock(&_shards[si]);

        }

        res[n++] = ok;

    }

    return n;

}


EXPORT u32 db_exec_cmdbuf_thread(u32 buf_ptr, u32 total_bytes, u32 scratch_off_local) {

    if (UNLIKELY(!buf_ptr || total_bytes == 0)) return 0;

    u8 *buf = MP(buf_ptr);

    u32 off = 0, n = 0;

    u8 *res = MP(scratch_off_local);

    while (off + 5u <= total_bytes && n < 65536u) {

        u8  op = buf[off++];

        u16 kl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        u16 vl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) { off += kl + vl; continue; }

        if (UNLIKELY(off + kl + vl > total_bytes)) break;

        u8 *k = buf + off; off += kl;

        u8 *v = buf + off; off += vl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);

        u8 ok = 0;

        if (op == WSET) {

            shard_lock(&_shards[si]);

            ok = (u8)shard_set(&_shards[si], k, kl, v, vl, fh);

            shard_unlock(&_shards[si]);

        } else if (op == WDEL) {

            shard_lock(&_shards[si]);

            ok = (u8)shard_delete(&_shards[si], k, kl, fh);

            shard_unlock(&_shards[si]);

        }

        res[n++] = ok;

    }

    return n;

}


EXPORT u32 db_mget_cmdbuf(u32 buf_ptr, u32 total_bytes, u32 max_keys) {

    if (UNLIKELY(!buf_ptr || total_bytes == 0)) return 0;

    u8 *buf = MP(buf_ptr);

    u32 off = 0, n = 0;

    u8  *mbuf = MP(_mget_buf_off);

    u32 *res  = (u32*)(mbuf + 4u);

    u32  cap  = MGET_BUF_ENTRIES;

    if (max_keys < cap) cap = max_keys;

    while (off + 2u <= total_bytes && n < cap) {

        u16 kl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE)) { off += kl; continue; }

        if (UNLIKELY(off + kl > total_bytes)) break;

        u8 *k = buf + off; off += kl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);

        shard_lock(&_shards[si]);

        Slot *s = shard_find(&_shards[si], k, kl, fh);

        res[n * 2]     = s ? s->voff : 0u;

        res[n * 2 + 1] = s ? s->vlen : 0u;

        shard_unlock(&_shards[si]);

        n++;

    }

    *(u32*)mbuf = n;

    return n;

}



EXPORT u32 db_scan_prefix(u32 pfx_ptr, u32 pl) {

    if (UNLIKELY(pl == 0 || pl > MAX_KEY_SIZE)) return 0;

    u8 *pfx = MP(pfx_ptr);

    u32 cnt = 0, cur = SCRATCH_RESULTS_OFF;

    u8 *scr = MP(_scratch_base);

    for (u32 si = 0; si < N_SHARDS && cnt < SCRATCH_MAX_RESULTS; si++) {

        Shard *sh = &_shards[si];

        shard_lock(sh);

        u8 *ctrl = sh_ctrl(sh);

        for (u32 i = 0; i < sh->cap && cnt < SCRATCH_MAX_RESULTS; i++) {

            if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

            Slot *s = sh_slot(sh, i);

            if (i + 2u < sh->cap) PREFETCH_R(sh_slot(sh, i + 2u));

            if (s->klen < (u16)pl || !xeq(slot_key(s), pfx, pl)) continue;

            u32 *p = (u32*)(scr + cur);

            p[0] = slot_key_wasm_off(s);

            p[1] = s->klen;

            p[2] = s->voff;

            p[3] = s->vlen;

            cur += 16u; cnt++;

        }

        shard_unlock(sh);

    }

    *(u32*)scr = cnt;

    return cnt;

}


EXPORT u32 db_scan_koff(u32 i) { return *(u32*)(MP(_scratch_base) + SCRATCH_RESULTS_OFF + i * 16u + 0u); }

EXPORT u32 db_scan_klen(u32 i) { return *(u32*)(MP(_scratch_base) + SCRATCH_RESULTS_OFF + i * 16u + 4u); }

EXPORT u32 db_scan_voff(u32 i) { return *(u32*)(MP(_scratch_base) + SCRATCH_RESULTS_OFF + i * 16u + 8u); }

EXPORT u32 db_scan_vlen(u32 i) { return *(u32*)(MP(_scratch_base) + SCRATCH_RESULTS_OFF + i * 16u + 12u); }


static void sl_rebuild(void) {

    u32 sz = (u32)sizeof(SLNode) * (SL_MAX_NODES + 1u);

    xzero(MP(_sl_base), sz);

    _sl_count = 0;

    for (u32 si = 0; si < N_SHARDS; si++) {

        Shard *sh = &_shards[si];

        shard_lock(sh);

        u8 *ctrl = sh_ctrl(sh);

        for (u32 i = 0; i < sh->cap; i++) {

            if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

            if (_sl_count >= SL_MAX_NODES) { shard_unlock(sh); goto done; }

            Slot *s = sh_slot(sh, i);

            sl_insert(slot_key_wasm_off(s), s->klen, s->voff, s->vlen);

        }

        shard_unlock(sh);

    }

done:;

}


EXPORT u32 db_scan_range(u32 from_ptr, u32 fl, u32 to_ptr, u32 tl) {

    if (UNLIKELY(fl == 0 || fl > MAX_KEY_SIZE || tl == 0 || tl > MAX_KEY_SIZE)) return 0;

    sl_rebuild();

    u32 cnt = 0, cur = SCRATCH_RESULTS_OFF;

    u8 *scr = MP(_scratch_base);

    SLNode *head = sl_node(0), *c = head;

    for (i32 i = (i32)SL_LEVELS - 1; i >= 0; i--) {

        while (c->next[i]) {

            SLNode *nx = sl_node(c->next[i]);

            if (xcmp(MP(nx->koff), nx->klen, MP(from_ptr), (u16)fl) < 0) c = nx;

            else break;

        }

    }

    u32 ni = c->next[0];

    while (ni && cnt < SCRATCH_MAX_RESULTS) {

        SLNode *n = sl_node(ni);

        if (xcmp(MP(n->koff), n->klen, MP(to_ptr), (u16)tl) > 0) break;

        u32 *p = (u32*)(scr + cur);

        p[0] = n->koff; p[1] = n->klen; p[2] = n->voff; p[3] = n->vlen;

        cur += 16u; cnt++;

        ni = n->next[0];

    }

    *(u32*)scr = cnt;

    return cnt;

}



typedef struct {

    u32 ctrl_snap, slot_snap, bloom_snap;

    u32 cap, count, deleted;

    u32 ctrl_off, slot_off, bloom_off;

} ShardSnap;


static ShardSnap _snap[N_SHARDS];

static u8  _cow_dirty[N_SHARDS];

static u32 _abump_snap = 0;

static u32 _snap_valid = 0;


static void cow_snap_shard(u32 si) {

    if (!_snap_valid || _cow_dirty[si]) return;

    Shard *sh = &_shards[si];

    u32 cap = sh->cap;

    u32 ctrl_sz = cap + GROUP_SIZE;

    u32 cs = aalloc(ctrl_sz);

    u32 ss = aalloc(cap * SLOT_SZ);

    u32 bs = aalloc(BLOOM_BYTES);

    if (!cs || !ss || !bs) return;

    xcp(MP(cs), MP(sh->ctrl_off), ctrl_sz);

    xcp(MP(ss), MP(sh->slot_off), cap * SLOT_SZ);

    xcp(MP(bs), MP(sh->bloom_off), BLOOM_BYTES);

    _snap[si].ctrl_snap = cs; _snap[si].slot_snap = ss; _snap[si].bloom_snap = bs;

    _snap[si].cap = cap; _snap[si].count = sh->count; _snap[si].deleted = sh->deleted;

    _snap[si].ctrl_off = sh->ctrl_off; _snap[si].slot_off = sh->slot_off;

    _snap[si].bloom_off = sh->bloom_off;

    _cow_dirty[si] = 1;

}


static i32 cow_shard_set(u32 si, const u8 *k, u32 kl, const u8 *v, u32 vl, u32 fh) {

    cow_snap_shard(si);

    return shard_set(&_shards[si], k, kl, v, vl, fh);

}


static i32 cow_shard_delete(u32 si, const u8 *k, u32 kl, u32 fh) {

    cow_snap_shard(si);

    return shard_delete(&_shards[si], k, kl, fh);

}


EXPORT void db_batch_begin(void) {

    _wact = 1; _wstart = _wbump;

    _abump_snap = au32_load_relaxed(&_abump_atomic);

    _snap_valid = 1;

    for (u32 i = 0; i < N_SHARDS; i++) _cow_dirty[i] = 0;

}


EXPORT void db_batch_commit(void) {

    wpu8_commit();

    _snap_valid = 0; _wact = 0;

    for (u32 i = 0; i < N_SHARDS; i++) _cow_dirty[i] = 0;

}


EXPORT void db_batch_rollback(void) {

    _wact = 0;

    if (!_snap_valid) { _wbump = _wstart; return; }

    for (u32 i = 0; i < N_SHARDS; i++) {

        if (!_cow_dirty[i]) continue;

        Shard *sh = &_shards[i];

        shard_lock(sh);

        u32 cap = _snap[i].cap;

        sh->ctrl_off = _snap[i].ctrl_off; sh->slot_off = _snap[i].slot_off;

        sh->bloom_off = _snap[i].bloom_off; sh->cap = cap;

        xcp(MP(sh->ctrl_off), MP(_snap[i].ctrl_snap), cap + GROUP_SIZE);

        xcp(MP(sh->slot_off), MP(_snap[i].slot_snap), cap * SLOT_SZ);

        xcp(MP(sh->bloom_off), MP(_snap[i].bloom_snap), BLOOM_BYTES);

        sh->count = _snap[i].count; sh->deleted = _snap[i].deleted;

        shard_unlock(sh);

    }

    au32_store_relaxed(&_abump_atomic, _abump_snap);

    _wbump = _wstart; _snap_valid = 0;

}


EXPORT u32 db_batch_exec_cmdbuf(u32 buf_ptr, u32 total_bytes) {

    if (UNLIKELY(!buf_ptr || total_bytes == 0)) return 0;

    u8 *buf = MP(buf_ptr);

    u32 off = 0, n = 0;

    u8 *res = MP(_scratch_base);

    while (off + 5u <= total_bytes && n < 65536u) {

        u8  op = buf[off++];

        u16 kl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        u16 vl = (u16)(buf[off] | ((u32)buf[off + 1] << 8u)); off += 2u;

        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) { off += kl + vl; continue; }

        if (UNLIKELY(off + kl + vl > total_bytes)) break;

        u8 *k = buf + off; off += kl;

        u8 *v = buf + off; off += vl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);

        u8 ok = 0;

        if (op == WSET) {

            wadd(WSET, k, kl, v, vl);

            shard_lock(&_shards[si]);

            ok = (u8)cow_shard_set(si, k, kl, v, vl, fh);

            shard_unlock(&_shards[si]);

        } else if (op == WDEL) {

            wadd(WDEL, k, kl, NULL, 0);

            shard_lock(&_shards[si]);

            ok = (u8)cow_shard_delete(si, k, kl, fh);

            shard_unlock(&_shards[si]);

        }

        res[n++] = ok;

    }

    return n;

}



static i32 db_wal_replay(void) {

    u32 off = 0; i32 rep = 0;

    u32 prev_wact = _wact, prev_wbump = _wbump;

    _wact = 2;

    u8 *wal = MP(_wal_off);

    u32 total = prev_wbump;

    u64 err_before = au64_load_relaxed(&_wal_errors);


    if (total >= WAL_HEADER_SZ) {

        u32 magic   = *(u32*)(wal + 0);

        u32 version = *(u32*)(wal + 4);

        if (magic == WAL_MAGIC && version == WAL_VERSION) {

            off = WAL_HEADER_SZ;

        }

    }


    while (off + 5u <= total) {

        if (off + 4u > total) break;

        u32 expected_crc = (u32)wal[off] | ((u32)wal[off + 1] << 8u) |

                           ((u32)wal[off + 2] << 16u) | ((u32)wal[off + 3] << 24u);

        u32 rec_start = off + 4u;

        u8 op = wal[rec_start];


        if (op == WCMT) {

            u32 actual = crc32c(wal + rec_start, 1u);

            if (actual != expected_crc) {

                au64_fetch_add_relaxed(&_wal_errors, 1);

                break;

            }

            rep++;

            off = rec_start + 1u;

            continue;

        }


        if (rec_start + 5u > total) break;

        u16 kl = (u16)(wal[rec_start + 1] | ((u32)wal[rec_start + 2] << 8u));

        u16 vl = (u16)(wal[rec_start + 3] | ((u32)wal[rec_start + 4] << 8u));

        u32 rec_end = rec_start + 5u + kl + vl;

        if (UNLIKELY(rec_end > total)) break;


        u32 rec_len = rec_end - rec_start;

        u32 actual  = crc32c(wal + rec_start, rec_len);

        if (actual != expected_crc) {

            au64_fetch_add_relaxed(&_wal_errors, 1);

            off = rec_end;

            continue;

        }


        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) {

            au64_fetch_add_relaxed(&_wal_errors, 1);

            off = rec_end;

            continue;

        }


        u8 *k = wal + rec_start + 5u;

        u8 *v = k + kl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);


        if (op == WSET) {

            shard_lock(&_shards[si]);

            shard_set(&_shards[si], k, kl, v, vl, fh);

            shard_unlock(&_shards[si]);

        } else if (op == WDEL) {

            shard_lock(&_shards[si]);

            shard_delete(&_shards[si], k, kl, fh);

            shard_unlock(&_shards[si]);

        }

        off = rec_end;

    }


    _wbump = WAL_HEADER_SZ;

    _wact = prev_wact;

    u64 errs = au64_load_relaxed(&_wal_errors) - err_before;

    return errs > 0 ? -(i32)errs : rep;

}



EXPORT u32 db_export_snapshot(u32 out_ptr, u32 max_bytes) {

    if (UNLIKELY(!out_ptr || max_bytes < 16u)) return 0;

    u8 *out = MP(out_ptr);

    u32 off = 0;

    *(u32*)(out + 0) = SNAP_MAGIC;

    *(u32*)(out + 4) = SNAP_VERSION;

    u32 total_entries_off = 8u;

    *(u32*)(out + 8) = N_SHARDS;

    *(u32*)(out + 12) = 0;

    off = 16u;

    u32 total = 0;

    for (u32 si = 0; si < N_SHARDS; si++) {

        Shard *sh = &_shards[si];

        shard_lock(sh);

        u8 *ctrl = sh_ctrl(sh);

        for (u32 i = 0; i < sh->cap; i++) {

            if (ctrl[i] == CTRL_EMPTY || ctrl[i] == CTRL_DELETED) continue;

            Slot *s = sh_slot(sh, i);

            u32 kl = s->klen, vl = s->vlen;

            u32 rec_size = 4u + 2u + 4u + kl + vl;

            if (UNLIKELY(off + rec_size > max_bytes)) { shard_unlock(sh); goto done; }

            u32 rec_start = off + 4u;

            *(u16*)(out + rec_start + 0) = (u16)kl;

            *(u32*)(out + rec_start + 2) = vl;

            xcp(out + rec_start + 6, slot_key(s), kl);

            xcp(out + rec_start + 6 + kl, MP(s->voff), vl);

            u32 crc = crc32c(out + rec_start, 2u + 4u + kl + vl);

            *(u32*)(out + off) = crc;

            off += rec_size;

            total++;

        }

        shard_unlock(sh);

    }

done:

    *(u32*)(out + total_entries_off + 4u) = total;

    return off;

}


EXPORT u32 db_import_snapshot(u32 in_ptr, u32 len) {

    if (UNLIKELY(!in_ptr || len < 16u)) return 0;

    u8 *in = MP(in_ptr);

    u32 magic   = *(u32*)(in + 0);

    u32 version = *(u32*)(in + 4);

    if (magic != SNAP_MAGIC || version != SNAP_VERSION) return 0;

    u32 expected = *(u32*)(in + 12);

    u32 off = 16u, imported = 0;

    while (off + 4u + 2u + 4u <= len && imported < expected) {

        u32 expected_crc = *(u32*)(in + off);

        u32 rec_start = off + 4u;

        u32 kl = *(u16*)(in + rec_start + 0);

        u32 vl = *(u32*)(in + rec_start + 2);

        if (UNLIKELY(kl == 0 || kl > MAX_KEY_SIZE || (u32)vl > MAX_VALUE_SIZE)) break;

        u32 rec_size = 2u + 4u + kl + vl;

        if (UNLIKELY(rec_start + rec_size > len)) break;

        u32 actual = crc32c(in + rec_start, rec_size);

        if (actual != expected_crc) {

            au64_fetch_add_relaxed(&_wal_errors, 1);

            break;

        }

        u8 *k = in + rec_start + 6u;

        u8 *v = k + kl;

        u32 fh = rapidhash32(k, kl), si = hshard(fh);

        shard_lock(&_shards[si]);

        shard_set(&_shards[si], k, kl, v, vl, fh);

        shard_unlock(&_shards[si]);

        imported++;

        off += 4u + rec_size;

    }

    return imported;

}



EXPORT u32 db_count(void)            { u32 t = 0; for (u32 i = 0; i < N_SHARDS; i++) t += _shards[i].count; return t; }

EXPORT u32 db_capacity(void)         { u32 t = 0; for (u32 i = 0; i < N_SHARDS; i++) t += _shards[i].cap; return t; }

EXPORT u64 db_hits(void)             { u64 t = 0; for (u32 i = 0; i < N_SHARDS; i++) t += au64_load_relaxed(&_shards[i].hits); return t; }

EXPORT u64 db_misses(void)           { u64 t = 0; for (u32 i = 0; i < N_SHARDS; i++) t += au64_load_relaxed(&_shards[i].misses); return t; }

EXPORT u64 db_ops(void)              { return au64_load_relaxed(&_ops_total); }

EXPORT u32 db_arena_used(void)       { return au32_load_relaxed(&_abump_atomic); }

EXPORT u32 db_wal_size(void)         { return _wbump; }

EXPORT u32 db_deleted_count(void)    { u32 t = 0; for (u32 i = 0; i < N_SHARDS; i++) t += _shards[i].deleted; return t; }

EXPORT u32 db_wact(void)             { return _wact; }

EXPORT u32 db_shard_count(void)      { return N_SHARDS; }

EXPORT u32 db_max_threads(void)      { return MAX_THREADS; }

EXPORT u32 db_inline_key_max(void)   { return AMORA_INLINE_KEY_MAX; }

EXPORT u32 db_max_key_size(void)     { return MAX_KEY_SIZE; }

EXPORT u32 db_max_value_size(void)   { return MAX_VALUE_SIZE; }

EXPORT u32 db_scratch_per_thread(void)  { return SCRATCH_PER_THREAD; }

EXPORT u32 db_scratch_max_results(void) { return SCRATCH_MAX_RESULTS; }

EXPORT u64 db_write_errors(void)     { return au64_load_relaxed(&_write_errors); }

EXPORT u64 db_wal_errors(void)       { return au64_load_relaxed(&_wal_errors); }

EXPORT u64 db_compactions(void)      { return au64_load_relaxed(&_compactions); }


EXPORT u32 db_fragmentation_pct(void) {

    u32 total_del = 0, total_cap = 0;

    for (u32 i = 0; i < N_SHARDS; i++) { total_del += _shards[i].deleted; total_cap += _shards[i].cap; }

    if (!total_cap) return 0;

    return (total_del * 100u) / total_cap;

}


EXPORT i32 db_heartbeat(void) { return 1; }



static f64 _bo[4] __attribute__((aligned(8)));


EXPORT u32 db_bench_ptr(void) { return (u32)(uptr)_bo; }


EXPORT i32 db_bench(u32 n) {

    Shard saved[N_SHARDS];

    u32 saved_wact  = _wact, saved_wbump = _wbump;

    u32 saved_abump = au32_load_relaxed(&_abump_atomic);

    for (u32 i = 0; i < N_SHARDS; i++) saved[i] = _shards[i];


    u32 bc = 131072u;

    for (u32 i = 0; i < N_SHARDS; i++) {

        if (!shard_init(&_shards[i], bc)) {

            for (u32 j = 0; j < N_SHARDS; j++) _shards[j] = saved[j];

            _bo[0] = _bo[1] = _bo[2] = _bo[3] = -1.0;

            return 0;

        }

    }

    _wact = 2;


    u8 k[12], v[12];

    f64 t = js_now();

    for (u32 i = 0; i < n; i++) {

        k[0] = 'k'; k[1] = ':';

        u32 x = i;

        for (i32 j = 9; j >= 2; j--) { k[j] = "0123456789abcdef"[x & 0xFu]; x >>= 4; }

        v[0] = 'v'; v[1] = ':';

        x = i * 7u + 13u;

        for (i32 j = 9; j >= 2; j--) { v[j] = "0123456789abcdef"[x & 0xFu]; x >>= 4; }

        u32 fh = rapidhash32(k, 10u), si = hshard(fh);

        shard_lock(&_shards[si]);

        shard_set(&_shards[si], k, 10u, v, 10u, fh);

        shard_unlock(&_shards[si]);

    }

    _bo[0] = js_now() - t;


    t = js_now();

    for (u32 i = 0; i < n; i++) {

        k[0] = 'k'; k[1] = ':';

        u32 x = i;

        for (i32 j = 9; j >= 2; j--) { k[j] = "0123456789abcdef"[x & 0xFu]; x >>= 4; }

        u32 fh = rapidhash32(k, 10u), si = hshard(fh);

        shard_lock(&_shards[si]);

        shard_find(&_shards[si], k, 10u, fh);

        shard_unlock(&_shards[si]);

    }

    _bo[1] = js_now() - t;


    t = js_now();

    for (u32 i = 0; i < n / 2u; i++) {

        k[0] = 'k'; k[1] = ':';

        u32 x = i;

        for (i32 j = 9; j >= 2; j--) { k[j] = "0123456789abcdef"[x & 0xFu]; x >>= 4; }

        u32 fh = rapidhash32(k, 10u), si = hshard(fh);

        shard_lock(&_shards[si]);

        shard_delete(&_shards[si], k, 10u, fh);

        shard_unlock(&_shards[si]);

    }

    _bo[2] = js_now() - t;


    t = js_now();

    u8 pfx[4] = {'k', ':', '0', '0'};

    db_scan_prefix((u32)(uptr)pfx, 4u);

    _bo[3] = js_now() - t;


    for (u32 i = 0; i < N_SHARDS; i++) _shards[i] = saved[i];

    _wact = saved_wact; _wbump = saved_wbump;

    au32_store_relaxed(&_abump_atomic, saved_abump);

    return 1;

}

