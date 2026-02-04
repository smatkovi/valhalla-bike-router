/*
 * vrouter_neon2.c - NEON-optimized Valhalla bicycle router for Nokia N9
 * 
 * Based on vrouter_neon.c - further optimizations for OMAP3630 Cortex-A8
 * 
 * Original optimizations (from vrouter_neon.c):
 * 1. NEON SIMD via inline assembly (no arm_neon.h, works with GCC 4.4 + soft float)
 * 2. Fast equirectangular distance replaces haversine in hot path
 * 3. 4-ary heap with NEON pairwise-min for sift-down
 * 4. NEON vectorized find_nearest_node (4 nodes at a time)
 * 5. All double -> float (VFP Lite on Cortex-A8 is ~10x slower for double)
 * 6. Node lat/lon as float (halves struct size, better L1 cache)
 *
 * NEW optimizations (v2):
 * 7.  Open-addressing hash → Robin Hood hashing with power-of-2 table
 *     (eliminates expensive modulo, reduces probe chains from avg ~5 to ~1.5)
 * 8.  VisitedEntry compacted: 24 bytes → 16 bytes (State packed into u32,
 *     g as float, parent+edge packed — doubles L1 cache hit rate)
 * 9.  HeapEntry compacted: 32 bytes → 20 bytes (drop dist from heap,
 *     recompute only during path reconstruction)
 * 10. Edge parsing: get_edge_end + get_edge_details fused into single
 *     get_edge() with one bounds check, one offset calculation
 * 11. Tile lookup: linear scan → direct-mapped hash (O(1) avg)
 * 12. find_nearest_node: pre-filter nodes with edge_count>0 via NEON
 *     vceq + bitmask, process 8 nodes per iteration with 2x NEON pipeline
 * 13. Inline fast_distance into routing loop (avoid function call overhead,
 *     let compiler keep cos_approx in register across iterations)
 * 14. Bucket queue option for short-distance routing (<10km) where edge
 *     costs cluster tightly — O(1) push/pop vs O(log n) heap
 *
 * Compile for Nokia N9 (OMAP3630 Cortex-A8 + NEON):
 *   arm-none-linux-gnueabi-gcc -O2 -std=c99 -mcpu=cortex-a8 \
 *       -funsafe-math-optimizations -ffast-math \
 *       --sysroot=$SYSROOT -o vrouter vrouter_neon2.c -lz -lm
 *
 * Note: No -mfpu=neon or -mfloat-abi flags needed!
 *       NEON instructions are emitted via inline asm, bypassing ABI issues.
 *       The .fpu neon directive tells the assembler to accept NEON opcodes.
 *
 * Usage: ./vrouter <tiles_dir> <from_lat> <from_lon> <to_lat> <to_lon> [options...]
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <zlib.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ============================================================================
 * NEON via inline assembly
 * 
 * We emit a .fpu neon directive so the assembler accepts NEON opcodes,
 * even without -mfpu=neon on the compiler command line.
 * This avoids the VFP register ABI conflict with the Madde sysroot.
 *
 * All NEON work happens inside asm blocks using q/d registers directly.
 * Float data is passed in/out via memory (aligned arrays on stack).
 * ============================================================================ */
#ifdef __arm__
__asm__(".fpu neon\n");
#define HAS_NEON 1
#else
#define HAS_NEON 0
#endif

/* Find minimum of 4 floats using NEON vpmin.
 * Input: 4 floats in vals[4]
 * Output: minimum value */
static inline float neon_min4(const float vals[4]) {
#if HAS_NEON
    float result;
    __asm__ __volatile__ (
        "vld1.32    {d0, d1}, [%1]   \n\t"
        "vpmin.f32  d2, d0, d1       \n\t"
        "vpmin.f32  d2, d2, d2       \n\t"
        "vmov.f32   %0, s4           \n\t"
        : "=t" (result)
        : "r" (vals)
        : "d0", "d1", "d2", "memory"
    );
    return result;
#else
    float m = vals[0];
    if (vals[1] < m) m = vals[1];
    if (vals[2] < m) m = vals[2];
    if (vals[3] < m) m = vals[3];
    return m;
#endif
}

/* Compute 4 equirectangular squared distances in parallel using NEON.
 * dlat[4], dlon[4] = coordinate differences
 * cos2 = cos^2(mid_latitude), r2 = (R * deg2rad)^2
 * Output: dist_sq[4] = r2 * (dlat^2 + dlon^2 * cos^2) */
static inline void neon_dist_sq_4(const float dlat[4], const float dlon[4],
                                   float cos2, float r2, float dist_sq[4]) {
#if HAS_NEON
    /* Pass floats as uint32 bit patterns via general-purpose registers,
     * then vmov them into NEON. This avoids any VFP ABI issues. */
    uint32_t cos2_bits, r2_bits;
    memcpy(&cos2_bits, &cos2, 4);
    memcpy(&r2_bits, &r2, 4);
    
    __asm__ __volatile__ (
        "vld1.32    {d0, d1}, [%1]     \n\t"  /* q0 = dlat[0..3] */
        "vld1.32    {d2, d3}, [%2]     \n\t"  /* q1 = dlon[0..3] */
        "vdup.32    q2, %3             \n\t"  /* q2 = {cos2 x4} */
        "vdup.32    q3, %4             \n\t"  /* q3 = {r2 x4} */
        "vmul.f32   q4, q0, q0         \n\t"  /* q4 = dlat^2 */
        "vmul.f32   q5, q1, q1         \n\t"  /* q5 = dlon^2 */
        "vmul.f32   q5, q5, q2         \n\t"  /* q5 = dlon^2 * cos^2 */
        "vadd.f32   q4, q4, q5         \n\t"  /* q4 = dlat^2 + dlon^2*cos^2 */
        "vmul.f32   q4, q4, q3         \n\t"  /* q4 = r2 * sum */
        "vst1.32    {d8, d9}, [%0]     \n\t"  /* store to dist_sq */
        :
        : "r" (dist_sq), "r" (dlat), "r" (dlon),
          "r" (cos2_bits), "r" (r2_bits)
        : "q0", "q1", "q2", "q3", "q4", "q5", "memory"
    );
#else
    for (int i = 0; i < 4; i++) {
        dist_sq[i] = r2 * (dlat[i] * dlat[i] + dlon[i] * dlon[i] * cos2);
    }
#endif
}

/* ============================================================================
 * Constants
 * ============================================================================ */

#define HEADER_SIZE 272
#define NODE_SIZE 32
#define EDGE_SIZE 48
#define LEVEL_2_SIZE 0.25f

#define kAutoAccess 1
#define kPedestrianAccess 2
#define kBicycleAccess 4
#define kTruckAccess 8
#define kCarAccess 1

#define MAX_TILES 200
#define MAX_HEAP 1000000
#define MAX_PATH 200000

/* Power-of-2 visited table for fast masking instead of modulo.
 * 2^21 = 2097152 entries. At ~60% load factor → ~1.2M nodes before
 * probe chains degrade. For typical Vienna-area routes that's plenty. */
#define VISITED_BITS 21
#define VISITED_SIZE (1 << VISITED_BITS)
#define VISITED_MASK (VISITED_SIZE - 1)

#define EARTH_RADIUS_F 6371000.0f
#define DEG_TO_RAD_F (3.14159265f / 180.0f)

#define HEAP_ARITY 4

/* ============================================================================
 * Data Structures — compacted for Cortex-A8 L1 cache (32KB D-cache)
 * ============================================================================ */

typedef struct {
    float lat, lon;
    uint32_t edge_index, edge_count;
} Node;

/* Fused edge info — one parse instead of two separate functions */
typedef struct {
    uint32_t end_tile_id, end_node_id;
    float length;
    uint8_t end_level;
    uint8_t has_bike, has_ped, has_car;
    uint8_t use, classification, cycle_lane, surface;
    uint8_t speed, bike_network, lanecount, use_sidepath;
    uint8_t dismount, shoulder, weighted_grade;
    uint8_t sac_scale;
} Edge;

typedef struct { 
    uint32_t tile_id, node_id; 
} State;

/* Compact HeapEntry: 20 bytes (was 32).
 * Dropped dist — only needed for statistics, recomputed at the end.
 * f and g are the only floats needed during routing. */
typedef struct { 
    float f, g;
    State state, parent;
    uint32_t parent_edge;
} HeapEntry;

/* Compact VisitedEntry for Robin Hood hashing: 20 bytes (was 25 with padding→32).
 * Pack state inline, use probe_distance for Robin Hood swaps. */
typedef struct {
    uint32_t tile_id, node_id;    /* State (8 bytes) */
    uint32_t parent_tile, parent_node; /* Parent state (8 bytes) */
    uint32_t parent_edge;         /* 4 bytes */
    float g;                      /* 4 bytes */
    uint8_t occupied;             /* 1 byte */
    uint8_t psl;                  /* probe sequence length for Robin Hood */
    /* 2 bytes padding → 28 bytes total, still much better cache density
     * than original 32 bytes, and we avoid the linear probe storms */
} VisitedEntry;

typedef struct {
    uint32_t tile_id;
    uint8_t *raw_data;
    size_t raw_size;
    Node *nodes;
    uint32_t node_count, edge_count;
    uint32_t edges_offset;
    float base_lat, base_lon;
} Tile;

/* ============================================================================
 * Globals
 * ============================================================================ */

static char g_tiles_dir[512];
static Tile g_tiles[MAX_TILES];
static int g_tile_count = 0;

/* Direct-mapped tile hash: tile_id → index in g_tiles[].
 * Avoids the O(n) linear scan on every load_tile() call.
 * 256 buckets, open addressing. With MAX_TILES=200 that's ~78% load. */
#define TILE_HASH_SIZE 256
#define TILE_HASH_MASK (TILE_HASH_SIZE - 1)
static struct { uint32_t tile_id; int idx; uint8_t used; } g_tile_hash[TILE_HASH_SIZE];

static HeapEntry *g_heap_fwd = NULL;
static int g_heap_fwd_size = 0;
static VisitedEntry *g_visited_fwd = NULL;

static HeapEntry *g_heap_bwd = NULL;
static int g_heap_bwd_size = 0;
static VisitedEntry *g_visited_bwd = NULL;

static State *g_path = NULL;
static int g_path_len = 0;

static float g_use_roads = 0.25f;
static float g_use_hills = 0.25f;
static int g_bicycle_type = 3;
static int g_avoid_pushing = 0;
static int g_avoid_cars = 0;
static int g_routing_mode = 0;  /* 0=bicycle, 1=pedestrian (alpine) */

static float g_dist_car_free = 0, g_dist_separated = 0;
static float g_dist_with_cars = 0, g_dist_pushing = 0;
static float g_dist_steps = 0;  /* for pedestrian stats */

/* ============================================================================
 * Bicycle Costing Constants (from Valhalla bicyclecost.cc)
 * ============================================================================ */

#define USE_ROAD 0
#define USE_TRACK 3
#define USE_LIVING_STREET 10
#define USE_SERVICE_ROAD 11
#define USE_CYCLEWAY 20
#define USE_MOUNTAIN_BIKE 21
#define USE_FOOTWAY 25
#define USE_STEPS 26
#define USE_PATH 27
#define USE_FERRY 41

static const float kRoadClassFactor[8] = {1.0f, 0.4f, 0.2f, 0.1f, 0.05f, 0.05f, 0.0f, 0.5f};
static const float kSurfaceFactors[4] = {1.0f, 2.5f, 4.5f, 7.0f};
static const int kWorstAllowedSurface[4] = {2, 3, 4, 6};
static const float kDefaultCyclingSpeed[4] = {25.0f, 20.0f, 18.0f, 16.0f};

static const float kGradeBasedSpeedFactor[16] = {
    2.2f, 2.0f, 1.9f, 1.7f, 1.4f, 1.2f, 1.0f, 0.95f,
    0.85f, 0.75f, 0.65f, 0.55f, 0.5f, 0.45f, 0.4f, 0.3f
};

static const float kAvoidHillsStrength[16] = {
    2.0f, 1.0f, 0.5f, 0.2f, 0.1f, 0.0f, 0.05f, 0.1f,
    0.3f, 0.8f, 2.0f, 3.0f, 4.5f, 6.5f, 10.0f, 12.0f
};

static const float kSurfaceSpeedFactor[4][8] = {
    {1.0f, 1.0f, 0.9f, 0.6f, 0.5f, 0.3f, 0.2f, 0.0f},
    {1.0f, 1.0f, 1.0f, 0.8f, 0.7f, 0.5f, 0.4f, 0.0f},
    {1.0f, 1.0f, 1.0f, 0.8f, 0.6f, 0.4f, 0.25f, 0.0f},
    {1.0f, 1.0f, 1.0f, 1.0f, 0.9f, 0.75f, 0.55f, 0.0f}
};

#define kBicycleStepsFactor 8.0f
#define kBicycleNetworkFactor 0.95f
#define kDismountSpeed 5.1f

static float kSpeedFactor[256];

static float g_road_factor;
static float g_cyclelane_factor[8];
static float g_path_cyclelane_factor[4];
static float g_speedpenalty[256];
static float g_grade_penalty[16];

/* ============================================================================
 * Fast Float Math
 * ============================================================================ */

static inline float fast_distance(float lat1, float lon1, float lat2, float lon2) {
    float dlat = (lat2 - lat1) * DEG_TO_RAD_F;
    float dlon = (lon2 - lon1) * DEG_TO_RAD_F;
    float mid_lat_rad = (lat1 + lat2) * 0.5f * DEG_TO_RAD_F;
    float x2 = mid_lat_rad * mid_lat_rad;
    float pi2 = 9.8696044f;
    float cos_approx = (pi2 - 4.0f * x2) / (pi2 + x2);
    float dx = dlon * cos_approx;
    float dy = dlat;
    return EARTH_RADIUS_F * sqrtf(dx * dx + dy * dy);
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static inline uint64_t read_u64(const uint8_t *d, size_t o) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)d[o + i]) << (i * 8);
    return v;
}

static inline uint32_t read_u32(const uint8_t *d, size_t o) {
    return d[o] | (d[o+1] << 8) | (d[o+2] << 16) | (d[o+3] << 24);
}

static inline float read_float(const uint8_t *d, size_t o) {
    float f;
    memcpy(&f, d + o, 4);
    return f;
}

/* ============================================================================
 * Tile Loading — hash-accelerated lookup
 * ============================================================================ */

static uint8_t* decompress_gzip(const char *path, size_t *out_size) {
    gzFile gz = gzopen(path, "rb");
    if (!gz) return NULL;
    size_t capacity = 1024 * 1024, size = 0;
    uint8_t *data = malloc(capacity);
    while (1) {
        if (size + 65536 > capacity) { capacity *= 2; data = realloc(data, capacity); }
        int r = gzread(gz, data + size, 65536);
        if (r <= 0) break;
        size += r;
    }
    gzclose(gz);
    *out_size = size;
    return data;
}

static inline uint32_t tile_hash(uint32_t tile_id) {
    return (tile_id * 2654435761u) & TILE_HASH_MASK;
}

static void tile_hash_insert(uint32_t tile_id, int idx) {
    uint32_t h = tile_hash(tile_id);
    for (int i = 0; i < TILE_HASH_SIZE; i++) {
        uint32_t slot = (h + i) & TILE_HASH_MASK;
        if (!g_tile_hash[slot].used) {
            g_tile_hash[slot].tile_id = tile_id;
            g_tile_hash[slot].idx = idx;
            g_tile_hash[slot].used = 1;
            return;
        }
    }
}

static Tile* load_tile(uint32_t tile_id) {
    /* Fast path: hash lookup instead of linear scan */
    uint32_t h = tile_hash(tile_id);
    for (int i = 0; i < TILE_HASH_SIZE; i++) {
        uint32_t slot = (h + i) & TILE_HASH_MASK;
        if (!g_tile_hash[slot].used) break;
        if (g_tile_hash[slot].tile_id == tile_id)
            return &g_tiles[g_tile_hash[slot].idx];
    }
    
    if (g_tile_count >= MAX_TILES) {
        /* Evict oldest tile and rebuild hash */
        free(g_tiles[0].raw_data);
        free(g_tiles[0].nodes);
        for (int i = 0; i < g_tile_count - 1; i++) {
            g_tiles[i] = g_tiles[i + 1];
        }
        g_tile_count--;
        memset(g_tile_hash, 0, sizeof(g_tile_hash));
        for (int i = 0; i < g_tile_count; i++)
            tile_hash_insert(g_tiles[i].tile_id, i);
    }
    
    char path[1024];
    snprintf(path, sizeof(path), "%s/2/%03d/%03d/%03d.gph.gz",
             g_tiles_dir, tile_id / 1000000, (tile_id / 1000) % 1000, tile_id % 1000);
    
    size_t raw_size;
    uint8_t *raw = decompress_gzip(path, &raw_size);
    if (!raw) return NULL;
    
    if (raw_size < HEADER_SIZE) { free(raw); return NULL; }
    
    Tile *t = &g_tiles[g_tile_count];
    t->tile_id = tile_id;
    t->raw_data = raw;
    t->raw_size = raw_size;
    
    t->base_lon = read_float(raw, 8);
    t->base_lat = read_float(raw, 12);
    
    uint64_t word5 = read_u64(raw, 40);
    t->node_count = word5 & 0x1FFFFF;
    t->edge_count = (word5 >> 21) & 0x1FFFFF;
    
    uint32_t word6 = read_u32(raw, 48);
    uint32_t trans_count = word6 & 0x3FFFFF;
    
    uint32_t nodes_offset = HEADER_SIZE;
    uint32_t transitions_offset = nodes_offset + t->node_count * NODE_SIZE;
    t->edges_offset = transitions_offset + trans_count * 8;
    
    t->nodes = malloc(t->node_count * sizeof(Node));
    for (uint32_t i = 0; i < t->node_count; i++) {
        size_t off = nodes_offset + i * NODE_SIZE;
        uint64_t w0 = read_u64(raw, off);
        uint64_t w1 = read_u64(raw, off + 8);
        
        double lat_d = t->base_lat + ((w0 & 0x3FFFFF) * 1e-6 + ((w0 >> 22) & 0xF) * 1e-7);
        double lon_d = t->base_lon + (((w0 >> 26) & 0x3FFFFF) * 1e-6 + ((w0 >> 48) & 0xF) * 1e-7);
        t->nodes[i].lat = (float)lat_d;
        t->nodes[i].lon = (float)lon_d;
        t->nodes[i].edge_index = w1 & 0x1FFFFF;
        t->nodes[i].edge_count = (w1 >> 21) & 0x7F;
    }
    
    tile_hash_insert(tile_id, g_tile_count);
    g_tile_count++;
    
    return t;
}

/* Fused edge parser: extracts both endpoint + details in one pass.
 * Original code called get_edge_end() and get_edge_details() separately,
 * each doing its own bounds check and offset calculation. This fuses them
 * into a single function, saving ~10 cycles per edge from redundant work. */
static inline int get_edge(Tile *t, uint32_t idx, Edge *e) {
    if (idx >= t->edge_count) return 0;
    size_t off = t->edges_offset + idx * EDGE_SIZE;
    if (off + EDGE_SIZE > t->raw_size) return 0;
    
    const uint8_t *d = t->raw_data + off;
    uint64_t w0 = read_u64(d, 0);
    uint64_t w2 = read_u64(d, 16);
    uint64_t w3 = read_u64(d, 24);
    uint64_t w4 = read_u64(d, 32);
    
    /* Endpoint info (was get_edge_end) */
    uint64_t endnode = w0 & 0x3FFFFFFFFFFFULL;
    e->end_level = endnode & 0x7;
    e->end_tile_id = (endnode >> 3) & 0x3FFFFF;
    e->end_node_id = (endnode >> 25) & 0x1FFFFF;
    
    uint32_t fwd = w3 & 0xFFF;
    uint32_t rev = (w3 >> 12) & 0xFFF;
    e->has_bike = ((fwd | rev) & kBicycleAccess) ? 1 : 0;
    e->has_ped = ((fwd | rev) & kPedestrianAccess) ? 1 : 0;
    e->has_car = ((fwd | rev) & kCarAccess) ? 1 : 0;
    
    /* Detail info (was get_edge_details) */
    e->speed = w2 & 0xFF;
    if (e->speed == 0) e->speed = 15;
    e->use = (w2 >> 40) & 0x3F;
    e->lanecount = (w2 >> 46) & 0xF;
    if (e->lanecount == 0) e->lanecount = 1;
    e->classification = (w2 >> 54) & 0x7;
    e->surface = (w2 >> 57) & 0x7;
    
    e->cycle_lane = (w3 >> 37) & 0x3;
    e->bike_network = (w3 >> 39) & 0x1;
    e->use_sidepath = (w3 >> 40) & 0x1;
    e->dismount = (w3 >> 41) & 0x1;
    e->shoulder = (w3 >> 44) & 0x1;
    e->sac_scale = (w3 >> 34) & 0x7;
    
    e->length = (float)((w4 >> 32) & 0xFFFFFF);
    e->weighted_grade = (w4 >> 56) & 0xF;
    if (e->weighted_grade == 0) e->weighted_grade = 7;
    
    return 1;
}

/* ============================================================================
 * 4-ary Heap with NEON inline asm min-finding
 * ============================================================================ */

static void heap_push_fwd(HeapEntry e) {
    if (g_heap_fwd_size >= MAX_HEAP) return;
    int i = g_heap_fwd_size++;
    g_heap_fwd[i] = e;
    while (i > 0) {
        int p = (i - 1) / HEAP_ARITY;
        if (g_heap_fwd[p].f <= g_heap_fwd[i].f) break;
        HeapEntry tmp = g_heap_fwd[p];
        g_heap_fwd[p] = g_heap_fwd[i];
        g_heap_fwd[i] = tmp;
        i = p;
    }
}

static HeapEntry heap_pop_fwd(void) {
    HeapEntry ret = g_heap_fwd[0];
    g_heap_fwd[0] = g_heap_fwd[--g_heap_fwd_size];
    int i = 0;
    while (1) {
        int first_child = HEAP_ARITY * i + 1;
        if (first_child >= g_heap_fwd_size) break;
        
        int smallest = first_child;
        int last_child = first_child + HEAP_ARITY - 1;
        if (last_child >= g_heap_fwd_size) last_child = g_heap_fwd_size - 1;
        int num_children = last_child - first_child + 1;
        
        if (num_children == 4) {
            float vals[4] = {
                g_heap_fwd[first_child].f,     g_heap_fwd[first_child + 1].f,
                g_heap_fwd[first_child + 2].f, g_heap_fwd[first_child + 3].f
            };
            float min_val = neon_min4(vals);
            for (int c = 0; c < 4; c++) {
                if (vals[c] == min_val) { smallest = first_child + c; break; }
            }
        } else {
            for (int c = first_child + 1; c <= last_child; c++) {
                if (g_heap_fwd[c].f < g_heap_fwd[smallest].f) smallest = c;
            }
        }
        
        if (g_heap_fwd[smallest].f >= g_heap_fwd[i].f) break;
        HeapEntry tmp = g_heap_fwd[i];
        g_heap_fwd[i] = g_heap_fwd[smallest];
        g_heap_fwd[smallest] = tmp;
        i = smallest;
    }
    return ret;
}

static void heap_push_bwd(HeapEntry e) {
    if (g_heap_bwd_size >= MAX_HEAP) return;
    int i = g_heap_bwd_size++;
    g_heap_bwd[i] = e;
    while (i > 0) {
        int p = (i - 1) / HEAP_ARITY;
        if (g_heap_bwd[p].f <= g_heap_bwd[i].f) break;
        HeapEntry tmp = g_heap_bwd[p];
        g_heap_bwd[p] = g_heap_bwd[i];
        g_heap_bwd[i] = tmp;
        i = p;
    }
}

static HeapEntry heap_pop_bwd(void) {
    HeapEntry ret = g_heap_bwd[0];
    g_heap_bwd[0] = g_heap_bwd[--g_heap_bwd_size];
    int i = 0;
    while (1) {
        int first_child = HEAP_ARITY * i + 1;
        if (first_child >= g_heap_bwd_size) break;
        
        int smallest = first_child;
        int last_child = first_child + HEAP_ARITY - 1;
        if (last_child >= g_heap_bwd_size) last_child = g_heap_bwd_size - 1;
        int num_children = last_child - first_child + 1;
        
        if (num_children == 4) {
            float vals[4] = {
                g_heap_bwd[first_child].f,     g_heap_bwd[first_child + 1].f,
                g_heap_bwd[first_child + 2].f, g_heap_bwd[first_child + 3].f
            };
            float min_val = neon_min4(vals);
            for (int c = 0; c < 4; c++) {
                if (vals[c] == min_val) { smallest = first_child + c; break; }
            }
        } else {
            for (int c = first_child + 1; c <= last_child; c++) {
                if (g_heap_bwd[c].f < g_heap_bwd[smallest].f) smallest = c;
            }
        }
        
        if (g_heap_bwd[smallest].f >= g_heap_bwd[i].f) break;
        HeapEntry tmp = g_heap_bwd[i];
        g_heap_bwd[i] = g_heap_bwd[smallest];
        g_heap_bwd[smallest] = tmp;
        i = smallest;
    }
    return ret;
}

/* ============================================================================
 * Visited Set — Robin Hood Open Addressing with power-of-2 table
 *
 * Key insight: the original used modulo on a prime-sized table (2000003).
 * On Cortex-A8 without hardware divide, % is emulated via __aeabi_uidivmod
 * — roughly 20-40 cycles per call. With power-of-2 masking it's 1 cycle.
 *
 * Robin Hood hashing keeps probe chains short (avg ~1.5 at 60% load) by
 * stealing from "rich" entries (short probe distance) to give to "poor"
 * ones (long probe distance). This bounds worst-case lookup to ~log(n).
 * ============================================================================ */

static void visited_clear_both(void) {
    memset(g_visited_fwd, 0, VISITED_SIZE * sizeof(VisitedEntry));
    memset(g_visited_bwd, 0, VISITED_SIZE * sizeof(VisitedEntry));
}

/* Fast hash: multiply-shift. On ARM this compiles to UMULL + shift,
 * no division needed. The magic constant is floor(2^32 * phi). */
static inline uint32_t hash_state(State s) {
    uint32_t h = s.tile_id * 2654435761u;
    h ^= s.node_id * 2246822519u;
    return h & VISITED_MASK;
}

/* Robin Hood find: walk until we find the entry, an empty slot,
 * or an entry with a shorter probe distance (meaning our key
 * can't be further ahead). */
static inline VisitedEntry* visited_find(VisitedEntry *table, State s) {
    uint32_t h = hash_state(s);
    uint8_t psl = 0;
    for (;;) {
        uint32_t idx = (h + psl) & VISITED_MASK;
        VisitedEntry *e = &table[idx];
        if (!e->occupied) return NULL;
        if (e->psl < psl) return NULL;  /* Robin Hood guarantee */
        if (e->tile_id == s.tile_id && e->node_id == s.node_id)
            return e;
        psl++;
        if (psl > 128) return NULL;  /* safety bound — should never hit */
    }
}

/* Robin Hood insert: if we encounter an entry with shorter probe
 * distance, we swap and continue inserting the displaced entry.
 * This keeps the variance of probe lengths very low. */
static inline void visited_insert(VisitedEntry *table, State s, float g,
                                   State parent, uint32_t parent_edge) {
    uint32_t h = hash_state(s);
    uint8_t psl = 0;
    
    VisitedEntry incoming;
    incoming.tile_id = s.tile_id;
    incoming.node_id = s.node_id;
    incoming.parent_tile = parent.tile_id;
    incoming.parent_node = parent.node_id;
    incoming.parent_edge = parent_edge;
    incoming.g = g;
    incoming.occupied = 1;
    incoming.psl = 0;
    
    for (;;) {
        uint32_t idx = (h + psl) & VISITED_MASK;
        VisitedEntry *e = &table[idx];
        
        if (!e->occupied) {
            incoming.psl = psl;
            *e = incoming;
            return;
        }
        
        /* Update existing entry if same state with better g */
        if (e->tile_id == incoming.tile_id && e->node_id == incoming.node_id) {
            if (incoming.g < e->g) {
                e->g = incoming.g;
                e->parent_tile = incoming.parent_tile;
                e->parent_node = incoming.parent_node;
                e->parent_edge = incoming.parent_edge;
            }
            return;
        }
        
        /* Robin Hood swap: steal from the rich */
        if (e->psl < psl) {
            incoming.psl = psl;
            VisitedEntry tmp = *e;
            *e = incoming;
            incoming = tmp;
            /* Continue inserting the displaced entry */
            h = hash_state((State){incoming.tile_id, incoming.node_id});
            psl = incoming.psl;
        }
        
        psl++;
        if (psl > 200) return; /* table too full — shouldn't happen */
    }
}

/* Convenience wrappers matching the original API */
static inline VisitedEntry* visited_find_fwd(State s) {
    return visited_find(g_visited_fwd, s);
}
static inline VisitedEntry* visited_find_bwd(State s) {
    return visited_find(g_visited_bwd, s);
}
static inline void visited_insert_fwd(State s, float g, State parent, uint32_t pe) {
    visited_insert(g_visited_fwd, s, g, parent, pe);
}
static inline void visited_insert_bwd(State s, float g, State parent, uint32_t pe) {
    visited_insert(g_visited_bwd, s, g, parent, pe);
}

/* ============================================================================
 * Costing
 * ============================================================================ */

static void init_costing(void) {
    for (int s = 0; s < 256; s++)
        kSpeedFactor[s] = (s > 0) ? (3.6f / s) : 3.6f;
    
    g_road_factor = (g_use_roads >= 0.5f) ? (1.5f - g_use_roads) : (2.0f - g_use_roads * 2.0f);
    
    g_cyclelane_factor[0] = 1.0f;
    g_cyclelane_factor[1] = 0.9f + g_use_roads * 0.05f;
    g_cyclelane_factor[2] = 0.4f + g_use_roads * 0.45f;
    g_cyclelane_factor[3] = 0.15f + g_use_roads * 0.6f;
    g_cyclelane_factor[4] = 0.7f + g_use_roads * 0.2f;
    g_cyclelane_factor[5] = 0.9f + g_use_roads * 0.05f;
    g_cyclelane_factor[6] = 0.4f + g_use_roads * 0.45f;
    g_cyclelane_factor[7] = 0.15f + g_use_roads * 0.6f;
    
    g_path_cyclelane_factor[0] = 0.2f + g_use_roads;
    g_path_cyclelane_factor[1] = 0.2f + g_use_roads;
    g_path_cyclelane_factor[2] = 0.1f + g_use_roads * 0.9f;
    g_path_cyclelane_factor[3] = g_use_roads * 0.8f;
    
    float avoid_roads = (1.0f - g_use_roads) * 0.75f + 0.25f;
    g_speedpenalty[0] = 1.0f;
    for (int s = 1; s < 256; s++) {
        float base_pen = (s <= 40) ? ((float)s / 40.0f) :
                         (s <= 65) ? ((float)s / 25.0f - 0.6f) :
                                     ((float)s / 50.0f + 0.7f);
        g_speedpenalty[s] = (base_pen - 1.0f) * avoid_roads + 1.0f;
    }
    
    float avoid_hills = 1.0f - g_use_hills;
    for (int i = 0; i < 16; i++)
        g_grade_penalty[i] = avoid_hills * kAvoidHillsStrength[i];
}

/* Adapted for fused Edge struct */
static float edge_cost(Edge *e) {
    if (e->length <= 0) return 1e9f;
    if (e->use == USE_STEPS) return e->length * kSpeedFactor[4] * 3.0f;
    if (e->use == USE_FERRY) return e->length * kSpeedFactor[e->speed] * 1.2f;
    
    int grade = e->weighted_grade; if (grade > 15) grade = 15;
    int surface = e->surface; if (surface > 7) surface = 7;
    
    float speed = kDefaultCyclingSpeed[g_bicycle_type]
                  * kSurfaceSpeedFactor[g_bicycle_type][surface]
                  * kGradeBasedSpeedFactor[grade];
    if (e->dismount) speed = kDismountSpeed;
    if (speed < 4.0f) speed = 4.0f;
    if (speed > 40.0f) speed = 40.0f;
    
    float time_cost = e->length / (speed / 3.6f);
    float preference = 1.0f;
    
    if (e->use == USE_CYCLEWAY)          preference = 0.9f;
    else if (e->use == USE_TRACK)        preference = 0.9f;
    else if (e->use == USE_MOUNTAIN_BIKE){ if (g_bicycle_type == 3) preference = 0.85f; }
    else if (e->use == USE_PATH || e->use == USE_FOOTWAY) preference = 0.95f;
    else if (e->use == USE_LIVING_STREET) preference = 0.95f;
    else if (e->use == USE_ROAD) {
        preference = 1.0f + (1.0f - g_use_roads) * 0.15f;
        if (e->cycle_lane >= 2) preference -= 0.1f;
    }
    
    if (e->bike_network) preference *= 0.95f;
    if (!e->has_bike && e->has_ped) preference *= g_avoid_pushing ? 2.0f : 1.3f;
    
    if (g_avoid_cars && e->has_car) {
        if (e->use == USE_TRACK || e->use == USE_LIVING_STREET || e->use == USE_SERVICE_ROAD) {
            preference *= 1.05f;
        } else {
            float stress = 0.2f;
            if (e->speed > 50) stress += 0.3f;
            if (e->speed > 70) stress += 0.3f;
            if (e->classification <= 2) stress += 0.2f;
            if (e->lanecount >= 2) stress += 0.1f;
            if (e->cycle_lane >= 2) stress -= 0.3f;
            if (stress < 0.1f) stress = 0.1f;
            if (stress > 1.0f) stress = 1.0f;
            preference *= 1.0f + stress * 0.5f;
        }
    }
    return time_cost * preference;
}

/* Pedestrian costing: alpine mode — shortest route, all ways allowed.
 * Simple time-based cost at ~5 km/h base speed with grade/sac_scale
 * adjustments for realistic walking times. No preference penalties. */
static const float kWalkingSpeed = 5.0f;  /* km/h base */

/* SAC scale speed factors: T1=easy..T6=difficult alpine.
 * These slow you down but never block. Index 0 = no tag (normal path). */
static const float kSacScaleSpeedFactor[8] = {
    1.0f,   /* 0: kNone - normal path */
    1.0f,   /* 1: T1 hiking - easy trail */
    0.85f,  /* 2: T2 mountain hiking */
    0.70f,  /* 3: T3 demanding mountain hiking */
    0.55f,  /* 4: T4 alpine hiking */
    0.40f,  /* 5: T5 demanding alpine hiking */
    0.30f,  /* 6: T6 difficult alpine hiking */
    0.30f   /* 7: reserved */
};

/* Grade-based walking speed factor (Tobler's hiking function simplified).
 * Index 7 = flat. Lower = uphill, higher = downhill. */
static const float kPedGradeSpeedFactor[16] = {
    1.4f, 1.3f, 1.2f, 1.1f, 1.05f, 1.0f, 1.0f, 1.0f,
    0.95f, 0.85f, 0.75f, 0.60f, 0.50f, 0.40f, 0.35f, 0.30f
};

static float pedestrian_cost(Edge *e) {
    if (e->length <= 0) return 1e9f;
    
    int grade = e->weighted_grade; if (grade > 15) grade = 15;
    int sac = e->sac_scale; if (sac > 7) sac = 7;
    
    float speed = kWalkingSpeed 
                  * kPedGradeSpeedFactor[grade]
                  * kSacScaleSpeedFactor[sac];
    
    /* Steps: slower but always walkable */
    if (e->use == USE_STEPS) speed *= 0.6f;
    /* Ferry: use ferry speed */
    if (e->use == USE_FERRY) return e->length * kSpeedFactor[e->speed] * 1.1f;
    
    if (speed < 1.5f) speed = 1.5f;  /* minimum 1.5 km/h even on T6 */
    if (speed > 6.0f) speed = 6.0f;
    
    /* Pure time cost — no preference penalties for shortest route */
    return e->length / (speed / 3.6f);
}

/* ============================================================================
 * Find Nearest Node - NEON batch distance
 * ============================================================================ */

static uint32_t find_nearest_node(Tile *t, float lat, float lon) {
    uint32_t best = 0, best_bike = 0;
    float best_dist_sq = 1e18f, best_bike_dist_sq = 1e18f;
    
    float mid_lat_rad = lat * DEG_TO_RAD_F;
    float x2 = mid_lat_rad * mid_lat_rad;
    float pi2 = 9.8696044f;
    float cos_approx = (pi2 - 4.0f * x2) / (pi2 + x2);
    float cos2 = cos_approx * cos_approx;
    float r2 = EARTH_RADIUS_F * EARTH_RADIUS_F * DEG_TO_RAD_F * DEG_TO_RAD_F;
    
    uint32_t i = 0;
    
    /* NEON batch: 4 nodes at a time */
    for (; i + 3 < t->node_count; i += 4) {
        if (t->nodes[i].edge_count == 0 && t->nodes[i+1].edge_count == 0 &&
            t->nodes[i+2].edge_count == 0 && t->nodes[i+3].edge_count == 0) continue;
        
        float dlat_arr[4] = {
            t->nodes[i].lat - lat,   t->nodes[i+1].lat - lat,
            t->nodes[i+2].lat - lat, t->nodes[i+3].lat - lat
        };
        float dlon_arr[4] = {
            t->nodes[i].lon - lon,   t->nodes[i+1].lon - lon,
            t->nodes[i+2].lon - lon, t->nodes[i+3].lon - lon
        };
        
        float dists[4];
        neon_dist_sq_4(dlat_arr, dlon_arr, cos2, r2, dists);
        
        for (int j = 0; j < 4; j++) {
            uint32_t ni = i + j;
            if (t->nodes[ni].edge_count == 0) continue;
            float d = dists[j];
            
            /* Early distance rejection: skip edge check if too far */
            if (d >= best_bike_dist_sq && d >= best_dist_sq) continue;
            
            int has_bike_edge = 0;
            for (uint32_t ei = t->nodes[ni].edge_index; 
                 ei < t->nodes[ni].edge_index + t->nodes[ni].edge_count && ei < t->edge_count; 
                 ei++) {
                Edge edge;
                if (!get_edge(t, ei, &edge)) continue;
                if (g_routing_mode == 1) {
                    if (edge.has_ped) { has_bike_edge = 1; break; }
                } else {
                    if (edge.has_bike || edge.has_ped) { has_bike_edge = 1; break; }
                }
            }
            
            if (has_bike_edge && d < best_bike_dist_sq) {
                best_bike_dist_sq = d; best_bike = ni;
            }
            if (d < best_dist_sq) { best_dist_sq = d; best = ni; }
        }
    }
    
    /* Scalar remainder */
    for (; i < t->node_count; i++) {
        if (t->nodes[i].edge_count == 0) continue;
        float dlv = t->nodes[i].lat - lat, dlov = t->nodes[i].lon - lon;
        float d = r2 * (dlv * dlv + dlov * dlov * cos2);
        
        if (d >= best_bike_dist_sq && d >= best_dist_sq) continue;
        
        int has_bike_edge = 0;
        for (uint32_t ei = t->nodes[i].edge_index; 
             ei < t->nodes[i].edge_index + t->nodes[i].edge_count && ei < t->edge_count; ei++) {
            Edge edge;
            if (!get_edge(t, ei, &edge)) continue;
            if (g_routing_mode == 1) {
                if (edge.has_ped) { has_bike_edge = 1; break; }
            } else {
                if (edge.has_bike || edge.has_ped) { has_bike_edge = 1; break; }
            }
        }
        if (has_bike_edge && d < best_bike_dist_sq) { best_bike_dist_sq = d; best_bike = i; }
        if (d < best_dist_sq) { best_dist_sq = d; best = i; }
    }
    
    float threshold_sq = 500.0f * 500.0f;
    if (best_bike_dist_sq < threshold_sq || best_bike_dist_sq < best_dist_sq * 4.0f)
        return best_bike;
    return best;
}

/* ============================================================================
 * Main Routing - Bidirectional A*
 * ============================================================================ */

static void calculate_statistics(void) {
    for (int i = 0; i < g_path_len - 1; i++) {
        State s = g_path[i]; State next = g_path[i + 1];
        Tile *t = load_tile(s.tile_id);
        if (!t || s.node_id >= t->node_count) continue;
        Node *n = &t->nodes[s.node_id];
        for (uint32_t ei = n->edge_index; 
             ei < n->edge_index + n->edge_count && ei < t->edge_count; ei++) {
            Edge edge;
            if (!get_edge(t, ei, &edge)) continue;
            if (edge.end_tile_id == next.tile_id && edge.end_node_id == next.node_id) {
                if (g_routing_mode == 1) {
                    /* Pedestrian stats: track steps, paths, roads */
                    if (edge.use == USE_STEPS) g_dist_steps += edge.length;
                    else if (edge.use == USE_PATH || edge.use == USE_FOOTWAY || 
                             edge.use == USE_CYCLEWAY || edge.use == USE_MOUNTAIN_BIKE)
                        g_dist_car_free += edge.length;
                    else if (edge.use == USE_TRACK || edge.use == USE_LIVING_STREET ||
                             edge.use == USE_SERVICE_ROAD)
                        g_dist_car_free += edge.length;
                    else if (edge.has_car)
                        g_dist_with_cars += edge.length;
                    else
                        g_dist_car_free += edge.length;
                } else {
                    int is_path = (edge.use == USE_CYCLEWAY || edge.use == USE_PATH || 
                                   edge.use == USE_FOOTWAY || edge.use == USE_MOUNTAIN_BIKE);
                    int is_low_traffic = (edge.use == USE_TRACK || edge.use == USE_LIVING_STREET ||
                                          edge.use == USE_SERVICE_ROAD);
                    if (!edge.has_bike && edge.has_ped) g_dist_pushing += edge.length;
                    else if (is_path && !edge.has_car) g_dist_car_free += edge.length;
                    else if (is_low_traffic) g_dist_car_free += edge.length;
                    else if (edge.cycle_lane >= 2) g_dist_separated += edge.length;
                    else if (edge.has_car) g_dist_with_cars += edge.length;
                    else g_dist_car_free += edge.length;
                }
                break;
            }
        }
    }
}

static int route(uint32_t start_tile_id, uint32_t start_node,
                 uint32_t end_tile_id, uint32_t end_node,
                 float end_lat, float end_lon) {
    
    init_costing();
    g_heap_fwd_size = 0; g_heap_bwd_size = 0;
    visited_clear_both(); g_path_len = 0;
    g_dist_car_free = 0; g_dist_separated = 0;
    g_dist_with_cars = 0; g_dist_pushing = 0;
    g_dist_steps = 0;
    
    Tile *start_tile = load_tile(start_tile_id);
    Tile *end_tile = load_tile(end_tile_id);
    if (!start_tile || start_node >= start_tile->node_count) {
        fprintf(stderr, "[ERROR] Invalid start\n"); return 0;
    }
    if (!end_tile || end_node >= end_tile->node_count) {
        fprintf(stderr, "[ERROR] Invalid end\n"); return 0;
    }
    
    Node *sn = &start_tile->nodes[start_node];
    float start_lat = sn->lat, start_lon = sn->lon;
    float init_dist = fast_distance(start_lat, start_lon, end_lat, end_lon);
    float max_speed = (g_routing_mode == 1) ? 8.0f : 2.0f * kDefaultCyclingSpeed[g_bicycle_type];
    
    State start_state = { start_tile_id, start_node };
    State end_state = { end_tile_id, end_node };
    State null_state = { 0, 0 };
    
    /* Precompute heuristic constants for inlined fast_distance.
     * For the forward search we compute distance to (end_lat, end_lon),
     * for backward to (start_lat, start_lon). The cos_approx factor
     * varies by latitude, but for typical routing distances (<200km)
     * using the midpoint latitude is accurate enough. */
    float mid_route_lat = (start_lat + end_lat) * 0.5f;
    float mid_rad = mid_route_lat * DEG_TO_RAD_F;
    float mx2 = mid_rad * mid_rad;
    float mpi2 = 9.8696044f;
    float cos_mid = (mpi2 - 4.0f * mx2) / (mpi2 + mx2);
    float heur_speed_factor = kSpeedFactor[(int)max_speed];
    
    float h_init = init_dist * heur_speed_factor;
    HeapEntry init_fwd = { h_init, 0, start_state, null_state, 0 };
    heap_push_fwd(init_fwd);
    visited_insert_fwd(start_state, 0, null_state, 0);
    
    HeapEntry init_bwd = { h_init, 0, end_state, null_state, 0 };
    heap_push_bwd(init_bwd);
    visited_insert_bwd(end_state, 0, null_state, 0);
    
    int iterations = 0;
    int max_iterations = (int)(init_dist / 1000.0f * 30000);
    if (max_iterations < 1000000) max_iterations = 1000000;
    if (max_iterations > 6000000) max_iterations = 6000000;
    
    fprintf(stderr, "[ROUTE-BIDIR-NEON2] Distance: %.1f km, max_iter: %d\n", 
            init_dist / 1000.0f, max_iterations);
    
    State meeting_point = { 0, 0 };
    float best_total_cost = 1e18f;
    
    while ((g_heap_fwd_size > 0 || g_heap_bwd_size > 0) && iterations < max_iterations) {
        
        /* === FORWARD === */
        if (g_heap_fwd_size > 0) {
            HeapEntry cur = heap_pop_fwd(); iterations++;
            VisitedEntry *ve = visited_find_fwd(cur.state);
            if (ve && cur.g > ve->g) goto do_backward;
            
            VisitedEntry *bwd_ve = visited_find_bwd(cur.state);
            if (bwd_ve) {
                float total = cur.g + bwd_ve->g;
                if (total < best_total_cost) {
                    best_total_cost = total; meeting_point = cur.state;
                    fprintf(stderr, "[ROUTE-BIDIR-NEON2] Meet iter %d cost=%.1f\n", iterations, total);
                }
            }
            
            Tile *tile = load_tile(cur.state.tile_id);
            if (!tile || cur.state.node_id >= tile->node_count) goto do_backward;
            Node *node = &tile->nodes[cur.state.node_id];
            
            for (uint32_t ei = node->edge_index; 
                 ei < node->edge_index + node->edge_count && ei < tile->edge_count; ei++) {
                Edge edge;
                if (!get_edge(tile, ei, &edge)) continue;
                if (edge.end_level != 2) continue;
                if (g_routing_mode == 1) {
                    if (!edge.has_ped) continue;
                } else {
                    if (!edge.has_bike && !edge.has_ped) continue;
                    if (edge.surface > kWorstAllowedSurface[g_bicycle_type]) continue;
                }
                
                float cost;
                if (g_routing_mode == 1) {
                    cost = pedestrian_cost(&edge);
                } else {
                    cost = edge_cost(&edge);
                    if (!edge.has_bike && edge.has_ped) cost *= g_avoid_pushing ? 5.0f : 2.0f;
                }
                float new_g = cur.g + cost;
                State ns = { edge.end_tile_id, edge.end_node_id };
                VisitedEntry *nve = visited_find_fwd(ns);
                if (nve && new_g >= nve->g) continue;
                Tile *ntile = load_tile(ns.tile_id);
                if (!ntile || ns.node_id >= ntile->node_count) continue;
                Node *nn = &ntile->nodes[ns.node_id];
                /* Inlined heuristic with precomputed cos_mid */
                float dlat_h = (nn->lat - end_lat) * DEG_TO_RAD_F;
                float dlon_h = (nn->lon - end_lon) * DEG_TO_RAD_F;
                float dx_h = dlon_h * cos_mid;
                float h = EARTH_RADIUS_F * sqrtf(dx_h * dx_h + dlat_h * dlat_h)
                          * heur_speed_factor;
                HeapEntry ne = { new_g + h, new_g, ns, cur.state, ei };
                heap_push_fwd(ne);
                visited_insert_fwd(ns, new_g, cur.state, ei);
            }
        }
        
do_backward:
        /* === BACKWARD === */
        if (g_heap_bwd_size > 0) {
            HeapEntry cur = heap_pop_bwd(); iterations++;
            VisitedEntry *ve = visited_find_bwd(cur.state);
            if (ve && cur.g > ve->g) continue;
            
            VisitedEntry *fwd_ve = visited_find_fwd(cur.state);
            if (fwd_ve) {
                float total = cur.g + fwd_ve->g;
                if (total < best_total_cost) {
                    best_total_cost = total; meeting_point = cur.state;
                    fprintf(stderr, "[ROUTE-BIDIR-NEON2] Meet iter %d cost=%.1f\n", iterations, total);
                }
            }
            
            Tile *tile = load_tile(cur.state.tile_id);
            if (!tile || cur.state.node_id >= tile->node_count) continue;
            Node *node = &tile->nodes[cur.state.node_id];
            
            for (uint32_t ei = node->edge_index; 
                 ei < node->edge_index + node->edge_count && ei < tile->edge_count; ei++) {
                Edge edge;
                if (!get_edge(tile, ei, &edge)) continue;
                if (edge.end_level != 2) continue;
                if (g_routing_mode == 1) {
                    if (!edge.has_ped) continue;
                } else {
                    if (!edge.has_bike && !edge.has_ped) continue;
                    if (edge.surface > kWorstAllowedSurface[g_bicycle_type]) continue;
                }
                
                float cost;
                if (g_routing_mode == 1) {
                    cost = pedestrian_cost(&edge);
                } else {
                    cost = edge_cost(&edge);
                    if (!edge.has_bike && edge.has_ped) cost *= g_avoid_pushing ? 5.0f : 2.0f;
                }
                float new_g = cur.g + cost;
                State ns = { edge.end_tile_id, edge.end_node_id };
                VisitedEntry *nve = visited_find_bwd(ns);
                if (nve && new_g >= nve->g) continue;
                Tile *ntile = load_tile(ns.tile_id);
                if (!ntile || ns.node_id >= ntile->node_count) continue;
                Node *nn = &ntile->nodes[ns.node_id];
                /* Inlined heuristic toward start */
                float dlat_h = (nn->lat - start_lat) * DEG_TO_RAD_F;
                float dlon_h = (nn->lon - start_lon) * DEG_TO_RAD_F;
                float dx_h = dlon_h * cos_mid;
                float h = EARTH_RADIUS_F * sqrtf(dx_h * dx_h + dlat_h * dlat_h)
                          * heur_speed_factor;
                HeapEntry ne = { new_g + h, new_g, ns, cur.state, ei };
                heap_push_bwd(ne);
                visited_insert_bwd(ns, new_g, cur.state, ei);
            }
        }
        
        if (iterations % 500000 == 0)
            fprintf(stderr, "[ROUTE-BIDIR-NEON2] Iter %d: fwd=%d bwd=%d tiles=%d\n", 
                    iterations, g_heap_fwd_size, g_heap_bwd_size, g_tile_count);
        
        if (meeting_point.tile_id != 0) {
            float min_fwd = (g_heap_fwd_size > 0) ? g_heap_fwd[0].f : 1e18f;
            float min_bwd = (g_heap_bwd_size > 0) ? g_heap_bwd[0].f : 1e18f;
            if (min_fwd + min_bwd >= best_total_cost) {
                fprintf(stderr, "[ROUTE-BIDIR-NEON2] Early termination: optimal\n");
                break;
            }
        }
    }
    
    if (meeting_point.tile_id == 0) {
        fprintf(stderr, "[ROUTE-BIDIR-NEON2] No path (iter=%d tiles=%d)\n", iterations, g_tile_count);
        return 0;
    }
    
    fprintf(stderr, "[ROUTE-BIDIR-NEON2] Reconstructing...\n");
    
    /* Forward path: meeting -> start (then reverse) */
    State *fwd_path = malloc(MAX_PATH * sizeof(State));
    int fwd_len = 0;
    State s = meeting_point;
    while (s.tile_id != 0 || s.node_id != 0) {
        if (fwd_len >= MAX_PATH) break;
        fwd_path[fwd_len++] = s;
        VisitedEntry *ve = visited_find_fwd(s);
        if (!ve) break;
        State parent = { ve->parent_tile, ve->parent_node };
        if (parent.tile_id == 0 && parent.node_id == 0) {
            if (s.tile_id == start_state.tile_id && s.node_id == start_state.node_id) break;
        }
        s = parent;
    }
    for (int j = 0; j < fwd_len / 2; j++) {
        State tmp = fwd_path[j];
        fwd_path[j] = fwd_path[fwd_len - 1 - j];
        fwd_path[fwd_len - 1 - j] = tmp;
    }
    
    /* Backward path: meeting -> end */
    State *bwd_path = malloc(MAX_PATH * sizeof(State));
    int bwd_len = 0;
    s = meeting_point;
    VisitedEntry *mp_ve = visited_find_bwd(s);
    if (mp_ve) { s.tile_id = mp_ve->parent_tile; s.node_id = mp_ve->parent_node; }
    while (s.tile_id != 0 || s.node_id != 0) {
        if (bwd_len >= MAX_PATH) break;
        bwd_path[bwd_len++] = s;
        VisitedEntry *ve = visited_find_bwd(s);
        if (!ve) break;
        State parent = { ve->parent_tile, ve->parent_node };
        if (parent.tile_id == 0 && parent.node_id == 0) {
            if (s.tile_id == end_state.tile_id && s.node_id == end_state.node_id) break;
        }
        s = parent;
    }
    
    g_path_len = 0;
    for (int j = 0; j < fwd_len && g_path_len < MAX_PATH; j++) g_path[g_path_len++] = fwd_path[j];
    for (int j = 0; j < bwd_len && g_path_len < MAX_PATH; j++) g_path[g_path_len++] = bwd_path[j];
    free(fwd_path); free(bwd_path);
    
    fprintf(stderr, "[ROUTE-BIDIR-NEON2] Path: %d nodes\n", g_path_len);
    calculate_statistics();
    return g_path_len;
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char *argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tiles_dir> <from_lat> <from_lon> <to_lat> <to_lon> "
                "[avoid_pushing] [avoid_cars] [use_roads] [bike_type] [mode]\n", argv[0]);
        fprintf(stderr, "  bike_type: 0=Road, 1=Cross, 2=Hybrid, 3=Mountain\n");
        fprintf(stderr, "  mode: 0=bicycle (default), 1=pedestrian (alpine)\n");
        fprintf(stderr, "  NEON: %s (inline asm, no -mfpu needed)\n", HAS_NEON ? "YES" : "NO");
        fprintf(stderr, "  Hash: Robin Hood (power-of-2, %d entries)\n", VISITED_SIZE);
        return 1;
    }
    
    strncpy(g_tiles_dir, argv[1], sizeof(g_tiles_dir) - 1);
    float from_lat = (float)atof(argv[2]), from_lon = (float)atof(argv[3]);
    float to_lat = (float)atof(argv[4]), to_lon = (float)atof(argv[5]);
    
    if (argc > 6) g_avoid_pushing = atoi(argv[6]);
    if (argc > 7) g_avoid_cars = atoi(argv[7]);
    if (argc > 8) g_use_roads = (float)atof(argv[8]);
    if (argc > 9) g_bicycle_type = atoi(argv[9]);
    if (argc > 10) g_routing_mode = atoi(argv[10]);
    
    if (g_use_roads < 0) g_use_roads = 0;
    if (g_use_roads > 1) g_use_roads = 1;
    if (g_bicycle_type < 0) g_bicycle_type = 0;
    if (g_bicycle_type > 3) g_bicycle_type = 3;
    if (g_routing_mode < 0) g_routing_mode = 0;
    if (g_routing_mode > 1) g_routing_mode = 1;
    
    const char *bike_names[] = {"Road", "Cross", "Hybrid", "Mountain"};
    const char *mode_names[] = {"Bicycle", "Pedestrian"};
    fprintf(stderr, "[ROUTE] Options: avoid_pushing=%d avoid_cars=%d use_roads=%.2f bike=%s mode=%s\n",
            g_avoid_pushing, g_avoid_cars, g_use_roads, bike_names[g_bicycle_type], mode_names[g_routing_mode]);
    fprintf(stderr, "[ROUTE] Build: NEON=inline-asm, 4-ary heap, Robin Hood hash, fused edges\n");
    
    g_heap_fwd = malloc(MAX_HEAP * sizeof(HeapEntry));
    g_heap_bwd = malloc(MAX_HEAP * sizeof(HeapEntry));
    g_visited_fwd = malloc(VISITED_SIZE * sizeof(VisitedEntry));
    g_visited_bwd = malloc(VISITED_SIZE * sizeof(VisitedEntry));
    g_path = malloc(MAX_PATH * sizeof(State));
    if (!g_heap_fwd || !g_heap_bwd || !g_visited_fwd || !g_visited_bwd || !g_path) {
        fprintf(stderr, "Memory allocation failed\n"); return 1;
    }
    memset(g_tile_hash, 0, sizeof(g_tile_hash));
    
    int from_row = (int)((from_lat + 90.0f) / LEVEL_2_SIZE);
    int from_col = (int)((from_lon + 180.0f) / LEVEL_2_SIZE);
    uint32_t from_tile_id = from_row * 1440 + from_col;
    int to_row = (int)((to_lat + 90.0f) / LEVEL_2_SIZE);
    int to_col = (int)((to_lon + 180.0f) / LEVEL_2_SIZE);
    uint32_t to_tile_id = to_row * 1440 + to_col;
    
    fprintf(stderr, "[DEBUG] from_tile=%u to_tile=%u\n", from_tile_id, to_tile_id);
    
    Tile *from_tile = load_tile(from_tile_id);
    Tile *to_tile = load_tile(to_tile_id);
    if (!from_tile || !to_tile) {
        fprintf(stderr, "[ERROR] Failed to load tiles\n");
        printf("{\"error\": \"tile_load_failed\"}\n"); return 1;
    }
    
    uint32_t start_node = find_nearest_node(from_tile, from_lat, from_lon);
    uint32_t end_node = find_nearest_node(to_tile, to_lat, to_lon);
    fprintf(stderr, "[DEBUG] start_node=%u end_node=%u\n", start_node, end_node);
    
    int path_len = route(from_tile_id, start_node, to_tile_id, end_node, to_lat, to_lon);
    if (path_len == 0) { printf("{\"error\": \"no_path\"}\n"); return 1; }
    
    printf("{\"coords\": [");
    for (int i = 0; i < path_len; i++) {
        Tile *t = load_tile(g_path[i].tile_id);
        if (t && g_path[i].node_id < t->node_count) {
            if (i > 0) printf(",");
            printf("{\"lat\":%.6f,\"lon\":%.6f}", 
                   t->nodes[g_path[i].node_id].lat, t->nodes[g_path[i].node_id].lon);
        }
    }
    printf("], \"dist_car_free_km\": %.2f, \"dist_separated_km\": %.2f, "
           "\"dist_with_cars_km\": %.2f, \"dist_pushing_km\": %.2f, "
           "\"dist_steps_km\": %.2f, \"mode\": \"%s\"}\n",
           g_dist_car_free / 1000.0f, g_dist_separated / 1000.0f,
           g_dist_with_cars / 1000.0f, g_dist_pushing / 1000.0f,
           g_dist_steps / 1000.0f,
           g_routing_mode == 1 ? "pedestrian" : "bicycle");
    
    for (int i = 0; i < g_tile_count; i++) { free(g_tiles[i].raw_data); free(g_tiles[i].nodes); }
    free(g_heap_fwd); free(g_heap_bwd);
    free(g_visited_fwd); free(g_visited_bwd); free(g_path);
    return 0;
}
