/*
 * vrouter_final.c - Working Valhalla bicycle router for Nokia N9
 * 
 * TESTED AND WORKING - reaches destination correctly
 * 
 * Key fixes:
 * 1. Larger visited set (2M entries)
 * 2. Better hash function
 * 3. Correct access bits (kBicycleAccess=4)
 * 4. Proper tile boundary handling
 *
 * Compile: arm-none-linux-gnueabi-gcc -O2 -std=c99 --sysroot=$SYSROOT \
 *          -o vrouter vrouter_final.c -lz -lm
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

/* Constants */
#define HEADER_SIZE 272
#define NODE_SIZE 32
#define EDGE_SIZE 48
#define LEVEL_2_SIZE 0.25

/* Access bits - CORRECT values from Valhalla */
#define kAutoAccess 1
#define kPedestrianAccess 2
#define kBicycleAccess 4
#define kTruckAccess 8
#define kCarAccess 1  /* Same as kAutoAccess */

/* Limits - increased for long routes */
#define MAX_TILES 200
#define MAX_HEAP 1000000
#define MAX_VISITED 2000003  /* Large prime number */
#define MAX_PATH 200000

#define EARTH_RADIUS 6371000.0
#define DEG_TO_RAD (M_PI / 180.0)

/* ============================================================================
 * Data Structures
 * ============================================================================ */

typedef struct {
    double lat, lon;
    uint32_t edge_index, edge_count;
} Node;

typedef struct {
    uint8_t end_level;
    uint32_t end_tile_id, end_node_id;
    uint8_t has_bike, has_ped, has_car;
} EdgeEnd;

typedef struct {
    float length;
    uint8_t use, classification, cycle_lane, surface;
    uint8_t speed, bike_network, lanecount, use_sidepath;
    uint8_t dismount, shoulder, weighted_grade;
} EdgeDetails;

typedef struct { 
    uint32_t tile_id, node_id; 
} State;

typedef struct { 
    float f, g, dist;
    State state, parent;
    uint32_t parent_edge;
} HeapEntry;

typedef struct {
    State state;
    State parent;
    uint32_t parent_edge;
    float g;
    uint8_t valid;
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

/* Forward search (from start) */
static HeapEntry *g_heap_fwd = NULL;
static int g_heap_fwd_size = 0;
static VisitedEntry *g_visited_fwd = NULL;

/* Backward search (from goal) */
static HeapEntry *g_heap_bwd = NULL;
static int g_heap_bwd_size = 0;
static VisitedEntry *g_visited_bwd = NULL;

static State *g_path = NULL;
static int g_path_len = 0;

/* Routing options */
static float g_use_roads = 0.25f;
static float g_use_hills = 0.25f;
static int g_bicycle_type = 3;  /* 0=Road, 1=Cross, 2=Hybrid, 3=Mountain */
static int g_avoid_pushing = 0;
static int g_avoid_cars = 0;

/* Statistics */
static float g_dist_car_free = 0, g_dist_separated = 0;
static float g_dist_with_cars = 0, g_dist_pushing = 0;

/* ============================================================================
 * Bicycle Costing Constants (from Valhalla bicyclecost.cc)
 * ============================================================================ */

#define USE_ROAD 0
#define USE_TRACK 3
#define USE_LIVING_STREET 10
#define USE_SERVICE_ROAD 11   /* Generic service road */
#define USE_CYCLEWAY 20
#define USE_MOUNTAIN_BIKE 21  /* Mountain bike trail - WICHTIG für MTB routing! */
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

/* Pre-computed costing factors */
static float g_road_factor;
static float g_cyclelane_factor[8];
static float g_path_cyclelane_factor[4];
static float g_speedpenalty[256];
static float g_grade_penalty[16];

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static inline double haversine(double lat1, double lon1, double lat2, double lon2) {
    double dlat = (lat2 - lat1) * DEG_TO_RAD;
    double dlon = (lon2 - lon1) * DEG_TO_RAD;
    double a = sin(dlat/2) * sin(dlat/2) +
               cos(lat1 * DEG_TO_RAD) * cos(lat2 * DEG_TO_RAD) *
               sin(dlon/2) * sin(dlon/2);
    return EARTH_RADIUS * 2 * atan2(sqrt(a), sqrt(1-a));
}

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
 * Tile Loading
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

static Tile* load_tile(uint32_t tile_id) {
    /* Check cache */
    for (int i = 0; i < g_tile_count; i++) {
        if (g_tiles[i].tile_id == tile_id) return &g_tiles[i];
    }
    
    /* If cache full, evict oldest tile (simple FIFO) */
    if (g_tile_count >= MAX_TILES) {
        /* Free the first (oldest) tile */
        free(g_tiles[0].raw_data);
        free(g_tiles[0].nodes);
        /* Shift all tiles down */
        for (int i = 0; i < g_tile_count - 1; i++) {
            g_tiles[i] = g_tiles[i + 1];
        }
        g_tile_count--;
    }
    
    char path[1024];
    snprintf(path, sizeof(path), "%s/2/%03d/%03d/%03d.gph.gz",
             g_tiles_dir, tile_id / 1000000, (tile_id / 1000) % 1000, tile_id % 1000);
    
    size_t raw_size;
    uint8_t *raw = decompress_gzip(path, &raw_size);
    if (!raw) return NULL;
    
    if (raw_size < HEADER_SIZE) { free(raw); return NULL; }
    
    Tile *t = &g_tiles[g_tile_count++];
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
    
    /* Parse nodes */
    t->nodes = malloc(t->node_count * sizeof(Node));
    for (uint32_t i = 0; i < t->node_count; i++) {
        size_t off = nodes_offset + i * NODE_SIZE;
        uint64_t w0 = read_u64(raw, off);
        uint64_t w1 = read_u64(raw, off + 8);
        
        t->nodes[i].lat = t->base_lat + ((w0 & 0x3FFFFF) * 1e-6 + ((w0 >> 22) & 0xF) * 1e-7);
        t->nodes[i].lon = t->base_lon + (((w0 >> 26) & 0x3FFFFF) * 1e-6 + ((w0 >> 48) & 0xF) * 1e-7);
        t->nodes[i].edge_index = w1 & 0x1FFFFF;
        t->nodes[i].edge_count = (w1 >> 21) & 0x7F;
    }
    
    return t;
}

static int get_edge_end(Tile *t, uint32_t idx, EdgeEnd *ee) {
    if (idx >= t->edge_count) return 0;
    size_t off = t->edges_offset + idx * EDGE_SIZE;
    if (off + EDGE_SIZE > t->raw_size) return 0;
    
    uint64_t w0 = read_u64(t->raw_data, off);
    uint64_t w3 = read_u64(t->raw_data, off + 24);
    
    uint64_t endnode = w0 & 0x3FFFFFFFFFFFULL;
    ee->end_level = endnode & 0x7;
    ee->end_tile_id = (endnode >> 3) & 0x3FFFFF;
    ee->end_node_id = (endnode >> 25) & 0x1FFFFF;
    
    uint32_t fwd = w3 & 0xFFF;
    uint32_t rev = (w3 >> 12) & 0xFFF;
    ee->has_bike = ((fwd | rev) & kBicycleAccess) ? 1 : 0;
    ee->has_ped = ((fwd | rev) & kPedestrianAccess) ? 1 : 0;
    ee->has_car = ((fwd | rev) & kCarAccess) ? 1 : 0;
    
    return 1;
}

static int get_edge_details(Tile *t, uint32_t idx, EdgeDetails *ed) {
    if (idx >= t->edge_count) return 0;
    size_t off = t->edges_offset + idx * EDGE_SIZE;
    if (off + EDGE_SIZE > t->raw_size) return 0;
    
    uint64_t w2 = read_u64(t->raw_data, off + 16);
    uint64_t w3 = read_u64(t->raw_data, off + 24);
    uint64_t w4 = read_u64(t->raw_data, off + 32);
    
    ed->speed = w2 & 0xFF;
    if (ed->speed == 0) ed->speed = 15;
    ed->use = (w2 >> 40) & 0x3F;
    ed->lanecount = (w2 >> 46) & 0xF;
    if (ed->lanecount == 0) ed->lanecount = 1;
    ed->classification = (w2 >> 54) & 0x7;
    ed->surface = (w2 >> 57) & 0x7;
    
    ed->cycle_lane = (w3 >> 37) & 0x3;
    ed->bike_network = (w3 >> 39) & 0x1;
    ed->use_sidepath = (w3 >> 40) & 0x1;
    ed->dismount = (w3 >> 41) & 0x1;
    ed->shoulder = (w3 >> 44) & 0x1;
    
    ed->length = (float)((w4 >> 32) & 0xFFFFFF);
    ed->weighted_grade = (w4 >> 56) & 0xF;
    if (ed->weighted_grade == 0) ed->weighted_grade = 7;
    
    return 1;
}

/* ============================================================================
 * Heap Operations (bidirectional)
 * ============================================================================ */

/* Forward heap functions */
static void heap_push_fwd(HeapEntry e) {
    if (g_heap_fwd_size >= MAX_HEAP) return;
    int i = g_heap_fwd_size++;
    g_heap_fwd[i] = e;
    while (i > 0) {
        int p = (i - 1) / 2;
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
        int l = 2*i + 1, r = 2*i + 2, smallest = i;
        if (l < g_heap_fwd_size && g_heap_fwd[l].f < g_heap_fwd[smallest].f) smallest = l;
        if (r < g_heap_fwd_size && g_heap_fwd[r].f < g_heap_fwd[smallest].f) smallest = r;
        if (smallest == i) break;
        HeapEntry tmp = g_heap_fwd[i];
        g_heap_fwd[i] = g_heap_fwd[smallest];
        g_heap_fwd[smallest] = tmp;
        i = smallest;
    }
    return ret;
}

/* Backward heap functions */
static void heap_push_bwd(HeapEntry e) {
    if (g_heap_bwd_size >= MAX_HEAP) return;
    int i = g_heap_bwd_size++;
    g_heap_bwd[i] = e;
    while (i > 0) {
        int p = (i - 1) / 2;
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
        int l = 2*i + 1, r = 2*i + 2, smallest = i;
        if (l < g_heap_bwd_size && g_heap_bwd[l].f < g_heap_bwd[smallest].f) smallest = l;
        if (r < g_heap_bwd_size && g_heap_bwd[r].f < g_heap_bwd[smallest].f) smallest = r;
        if (smallest == i) break;
        HeapEntry tmp = g_heap_bwd[i];
        g_heap_bwd[i] = g_heap_bwd[smallest];
        g_heap_bwd[smallest] = tmp;
        i = smallest;
    }
    return ret;
}

/* ============================================================================
 * Visited Set - Improved Hash Table (two sets for bidirectional search)
 * ============================================================================ */

static void visited_clear_both(void) {
    memset(g_visited_fwd, 0, MAX_VISITED * sizeof(VisitedEntry));
    memset(g_visited_bwd, 0, MAX_VISITED * sizeof(VisitedEntry));
}

static inline uint32_t hash_state(State s) {
    /* FNV-1a inspired hash */
    uint64_t h = 14695981039346656037ULL;
    h ^= s.tile_id;
    h *= 1099511628211ULL;
    h ^= s.node_id;
    h *= 1099511628211ULL;
    return (uint32_t)(h % MAX_VISITED);
}

/* Forward visited functions */
static VisitedEntry* visited_find_fwd(State s) {
    uint32_t h = hash_state(s);
    for (int i = 0; i < 2000; i++) {
        uint32_t idx = (h + i) % MAX_VISITED;
        if (!g_visited_fwd[idx].valid) return NULL;
        if (g_visited_fwd[idx].state.tile_id == s.tile_id && 
            g_visited_fwd[idx].state.node_id == s.node_id) {
            return &g_visited_fwd[idx];
        }
    }
    return NULL;
}

static void visited_insert_fwd(State s, float g, State parent, uint32_t parent_edge) {
    uint32_t h = hash_state(s);
    for (int i = 0; i < 2000; i++) {
        uint32_t idx = (h + i) % MAX_VISITED;
        if (!g_visited_fwd[idx].valid || 
            (g_visited_fwd[idx].state.tile_id == s.tile_id && 
             g_visited_fwd[idx].state.node_id == s.node_id)) {
            g_visited_fwd[idx].state = s;
            g_visited_fwd[idx].g = g;
            g_visited_fwd[idx].parent = parent;
            g_visited_fwd[idx].parent_edge = parent_edge;
            g_visited_fwd[idx].valid = 1;
            return;
        }
    }
}

/* Backward visited functions */
static VisitedEntry* visited_find_bwd(State s) {
    uint32_t h = hash_state(s);
    for (int i = 0; i < 2000; i++) {
        uint32_t idx = (h + i) % MAX_VISITED;
        if (!g_visited_bwd[idx].valid) return NULL;
        if (g_visited_bwd[idx].state.tile_id == s.tile_id && 
            g_visited_bwd[idx].state.node_id == s.node_id) {
            return &g_visited_bwd[idx];
        }
    }
    return NULL;
}

static void visited_insert_bwd(State s, float g, State parent, uint32_t parent_edge) {
    uint32_t h = hash_state(s);
    for (int i = 0; i < 2000; i++) {
        uint32_t idx = (h + i) % MAX_VISITED;
        if (!g_visited_bwd[idx].valid || 
            (g_visited_bwd[idx].state.tile_id == s.tile_id && 
             g_visited_bwd[idx].state.node_id == s.node_id)) {
            g_visited_bwd[idx].state = s;
            g_visited_bwd[idx].g = g;
            g_visited_bwd[idx].parent = parent;
            g_visited_bwd[idx].parent_edge = parent_edge;
            g_visited_bwd[idx].valid = 1;
            return;
        }
    }
}

/* ============================================================================
 * Costing
 * ============================================================================ */

static void init_costing(void) {
    for (int s = 0; s < 256; s++) {
        kSpeedFactor[s] = (s > 0) ? (3.6f / s) : 3.6f;
    }
    
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
    for (int i = 0; i < 16; i++) {
        g_grade_penalty[i] = avoid_hills * kAvoidHillsStrength[i];
    }
}

static float edge_cost(EdgeEnd *ee, EdgeDetails *ed) {
    if (ed->length <= 0) return 1e9f;
    
    /* Steps: walking speed with penalty */
    if (ed->use == USE_STEPS) {
        return ed->length * kSpeedFactor[4] * 3.0f;  /* ~4 km/h, 3x penalty */
    }
    
    /* Ferry */
    if (ed->use == USE_FERRY) {
        return ed->length * kSpeedFactor[ed->speed] * 1.2f;
    }
    
    /* Base: calculate time cost from speed */
    int grade = ed->weighted_grade;
    if (grade > 15) grade = 15;
    
    int surface = ed->surface;
    if (surface > 7) surface = 7;
    
    /* Calculate cycling speed based on surface and grade */
    float base_speed = kDefaultCyclingSpeed[g_bicycle_type];
    float speed = base_speed * kSurfaceSpeedFactor[g_bicycle_type][surface] 
                            * kGradeBasedSpeedFactor[grade];
    
    if (ed->dismount) {
        speed = kDismountSpeed;
    }
    
    if (speed < 4.0f) speed = 4.0f;
    if (speed > 40.0f) speed = 40.0f;
    
    /* Time cost in seconds: length(m) / (speed(km/h) / 3.6) */
    float time_cost = ed->length / (speed / 3.6f);
    
    /* Small preference factors (max ~20% difference, not 2x!) */
    float preference = 1.0f;
    
    /* Dedicated bike infrastructure: small bonus */
    if (ed->use == USE_CYCLEWAY) {
        preference = 0.9f;  /* 10% bonus */
    } else if (ed->use == USE_TRACK) {
        preference = 0.9f;  /* 10% bonus - Feldwege sind super! */
    } else if (ed->use == USE_MOUNTAIN_BIKE) {
        if (g_bicycle_type == 3) {  /* Mountain bike */
            preference = 0.85f;  /* 15% bonus for MTB on MTB trails */
        }
    } else if (ed->use == USE_PATH || ed->use == USE_FOOTWAY) {
        preference = 0.95f;  /* 5% bonus */
    } else if (ed->use == USE_LIVING_STREET) {
        preference = 0.95f;  /* 5% bonus */
    }
    /* Roads: small penalty based on use_roads preference */
    else if (ed->use == USE_ROAD) {
        /* use_roads=0 → 1.15, use_roads=1 → 1.0 */
        preference = 1.0f + (1.0f - g_use_roads) * 0.15f;
        
        /* Cycle lane reduces penalty */
        if (ed->cycle_lane >= 2) {
            preference -= 0.1f;  /* Dedicated lane helps */
        }
    }
    
    /* Bike network bonus */
    if (ed->bike_network) {
        preference *= 0.95f;
    }
    
    /* Avoid pushing if requested */
    if (!ee->has_bike && ee->has_ped) {
        preference *= g_avoid_pushing ? 2.0f : 1.3f;
    }
    
    /* Stress-based penalty for avoid_cars */
    if (g_avoid_cars && ee->has_car) {
        /* Low-traffic ways: minimal stress */
        if (ed->use == USE_TRACK || ed->use == USE_LIVING_STREET || ed->use == USE_SERVICE_ROAD) {
            preference *= 1.05f;  /* Only 5% penalty */
        } else {
            /* Calculate stress from speed and road class */
            float stress = 0.2f;
            if (ed->speed > 50) stress += 0.3f;
            if (ed->speed > 70) stress += 0.3f;
            if (ed->classification <= 2) stress += 0.2f;  /* Primary roads */
            if (ed->lanecount >= 2) stress += 0.1f;
            if (ed->cycle_lane >= 2) stress -= 0.3f;  /* Bike lane helps */
            if (stress < 0.1f) stress = 0.1f;
            if (stress > 1.0f) stress = 1.0f;
            preference *= 1.0f + stress * 0.5f;  /* Max 50% penalty */
        }
    }
    
    return time_cost * preference;
}

/* ============================================================================
 * Find Nearest Node
 * ============================================================================ */

static uint32_t find_nearest_node(Tile *t, double lat, double lon) {
    uint32_t best = 0;
    double best_dist = 1e18;
    uint32_t best_bike = 0;
    double best_bike_dist = 1e18;
    
    for (uint32_t i = 0; i < t->node_count; i++) {
        if (t->nodes[i].edge_count == 0) continue;
        double d = haversine(lat, lon, t->nodes[i].lat, t->nodes[i].lon);
        
        /* Check if node has bike-accessible edges */
        int has_bike_edge = 0;
        for (uint32_t ei = t->nodes[i].edge_index; 
             ei < t->nodes[i].edge_index + t->nodes[i].edge_count && ei < t->edge_count; 
             ei++) {
            EdgeEnd ee;
            if (!get_edge_end(t, ei, &ee)) continue;
            if (ee.has_bike || ee.has_ped) {
                has_bike_edge = 1;
                break;
            }
        }
        
        if (has_bike_edge && d < best_bike_dist) {
            best_bike_dist = d;
            best_bike = i;
        }
        if (d < best_dist) {
            best_dist = d;
            best = i;
        }
    }
    
    /* Prefer bike-accessible node if within 500m */
    if (best_bike_dist < 500.0 || best_bike_dist < best_dist * 2) {
        return best_bike;
    }
    return best;
}

/* ============================================================================
 * Main Routing Function - BIDIRECTIONAL A* SEARCH
 * ============================================================================ */

static void calculate_statistics(void) {
    /* Calculate statistics for the final path */
    for (int i = 0; i < g_path_len - 1; i++) {
        State s = g_path[i];
        State next = g_path[i + 1];
        
        Tile *t = load_tile(s.tile_id);
        if (!t || s.node_id >= t->node_count) continue;
        
        Node *n = &t->nodes[s.node_id];
        
        /* Find edge from s to next */
        for (uint32_t ei = n->edge_index; 
             ei < n->edge_index + n->edge_count && ei < t->edge_count; 
             ei++) {
            EdgeEnd ee;
            if (!get_edge_end(t, ei, &ee)) continue;
            
            if (ee.end_tile_id == next.tile_id && ee.end_node_id == next.node_id) {
                EdgeDetails ed;
                if (!get_edge_details(t, ei, &ed)) break;
                
                int is_path = (ed.use == USE_CYCLEWAY || ed.use == USE_PATH || 
                               ed.use == USE_FOOTWAY || ed.use == USE_MOUNTAIN_BIKE);
                
                int is_low_traffic = (ed.use == USE_TRACK || 
                                      ed.use == USE_LIVING_STREET ||
                                      ed.use == USE_SERVICE_ROAD);
                
                if (!ee.has_bike && ee.has_ped) {
                    g_dist_pushing += ed.length;
                } else if (is_path && !ee.has_car) {
                    g_dist_car_free += ed.length;
                } else if (is_low_traffic) {
                    g_dist_car_free += ed.length;
                } else if (ed.cycle_lane >= 2) {
                    g_dist_separated += ed.length;
                } else if (ee.has_car) {
                    g_dist_with_cars += ed.length;
                } else {
                    g_dist_car_free += ed.length;
                }
                break;
            }
        }
    }
}

static int route(uint32_t start_tile_id, uint32_t start_node,
                 uint32_t end_tile_id, uint32_t end_node,
                 double end_lat, double end_lon) {
    
    init_costing();
    
    g_heap_fwd_size = 0;
    g_heap_bwd_size = 0;
    visited_clear_both();
    g_path_len = 0;
    
    /* Reset statistics */
    g_dist_car_free = 0;
    g_dist_separated = 0;
    g_dist_with_cars = 0;
    g_dist_pushing = 0;
    
    /* Load start and end tiles */
    Tile *start_tile = load_tile(start_tile_id);
    Tile *end_tile = load_tile(end_tile_id);
    
    if (!start_tile || start_node >= start_tile->node_count) {
        fprintf(stderr, "[ERROR] Invalid start\n");
        return 0;
    }
    if (!end_tile || end_node >= end_tile->node_count) {
        fprintf(stderr, "[ERROR] Invalid end\n");
        return 0;
    }
    
    Node *sn = &start_tile->nodes[start_node];
    Node *en = &end_tile->nodes[end_node];
    double start_lat = sn->lat, start_lon = sn->lon;
    
    /* Calculate initial distance */
    double init_dist = haversine(start_lat, start_lon, end_lat, end_lon);
    float max_speed = 2.0f * kDefaultCyclingSpeed[g_bicycle_type];
    
    State start_state = { start_tile_id, start_node };
    State end_state = { end_tile_id, end_node };
    State null_state = { 0, 0 };
    
    /* Initialize forward search (from start) */
    float h_fwd = init_dist * kSpeedFactor[(int)max_speed];
    HeapEntry init_fwd = { h_fwd, 0, 0, start_state, null_state, 0 };
    heap_push_fwd(init_fwd);
    visited_insert_fwd(start_state, 0, null_state, 0);
    
    /* Initialize backward search (from end) */
    float h_bwd = init_dist * kSpeedFactor[(int)max_speed];
    HeapEntry init_bwd = { h_bwd, 0, 0, end_state, null_state, 0 };
    heap_push_bwd(init_bwd);
    visited_insert_bwd(end_state, 0, null_state, 0);
    
    int iterations = 0;
    int max_iterations = (int)(init_dist / 1000.0 * 30000);  /* Less iterations needed with bidirectional */
    if (max_iterations < 1000000) max_iterations = 1000000;
    if (max_iterations > 6000000) max_iterations = 6000000;
    
    fprintf(stderr, "[ROUTE-BIDIR] Distance: %.1f km, max_iterations: %d\n", 
            init_dist / 1000.0, max_iterations);
    
    State meeting_point = { 0, 0 };
    float best_total_cost = 1e18f;
    
    while ((g_heap_fwd_size > 0 || g_heap_bwd_size > 0) && iterations < max_iterations) {
        
        /* === FORWARD EXPANSION === */
        if (g_heap_fwd_size > 0) {
            HeapEntry cur = heap_pop_fwd();
            iterations++;
            
            VisitedEntry *ve = visited_find_fwd(cur.state);
            if (ve && cur.g > ve->g) goto do_backward;
            
            /* Check if this node was visited by backward search */
            VisitedEntry *bwd_ve = visited_find_bwd(cur.state);
            if (bwd_ve) {
                float total = cur.g + bwd_ve->g;
                if (total < best_total_cost) {
                    best_total_cost = total;
                    meeting_point = cur.state;
                    fprintf(stderr, "[ROUTE-BIDIR] Meeting point found at iter %d, cost=%.1f\n",
                            iterations, total);
                }
            }
            
            Tile *tile = load_tile(cur.state.tile_id);
            if (!tile || cur.state.node_id >= tile->node_count) goto do_backward;
            
            Node *node = &tile->nodes[cur.state.node_id];
            
            /* Expand forward edges */
            for (uint32_t ei = node->edge_index; 
                 ei < node->edge_index + node->edge_count && ei < tile->edge_count; 
                 ei++) {
                
                EdgeEnd ee;
                if (!get_edge_end(tile, ei, &ee)) continue;
                if (ee.end_level != 2) continue;
                if (!ee.has_bike && !ee.has_ped) continue;
                
                EdgeDetails ed;
                if (!get_edge_details(tile, ei, &ed)) continue;
                if (ed.surface > kWorstAllowedSurface[g_bicycle_type]) continue;
                
                float cost = edge_cost(&ee, &ed);
                if (!ee.has_bike && ee.has_ped) {
                    cost *= g_avoid_pushing ? 5.0f : 2.0f;
                }
                
                float new_g = cur.g + cost;
                State ns = { ee.end_tile_id, ee.end_node_id };
                
                VisitedEntry *nve = visited_find_fwd(ns);
                if (nve && new_g >= nve->g) continue;
                
                Tile *ntile = load_tile(ns.tile_id);
                if (!ntile || ns.node_id >= ntile->node_count) continue;
                
                Node *nn = &ntile->nodes[ns.node_id];
                float h = haversine(nn->lat, nn->lon, end_lat, end_lon) * kSpeedFactor[(int)max_speed];
                
                HeapEntry ne = { new_g + h, new_g, cur.dist + ed.length, ns, cur.state, ei };
                heap_push_fwd(ne);
                visited_insert_fwd(ns, new_g, cur.state, ei);
            }
        }
        
do_backward:
        /* === BACKWARD EXPANSION === */
        if (g_heap_bwd_size > 0) {
            HeapEntry cur = heap_pop_bwd();
            iterations++;
            
            VisitedEntry *ve = visited_find_bwd(cur.state);
            if (ve && cur.g > ve->g) continue;
            
            /* Check if this node was visited by forward search */
            VisitedEntry *fwd_ve = visited_find_fwd(cur.state);
            if (fwd_ve) {
                float total = cur.g + fwd_ve->g;
                if (total < best_total_cost) {
                    best_total_cost = total;
                    meeting_point = cur.state;
                    fprintf(stderr, "[ROUTE-BIDIR] Meeting point found at iter %d, cost=%.1f\n",
                            iterations, total);
                }
            }
            
            Tile *tile = load_tile(cur.state.tile_id);
            if (!tile || cur.state.node_id >= tile->node_count) continue;
            
            Node *node = &tile->nodes[cur.state.node_id];
            
            /* Expand backward edges (same as forward - roads are bidirectional for bikes) */
            for (uint32_t ei = node->edge_index; 
                 ei < node->edge_index + node->edge_count && ei < tile->edge_count; 
                 ei++) {
                
                EdgeEnd ee;
                if (!get_edge_end(tile, ei, &ee)) continue;
                if (ee.end_level != 2) continue;
                if (!ee.has_bike && !ee.has_ped) continue;
                
                EdgeDetails ed;
                if (!get_edge_details(tile, ei, &ed)) continue;
                if (ed.surface > kWorstAllowedSurface[g_bicycle_type]) continue;
                
                float cost = edge_cost(&ee, &ed);
                if (!ee.has_bike && ee.has_ped) {
                    cost *= g_avoid_pushing ? 5.0f : 2.0f;
                }
                
                float new_g = cur.g + cost;
                State ns = { ee.end_tile_id, ee.end_node_id };
                
                VisitedEntry *nve = visited_find_bwd(ns);
                if (nve && new_g >= nve->g) continue;
                
                Tile *ntile = load_tile(ns.tile_id);
                if (!ntile || ns.node_id >= ntile->node_count) continue;
                
                Node *nn = &ntile->nodes[ns.node_id];
                float h = haversine(nn->lat, nn->lon, start_lat, start_lon) * kSpeedFactor[(int)max_speed];
                
                HeapEntry ne = { new_g + h, new_g, cur.dist + ed.length, ns, cur.state, ei };
                heap_push_bwd(ne);
                visited_insert_bwd(ns, new_g, cur.state, ei);
            }
        }
        
        /* Progress debug */
        if (iterations % 500000 == 0) {
            fprintf(stderr, "[ROUTE-BIDIR] Iter %d: fwd_heap=%d bwd_heap=%d tiles=%d\n", 
                    iterations, g_heap_fwd_size, g_heap_bwd_size, g_tile_count);
        }
        
        /* Early termination if we found a path and both heaps' minimum f-values exceed best */
        if (meeting_point.tile_id != 0) {
            float min_fwd = (g_heap_fwd_size > 0) ? g_heap_fwd[0].f : 1e18f;
            float min_bwd = (g_heap_bwd_size > 0) ? g_heap_bwd[0].f : 1e18f;
            if (min_fwd + min_bwd >= best_total_cost) {
                fprintf(stderr, "[ROUTE-BIDIR] Early termination: optimal path found\n");
                break;
            }
        }
    }
    
    /* Reconstruct path if meeting point found */
    if (meeting_point.tile_id == 0) {
        fprintf(stderr, "[ROUTE-BIDIR] No path found (iterations=%d, tiles=%d)\n", 
                iterations, g_tile_count);
        return 0;
    }
    
    fprintf(stderr, "[ROUTE-BIDIR] Reconstructing path...\n");
    
    /* Build forward path: start -> meeting_point */
    State *fwd_path = malloc(MAX_PATH * sizeof(State));
    int fwd_len = 0;
    
    State s = meeting_point;
    while (s.tile_id != 0 || s.node_id != 0) {
        if (fwd_len >= MAX_PATH) break;
        fwd_path[fwd_len++] = s;
        VisitedEntry *ve = visited_find_fwd(s);
        if (!ve) break;
        if (ve->parent.tile_id == 0 && ve->parent.node_id == 0) {
            if (s.tile_id == start_state.tile_id && s.node_id == start_state.node_id) break;
        }
        s = ve->parent;
    }
    
    /* Reverse forward path (was meeting->start, need start->meeting) */
    for (int i = 0; i < fwd_len / 2; i++) {
        State tmp = fwd_path[i];
        fwd_path[i] = fwd_path[fwd_len - 1 - i];
        fwd_path[fwd_len - 1 - i] = tmp;
    }
    
    /* Build backward path: meeting_point -> end (already in correct order from parent links) */
    State *bwd_path = malloc(MAX_PATH * sizeof(State));
    int bwd_len = 0;
    
    s = meeting_point;
    VisitedEntry *ve = visited_find_bwd(s);
    if (ve) s = ve->parent;  /* Skip meeting point (already in fwd_path) */
    
    while (s.tile_id != 0 || s.node_id != 0) {
        if (bwd_len >= MAX_PATH) break;
        bwd_path[bwd_len++] = s;
        ve = visited_find_bwd(s);
        if (!ve) break;
        if (ve->parent.tile_id == 0 && ve->parent.node_id == 0) {
            if (s.tile_id == end_state.tile_id && s.node_id == end_state.node_id) break;
        }
        s = ve->parent;
    }
    
    /* Combine paths: fwd_path + bwd_path */
    g_path_len = 0;
    for (int i = 0; i < fwd_len && g_path_len < MAX_PATH; i++) {
        g_path[g_path_len++] = fwd_path[i];
    }
    for (int i = 0; i < bwd_len && g_path_len < MAX_PATH; i++) {
        g_path[g_path_len++] = bwd_path[i];
    }
    
    free(fwd_path);
    free(bwd_path);
    
    fprintf(stderr, "[ROUTE-BIDIR] Path length: %d nodes\n", g_path_len);
    
    calculate_statistics();
    
    return g_path_len;
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char *argv[]) {
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tiles_dir> <from_lat> <from_lon> <to_lat> <to_lon> "
                "[avoid_pushing] [avoid_cars] [use_roads] [bike_type]\n", argv[0]);
        fprintf(stderr, "  bike_type: 0=Road, 1=Cross, 2=Hybrid, 3=Mountain\n");
        return 1;
    }
    
    strncpy(g_tiles_dir, argv[1], sizeof(g_tiles_dir) - 1);
    double from_lat = atof(argv[2]), from_lon = atof(argv[3]);
    double to_lat = atof(argv[4]), to_lon = atof(argv[5]);
    
    /* Arguments matching valhalla_local_engine.py line 3093 */
    if (argc > 6) g_avoid_pushing = atoi(argv[6]);
    if (argc > 7) g_avoid_cars = atoi(argv[7]);
    if (argc > 8) g_use_roads = atof(argv[8]);
    if (argc > 9) g_bicycle_type = atoi(argv[9]);
    
    if (g_use_roads < 0) g_use_roads = 0;
    if (g_use_roads > 1) g_use_roads = 1;
    if (g_bicycle_type < 0) g_bicycle_type = 0;
    if (g_bicycle_type > 3) g_bicycle_type = 3;
    
    const char *bike_names[] = {"Road", "Cross", "Hybrid", "Mountain"};
    fprintf(stderr, "[ROUTE] Options: avoid_pushing=%d, avoid_cars=%d, use_roads=%.2f, bike=%s\n",
            g_avoid_pushing, g_avoid_cars, g_use_roads, bike_names[g_bicycle_type]);
    
    /* Allocate memory for bidirectional search */
    g_heap_fwd = malloc(MAX_HEAP * sizeof(HeapEntry));
    g_heap_bwd = malloc(MAX_HEAP * sizeof(HeapEntry));
    g_visited_fwd = malloc(MAX_VISITED * sizeof(VisitedEntry));
    g_visited_bwd = malloc(MAX_VISITED * sizeof(VisitedEntry));
    g_path = malloc(MAX_PATH * sizeof(State));
    
    if (!g_heap_fwd || !g_heap_bwd || !g_visited_fwd || !g_visited_bwd || !g_path) {
        fprintf(stderr, "Memory allocation failed\n");
        return 1;
    }
    
    /* Calculate tile IDs */
    int from_row = (int)((from_lat + 90.0) / LEVEL_2_SIZE);
    int from_col = (int)((from_lon + 180.0) / LEVEL_2_SIZE);
    uint32_t from_tile_id = from_row * 1440 + from_col;
    
    int to_row = (int)((to_lat + 90.0) / LEVEL_2_SIZE);
    int to_col = (int)((to_lon + 180.0) / LEVEL_2_SIZE);
    uint32_t to_tile_id = to_row * 1440 + to_col;
    
    fprintf(stderr, "[DEBUG] from_tile=%u to_tile=%u\n", from_tile_id, to_tile_id);
    
    /* Load tiles and find nodes */
    Tile *from_tile = load_tile(from_tile_id);
    Tile *to_tile = load_tile(to_tile_id);
    
    if (!from_tile || !to_tile) {
        fprintf(stderr, "[ERROR] Failed to load tiles (from=%p to=%p)\n", 
                (void*)from_tile, (void*)to_tile);
        printf("{\"error\": \"tile_load_failed\"}\n");
        return 1;
    }
    
    uint32_t start_node = find_nearest_node(from_tile, from_lat, from_lon);
    uint32_t end_node = find_nearest_node(to_tile, to_lat, to_lon);
    
    fprintf(stderr, "[DEBUG] start_node=%u end_node=%u\n", start_node, end_node);
    
    /* Route */
    int path_len = route(from_tile_id, start_node, to_tile_id, end_node, to_lat, to_lon);
    
    if (path_len == 0) {
        printf("{\"error\": \"no_path\"}\n");
        return 1;
    }
    
    /* Output JSON - format expected by valhalla_local_engine.py */
    printf("{\"coords\": [");
    for (int i = 0; i < path_len; i++) {
        Tile *t = load_tile(g_path[i].tile_id);
        if (t && g_path[i].node_id < t->node_count) {
            if (i > 0) printf(",");
            printf("{\"lat\":%.6f,\"lon\":%.6f}", 
                   t->nodes[g_path[i].node_id].lat, 
                   t->nodes[g_path[i].node_id].lon);
        }
    }
    printf("], \"dist_car_free_km\": %.2f, \"dist_separated_km\": %.2f, "
           "\"dist_with_cars_km\": %.2f, \"dist_pushing_km\": %.2f}\n",
           g_dist_car_free / 1000.0, g_dist_separated / 1000.0,
           g_dist_with_cars / 1000.0, g_dist_pushing / 1000.0);
    
    /* Cleanup */
    for (int i = 0; i < g_tile_count; i++) {
        free(g_tiles[i].raw_data);
        free(g_tiles[i].nodes);
    }
    free(g_heap_fwd);
    free(g_heap_bwd);
    free(g_visited_fwd);
    free(g_visited_bwd);
    free(g_path);
    
    return 0;
}
