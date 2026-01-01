/*
 * vrouter.c - Fast Valhalla-compatible bicycle router for Nokia N9
 * 
 * Compile with MADDE SDK:
 *   arm-none-linux-gnueabi-gcc -O3 -std=c99 -march=armv7-a -mtune=cortex-a8 \
 *       --sysroot=$SYSROOT -lz -lm -o vrouter vrouter.c
 *
 * Usage: vrouter <tiles_dir> <from_lat> <from_lon> <to_lat> <to_lon>
 * Output: JSON with route coordinates
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <zlib.h>
#include <sys/stat.h>
#include <dirent.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ============================================================================
 * Constants - must match Python version exactly
 * ============================================================================ */

#define HEADER_SIZE 272  /* Valhalla 3.x header size - MUST match Python! */
#define NODE_SIZE 32
#define EDGE_SIZE 48   /* DirectedEdge is 48 bytes, not 40! */
#define HEADER_EDGEINFO_OFFSET 56
#define HEADER_TEXTLIST_OFFSET 60

#define LEVEL_0_SIZE 4.0
#define LEVEL_1_SIZE 1.0
#define LEVEL_2_SIZE 0.25

#define kPedestrianAccess 2  /* Pedestrian access bit mask (bit 1) */
#define kBicycleAccess 4  /* Bicycle access bit mask (bit 2) */

#define MAX_TILES 100
#define MAX_NODES_PER_TILE 200000
#define MAX_EDGES_PER_TILE 500000
#define MAX_HEAP 1000000
#define MAX_VISITED 1000003  /* Prime number, ~32MB for hash table */
#define MAX_PATH 50000

#define EARTH_RADIUS 6371000.0
#define DEG_TO_RAD (M_PI / 180.0)

/* ============================================================================
 * Data Structures
 * ============================================================================ */

typedef struct {
    double lat;
    double lon;
    uint32_t edge_index;
    uint32_t edge_count;
    uint8_t trans_up;
    uint8_t trans_down;
    uint8_t trans_idx;
} Node;

typedef struct {
    uint8_t end_level;
    uint32_t end_tile_id;
    uint32_t end_node_id;
    uint8_t has_bike;      /* Forward bicycle access */
    uint8_t has_bike_rev;  /* Reverse bicycle access */
    uint8_t has_ped;       /* Forward pedestrian access (for pushing bike) */
    uint8_t opp_index;
    uint32_t edgeinfo_offset;
} EdgeEnd;

typedef struct {
    float length;
    uint8_t use;
    uint8_t classification;
    uint8_t cycle_lane;
    uint8_t surface;
} EdgeDetails;

typedef struct {
    int level;
    uint32_t tile_id;
    double base_lat;
    double base_lon;
    
    uint32_t node_count;
    uint32_t edge_count;
    uint32_t transition_count;
    
    Node *nodes;
    EdgeEnd *edge_ends;
    
    uint8_t *raw_data;
    size_t raw_size;
    uint32_t edges_offset;
    uint32_t edgeinfo_offset;
    uint32_t transitions_offset;
    
    int loaded;
} Tile;

/* State for A* - packed to avoid padding issues */
typedef struct __attribute__((packed)) {
    uint8_t level;
    uint32_t tile_id;
    uint32_t node_id;
} State;

/* Heap entry */
typedef struct {
    float f;
    float g;
    float time;
    float dist;
    State state;
} HeapEntry;

/* Hash table entry for visited/came_from */
typedef struct {
    State state;
    State prev;
    float g;
    uint8_t has_prev;
    uint8_t in_use;
} VisitedEntry;

/* ============================================================================
 * Globals
 * ============================================================================ */

static char g_tiles_dir[512];
static Tile g_tiles[MAX_TILES];
static int g_tile_count = 0;

static HeapEntry g_heap[MAX_HEAP];
static int g_heap_size = 0;

static VisitedEntry *g_visited = NULL;
static int g_visited_capacity = 0;

static State g_path[MAX_PATH];
static int g_path_len = 0;

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static inline double haversine(double lat1, double lon1, double lat2, double lon2) {
    double dlat = (lat2 - lat1) * DEG_TO_RAD;
    double dlon = (lon2 - lon1) * DEG_TO_RAD;
    double a = sin(dlat/2) * sin(dlat/2) +
               cos(lat1 * DEG_TO_RAD) * cos(lat2 * DEG_TO_RAD) *
               sin(dlon/2) * sin(dlon/2);
    double c = 2 * atan2(sqrt(a), sqrt(1-a));
    return EARTH_RADIUS * c;
}

static inline uint64_t read_u64(const uint8_t *data, size_t offset) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= ((uint64_t)data[offset + i]) << (i * 8);
    }
    return v;
}

static inline uint32_t read_u32(const uint8_t *data, size_t offset) {
    return data[offset] | (data[offset+1] << 8) | 
           (data[offset+2] << 16) | (data[offset+3] << 24);
}

static inline float read_f32(const uint8_t *data, size_t offset) {
    union { uint32_t i; float f; } u;
    u.i = read_u32(data, offset);
    return u.f;
}

/* State hash for hash table - better distribution */
static inline uint32_t state_hash(State s) {
    /* FNV-1a inspired hash */
    uint32_t h = 2166136261u;
    h ^= s.level;
    h *= 16777619u;
    h ^= s.tile_id;
    h *= 16777619u;
    h ^= s.node_id;
    h *= 16777619u;
    return h;
}

static inline int state_eq(State a, State b) {
    return a.level == b.level && a.tile_id == b.tile_id && a.node_id == b.node_id;
}

/* ============================================================================
 * Tile ID Calculation
 * ============================================================================ */

static uint32_t get_tile_id(double lat, double lon, int level) {
    double tile_size;
    switch (level) {
        case 0: tile_size = LEVEL_0_SIZE; break;
        case 1: tile_size = LEVEL_1_SIZE; break;
        default: tile_size = LEVEL_2_SIZE; break;
    }
    int tiles_per_row = (int)(360.0 / tile_size);
    int col = (int)((lon + 180.0) / tile_size);
    int row = (int)((lat + 90.0) / tile_size);
    return row * tiles_per_row + col;
}

/* ============================================================================
 * Gzip Decompression
 * ============================================================================ */

static uint8_t* decompress_gzip(const char *path, size_t *out_size) {
    gzFile gz = gzopen(path, "rb");
    if (!gz) return NULL;
    
    /* Read in chunks */
    size_t capacity = 1024 * 1024;  /* 1MB initial */
    uint8_t *data = malloc(capacity);
    size_t total = 0;
    
    while (1) {
        if (total + 65536 > capacity) {
            capacity *= 2;
            data = realloc(data, capacity);
        }
        int n = gzread(gz, data + total, 65536);
        if (n <= 0) break;
        total += n;
    }
    
    gzclose(gz);
    *out_size = total;
    return data;
}

/* ============================================================================
 * Tile Loading
 * ============================================================================ */

static void build_tile_path(char *buf, int level, uint32_t tile_id) {
    if (level == 2) {
        int dir1 = tile_id / 1000000;
        int dir2 = (tile_id / 1000) % 1000;
        int fname = tile_id % 1000;
        sprintf(buf, "%s/%d/%03d/%03d/%03d.gph.gz", g_tiles_dir, level, dir1, dir2, fname);
    } else {
        int dir1 = tile_id / 1000;
        int fname = tile_id % 1000;
        sprintf(buf, "%s/%d/%03d/%03d.gph.gz", g_tiles_dir, level, dir1, fname);
    }
}

static Tile* find_tile(int level, uint32_t tile_id) {
    /* Check if already loaded */
    for (int i = 0; i < g_tile_count; i++) {
        if (g_tiles[i].loaded && g_tiles[i].level == level && g_tiles[i].tile_id == tile_id) {
            return &g_tiles[i];
        }
    }
    return NULL;
}

static int g_debug_tile_fails = 0;

static Tile* load_tile(int level, uint32_t tile_id) {
    Tile *t = find_tile(level, tile_id);
    if (t) return t;
    
    if (g_tile_count >= MAX_TILES) {
        fprintf(stderr, "Too many tiles loaded\n");
        return NULL;
    }
    
    char path[1024];
    build_tile_path(path, level, tile_id);
    
    if (g_debug_tile_fails < 3) {
        fprintf(stderr, "[DEBUG] Loading tile level=%d id=%u path=%s\n", level, tile_id, path);
    }
    
    size_t data_size;
    uint8_t *data = decompress_gzip(path, &data_size);
    if (!data) {
        /* Try without .gz */
        path[strlen(path) - 3] = '\0';
        FILE *f = fopen(path, "rb");
        if (!f) {
            g_debug_tile_fails++;
            if (g_debug_tile_fails <= 3) {
                fprintf(stderr, "[DEBUG] Failed to open tile (fail #%d)\n", g_debug_tile_fails);
            }
            return NULL;
        }
        fseek(f, 0, SEEK_END);
        data_size = ftell(f);
        fseek(f, 0, SEEK_SET);
        data = malloc(data_size);
        fread(data, 1, data_size, f);
        fclose(f);
    }
    
    fprintf(stderr, "[DEBUG] Loaded %zu bytes\n", data_size);
    
    if (data_size < HEADER_SIZE) {
        free(data);
        return NULL;
    }
    
    t = &g_tiles[g_tile_count++];
    memset(t, 0, sizeof(Tile));
    t->raw_data = data;
    t->raw_size = data_size;
    t->loaded = 1;
    
    /* Parse header */
    uint64_t graphid = read_u64(data, 0);
    t->level = graphid & 0x7;
    t->tile_id = (graphid >> 3) & 0x3FFFFF;
    
    t->base_lon = read_f32(data, 8);
    t->base_lat = read_f32(data, 12);
    
    uint64_t word5 = read_u64(data, 40);
    t->node_count = word5 & 0x1FFFFF;
    t->edge_count = (word5 >> 21) & 0x1FFFFF;
    
    uint32_t word6 = read_u32(data, 48);
    t->transition_count = word6 & 0x3FFFFF;
    
    t->edgeinfo_offset = read_u32(data, HEADER_EDGEINFO_OFFSET);
    t->transitions_offset = HEADER_SIZE + t->node_count * NODE_SIZE;
    t->edges_offset = t->transitions_offset + t->transition_count * 8;
    
    fprintf(stderr, "[DEBUG] Tile %u: nodes=%u edges=%u trans=%u trans_off=%u edges_off=%u\n",
        t->tile_id, t->node_count, t->edge_count, t->transition_count, 
        t->transitions_offset, t->edges_offset);
    
    /* Allocate and parse nodes */
    t->nodes = calloc(t->node_count, sizeof(Node));
    for (uint32_t i = 0; i < t->node_count; i++) {
        size_t offset = HEADER_SIZE + i * NODE_SIZE;
        
        uint64_t w0 = read_u64(data, offset);
        t->nodes[i].lat = t->base_lat + ((w0 & 0x3FFFFF) * 1e-6 + ((w0 >> 22) & 0xF) * 1e-7);
        t->nodes[i].lon = t->base_lon + (((w0 >> 26) & 0x3FFFFF) * 1e-6 + ((w0 >> 48) & 0xF) * 1e-7);
        
        uint64_t w1 = read_u64(data, offset + 8);
        t->nodes[i].edge_index = w1 & 0x1FFFFF;
        t->nodes[i].edge_count = (w1 >> 21) & 0x7F;
        t->nodes[i].trans_idx = (w1 >> 49) & 0x7F;
        t->nodes[i].trans_up = (w1 >> 56) & 1;
        t->nodes[i].trans_down = (w1 >> 57) & 1;
    }
    
    /* Allocate and parse edge ends */
    t->edge_ends = calloc(t->edge_count, sizeof(EdgeEnd));
    for (uint32_t i = 0; i < t->edge_count; i++) {
        size_t offset = t->edges_offset + i * EDGE_SIZE;
        
        /* Word 0: endnode (46 bits) 
         * GraphId bit layout: [ ID (21 bits) | Tile ID (22 bits) | Level (3 bits) ]
         * From LSB to MSB: level(3) + tile_id(22) + node_id(21)
         */
        uint64_t w0 = read_u64(data, offset);
        uint64_t endnode = w0 & 0x3FFFFFFFFFFFull;  /* 46 bits */
        t->edge_ends[i].end_level = endnode & 0x7;                  /* bits 0-2: 3 bits */
        t->edge_ends[i].end_tile_id = (endnode >> 3) & 0x3FFFFF;    /* bits 3-24: 22 bits */
        t->edge_ends[i].end_node_id = (endnode >> 25) & 0x1FFFFF;   /* bits 25-45: 21 bits */
        t->edge_ends[i].opp_index = (w0 >> 54) & 0x7F;
        
        uint64_t w1 = read_u64(data, offset + 8);
        t->edge_ends[i].edgeinfo_offset = w1 & 0x1FFFFFF;
        
        /* Check bicycle access - combine forward + reverse like Python */
        uint64_t w3 = read_u64(data, offset + 24);
        uint32_t fwd_access = w3 & 0xFFF;
        uint32_t rev_access = (w3 >> 12) & 0xFFF;
        /* has_bike = forward OR reverse has bike access (like Python) */
        t->edge_ends[i].has_bike = ((fwd_access | rev_access) & kBicycleAccess) ? 1 : 0;
        t->edge_ends[i].has_bike_rev = (rev_access & kBicycleAccess) ? 1 : 0;
        t->edge_ends[i].has_ped = ((fwd_access | rev_access) & kPedestrianAccess) ? 1 : 0;
    }
    
    return t;
}

/* ============================================================================
 * Edge Details
 * ============================================================================ */

static int get_edge_details(Tile *t, uint32_t edge_idx, EdgeDetails *out) {
    if (!t || edge_idx >= t->edge_count) return 0;
    
    size_t offset = t->edges_offset + edge_idx * EDGE_SIZE;
    uint8_t *data = t->raw_data;
    
    /* Word 4 (offset 32): turntype(24) + edge_to_left(8) + length(24) + grade(4) + curv(4) */
    uint64_t w4 = read_u64(data, offset + 32);
    out->length = (float)((w4 >> 32) & 0xFFFFFF);  /* Length in meters (bits 32-55) */
    
    /* Word 2 (offset 16): speed(8)+ffs(8)+cfs(8)+ts(8)+nc(8)+use(6)+lc(4)+dens(4)+class(3)+surf(3)+... */
    uint64_t w2 = read_u64(data, offset + 16);
    out->use = (w2 >> 40) & 0x3F;              /* Use: bits 40-45 (6 bits) */
    out->classification = (w2 >> 54) & 0x7;   /* Classification: bits 54-56 (3 bits) */
    out->surface = (w2 >> 57) & 0x7;          /* Surface: bits 57-59 (3 bits) */
    
    /* Word 3 (offset 24): fwd(12)+rev(12)+slopes(10)+sac(3)+cycle_lane(2)+... */
    uint64_t w3 = read_u64(data, offset + 24);
    out->cycle_lane = (w3 >> 37) & 0x3;       /* Cycle lane: bits 37-38 (2 bits) */
    
    return 1;
}

/* ============================================================================
 * Find Nearest Node (like Python version)
 * ============================================================================ */

/* Count usable edges from a node */
static int count_usable_edges(Tile *t, uint32_t node_id) {
    if (!t || node_id >= t->node_count) return 0;
    
    Node *n = &t->nodes[node_id];
    int count = 0;
    
    for (uint32_t ei = n->edge_index; ei < n->edge_index + n->edge_count; ei++) {
        if (ei < t->edge_count) {
            EdgeEnd *ee = &t->edge_ends[ei];
            if (ee->end_level == 2) count++;
        }
    }
    return count;
}

static int find_nearest_node(Tile *t, double lat, double lon, uint32_t *out_node) {
    if (!t || t->node_count == 0) return 0;
    
    double best_dist = 1e18;
    uint32_t best_node = 0;
    
    /* Simple: find closest node (like Python) */
    for (uint32_t i = 0; i < t->node_count; i++) {
        double d = haversine(lat, lon, t->nodes[i].lat, t->nodes[i].lon);
        if (d < best_dist) {
            best_dist = d;
            best_node = i;
        }
    }
    
    if (best_dist > 5000) return 0;  /* Max 5km from road */
    
    int edges = count_usable_edges(t, best_node);
    *out_node = best_node;
    fprintf(stderr, "[DEBUG] find_nearest_node: node=%u dist=%.1fm edges=%d\n", 
            best_node, best_dist, edges);
    return 1;
}

/* ============================================================================
 * Min-Heap Operations
 * ============================================================================ */

static void heap_push(HeapEntry e) {
    if (g_heap_size >= MAX_HEAP) return;
    
    int i = g_heap_size++;
    g_heap[i] = e;
    
    /* Bubble up */
    while (i > 0) {
        int p = (i - 1) / 2;
        if (g_heap[p].f <= g_heap[i].f) break;
        HeapEntry tmp = g_heap[p];
        g_heap[p] = g_heap[i];
        g_heap[i] = tmp;
        i = p;
    }
}

static HeapEntry heap_pop(void) {
    HeapEntry result = g_heap[0];
    g_heap[0] = g_heap[--g_heap_size];
    
    /* Bubble down */
    int i = 0;
    while (1) {
        int smallest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;
        
        if (left < g_heap_size && g_heap[left].f < g_heap[smallest].f)
            smallest = left;
        if (right < g_heap_size && g_heap[right].f < g_heap[smallest].f)
            smallest = right;
        
        if (smallest == i) break;
        
        HeapEntry tmp = g_heap[i];
        g_heap[i] = g_heap[smallest];
        g_heap[smallest] = tmp;
        i = smallest;
    }
    
    return result;
}

/* ============================================================================
 * Visited Hash Table
 * ============================================================================ */

static void visited_init(void) {
    g_visited_capacity = MAX_VISITED;
    g_visited = calloc(g_visited_capacity, sizeof(VisitedEntry));
}

static int g_visited_count = 0;
static int g_visited_collisions = 0;

static void visited_clear(void) {
    memset(g_visited, 0, g_visited_capacity * sizeof(VisitedEntry));
    g_visited_count = 0;
    g_visited_collisions = 0;
}

static VisitedEntry* visited_get(State s) {
    uint32_t h = state_hash(s) % g_visited_capacity;
    int probes = 0;
    while (probes < g_visited_capacity) {
        VisitedEntry *e = &g_visited[h];
        if (!e->in_use) return NULL;
        if (state_eq(e->state, s)) return e;
        h = (h + 1) % g_visited_capacity;
        probes++;
    }
    return NULL;
}

static VisitedEntry* visited_set(State s, State prev, float g, int has_prev) {
    uint32_t h = state_hash(s) % g_visited_capacity;
    int probes = 0;
    while (probes < g_visited_capacity) {
        VisitedEntry *e = &g_visited[h];
        if (!e->in_use) {
            e->state = s;
            e->prev = prev;
            e->g = g;
            e->has_prev = has_prev;
            e->in_use = 1;
            g_visited_count++;
            if (probes > 0) g_visited_collisions++;
            return e;
        }
        if (state_eq(e->state, s)) {
            if (g < e->g) {
                e->prev = prev;
                e->g = g;
                e->has_prev = has_prev;
            }
            return e;
        }
        h = (h + 1) % g_visited_capacity;
        probes++;
    }
    fprintf(stderr, "[ERROR] Hash table full after %d probes!\n", probes);
    return NULL;
}

/* ============================================================================
 * A* Router
 * ============================================================================ */

static int route(double from_lat, double from_lon, double to_lat, double to_lon) {
    g_heap_size = 0;
    g_path_len = 0;
    visited_clear();
    
    int start_level = 2;
    uint32_t from_tile_id = get_tile_id(from_lat, from_lon, start_level);
    uint32_t to_tile_id = get_tile_id(to_lat, to_lon, start_level);
    
    fprintf(stderr, "[DEBUG] from_tile_id=%u to_tile_id=%u\n", from_tile_id, to_tile_id);
    
    Tile *from_tile = load_tile(start_level, from_tile_id);
    Tile *to_tile = load_tile(start_level, to_tile_id);
    
    if (!from_tile || !to_tile) {
        fprintf(stderr, "Could not load tiles (from=%p to=%p)\n", (void*)from_tile, (void*)to_tile);
        return 0;
    }
    
    fprintf(stderr, "[DEBUG] from_tile: nodes=%u edges=%u\n", from_tile->node_count, from_tile->edge_count);
    
    uint32_t start_node, end_node;
    if (!find_nearest_node(from_tile, from_lat, from_lon, &start_node) ||
        !find_nearest_node(to_tile, to_lat, to_lon, &end_node)) {
        fprintf(stderr, "Could not find nearby nodes\n");
        return 0;
    }
    
    fprintf(stderr, "[DEBUG] start_node=%u end_node=%u\n", start_node, end_node);
    
    double end_lat = to_tile->nodes[end_node].lat;
    double end_lon = to_tile->nodes[end_node].lon;
    
    State start_state = { start_level, from_tile_id, start_node };
    State end_state = { start_level, to_tile_id, end_node };
    
    /* Initial heuristic */
    double h0 = haversine(from_tile->nodes[start_node].lat, 
                          from_tile->nodes[start_node].lon,
                          end_lat, end_lon) / 25.0 * 3.6;
    
    HeapEntry initial = { h0, 0, 0, 0, start_state };
    heap_push(initial);
    
    State null_state = { 0, 0, 0 };
    visited_set(start_state, null_state, 0, 0);
    
    int iterations = 0;
    int max_iterations = 300000;
    
    /* Adaptive max based on distance */
    double dist_km = haversine(from_lat, from_lon, to_lat, to_lon) / 1000.0;
    if (dist_km < 5) max_iterations = 50000;
    else if (dist_km < 20) max_iterations = 100000;
    else if (dist_km < 50) max_iterations = 200000;
    
    while (g_heap_size > 0 && iterations < max_iterations) {
        iterations++;
        
        HeapEntry cur = heap_pop();
        State cs = cur.state;
        
        /* Check if reached goal */
        if (state_eq(cs, end_state)) {
            /* Reconstruct path */
            fprintf(stderr, "[DEBUG] Reconstructing path from goal...\n");
            State s = cs;
            int steps = 0;
            double prev_lat = 0, prev_lon = 0;
            while (1) {
                if (g_path_len >= MAX_PATH) break;
                g_path[g_path_len++] = s;
                
                /* Get coordinates for this step */
                Tile *st = find_tile(s.level, s.tile_id);
                double slat = 0, slon = 0;
                if (st && s.node_id < st->node_count) {
                    slat = st->nodes[s.node_id].lat;
                    slon = st->nodes[s.node_id].lon;
                }
                
                VisitedEntry *ve = visited_get(s);
                if (!ve) {
                    fprintf(stderr, "[DEBUG] Path step %d: tile=%u node=%u - NO VISITED ENTRY!\n",
                        steps, s.tile_id, s.node_id);
                    break;
                }
                
                /* Verify the entry is for the right state */
                if (!state_eq(ve->state, s)) {
                    fprintf(stderr, "[DEBUG] Path step %d: HASH COLLISION! wanted tile=%u node=%u, got tile=%u node=%u\n",
                        steps, s.tile_id, s.node_id, ve->state.tile_id, ve->state.node_id);
                    break;
                }
                
                if (!ve->has_prev) {
                    fprintf(stderr, "[DEBUG] Path step %d: tile=%u node=%u (%.4f,%.4f) - start reached\n",
                        steps, s.tile_id, s.node_id, slat, slon);
                    break;
                }
                
                /* Check for big jumps */
                double jump = 0;
                if (prev_lat != 0) {
                    jump = haversine(prev_lat, prev_lon, slat, slon);
                }
                
                if (steps < 10 || steps % 10 == 0 || jump > 1000) {
                    fprintf(stderr, "[DEBUG] Path step %d: tile=%u node=%u (%.4f,%.4f) -> prev tile=%u node=%u (g=%.1f)%s\n",
                        steps, s.tile_id, s.node_id, slat, slon,
                        ve->prev.tile_id, ve->prev.node_id, ve->g,
                        jump > 1000 ? " *** BIG JUMP ***" : "");
                }
                prev_lat = slat;
                prev_lon = slon;
                s = ve->prev;
                steps++;
                if (steps > MAX_PATH) {
                    fprintf(stderr, "[DEBUG] Path reconstruction loop detected!\n");
                    break;
                }
            }
            
            /* Reverse path */
            for (int i = 0; i < g_path_len / 2; i++) {
                State tmp = g_path[i];
                g_path[i] = g_path[g_path_len - 1 - i];
                g_path[g_path_len - 1 - i] = tmp;
            }
            
            fprintf(stderr, "[ROUTE] Found! dist=%.1f km, iters=%d, path_len=%d\n",
                    cur.dist / 1000.0, iterations, g_path_len);
            fprintf(stderr, "[DEBUG] Hash stats: entries=%d collisions=%d (%.1f%%)\n",
                    g_visited_count, g_visited_collisions, 
                    g_visited_count > 0 ? 100.0 * g_visited_collisions / g_visited_count : 0);
            return 1;
        }
        
        if (iterations <= 3) {
            fprintf(stderr, "[DEBUG] Iter %d: popped state level=%d tile=%u node=%u (g=%.1f)\n", 
                iterations, cs.level, cs.tile_id, cs.node_id, cur.g);
        }
        
        /* Skip if already visited with better cost */
        VisitedEntry *ve = visited_get(cs);
        if (ve && cur.g > ve->g + 0.001) continue;
        
        Tile *tile = load_tile(cs.level, cs.tile_id);
        if (!tile || cs.node_id >= tile->node_count) continue;
        
        Node *node = &tile->nodes[cs.node_id];
        uint32_t start_edge = node->edge_index;
        uint32_t end_edge = start_edge + node->edge_count;
        if (end_edge > tile->edge_count) end_edge = tile->edge_count;
        
        if (iterations <= 10) {
            fprintf(stderr, "[DEBUG] Iter %d: expanding node %u (edge_index=%u count=%u, edges %u-%u)\n", 
                iterations, cs.node_id, node->edge_index, node->edge_count, start_edge, end_edge);
        }
        
        for (uint32_t ei = start_edge; ei < end_edge; ei++) {
            EdgeEnd *ee = &tile->edge_ends[ei];
            
            if (iterations <= 10 && ei < start_edge + 10) {
                /* Print debug info */
                size_t raw_off = tile->edges_offset + ei * EDGE_SIZE;
                if (ei == start_edge) {
                    fprintf(stderr, "[DEBUG] First edge offset: %zu (file size: %zu)\n", raw_off, tile->raw_size);
                }
                uint64_t raw_w3 = read_u64(tile->raw_data, raw_off + 24);
                uint64_t raw_w4 = read_u64(tile->raw_data, raw_off + 32);
                uint32_t fwd = raw_w3 & 0xFFF;
                uint32_t length = (raw_w4 >> 32) & 0xFFFFFF;
                fprintf(stderr, "[DEBUG] Edge %u: tile=%u->%u node=%u bike=%d ped=%d len=%um\n",
                    ei, tile->tile_id, ee->end_tile_id, ee->end_node_id, ee->has_bike, ee->has_ped, length);
            }
            
            /* Only follow edges to valid levels (0, 1, 2) */
            if (ee->end_level > 2) continue;
            
            /* For now, stay on level 2 only (no hierarchy) */
            if (ee->end_level != 2) continue;
            
            /* Get edge details */
            EdgeDetails ed;
            if (!get_edge_details(tile, ei, &ed)) continue;
            
            /* Base cost: length / speed */
            float cost = ed.length / 15.0 * 3.6;  /* ~15 km/h cycling */
            
            /* Access-based cost multiplier */
            if (ee->has_bike) {
                /* Normal cycling - no penalty */
            } else if (ee->has_ped) {
                /* Walking/pushing bike - 3x slower */
                cost *= 3.0;
            } else {
                /* No official access - very high penalty but still possible */
                cost *= 10.0;
            }
            
            /* Road type penalties */
            if (ed.classification <= 2) cost *= 1.5;  /* Major roads */
            if (ed.cycle_lane > 0) cost *= 0.8;       /* Cycle lanes preferred */
            
            float new_g = cur.g + cost;
            float new_dist = cur.dist + ed.length;
            
            State ns = { ee->end_level, ee->end_tile_id, ee->end_node_id };
            
            /* Skip edges to different levels (transitions) - they're for hierarchical routing */
            if (ee->end_level != cs.level) {
                if (iterations <= 100) {
                    fprintf(stderr, "[DEBUG] Skipping transition edge from level %d to %d\n",
                        cs.level, ee->end_level);
                }
                continue;
            }
            
            /* Check if better path */
            VisitedEntry *nve = visited_get(ns);
            if (nve && new_g >= nve->g) continue;
            
            /* Get neighbor coordinates for heuristic - skip edges to missing tiles */
            Tile *ntile = load_tile(ns.level, ns.tile_id);
            if (!ntile || ns.node_id >= ntile->node_count) {
                /* Edge goes to non-existent tile - skip silently */
                continue;
            }
            
            Node *nnode = &ntile->nodes[ns.node_id];
            
            /* Debug: check for suspiciously long edges */
            double edge_dist = haversine(tile->nodes[cs.node_id].lat, tile->nodes[cs.node_id].lon,
                                         nnode->lat, nnode->lon);
            if (edge_dist > 5000) {
                fprintf(stderr, "[DEBUG] LONG EDGE: iter=%d from tile=%u node=%u (%.4f,%.4f) to tile=%u node=%u (%.4f,%.4f) dist=%.0fm\n",
                    iterations, cs.tile_id, cs.node_id,
                    tile->nodes[cs.node_id].lat, tile->nodes[cs.node_id].lon,
                    ns.tile_id, ns.node_id, nnode->lat, nnode->lon, edge_dist);
            }
            
            double h = haversine(nnode->lat, nnode->lon, end_lat, end_lon) / 25.0 * 3.6;
            
            visited_set(ns, cs, new_g, 1);
            
            HeapEntry ne = { new_g + h, new_g, 0, new_dist, ns };
            heap_push(ne);
        }
        
        if (iterations <= 10) {
            fprintf(stderr, "[DEBUG] Iter %d done: heap_size=%d\n", iterations, g_heap_size);
        }
    }
    
    fprintf(stderr, "[ROUTE] No route found after %d iterations (heap_size=%d)\n", iterations, g_heap_size);
    return 0;
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char **argv) {
    if (argc != 6) {
        fprintf(stderr, "Usage: %s <tiles_dir> <from_lat> <from_lon> <to_lat> <to_lon>\n", argv[0]);
        return 1;
    }
    
    strncpy(g_tiles_dir, argv[1], sizeof(g_tiles_dir) - 1);
    double from_lat = atof(argv[2]);
    double from_lon = atof(argv[3]);
    double to_lat = atof(argv[4]);
    double to_lon = atof(argv[5]);
    
    visited_init();
    
    if (!route(from_lat, from_lon, to_lat, to_lon)) {
        printf("{\"error\":\"No route found\"}\n");
        return 1;
    }
    
    /* Output JSON */
    printf("{\"coords\":[");
    double total_dist = 0;
    double prev_lat = 0, prev_lon = 0;
    for (int i = 0; i < g_path_len; i++) {
        Tile *t = find_tile(g_path[i].level, g_path[i].tile_id);
        if (t && g_path[i].node_id < t->node_count) {
            Node *n = &t->nodes[g_path[i].node_id];
            if (i > 0) {
                printf(",");
                total_dist += haversine(prev_lat, prev_lon, n->lat, n->lon);
            }
            printf("{\"lat\":%.7f,\"lon\":%.7f}", n->lat, n->lon);
            prev_lat = n->lat;
            prev_lon = n->lon;
        }
    }
    printf("],\"nodes\":%d,\"total_dist_km\":%.2f}\n", g_path_len, total_dist / 1000.0);
    
    fprintf(stderr, "[DEBUG] Output path: %d nodes, %.2f km total\n", g_path_len, total_dist / 1000.0);
    
    return 0;
}
