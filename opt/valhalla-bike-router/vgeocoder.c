/*
 * vgeocoder.c - Fast offline geocoder for Nokia N9
 * 
 * Searches geocoder-nlp SQLite databases directly without libpostal.
 * Much faster than Python+libpostal (~0.1s vs ~5s).
 *
 * Features:
 * - Multi-region search (searches all available region databases)
 * - Street abbreviation expansion (str->straße, g->gasse, etc.)
 * - Proximity sorting when coordinates provided
 * - JSON output compatible with Python geocoder
 *
 * Compile with MADDE SDK:
 *   arm-none-linux-gnueabi-gcc -O3 -std=c99 -march=armv7-a -mtune=cortex-a8 \
 *       --sysroot=$SYSROOT -o vgeocoder vgeocoder.c -lsqlite3 -lm
 *
 * Usage: vgeocoder <geocoder_dir> <query> [limit] [near_lat] [near_lon]
 * Output: JSON array of results
 *
 * Example:
 *   vgeocoder /home/user/MyDocs/Maps.OSM/geocoder-nlp "Stephansplatz" 10
 *   vgeocoder /home/user/MyDocs/Maps.OSM/geocoder-nlp "Hauptbahnhof" 5 48.2 16.4
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <math.h>
#include <ctype.h>
#include <sqlite3.h>

#define MAX_REGIONS 20
#define MAX_RESULTS 100
#define MAX_QUERY_LEN 512
#define MAX_TERMS 10
#define DB_NAME "geonlp-primary.sqlite"

/* Result structure */
typedef struct {
    char name[512];
    char name_extra[256];
    double lat;
    double lon;
    int search_rank;
    int type_id;
    double dist_sq;  /* Distance squared for sorting */
    char region[64];
    char type_name[64];
} GeoResult;

/* Global results */
static GeoResult g_results[MAX_RESULTS];
static int g_result_count = 0;

/* Type names cache */
typedef struct {
    int id;
    char name[64];
} TypeEntry;

static TypeEntry g_types[500];
static int g_type_count = 0;

/* Region paths */
static char g_regions[MAX_REGIONS][256];
static int g_region_count = 0;

/* ========================================================================== */
/* Utility functions                                                          */
/* ========================================================================== */

/* JSON string escaping */
static void json_escape(const char *src, char *dst, int max_len) {
    int j = 0;
    for (int i = 0; src[i] && j < max_len - 2; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '"') {
            dst[j++] = '\\'; dst[j++] = '"';
        } else if (c == '\\') {
            dst[j++] = '\\'; dst[j++] = '\\';
        } else if (c == '\n') {
            dst[j++] = '\\'; dst[j++] = 'n';
        } else if (c == '\r') {
            dst[j++] = '\\'; dst[j++] = 'r';
        } else if (c == '\t') {
            dst[j++] = '\\'; dst[j++] = 't';
        } else if (c < 32) {
            /* Skip other control characters */
        } else {
            dst[j++] = c;
        }
    }
    dst[j] = '\0';
}

/* Case-insensitive string ending check */
static int str_ends_with_i(const char *str, const char *suffix) {
    size_t str_len = strlen(str);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > str_len) return 0;
    
    const char *str_end = str + str_len - suffix_len;
    for (size_t i = 0; i < suffix_len; i++) {
        if (tolower((unsigned char)str_end[i]) != tolower((unsigned char)suffix[i])) {
            return 0;
        }
    }
    return 1;
}

/* ========================================================================== */
/* Street abbreviation expansion                                              */
/* ========================================================================== */

/* Expand German/Austrian street abbreviations */
static void expand_street_abbrev(const char *input, char *output, int max_len) {
    strncpy(output, input, max_len - 1);
    output[max_len - 1] = '\0';
    
    size_t len = strlen(output);
    if (len < 2) return;
    
    /* Common abbreviations (case-insensitive check at end) */
    /* str. or str -> straße */
    if (str_ends_with_i(output, "str.")) {
        output[len - 4] = '\0';
        strncat(output, "straße", max_len - strlen(output) - 1);
    } else if (str_ends_with_i(output, "str")) {
        output[len - 3] = '\0';
        strncat(output, "straße", max_len - strlen(output) - 1);
    }
    /* g. or g -> gasse (only if preceded by letter) */
    else if (len >= 2 && str_ends_with_i(output, "g.") && isalpha((unsigned char)output[len-3])) {
        output[len - 2] = '\0';
        strncat(output, "gasse", max_len - strlen(output) - 1);
    } else if (len >= 1 && tolower((unsigned char)output[len-1]) == 'g' && 
               len >= 2 && isalpha((unsigned char)output[len-2])) {
        /* Check it's not a common word ending in 'g' */
        if (!str_ends_with_i(output, "weg") && !str_ends_with_i(output, "ring") &&
            !str_ends_with_i(output, "berg") && !str_ends_with_i(output, "burg")) {
            output[len - 1] = '\0';
            strncat(output, "gasse", max_len - strlen(output) - 1);
        }
    }
    /* pl. or pl -> platz */
    else if (str_ends_with_i(output, "pl.")) {
        output[len - 3] = '\0';
        strncat(output, "platz", max_len - strlen(output) - 1);
    } else if (str_ends_with_i(output, "pl")) {
        output[len - 2] = '\0';
        strncat(output, "platz", max_len - strlen(output) - 1);
    }
}

/* ========================================================================== */
/* Query parsing                                                              */
/* ========================================================================== */

/* Extract house number from string, return pointer to it or NULL */
static const char* extract_house_number(const char *str, char *house_num, int max_len) {
    house_num[0] = '\0';
    size_t len = strlen(str);
    
    /* Look for house number at end: "Hauptstraße 12" or "Hauptstraße 12a" */
    const char *space = strrchr(str, ' ');
    if (space && space[1]) {
        const char *num = space + 1;
        /* Check if starts with digit */
        if (isdigit((unsigned char)num[0])) {
            /* Verify it's a valid house number format */
            int valid = 1;
            for (const char *p = num; *p && valid; p++) {
                if (!isdigit((unsigned char)*p) && !isalpha((unsigned char)*p) && 
                    *p != '/' && *p != '-') {
                    valid = 0;
                }
            }
            if (valid) {
                strncpy(house_num, num, max_len - 1);
                house_num[max_len - 1] = '\0';
                return space; /* Return pointer to space before house number */
            }
        }
    }
    
    return NULL;
}

/* Parse query into search terms */
static int parse_query(const char *query, char terms[][256], char *house_number, int max_terms) {
    int count = 0;
    char buf[512];
    char expanded[256];
    
    house_number[0] = '\0';
    strncpy(buf, query, 511);
    buf[511] = '\0';
    
    /* Split by comma */
    char *saveptr = NULL;
    char *token = strtok_r(buf, ",", &saveptr);
    
    while (token && count < max_terms) {
        /* Trim whitespace */
        while (*token == ' ') token++;
        char *end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';
        
        /* Skip empty/short tokens */
        if (strlen(token) < 2) {
            token = strtok_r(NULL, ",", &saveptr);
            continue;
        }
        
        /* Extract house number from first term */
        if (count == 0) {
            char hn[32];
            const char *space_pos = extract_house_number(token, hn, sizeof(hn));
            if (space_pos) {
                strncpy(house_number, hn, 31);
                house_number[31] = '\0';
                /* Truncate token at the space */
                char temp[256];
                size_t name_len = space_pos - token;
                if (name_len > 0 && name_len < 256) {
                    strncpy(temp, token, name_len);
                    temp[name_len] = '\0';
                    strcpy(token, temp);
                }
            }
        }
        
        /* Expand street abbreviations */
        expand_street_abbrev(token, expanded, sizeof(expanded));
        
        strncpy(terms[count], expanded, 255);
        terms[count][255] = '\0';
        count++;
        
        token = strtok_r(NULL, ",", &saveptr);
    }
    
    return count;
}

/* ========================================================================== */
/* Region discovery                                                           */
/* ========================================================================== */

/* Find all region directories with geocoder databases */
static int find_regions(const char *geocoder_dir) {
    g_region_count = 0;
    
    DIR *dir = opendir(geocoder_dir);
    if (!dir) {
        fprintf(stderr, "[GEOCODER] Cannot open directory: %s\n", geocoder_dir);
        return 0;
    }
    
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL && g_region_count < MAX_REGIONS) {
        if (entry->d_name[0] == '.') continue;
        
        /* Build path to database */
        char db_path[512];
        snprintf(db_path, sizeof(db_path), "%s/%s/%s", 
                 geocoder_dir, entry->d_name, DB_NAME);
        
        /* Check if database exists */
        struct stat st;
        if (stat(db_path, &st) == 0 && S_ISREG(st.st_mode)) {
            snprintf(g_regions[g_region_count], 256, "%s/%s", 
                     geocoder_dir, entry->d_name);
            g_region_count++;
        }
    }
    
    closedir(dir);
    return g_region_count;
}

/* ========================================================================== */
/* Type cache                                                                 */
/* ========================================================================== */

/* Get type name by ID */
static const char* get_type_name(int type_id) {
    for (int i = 0; i < g_type_count; i++) {
        if (g_types[i].id == type_id) {
            return g_types[i].name;
        }
    }
    return "";
}

/* Load type names from database (call once per region) */
static void load_types(sqlite3 *db) {
    /* Don't reload if already loaded */
    if (g_type_count > 0) return;
    
    sqlite3_stmt *stmt;
    const char *sql = "SELECT id, name FROM type LIMIT 500";
    
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return;
    }
    
    while (sqlite3_step(stmt) == SQLITE_ROW && g_type_count < 500) {
        g_types[g_type_count].id = sqlite3_column_int(stmt, 0);
        const char *name = (const char*)sqlite3_column_text(stmt, 1);
        if (name) {
            strncpy(g_types[g_type_count].name, name, 63);
            g_types[g_type_count].name[63] = '\0';
        }
        g_type_count++;
    }
    
    sqlite3_finalize(stmt);
}

/* ========================================================================== */
/* Hierarchy building - get full address with parent names                    */
/* ========================================================================== */

/* Build full display name by following parent chain */
static void build_full_name(sqlite3 *db, int obj_id, const char *base_name, 
                            const char *house_number, char *full_name, int max_len) {
    char parts[5][256];  /* Up to 5 levels: street, district, city, region, country */
    int part_count = 0;
    
    /* Start with base name + house number */
    if (house_number && house_number[0]) {
        snprintf(parts[0], 256, "%s %s", base_name, house_number);
    } else {
        strncpy(parts[0], base_name, 255);
        parts[0][255] = '\0';
    }
    part_count = 1;
    
    /* Follow parent chain */
    sqlite3_stmt *stmt;
    const char *sql = "SELECT name, parent FROM object_primary WHERE id = ?";
    
    int current_id = obj_id;
    int levels = 0;
    
    /* First get parent of current object */
    if (sqlite3_prepare_v2(db, "SELECT parent FROM object_primary WHERE id = ?", -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(stmt, 1, obj_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            current_id = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    
    /* Now traverse parents */
    while (current_id > 0 && part_count < 5 && levels < 10) {
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            break;
        }
        
        sqlite3_bind_int(stmt, 1, current_id);
        
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *parent_name = (const char*)sqlite3_column_text(stmt, 0);
            int parent_id = sqlite3_column_int(stmt, 1);
            
            if (parent_name && strlen(parent_name) > 0) {
                strncpy(parts[part_count], parent_name, 255);
                parts[part_count][255] = '\0';
                part_count++;
            }
            
            current_id = parent_id;
        } else {
            sqlite3_finalize(stmt);
            break;
        }
        
        sqlite3_finalize(stmt);
        levels++;
    }
    
    /* Join parts with ", " */
    full_name[0] = '\0';
    for (int i = 0; i < part_count; i++) {
        if (i > 0) {
            strncat(full_name, ", ", max_len - strlen(full_name) - 1);
        }
        strncat(full_name, parts[i], max_len - strlen(full_name) - 1);
    }
}

/* ========================================================================== */
/* Search functions                                                           */
/* ========================================================================== */

/* Check if result already exists (by lat/lon) */
static int result_exists(double lat, double lon) {
    for (int i = 0; i < g_result_count; i++) {
        double dlat = g_results[i].lat - lat;
        double dlon = g_results[i].lon - lon;
        if (dlat * dlat + dlon * dlon < 0.0000001) {
            return 1;
        }
    }
    return 0;
}

/* Add result if not duplicate */
static void add_result(const char *name, const char *name_extra, 
                       double lat, double lon, int search_rank, int type_id,
                       double dist_sq, const char *region) {
    if (g_result_count >= MAX_RESULTS) return;
    if (result_exists(lat, lon)) return;
    
    GeoResult *r = &g_results[g_result_count];
    strncpy(r->name, name ? name : "", 511);
    r->name[511] = '\0';
    strncpy(r->name_extra, name_extra ? name_extra : "", 255);
    r->name_extra[255] = '\0';
    r->lat = lat;
    r->lon = lon;
    r->search_rank = search_rank;
    r->type_id = type_id;
    r->dist_sq = dist_sq;
    
    /* Extract region name from path */
    const char *slash = strrchr(region, '/');
    strncpy(r->region, slash ? slash + 1 : region, 63);
    r->region[63] = '\0';
    
    /* Get type name */
    strncpy(r->type_name, get_type_name(type_id), 63);
    r->type_name[63] = '\0';
    
    g_result_count++;
}

/* Search in a single region database */
static int search_region(const char *region_path, const char *search_term,
                         const char *house_number,
                         int limit, double near_lat, double near_lon, int use_near) {
    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/%s", region_path, DB_NAME);
    
    sqlite3 *db;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "[GEOCODER] Cannot open: %s\n", db_path);
        return 0;
    }
    
    /* Load types if not already loaded */
    load_types(db);
    
    int found = 0;
    sqlite3_stmt *stmt;
    
    /* Build search pattern */
    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s%%", search_term);
    
    const char *sql;
    if (use_near) {
        sql = "SELECT id, name, name_extra, latitude, longitude, search_rank, type_id, "
              "((latitude - ?1) * (latitude - ?1) + "
              " (longitude - ?2) * (longitude - ?2) * 0.5) as dist_sq "
              "FROM object_primary "
              "WHERE name LIKE ?3 COLLATE NOCASE "
              "ORDER BY dist_sq ASC "
              "LIMIT ?4";
    } else {
        sql = "SELECT id, name, name_extra, latitude, longitude, search_rank, type_id, "
              "0 as dist_sq "
              "FROM object_primary "
              "WHERE name LIKE ?3 COLLATE NOCASE "
              "ORDER BY search_rank DESC "
              "LIMIT ?4";
    }
    
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[GEOCODER] SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 0;
    }
    
    sqlite3_bind_double(stmt, 1, near_lat);
    sqlite3_bind_double(stmt, 2, near_lon);
    sqlite3_bind_text(stmt, 3, pattern, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 4, limit * 3);  /* Get extra to allow for duplicates */
    
    while (sqlite3_step(stmt) == SQLITE_ROW && g_result_count < limit) {
        int obj_id = sqlite3_column_int(stmt, 0);
        const char *name = (const char*)sqlite3_column_text(stmt, 1);
        const char *name_extra = (const char*)sqlite3_column_text(stmt, 2);
        double lat = sqlite3_column_double(stmt, 3);
        double lon = sqlite3_column_double(stmt, 4);
        int rank = sqlite3_column_int(stmt, 5);
        int type_id = sqlite3_column_int(stmt, 6);
        double dist_sq = sqlite3_column_double(stmt, 7);
        
        /* Build full name with parent hierarchy */
        char full_name[512];
        build_full_name(db, obj_id, name ? name : "", house_number, full_name, sizeof(full_name));
        
        add_result(full_name, name_extra, lat, lon, rank, type_id, dist_sq, region_path);
        found++;
    }
    
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    
    return found;
}

/* Compare results for sorting */
static int compare_results(const void *a, const void *b) {
    const GeoResult *ra = (const GeoResult*)a;
    const GeoResult *rb = (const GeoResult*)b;
    
    /* Sort by distance first, then by rank */
    if (ra->dist_sq < rb->dist_sq) return -1;
    if (ra->dist_sq > rb->dist_sq) return 1;
    if (ra->search_rank > rb->search_rank) return -1;
    if (ra->search_rank < rb->search_rank) return 1;
    return 0;
}

/* Main search function */
static int search_all_regions(const char *geocoder_dir, const char *query,
                              int limit, double near_lat, double near_lon) {
    char terms[MAX_TERMS][256];
    char house_number[32];
    int use_near = (near_lat != 0.0 || near_lon != 0.0);
    
    /* Parse query */
    int term_count = parse_query(query, terms, house_number, MAX_TERMS);
    if (term_count == 0) {
        fprintf(stderr, "[GEOCODER] No search terms extracted\n");
        return 0;
    }
    
    fprintf(stderr, "[GEOCODER] Terms: ");
    for (int i = 0; i < term_count; i++) {
        fprintf(stderr, "'%s' ", terms[i]);
    }
    fprintf(stderr, "\n");
    
    if (house_number[0]) {
        fprintf(stderr, "[GEOCODER] House number: %s\n", house_number);
    }
    
    /* Find regions */
    if (find_regions(geocoder_dir) == 0) {
        fprintf(stderr, "[GEOCODER] No regions found in: %s\n", geocoder_dir);
        return 0;
    }
    
    fprintf(stderr, "[GEOCODER] Searching %d regions\n", g_region_count);
    
    /* Search primary term (usually street name - first term after reversing) */
    const char *primary = terms[0];
    
    /* Also keep original (unexpanded) for fallback */
    char original[256];
    strncpy(original, query, 255);
    original[255] = '\0';
    /* Remove house number from original */
    char *space = strrchr(original, ' ');
    if (space && isdigit((unsigned char)space[1])) {
        *space = '\0';
    }
    /* Remove comma and everything after */
    char *comma = strchr(original, ',');
    if (comma) *comma = '\0';
    /* Trim */
    size_t len = strlen(original);
    while (len > 0 && original[len-1] == ' ') original[--len] = '\0';
    
    /* Search all regions with expanded term */
    for (int i = 0; i < g_region_count && g_result_count < limit; i++) {
        search_region(g_regions[i], primary, house_number, limit, near_lat, near_lon, use_near);
    }
    
    /* If no results, try original (unexpanded) term */
    if (g_result_count == 0 && strcmp(primary, original) != 0) {
        fprintf(stderr, "[GEOCODER] Trying original: '%s'\n", original);
        for (int i = 0; i < g_region_count && g_result_count < limit; i++) {
            search_region(g_regions[i], original, house_number, limit, near_lat, near_lon, use_near);
        }
    }
    
    /* If still no results, try infix search (slower) */
    if (g_result_count == 0) {
        fprintf(stderr, "[GEOCODER] Trying infix search\n");
        char infix_pattern[256];
        snprintf(infix_pattern, sizeof(infix_pattern), "%%%s", original);
        
        for (int i = 0; i < g_region_count && g_result_count < limit; i++) {
            char db_path[512];
            snprintf(db_path, sizeof(db_path), "%s/%s", g_regions[i], DB_NAME);
            
            sqlite3 *db;
            if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
                continue;
            }
            
            load_types(db);
            
            sqlite3_stmt *stmt;
            const char *sql = "SELECT id, name, name_extra, latitude, longitude, search_rank, type_id "
                              "FROM object_primary "
                              "WHERE name LIKE ?1 COLLATE NOCASE "
                              "ORDER BY search_rank DESC "
                              "LIMIT ?2";
            
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_text(stmt, 1, infix_pattern, -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, limit * 2);
                
                while (sqlite3_step(stmt) == SQLITE_ROW && g_result_count < limit) {
                    int obj_id = sqlite3_column_int(stmt, 0);
                    const char *name = (const char*)sqlite3_column_text(stmt, 1);
                    const char *name_extra = (const char*)sqlite3_column_text(stmt, 2);
                    double lat = sqlite3_column_double(stmt, 3);
                    double lon = sqlite3_column_double(stmt, 4);
                    int rank = sqlite3_column_int(stmt, 5);
                    int type_id = sqlite3_column_int(stmt, 6);
                    
                    /* Build full name with hierarchy */
                    char full_name[512];
                    build_full_name(db, obj_id, name ? name : "", house_number, full_name, sizeof(full_name));
                    
                    add_result(full_name, name_extra, lat, lon, rank, type_id, 999999.0, g_regions[i]);
                }
                sqlite3_finalize(stmt);
            }
            sqlite3_close(db);
        }
    }
    
    /* Sort results */
    if (g_result_count > 1) {
        qsort(g_results, g_result_count, sizeof(GeoResult), compare_results);
    }
    
    return g_result_count;
}

/* ========================================================================== */
/* JSON output                                                                */
/* ========================================================================== */

static void output_json(int limit) {
    char escaped_name[1024];
    char escaped_type[128];
    char escaped_region[128];
    
    printf("[");
    
    int output_count = (g_result_count < limit) ? g_result_count : limit;
    
    for (int i = 0; i < output_count; i++) {
        GeoResult *r = &g_results[i];
        
        json_escape(r->name, escaped_name, sizeof(escaped_name));
        json_escape(r->type_name, escaped_type, sizeof(escaped_type));
        json_escape(r->region, escaped_region, sizeof(escaped_region));
        
        if (i > 0) printf(",");
        printf("\n  {");
        printf("\"name\":\"%s\",", escaped_name);
        printf("\"lat\":%.6f,", r->lat);
        printf("\"lng\":%.6f,", r->lon);
        printf("\"type\":\"%s\",", escaped_type);
        printf("\"region\":\"%s\",", escaped_region);
        printf("\"source\":\"offline\"");
        printf("}");
    }
    
    printf("\n]\n");
}

/* ========================================================================== */
/* Main                                                                       */
/* ========================================================================== */

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <geocoder_dir> <query> [limit] [near_lat] [near_lon]\n", argv[0]);
        fprintf(stderr, "Example: %s /home/user/MyDocs/Maps.OSM/geocoder-nlp \"Stephansplatz\" 10\n", argv[0]);
        return 1;
    }
    
    const char *geocoder_dir = argv[1];
    const char *query = argv[2];
    int limit = (argc > 3) ? atoi(argv[3]) : 10;
    double near_lat = (argc > 4) ? atof(argv[4]) : 0.0;
    double near_lon = (argc > 5) ? atof(argv[5]) : 0.0;
    
    if (limit < 1) limit = 1;
    if (limit > MAX_RESULTS) limit = MAX_RESULTS;
    
    fprintf(stderr, "[GEOCODER] Query: '%s', limit: %d\n", query, limit);
    if (near_lat != 0.0 || near_lon != 0.0) {
        fprintf(stderr, "[GEOCODER] Near: %.4f, %.4f\n", near_lat, near_lon);
    }
    
    int count = search_all_regions(geocoder_dir, query, limit, near_lat, near_lon);
    
    fprintf(stderr, "[GEOCODER] Found %d results\n", count);
    
    output_json(limit);
    
    return (count > 0) ? 0 : 1;
}
