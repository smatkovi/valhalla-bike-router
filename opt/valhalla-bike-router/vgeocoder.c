/*
 * vgeocoder.c - Fast offline geocoder for Nokia N9
 * 
 * Searches geocoder-nlp SQLite databases directly without libpostal.
 * Much faster than Python+libpostal (~0.1s vs ~5s).
 *
 * Compile with MADDE SDK:
 *   arm-none-linux-gnueabi-gcc -O3 -std=c99 -march=armv7-a -mtune=cortex-a8 \
 *       --sysroot=$SYSROOT -lsqlite3 -lm -o vgeocoder vgeocoder.c
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
#include <sqlite3.h>

#define MAX_REGIONS 20
#define MAX_RESULTS 100
#define MAX_QUERY_LEN 512
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

/* JSON string escaping */
static void json_escape(const char *src, char *dst, int max_len) {
    int j = 0;
    for (int i = 0; src[i] && j < max_len - 2; i++) {
        switch (src[i]) {
            case '"':  dst[j++] = '\\'; dst[j++] = '"'; break;
            case '\\': dst[j++] = '\\'; dst[j++] = '\\'; break;
            case '\n': dst[j++] = '\\'; dst[j++] = 'n'; break;
            case '\r': dst[j++] = '\\'; dst[j++] = 'r'; break;
            case '\t': dst[j++] = '\\'; dst[j++] = 't'; break;
            default:   dst[j++] = src[i]; break;
        }
    }
    dst[j] = '\0';
}

/* Get type name by ID */
static const char* get_type_name(int type_id) {
    for (int i = 0; i < g_type_count; i++) {
        if (g_types[i].id == type_id) {
            return g_types[i].name;
        }
    }
    return "";
}

/* Load type names from database */
static void load_types(sqlite3 *db) {
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

/* Primitive query parser - extract search terms */
static int parse_query(const char *query, char terms[][256], int max_terms) {
    int count = 0;
    char buf[512];
    strncpy(buf, query, 511);
    buf[511] = '\0';
    
    /* Split by comma */
    char *token = strtok(buf, ",");
    while (token && count < max_terms) {
        /* Trim whitespace */
        while (*token == ' ') token++;
        char *end = token + strlen(token) - 1;
        while (end > token && *end == ' ') *end-- = '\0';
        
        /* Skip empty tokens */
        if (strlen(token) < 2) {
            token = strtok(NULL, ",");
            continue;
        }
        
        /* Remove leading/trailing house numbers */
        /* "Hauptstraße 12" -> "Hauptstraße" */
        char *space = strrchr(token, ' ');
        if (space) {
            char *num = space + 1;
            int is_number = 1;
            for (char *p = num; *p; p++) {
                if (!(*p >= '0' && *p <= '9') && *p != '/' && *p != '-' && 
                    !(*p >= 'a' && *p <= 'z') && !(*p >= 'A' && *p <= 'Z')) {
                    is_number = 0;
                    break;
                }
            }
            /* Check if it starts with a digit