#!/usr/bin/env python3
"""
Offline Geocoder for Valhalla Bike Router
Uses geocoder-nlp SQLite database from OSM Scout Server / modrana.org

Database source: https://data.modrana.org/osm_scout_server/geocoder-nlp-39/
For Austria: europe-austria/

Files needed in {tiles_dir}/geocoder-nlp/:
- geonlp-primary.sqlite (main database with places)
- geonlp-normalized.trie (optional: for fuzzy matching)
- geonlp-normalized-id.kch (optional: trie ID mapping)

Address parsing:
- Uses libpostal if available (ML-based, 99% accuracy)
- Falls back to primitive parser (comma-split, house number removal)
"""

import os
import sys
import sqlite3
import math
from typing import List, Dict, Optional, Tuple

# Geocoder data directory
GEOCODER_DIR = '/home/user/MyDocs/Maps.OSM/geocoder-nlp'
GEOCODER_DB_NAME = 'geonlp-primary.sqlite'

# Try to import libpostal wrapper
_libpostal = None
_libpostal_available = None
_libpostal_loading = False
_libpostal_ready = False

def _init_libpostal():
    """Check if libpostal is available (but don't load yet)."""
    global _libpostal, _libpostal_available
    
    if _libpostal_available is not None:
        return _libpostal_available
    
    try:
        from libpostal_wrapper import LibpostalWrapper
        _libpostal = LibpostalWrapper()
        if _libpostal.is_available():
            _libpostal_available = True
            print("[GEOCODER] libpostal available (not loaded yet)", file=sys.stderr)
        else:
            _libpostal_available = False
            print("[GEOCODER] libpostal not found, using primitive parser", file=sys.stderr)
    except ImportError:
        _libpostal_available = False
        print("[GEOCODER] libpostal_wrapper not found, using primitive parser", file=sys.stderr)
    except Exception as e:
        _libpostal_available = False
        print(f"[GEOCODER] libpostal init error: {e}, using primitive parser", file=sys.stderr)
    
    return _libpostal_available


def warmup_libpostal():
    """Load libpostal in background. Call this at server start."""
    global _libpostal, _libpostal_loading, _libpostal_ready
    
    if _libpostal_ready or _libpostal_loading:
        return
    
    if not _init_libpostal() or _libpostal is None:
        return
    
    _libpostal_loading = True
    print("[GEOCODER] Loading libpostal models (this takes a few minutes)...", file=sys.stderr)
    
    try:
        import time
        start = time.time()
        if _libpostal.setup():
            elapsed = time.time() - start
            _libpostal_ready = True
            print(f"[GEOCODER] libpostal ready! (loaded in {elapsed:.1f}s)", file=sys.stderr)
        else:
            print("[GEOCODER] libpostal setup failed", file=sys.stderr)
    except Exception as e:
        print(f"[GEOCODER] libpostal setup error: {e}", file=sys.stderr)
    finally:
        _libpostal_loading = False


def is_libpostal_ready():
    """Check if libpostal is loaded and ready to use."""
    return _libpostal_ready


class OfflineGeocoder:
    """Offline geocoding using geocoder-nlp SQLite databases.
    
    Supports multiple regions - searches all available region databases.
    Structure: {geocoder_dir}/{region}/geonlp-primary.sqlite
    """
    
    def __init__(self, data_dir: str = None):
        """
        Initialize geocoder.
        
        Args:
            data_dir: Optional explicit geocoder-nlp directory
        """
        self.geocoder_dir = data_dir or GEOCODER_DIR
        self.connections = {}  # region -> connection
        self._type_caches = {}  # region -> type cache
        self._available = None
        self._regions = None
        
    def _find_regions(self) -> List[str]:
        """Find all available region directories with geocoder databases."""
        if self._regions is not None:
            return self._regions
            
        self._regions = []
        if not os.path.exists(self.geocoder_dir):
            return self._regions
            
        for entry in os.listdir(self.geocoder_dir):
            region_dir = os.path.join(self.geocoder_dir, entry)
            db_path = os.path.join(region_dir, GEOCODER_DB_NAME)
            if os.path.isdir(region_dir) and os.path.exists(db_path):
                self._regions.append(entry)
        
        return self._regions
        
    def is_available(self) -> bool:
        """Check if any offline geocoder database is available."""
        if self._available is None:
            self._available = len(self._find_regions()) > 0
        return self._available
    
    def get_available_regions(self) -> List[str]:
        """Get list of available geocoder regions."""
        return self._find_regions()
        
    def _get_connection(self, region: str):
        """Get or create database connection for a region."""
        if region not in self.connections:
            db_path = os.path.join(self.geocoder_dir, region, GEOCODER_DB_NAME)
            if not os.path.exists(db_path):
                return None
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            self.connections[region] = conn
            
            # Load type cache for this region
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM type")
            self._type_caches[region] = {row['id']: row['name'] for row in cursor.fetchall()}
            
            # Check/create index for fast name search (crucial for performance!)
            self._ensure_name_index(conn, region)
            
        return self.connections[region]
    
    def _ensure_name_index(self, conn, region: str):
        """Ensure index exists on name column for fast LIKE searches."""
        cursor = conn.cursor()
        
        # Check if index exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_object_name'")
        if cursor.fetchone():
            return  # Index already exists
        
        # Create index - this takes ~30-60 seconds but only once!
        print(f"[GEOCODER] Creating name index for {region} (one-time, ~30-60s)...", file=sys.stderr)
        try:
            cursor.execute("CREATE INDEX idx_object_name ON object_primary(name COLLATE NOCASE)")
            conn.commit()
            print(f"[GEOCODER] Index created for {region}!", file=sys.stderr)
        except sqlite3.OperationalError as e:
            # Database might be read-only
            print(f"[GEOCODER] Could not create index (read-only?): {e}", file=sys.stderr)
    
    def _get_type_cache(self, region: str) -> Dict:
        """Get type cache for a region."""
        return self._type_caches.get(region, {})
    
    def close(self):
        """Close all database connections."""
        for conn in self.connections.values():
            try:
                conn.close()
            except:
                pass
        self.connections = {}
        self._type_caches = {}
    
    def _type_to_category(self, type_name: str) -> str:
        """Convert type name to a simple category for UI."""
        if not type_name:
            return 'place'
        
        type_lower = type_name.lower()
        
        # Transport
        if any(x in type_lower for x in ['station', 'stop', 'terminal', 'airport', 'aerodrome', 'helipad']):
            return 'transport'
        if 'parking' in type_lower:
            return 'parking'
        
        # Food & Drink
        if any(x in type_lower for x in ['restaurant', 'cafe', 'bar', 'pub', 'fast_food', 'biergarten']):
            return 'food'
        
        # Shopping
        if 'shop' in type_lower or 'supermarket' in type_lower or 'mall' in type_lower:
            return 'shop'
            
        # Accommodation
        if any(x in type_lower for x in ['hotel', 'hostel', 'guest', 'camp', 'motel']):
            return 'accommodation'
        
        # POI / Tourism
        if any(x in type_lower for x in ['tourism', 'museum', 'attraction', 'viewpoint', 'castle', 'monument']):
            return 'tourism'
        
        # Nature
        if any(x in type_lower for x in ['park', 'forest', 'water', 'river', 'lake', 'mountain', 'peak']):
            return 'nature'
        
        # Admin boundaries
        if 'admin' in type_lower or 'boundary' in type_lower:
            return 'admin'
        
        # Address / Street
        if any(x in type_lower for x in ['house', 'address', 'street', 'road', 'highway', 'path', 'residential']):
            return 'address'
        
        # Amenities
        if any(x in type_lower for x in ['school', 'university', 'hospital', 'pharmacy', 'bank', 'post']):
            return 'amenity'
        
        # Sports/Leisure
        if any(x in type_lower for x in ['sport', 'pitch', 'stadium', 'swimming', 'golf', 'leisure']):
            return 'leisure'
        
        return 'place'
    
    def _build_display_name(self, row: sqlite3.Row, type_cache: Dict) -> str:
        """Build a display name from the row data."""
        parts = []
        
        name = row['name']
        if name:
            parts.append(name)
        
        # Add name_extra if available (often contains location context)
        name_extra = row.get('name_extra') if hasattr(row, 'keys') else None
        try:
            name_extra = row['name_extra']
        except (IndexError, KeyError):
            name_extra = None
        
        if name_extra:
            parts.append(name_extra)
        
        # For short names without extra info, add type info
        type_name = type_cache.get(row['type_id'], '')
        if type_name and len(name or '') < 20 and not name_extra:
            # Clean up type name for display
            type_display = type_name.replace('_', ' ').replace('amenity ', '').replace('tourism ', '')
            if type_display not in (name or '').lower():
                parts.append('(' + type_display + ')')
        
        return ', '.join(parts) if parts else 'Unknown'
    
    def _format_result(self, row: sqlite3.Row, type_cache: Dict, region: str = None) -> Dict:
        """Format a database row as a geocoding result (compatible with Photon/API format)."""
        name = row['name'] or ''
        type_name = type_cache.get(row['type_id'], '')
        
        # Get full name including parent hierarchy (city, region, country)
        full_name = self._get_full_name(row['id'], region, max_levels=3)
        
        result = {
            'name': full_name if full_name else self._build_display_name(row, type_cache),
            'lat': row['latitude'],
            'lng': row['longitude'],  # Note: API uses 'lng' not 'lon'
            'type': self._type_to_category(type_name),
            'osm_type': type_name,
            'source': 'offline'
        }
        if region:
            result['region'] = region
        return result
    
    def _get_full_name(self, obj_id: int, region: str, max_levels: int = 3) -> Optional[str]:
        """
        Get full hierarchical name by following parent links.
        Like geocoder-nlp's get_name function.
        
        Returns: "Ahornweg, Ried im Innkreis, Oberösterreich" or None
        """
        conn = self._get_connection(region)
        if not conn:
            return None
        
        cursor = conn.cursor()
        parts = []
        current_id = obj_id
        levels = 0
        
        while current_id and current_id > 0 and levels < max_levels:
            cursor.execute("""
                SELECT name, name_extra, parent 
                FROM object_primary 
                WHERE id = ?
            """, (current_id,))
            row = cursor.fetchone()
            
            if not row:
                break
            
            name = row['name']
            name_extra = row['name_extra']
            parent = row['parent']
            
            if name:
                # Use name_extra if different and available (for first level only)
                if levels == 0 and name_extra and name_extra != name:
                    parts.append(f"{name_extra}, {name}")
                else:
                    parts.append(name)
            
            current_id = parent if parent and parent > 0 else None
            levels += 1
        
        return ', '.join(parts) if parts else None
    
    def _parse_query_primitive(self, query: str) -> List[str]:
        """
        Primitive address parser (like OSM Scout Server).
        Splits by comma and extracts searchable terms.
        
        "Ahornweg 14, Ried im Innkreis" -> ["Ried im Innkreis", "Ahornweg"]
        (reversed order: coarsest to finest, like geocoder-nlp hierarchy)
        """
        import re
        
        # Split by comma
        parts = [p.strip() for p in query.split(',')]
        
        # Reverse order (OSM Scout Server does this - finest detail last)
        parts = list(reversed(parts))
        
        search_terms = []
        for part in parts:
            if not part:
                continue
            
            # Remove house numbers (digits at end or start)
            # "Ahornweg 14" -> "Ahornweg"
            # "14 Main Street" -> "Main Street"
            cleaned = re.sub(r'^\d+[a-zA-Z]?\s+', '', part)  # Leading number
            cleaned = re.sub(r'\s+\d+[a-zA-Z]?$', '', cleaned)  # Trailing number
            cleaned = re.sub(r'\s+\d+[a-zA-Z]?[-/]\d+[a-zA-Z]?$', '', cleaned)  # "12-14" or "12/A"
            
            if cleaned and len(cleaned) >= 2:
                search_terms.append(cleaned)
        
        return search_terms
    
    def _parse_query_libpostal(self, query: str) -> Tuple[List[str], Optional[str]]:
        """
        Parse query using libpostal (ML-based).
        
        Returns:
            Tuple of (search_terms, house_number)
            
        Example:
            "Ahornweg 14, Ried im Innkreis" -> (["Ried im Innkreis", "Ahornweg"], "14")
        """
        global _libpostal
        import re
        
        # Extract house number first (before any parsing)
        house_number = self._extract_house_number(query)
        
        # Only use libpostal if it's fully loaded and ready
        if not is_libpostal_ready() or _libpostal is None:
            return self._parse_query_primitive(query), house_number
        
        # Parse with libpostal
        try:
            parsed = _libpostal.parse_address(query)
            print(f"[GEOCODER] libpostal parsed: {parsed}", file=sys.stderr)
        except Exception as e:
            print(f"[GEOCODER] libpostal parse error: {e}", file=sys.stderr)
            return self._parse_query_primitive(query), house_number
        
        # Check if libpostal gave us useful structure
        # If it just put everything in one label (like 'house'), use primitive parser
        if len(parsed) == 1:
            label = parsed[0][0]  # First element is the label
            if label in ('house', 'road', 'city'):
                # libpostal didn't really parse it, fall back to primitive
                print(f"[GEOCODER] libpostal single block ({label}), using primitive parser", file=sys.stderr)
                return self._parse_query_primitive(query), house_number
        
        # Extract searchable components following OSM Scout Server hierarchy
        # Order: country -> state -> city -> suburb -> road -> house
        search_terms = []
        
        # Priority order for hierarchy (coarsest to finest)
        hierarchy_labels = ['country', 'state', 'state_district', 'city', 
                           'city_district', 'suburb', 'road', 'house']
        
        # First pass: collect by label
        # Note: libpostal returns (label, component) not (component, label)!
        components_by_label = {}
        for label, component in parsed:
            if label == 'house_number':
                if not house_number:
                    house_number = component
            elif component and len(component) >= 2:
                if label not in components_by_label:
                    components_by_label[label] = []
                # Clean house number from component if present
                cleaned = re.sub(r'\s+\d+[a-zA-Z]?$', '', component)
                cleaned = re.sub(r'^\d+[a-zA-Z]?\s+', '', cleaned)
                if cleaned and len(cleaned) >= 2:
                    components_by_label[label].append(cleaned)
        
        # Build search terms in hierarchy order
        for label in hierarchy_labels:
            if label in components_by_label:
                for comp in components_by_label[label]:
                    if comp not in search_terms:
                        search_terms.append(comp)
        
        # Add any remaining labels not in our hierarchy
        for label, components in components_by_label.items():
            if label not in hierarchy_labels:
                for comp in components:
                    if comp not in search_terms:
                        search_terms.append(comp)
        
        if not search_terms:
            # Fallback to primitive parser if libpostal didn't find anything useful
            return self._parse_query_primitive(query), house_number
        
        return search_terms, house_number
    
    def _extract_house_number(self, text: str) -> Optional[str]:
        """Extract house number from text."""
        import re
        # Match common house number patterns at end: "14", "14a", "14-16", "14/2"
        match = re.search(r'\s(\d+[a-zA-Z]?(?:[-/]\d+[a-zA-Z]?)?)(?:\s*,|\s*$)', text)
        if match:
            return match.group(1)
        # Match at start: "14 Hauptstraße"
        match = re.search(r'^(\d+[a-zA-Z]?)\s', text)
        if match:
            return match.group(1)
        return None
    
    def _find_location_coords(self, name: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Find coordinates for a location name (city, town, etc.).
        Returns (latitude, longitude) or (None, None) if not found.
        """
        regions = self._find_regions()
        
        # Prefer exact matches for cities/towns
        for region in regions:
            conn = self._get_connection(region)
            if not conn:
                continue
            
            cursor = conn.cursor()
            
            # Search for city/town/village with this name
            # Use LIKE for case-insensitive match, prioritize by search_rank
            sql = """
                SELECT o.latitude, o.longitude, o.search_rank, t.name as type_name
                FROM object_primary o
                JOIN type t ON o.type_id = t.id
                WHERE o.name LIKE ? COLLATE NOCASE
                  AND (t.name LIKE '%city%' OR t.name LIKE '%town%' OR t.name LIKE '%village%' 
                       OR t.name LIKE '%municipality%' OR t.name LIKE '%place%')
                ORDER BY o.search_rank DESC
                LIMIT 1
            """
            cursor.execute(sql, (name,))
            row = cursor.fetchone()
            
            if row:
                return row['latitude'], row['longitude']
            
            # Fallback: any match with this exact name
            sql = """
                SELECT o.latitude, o.longitude
                FROM object_primary o
                WHERE o.name LIKE ? COLLATE NOCASE
                ORDER BY o.search_rank DESC
                LIMIT 1
            """
            cursor.execute(sql, (name,))
            row = cursor.fetchone()
            
            if row:
                return row['latitude'], row['longitude']
        
        return None, None
    
    def _parse_query(self, query: str) -> Tuple[List[str], Optional[str]]:
        """
        Parse query using best available method.
        
        Returns:
            Tuple of (search_terms, house_number)
        """
        # Use libpostal if loaded, otherwise primitive parser
        if is_libpostal_ready():
            return self._parse_query_libpostal(query)
        
        # Fallback to primitive parser (fast, always available)
        return self._parse_query_primitive(query), None
    
    def search(self, query: str, limit: int = 10, 
               near_lat: Optional[float] = None, 
               near_lon: Optional[float] = None) -> List[Dict]:
        """
        Search for locations by name across all available regions.
        Uses libpostal for address parsing if available, otherwise primitive parser.
        
        Args:
            query: Search string (e.g. "Bóné Kálmán utca 6, Budapest")
            limit: Maximum number of results
            near_lat: Optional latitude for proximity sorting
            near_lon: Optional longitude for proximity sorting
            
        Returns:
            List of location dictionaries
        """
        import time
        start_time = time.time()
        
        if not self.is_available():
            print(f"[GEOCODER] Not available", file=sys.stderr)
            return []
        
        regions = self._find_regions()
        print(f"[GEOCODER] Searching in regions: {regions}", file=sys.stderr)
        
        # Parse query (tries libpostal first, falls back to primitive)
        parse_start = time.time()
        search_terms, house_number = self._parse_query(query)
        if not search_terms:
            # Fallback to original query
            search_terms = [query.strip()]
        parse_time = time.time() - parse_start
        
        print(f"[GEOCODER] Search terms: {search_terms} (parsed in {parse_time*1000:.0f}ms)", file=sys.stderr)
        if house_number:
            print(f"[GEOCODER] House number: {house_number}", file=sys.stderr)
        
        all_results = []
        
        # Strategy: If we have multiple search terms (street + city),
        # first find the city to get coordinates, then search for street nearby
        
        # The LAST term in search_terms is usually the most specific (street)
        # after the primitive parser reverses the hierarchy
        primary_term = search_terms[-1] if search_terms else query
        secondary_terms = search_terms[:-1] if len(search_terms) > 1 else []
        
        # Try to find coordinates for secondary terms (city/region)
        city_lat, city_lon = None, None
        if secondary_terms and near_lat is None:
            city_lat, city_lon = self._find_location_coords(secondary_terms[0])
            if city_lat:
                print(f"[GEOCODER] Found city '{secondary_terms[0]}' at {city_lat:.4f}, {city_lon:.4f}", file=sys.stderr)
        
        # Use city coords as reference point if we found them
        ref_lat = near_lat if near_lat is not None else city_lat
        ref_lon = near_lon if near_lon is not None else city_lon
        
        print(f"[GEOCODER] Primary search: '{primary_term}', city filter: {secondary_terms}", file=sys.stderr)
        
        # Search in all available regions
        for region in regions:
            conn = self._get_connection(region)
            if not conn:
                print(f"[GEOCODER] No connection to {region}", file=sys.stderr)
                continue
            
            print(f"[GEOCODER] Searching in {region}...", file=sys.stderr)
            region_start = time.time()
            
            type_cache = self._get_type_cache(region)
            cursor = conn.cursor()
            seen_ids = set()
            
            # Only use prefix match (fast with index)
            pattern = primary_term + '%'
            
            if ref_lat is not None and ref_lon is not None:
                # Search with distance sorting
                sql = """
                    SELECT o.id, o.name, o.name_en, o.name_extra, 
                           o.latitude, o.longitude, o.search_rank, o.type_id,
                           ((o.latitude - ?) * (o.latitude - ?) + 
                            (o.longitude - ?) * (o.longitude - ?) * 0.5) as dist_sq
                    FROM object_primary o
                    WHERE o.name LIKE ? COLLATE NOCASE
                    ORDER BY dist_sq ASC
                    LIMIT ?
                """
                cursor.execute(sql, (ref_lat, ref_lat, ref_lon, ref_lon,
                                    pattern, limit * 5))
            else:
                sql = """
                    SELECT o.id, o.name, o.name_en, o.name_extra,
                           o.latitude, o.longitude, o.search_rank, o.type_id
                    FROM object_primary o
                    WHERE o.name LIKE ? COLLATE NOCASE
                    ORDER BY o.search_rank DESC
                    LIMIT ?
                """
                cursor.execute(sql, (pattern, limit * 5))
            
            for row in cursor.fetchall():
                row_id = f"{region}:{row['id']}"
                if row_id in seen_ids:
                    continue
                seen_ids.add(row_id)
                
                result = self._format_result(row, type_cache, region)
                result['_rank'] = row['search_rank']
                if 'dist_sq' in row.keys():
                    result['_dist_sq'] = row['dist_sq']
                else:
                    result['_dist_sq'] = 999999
                
                all_results.append(result)
            
            region_time = time.time() - region_start
            print(f"[GEOCODER] {region}: found {len(all_results)} results in {region_time*1000:.0f}ms", file=sys.stderr)
            
            # Early exit: if we have enough good results, skip other regions
            if len(all_results) >= limit * 2:
                print(f"[GEOCODER] Enough results, skipping remaining regions", file=sys.stderr)
                break
        
        # Sort combined results - by distance if we have reference point, else by rank
        all_results.sort(key=lambda x: (x.get('_dist_sq', 999999), -x.get('_rank', 0)))
        
        # Clean up internal sort keys and limit results
        for r in all_results:
            r.pop('_rank', None)
            r.pop('_dist_sq', None)
        
        # Add house number to results if we extracted one
        if house_number:
            for r in all_results:
                # Add house number to display name 
                current_name = r.get('name', '')
                # Check if it's a street/road type result
                osm_type = r.get('osm_type', '').lower()
                if 'highway' in osm_type or 'street' in osm_type or 'road' in osm_type or not osm_type:
                    # Insert house number after street name but before city
                    # "Ahornweg, Ried im Innkreis" -> "Ahornweg 14, Ried im Innkreis"
                    parts = current_name.split(', ', 1)
                    if len(parts) >= 1:
                        parts[0] = f"{parts[0]} {house_number}"
                        r['name'] = ', '.join(parts)
                    else:
                        r['name'] = f"{current_name} {house_number}"
                # Also store house number separately
                r['house_number'] = house_number
        
        total_time = time.time() - start_time
        print(f"[GEOCODER] Returning {min(len(all_results), limit)} results (total: {total_time*1000:.0f}ms)", file=sys.stderr)
        return all_results[:limit]
    
    def reverse(self, lat: float, lon: float, radius_km: float = 0.5, 
                limit: int = 5) -> List[Dict]:
        """
        Reverse geocode: find places near a coordinate across all regions.
        
        Args:
            lat: Latitude
            lon: Longitude
            radius_km: Search radius in kilometers
            limit: Maximum number of results
            
        Returns:
            List of nearby location dictionaries
        """
        if not self.is_available():
            return []
        
        # Approximate degree offset for radius
        # 1 degree latitude ≈ 111km
        lat_offset = radius_km / 111.0
        lon_offset = radius_km / (111.0 * max(0.1, math.cos(math.radians(lat))))
        
        all_results = []
        
        # Search in all available regions
        for region in self._find_regions():
            conn = self._get_connection(region)
            if not conn:
                continue
                
            type_cache = self._get_type_cache(region)
            cursor = conn.cursor()
            
            sql = """
                SELECT o.*, 
                       ((o.latitude - ?) * (o.latitude - ?) + 
                        (o.longitude - ?) * (o.longitude - ?)) as dist_sq
                FROM object_primary o
                WHERE o.latitude BETWEEN ? AND ?
                  AND o.longitude BETWEEN ? AND ?
                ORDER BY dist_sq ASC, o.search_rank DESC
                LIMIT ?
            """
            
            cursor.execute(sql, (
                lat, lat, lon, lon,
                lat - lat_offset, lat + lat_offset,
                lon - lon_offset, lon + lon_offset,
                limit * 2
            ))
            
            for row in cursor.fetchall():
                result = self._format_result(row, type_cache, region)
                # Add distance in meters
                dist_km = math.sqrt(row['dist_sq']) * 111.0
                result['distance_m'] = int(dist_km * 1000)
                result['_dist_sq'] = row['dist_sq']
                all_results.append(result)
        
        # Sort by distance and limit
        all_results.sort(key=lambda x: x.get('_dist_sq', 999999))
        
        # Clean up internal sort key
        for r in all_results:
            r.pop('_dist_sq', None)
        
        return all_results[:limit]


# =============================================================================
# Global instance for easy integration
# =============================================================================

_geocoder = None

def get_geocoder(data_dir: str = None) -> OfflineGeocoder:
    """Get or create the global geocoder instance."""
    global _geocoder
    if _geocoder is None:
        _geocoder = OfflineGeocoder(data_dir)
    return _geocoder


def is_offline_available(data_dir: str = None) -> bool:
    """Check if offline geocoding is available."""
    return get_geocoder(data_dir).is_available()


def search_offline(query: str, limit: int = 10, 
                   near_lat: float = None, near_lon: float = None,
                   data_dir: str = None) -> List[Dict]:
    """
    Search for locations offline.
    
    Args:
        query: Search string
        limit: Maximum results
        near_lat, near_lon: Optional location for proximity sorting
        data_dir: Optional data directory
        
    Returns:
        List of location results in API-compatible format
    """
    geocoder = get_geocoder(data_dir)
    if not geocoder.is_available():
        return []
    
    try:
        return geocoder.search(query, limit, near_lat, near_lon)
    except Exception as e:
        print(f"[GEOCODER] Offline search error: {e}")
        return []


def reverse_geocode_offline(lat: float, lon: float, 
                            radius_km: float = 0.5,
                            data_dir: str = None) -> List[Dict]:
    """
    Reverse geocode offline.
    
    Args:
        lat, lon: Coordinates
        radius_km: Search radius
        data_dir: Optional data directory
        
    Returns:
        List of nearby locations
    """
    geocoder = get_geocoder(data_dir)
    if not geocoder.is_available():
        return []
    
    try:
        return geocoder.reverse(lat, lon, radius_km)
    except Exception as e:
        print(f"[GEOCODER] Reverse geocode error: {e}")
        return []


# =============================================================================
# Test
# =============================================================================

if __name__ == '__main__':
    import sys
    
    # Test with local data
    # Expected structure: {data_dir}/geocoder-nlp/{region}/geonlp-primary.sqlite
    # Or for direct testing: {data_dir}/{region}/geonlp-primary.sqlite
    data_dir = sys.argv[1] if len(sys.argv) > 1 else '/home/user/MyDocs/valhalla'
    
    geocoder = OfflineGeocoder(data_dir)
    
    # For testing, allow direct path with region subfolder
    if not geocoder.is_available() and len(sys.argv) > 1:
        # Try treating arg as direct geocoder-nlp dir
        geocoder.geocoder_dir = data_dir
        geocoder._regions = None
        geocoder._available = None
    
    if not geocoder.is_available():
        print(f"No geocoder databases found in: {geocoder.geocoder_dir}")
        print("Expected structure: {dir}/{region}/geonlp-primary.sqlite")
        sys.exit(1)
    
    print("=== Offline Geocoder Test ===")
    print(f"Geocoder dir: {geocoder.geocoder_dir}")
    print(f"Available regions: {geocoder.get_available_regions()}")
    
    # Test search
    print("\n--- Search Test ---")
    queries = ['Wien', 'hotel', 'airport', 'Hauptbahnhof']
    for q in queries:
        results = geocoder.search(q, limit=3)
        print(f"\n'{q}':")
        for r in results:
            region = r.get('region', '?')
            print(f"  {r['name']} ({r['type']}) @ {r['lat']:.4f}, {r['lng']:.4f} [{region}]")
    
    # Test reverse geocoding
    print("\n--- Reverse Geocode Test ---")
    lat, lon = 48.2082, 16.3738  # Vienna center
    results = geocoder.reverse(lat, lon, radius_km=1.0)
    print(f"\nNear {lat}, {lon}:")
    for r in results:
        region = r.get('region', '?')
        print(f"  {r['name']} ({r['type']}) - {r.get('distance_m', '?')}m [{region}]")
    
    geocoder.close()
    print("\nDone!")
