#!/opt/wunderw/bin/python3.11
# -*- coding: utf-8 -*-
"""
Bicycle Routing API Client
Supports multiple backends: Valhalla, OpenRouteService, OSRM
Version 1.1.0
"""

import sys
import os

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

import json
import urllib.request
import urllib.parse
import ssl
from datetime import datetime

# SSL context
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

# =============================================================================
# ROUTING BACKENDS
# =============================================================================

ROUTING_BACKENDS = {
    'valhalla': {
        'id': 'valhalla',
        'name': 'Valhalla (FOSSGIS)',
        'url': 'https://valhalla1.openstreetmap.de',
        'description': 'Fast routing, good bike support'
    },
    'ors': {
        'id': 'ors',
        'name': 'OpenRouteService',
        'url': 'https://api.openrouteservice.org',
        'description': 'Detailed routing with elevation'
    },
    'osrm': {
        'id': 'osrm',
        'name': 'OSRM',
        'url': 'https://routing.openstreetmap.de/routed-bike',
        'description': 'Simple and fast bike routing'
    },
    'local': {
        'id': 'local',
        'name': 'OSMScout (Offline)',
        'url': 'http://127.0.0.1:8553',
        'description': 'Offline routing via OSMScout Server'
    }
}

# Bicycle types with their characteristics
BICYCLE_TYPES = {
    'Mountain': {
        'id': 'Mountain',
        'name': 'Mountain Bike',
        'default_speed': 20.0,
        'description': 'For terrain, trails and unpaved roads',
        'ors_profile': 'cycling-mountain',
        'valhalla_type': 'Mountain'
    },
    'Road': {
        'id': 'Road',
        'name': 'Road Bike',
        'default_speed': 25.0,
        'description': 'For paved roads',
        'ors_profile': 'cycling-road',
        'valhalla_type': 'Road'
    },
    'Hybrid': {
        'id': 'Hybrid',
        'name': 'City/Hybrid',
        'default_speed': 18.0,
        'description': 'For city and paved paths',
        'ors_profile': 'cycling-regular',
        'valhalla_type': 'Hybrid'
    },
    'Cross': {
        'id': 'Cross',
        'name': 'Cyclocross',
        'default_speed': 22.0,
        'description': 'For mixed terrain',
        'ors_profile': 'cycling-regular',
        'valhalla_type': 'Cross'
    }
}


def log(msg):
    try:
        print("[ROUTING] " + str(msg), file=sys.stderr)
    except:
        pass


# =============================================================================
# POLYLINE DECODING
# =============================================================================

def decode_polyline(encoded, precision=6):
    """Decode encoded polyline to list of coordinates."""
    if not encoded:
        return []
    
    inv = 1.0 / (10 ** precision)
    decoded = []
    previous = [0, 0]
    i = 0
    
    while i < len(encoded):
        ll = [0, 0]
        for j in [0, 1]:
            shift = 0
            byte = 0x20
            
            while byte >= 0x20:
                if i >= len(encoded):
                    break
                byte = ord(encoded[i]) - 63
                i += 1
                ll[j] |= (byte & 0x1f) << shift
                shift += 5
            
            ll[j] = previous[j] + (~(ll[j] >> 1) if ll[j] & 1 else (ll[j] >> 1))
            previous[j] = ll[j]
        
        decoded.append({
            'latitude': round(ll[0] * inv, 6),
            'longitude': round(ll[1] * inv, 6)
        })
    
    return decoded


# =============================================================================
# GEOCODING (Photon)
# =============================================================================

def search_location(query):
    """Search for location using Photon (Komoot's geocoder)."""
    try:
        base_url = "https://photon.komoot.io/api/"
        params = {
            'q': query,
            'limit': 10,
            'lang': 'de'
        }
        
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'BikeRouter/1.1 MeeGo')
        req.add_header('Accept', 'application/json')
        
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=15) as response:
            data = response.read().decode('utf-8')
            result = json.loads(data)
        
        locations = []
        features = result.get('features', [])
        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry', {})
            coords = geom.get('coordinates', [0, 0])
            
            # Build display name
            name_parts = []
            if props.get('name'):
                name_parts.append(props['name'])
            if props.get('street'):
                street = props['street']
                if props.get('housenumber'):
                    street += ' ' + props['housenumber']
                name_parts.append(street)
            if props.get('city'):
                name_parts.append(props['city'])
            elif props.get('town'):
                name_parts.append(props['town'])
            elif props.get('village'):
                name_parts.append(props['village'])
            if props.get('country'):
                name_parts.append(props['country'])
            
            display_name = ', '.join(name_parts) if name_parts else 'Unknown'
            
            locations.append({
                'name': display_name,
                'lat': coords[1],
                'lng': coords[0],
                'type': props.get('osm_value', ''),
            })
        
        return {'success': True, 'locations': locations}
        
    except Exception as e:
        log(f"Location search error: {e}")
        return {'success': False, 'error': str(e)}


# =============================================================================
# VALHALLA ROUTING
# =============================================================================

def route_valhalla(from_lat, from_lng, to_lat, to_lng, bicycle_type='Mountain', 
                   use_roads=0.5, use_hills=0.5):
    """Route using Valhalla API."""
    try:
        bike_info = BICYCLE_TYPES.get(bicycle_type, BICYCLE_TYPES['Mountain'])
        
        params = {
            'locations': [
                {'lat': float(from_lat), 'lon': float(from_lng)},
                {'lat': float(to_lat), 'lon': float(to_lng)}
            ],
            'costing': 'bicycle',
            'costing_options': {
                'bicycle': {
                    'bicycle_type': bike_info['valhalla_type'],
                    'use_roads': float(use_roads),
                    'use_hills': float(use_hills),
                    'cycling_speed': bike_info['default_speed']
                }
            },
            'directions_options': {
                'units': 'kilometers',
                'language': 'de-DE'
            }
        }
        
        json_params = json.dumps(params, ensure_ascii=False)
        url = f"{ROUTING_BACKENDS['valhalla']['url']}/route?json={urllib.parse.quote(json_params)}"
        
        log(f"Valhalla request: {bicycle_type}")
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'BikeRouter/1.1 MeeGo')
        req.add_header('Accept', 'application/json')
        
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as response:
            data = response.read().decode('utf-8')
            result = json.loads(data)
        
        if 'error' in result:
            return {'success': False, 'error': result.get('error', 'Unknown error')}
        
        trip = result.get('trip', {})
        legs = trip.get('legs', [])
        summary = trip.get('summary', {})
        
        if not legs:
            return {'success': False, 'error': 'No route found'}
        
        # Extract polyline
        all_points = []
        for leg in legs:
            shape = leg.get('shape', '')
            points = decode_polyline(shape, precision=6)
            all_points.extend(points)
        
        return {
            'success': True,
            'backend': 'valhalla',
            'backend_name': 'Valhalla',
            'bicycle_type': bicycle_type,
            'bicycle_name': bike_info['name'],
            'distance': round(summary.get('length', 0), 2),
            'distance_text': f"{summary.get('length', 0):.1f} km",
            'duration': int(summary.get('time', 0)),
            'duration_text': format_duration(summary.get('time', 0)),
            'polyline': all_points,
            'start': {'lat': float(from_lat), 'lng': float(from_lng)},
            'end': {'lat': float(to_lat), 'lng': float(to_lng)}
        }
        
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', 'replace')
        log(f"Valhalla HTTP error: {e.code} - {error_body[:200]}")
        return {'success': False, 'error': f'HTTP {e.code}'}
    except Exception as e:
        log(f"Valhalla error: {e}")
        return {'success': False, 'error': str(e)}


# =============================================================================
# OPENROUTESERVICE ROUTING
# =============================================================================

def route_ors(from_lat, from_lng, to_lat, to_lng, bicycle_type='Mountain'):
    """Route using OpenRouteService API (no API key needed for limited use)."""
    try:
        bike_info = BICYCLE_TYPES.get(bicycle_type, BICYCLE_TYPES['Mountain'])
        profile = bike_info.get('ors_profile', 'cycling-regular')
        
        # ORS uses lon,lat order!
        url = f"https://api.openrouteservice.org/v2/directions/{profile}?start={from_lng},{from_lat}&end={to_lng},{to_lat}"
        
        log(f"ORS request: {profile}")
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'BikeRouter/1.1 MeeGo')
        req.add_header('Accept', 'application/json, application/geo+json')
        
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as response:
            data = response.read().decode('utf-8')
            result = json.loads(data)
        
        if 'error' in result:
            return {'success': False, 'error': result.get('error', {}).get('message', 'Unknown error')}
        
        features = result.get('features', [])
        if not features:
            return {'success': False, 'error': 'No route found'}
        
        feature = features[0]
        props = feature.get('properties', {})
        segments = props.get('segments', [{}])
        segment = segments[0] if segments else {}
        
        geometry = feature.get('geometry', {})
        coordinates = geometry.get('coordinates', [])
        
        # Convert coordinates to our format
        all_points = []
        for coord in coordinates:
            all_points.append({
                'latitude': round(coord[1], 6),
                'longitude': round(coord[0], 6)
            })
        
        distance_km = segment.get('distance', 0) / 1000.0
        duration_sec = segment.get('duration', 0)
        
        return {
            'success': True,
            'backend': 'ors',
            'backend_name': 'OpenRouteService',
            'bicycle_type': bicycle_type,
            'bicycle_name': bike_info['name'],
            'distance': round(distance_km, 2),
            'distance_text': f"{distance_km:.1f} km",
            'duration': int(duration_sec),
            'duration_text': format_duration(duration_sec),
            'polyline': all_points,
            'start': {'lat': float(from_lat), 'lng': float(from_lng)},
            'end': {'lat': float(to_lat), 'lng': float(to_lng)}
        }
        
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', 'replace')
        log(f"ORS HTTP error: {e.code} - {error_body[:200]}")
        try:
            err = json.loads(error_body)
            return {'success': False, 'error': err.get('error', {}).get('message', f'HTTP {e.code}')}
        except:
            return {'success': False, 'error': f'HTTP {e.code}'}
    except Exception as e:
        log(f"ORS error: {e}")
        return {'success': False, 'error': str(e)}


# =============================================================================
# OSRM ROUTING
# =============================================================================

def route_osrm(from_lat, from_lng, to_lat, to_lng, bicycle_type='Mountain'):
    """Route using OSRM API."""
    try:
        bike_info = BICYCLE_TYPES.get(bicycle_type, BICYCLE_TYPES['Mountain'])
        
        # OSRM uses lon,lat order!
        coords = f"{from_lng},{from_lat};{to_lng},{to_lat}"
        
        url = f"{ROUTING_BACKENDS['osrm']['url']}/route/v1/bike/{coords}?overview=full&geometries=polyline6"
        
        log(f"OSRM request")
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'BikeRouter/1.1 MeeGo')
        req.add_header('Accept', 'application/json')
        
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as response:
            data = response.read().decode('utf-8')
            result = json.loads(data)
        
        if result.get('code') != 'Ok':
            return {'success': False, 'error': result.get('message', 'Routing failed')}
        
        routes = result.get('routes', [])
        if not routes:
            return {'success': False, 'error': 'No route found'}
        
        route = routes[0]
        geometry = route.get('geometry', '')
        
        # Decode polyline (OSRM uses precision 6)
        all_points = decode_polyline(geometry, precision=6)
        
        distance_km = route.get('distance', 0) / 1000.0
        duration_sec = route.get('duration', 0)
        
        return {
            'success': True,
            'backend': 'osrm',
            'backend_name': 'OSRM',
            'bicycle_type': bicycle_type,
            'bicycle_name': bike_info['name'],
            'distance': round(distance_km, 2),
            'distance_text': f"{distance_km:.1f} km",
            'duration': int(duration_sec),
            'duration_text': format_duration(duration_sec),
            'polyline': all_points,
            'start': {'lat': float(from_lat), 'lng': float(from_lng)},
            'end': {'lat': float(to_lat), 'lng': float(to_lng)}
        }
        
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8', 'replace')
        log(f"OSRM HTTP error: {e.code} - {error_body[:200]}")
        return {'success': False, 'error': f'HTTP {e.code}'}
    except Exception as e:
        log(f"OSRM error: {e}")
        return {'success': False, 'error': str(e)}


# =============================================================================
# LOCAL ROUTING (Valhalla-Compatible Local Engine)
# =============================================================================

LOCAL_SERVER_PID = None
TILES_DIR = "/home/user/MyDocs/Maps.OSM/valhalla/tiles"

def check_local_server():
    """Check if local routing server is running"""
    try:
        url = ROUTING_BACKENDS['local']['url'] + "/status"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as response:
            return response.status == 200
    except:
        return False


def start_local_server():
    """Start the local Valhalla-compatible routing engine"""
    global LOCAL_SERVER_PID
    
    import subprocess
    
    # Check if tiles exist
    if not os.path.isdir(TILES_DIR):
        log(f"Tiles directory not found: {TILES_DIR}")
        return False
    
    # Check for level 2 tiles (most important for bike routing)
    level2_dir = os.path.join(TILES_DIR, "2")
    if not os.path.isdir(level2_dir):
        log("No level 2 tiles found - copy tiles from OSMScout Server first!")
        return False
    
    # Start the engine
    engine_path = "/opt/valhalla-bike-router/valhalla_local_engine.py"
    
    if not os.path.exists(engine_path):
        log(f"Engine not found: {engine_path}")
        return False
    
    try:
        # Use python from PATH or wunderw
        python_cmd = "/opt/wunderw/bin/python3.11"
        if not os.path.exists(python_cmd):
            python_cmd = "python"
        
        log(f"Starting local routing engine...")
        
        # Log file for server output
        log_file = "/home/user/MyDocs/Maps.OSM/valhalla/server.log"
        log_dir = os.path.dirname(log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Truncate log file at start
        log_fd = open(log_file, 'w')
        import datetime
        log_fd.write("=== Server starting at %s ===\n" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_fd.flush()
        
        proc = subprocess.Popen(
            [python_cmd, engine_path, "--tiles", TILES_DIR],
            stdout=log_fd,
            stderr=log_fd,
            start_new_session=True
        )
        
        LOCAL_SERVER_PID = proc.pid
        log(f"Local engine started (PID {proc.pid})")
        
        # Wait for server to be ready
        import time
        for i in range(10):
            time.sleep(0.5)
            if check_local_server():
                log("Local engine ready!")
                return True
        
        log("Local engine started but not responding")
        return True  # Still try to use it
        
    except Exception as e:
        log(f"Failed to start local engine: {e}")
        return False


def ensure_local_server():
    """Ensure local routing server is running"""
    if check_local_server():
        return True
    
    log("Local server not running, starting...")
    return start_local_server()


def route_local(from_lat, from_lng, to_lat, to_lng, bicycle_type='Mountain', avoid_cars=False):
    """Route using local Valhalla-compatible engine."""
    
    # Auto-start server if needed
    if not ensure_local_server():
        # Check why it failed
        if not os.path.isdir(TILES_DIR):
            return {
                'success': False, 
                'error': 'No map tiles found. Copy valhalla/tiles from OSMScout Server to MyDocs/Maps.OSM/'
            }
        return {'success': False, 'error': 'Could not start local routing engine'}
    
    try:
        bike_info = BICYCLE_TYPES.get(bicycle_type, BICYCLE_TYPES['Mountain'])
        
        # Build Valhalla-style request for OSMScout Server
        params = {
            'locations': [
                {'lat': float(from_lat), 'lon': float(from_lng)},
                {'lat': float(to_lat), 'lon': float(to_lng)}
            ],
            'costing': 'bicycle',
            'costing_options': {
                'bicycle': {
                    'bicycle_type': bike_info.get('valhalla_type', 'Hybrid'),
                    'avoid_cars': avoid_cars
                }
            },
            'directions_options': {
                'units': 'kilometers',
                'language': 'de-DE'
            }
        }
        
        url = f"{ROUTING_BACKENDS['local']['url']}/v2/route"
        
        log(f"OSMScout local routing: {bicycle_type}, avoid_cars={avoid_cars}")
        
        req = urllib.request.Request(
            url,
            data=json.dumps(params).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )
        
        # Local server - no SSL, longer timeout for first request (tiles may need parsing)
        with urllib.request.urlopen(req, timeout=300) as response:
            data = response.read().decode('utf-8')
            result = json.loads(data)
        
        if 'error' in result:
            return {'success': False, 'error': result.get('error', 'Routing failed')}
        
        trip = result.get('trip', {})
        legs = trip.get('legs', [])
        summary = trip.get('summary', {})
        
        # Log debug info if present
        debug = summary.get('debug', {})
        level_trans = summary.get('level_transitions', 0)
        level_usage = summary.get('level_usage', {})
        if debug or level_trans or level_usage:
            log(f"Route debug: trans={level_trans}, usage={level_usage}, debug={debug}")
        
        if not legs:
            return {'success': False, 'error': 'No route found'}
        
        # Extract polyline
        all_points = []
        for leg in legs:
            shape = leg.get('shape', '')
            if shape:
                points = decode_polyline(shape, precision=6)
                all_points.extend(points)
        
        # Valhalla returns length in km
        distance_km = summary.get('length', 0)
        car_distance_km = summary.get('car_distance', 0)
        cycleway_distance_km = summary.get('cycleway_distance', 0)
        
        return {
            'success': True,
            'backend': 'local',
            'backend_name': 'Lokal (Offline)',
            'bicycle_type': bicycle_type,
            'bicycle_name': bike_info['name'],
            'distance': round(distance_km, 2),
            'distance_text': f"{distance_km:.1f} km",
            'duration': int(summary.get('time', 0)),
            'duration_text': format_duration(summary.get('time', 0)),
            'car_distance': round(car_distance_km, 2),
            'car_distance_text': f"{car_distance_km:.1f} km with cars",
            'cycleway_distance': round(cycleway_distance_km, 2),
            'cycleway_distance_text': f"{cycleway_distance_km:.1f} km car-free",
            'polyline': all_points,
            'start': {'lat': float(from_lat), 'lng': float(from_lng)},
            'end': {'lat': float(to_lat), 'lng': float(to_lng)}
        }
        
    except urllib.error.URLError as e:
        log(f"Local server error: {e}")
        # Try to provide helpful error
        if not os.path.isdir(TILES_DIR):
            return {'success': False, 'error': 'No map tiles. Copy valhalla/tiles folder from PC.'}
        return {'success': False, 'error': 'Local routing server error. Try again.'}
    except Exception as e:
        log(f"Local routing error: {e}")
        return {'success': False, 'error': str(e)}


# =============================================================================
# MAIN ROUTING FUNCTION
# =============================================================================

def search_route(from_lat, from_lng, to_lat, to_lng, bicycle_type='Mountain',
                 use_roads=0.5, use_hills=0.5, backend='valhalla', avoid_cars=False):
    """Search for bicycle route using specified backend."""
    log(f"Routing with {backend}: ({from_lat},{from_lng}) -> ({to_lat},{to_lng}), avoid_cars={avoid_cars}")
    
    if backend == 'valhalla':
        return route_valhalla(from_lat, from_lng, to_lat, to_lng, bicycle_type, use_roads, use_hills)
    elif backend == 'ors':
        return route_ors(from_lat, from_lng, to_lat, to_lng, bicycle_type)
    elif backend == 'osrm':
        return route_osrm(from_lat, from_lng, to_lat, to_lng, bicycle_type)
    elif backend == 'local':
        return route_local(from_lat, from_lng, to_lat, to_lng, bicycle_type, avoid_cars)
    else:
        return {'success': False, 'error': f'Unknown backend: {backend}'}


def format_duration(seconds):
    """Format duration in seconds to human-readable string."""
    if seconds < 0:
        return ""
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    
    if hours > 0:
        return f"{hours}h {minutes}min"
    return f"{minutes} min"


def get_bicycle_types():
    """Return available bicycle types."""
    return {
        'success': True,
        'types': [
            {
                'id': bt['id'],
                'name': bt['name'],
                'description': bt['description'],
                'default_speed': bt['default_speed']
            }
            for bt in BICYCLE_TYPES.values()
        ]
    }


def get_routing_backends():
    """Return available routing backends."""
    return {
        'success': True,
        'backends': [
            {
                'id': b['id'],
                'name': b['name'],
                'description': b['description']
            }
            for b in ROUTING_BACKENDS.values()
        ]
    }


# =============================================================================
# TILE DOWNLOAD FUNCTIONS
# =============================================================================

def _load_countries_json():
    """Load countries_provided.json from local file or download"""
    import os
    
    # Try local file first (bundled with app)
    # Check multiple locations
    script_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        "/opt/valhalla-bike-router/countries_provided.json",
        os.path.join(script_dir, "countries_provided.json"),
    ]
    
    for local_path in possible_paths:
        if os.path.exists(local_path):
            try:
                with open(local_path, 'r') as f:
                    return json.load(f)
            except:
                pass
    
    # Try to download
    COUNTRIES_JSON_URL = "https://data.modrana.org/osm_scout_server/countries_provided.json"
    try:
        req = urllib.request.Request(COUNTRIES_JSON_URL)
        req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.loads(response.read().decode('utf-8'))
    except:
        return None


# Nice display names for continents
CONTINENT_NAMES = {
    'africa': 'Africa',
    'asia': 'Asia',
    'australia-oceania': 'Australia & Oceania',
    'central-america': 'Central America',
    'europe': 'Europe',
    'north-america': 'North America',
    'russia': 'Russia',
    'south-america': 'South America'
}


def get_installed_regions():
    """Get list of installed regions from local server"""
    if not ensure_local_server():
        return []
    
    try:
        url = ROUTING_BACKENDS['local']['url'] + "/installed"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data.get('installed', [])
    except:
        return []


def browse_regions(parent_path=""):
    """Browse regions hierarchically. Returns children of the given path."""
    data = _load_countries_json()
    if not data:
        return {'success': False, 'items': [], 'error': 'Could not load country data'}
    
    # Get installed regions from local server
    installed_regions = get_installed_regions()
    
    children = {}
    parent_depth = len(parent_path.split('/')) if parent_path else 0
    
    for country_id, country_data in data.items():
        if not isinstance(country_data, dict):
            continue
        if 'valhalla' not in country_data:
            continue
        valhalla = country_data.get('valhalla')
        if not isinstance(valhalla, dict):
            continue
        packages = valhalla.get('packages', [])
        if not packages:
            continue
        
        # Check if this is a child of parent
        if parent_path:
            if not country_id.startswith(parent_path + '/'):
                continue
        
        parts = country_id.split('/')
        
        # Get the immediate child level
        child_depth = parent_depth + 1
        if len(parts) < child_depth:
            continue
        
        # Get the child path (parent + one level)
        child_path = '/'.join(parts[:child_depth])
        
        if child_path not in children:
            # Get display name
            if child_depth == 1:
                # Continent level
                display_name = CONTINENT_NAMES.get(child_path, child_path.replace('-', ' ').title())
            else:
                # Country/region level - use name from JSON or format nicely
                display_name = child_path.split('/')[-1].replace('-', ' ').title()
            
            children[child_path] = {
                'id': child_path,
                'name': display_name,
                'has_subregions': False,
                'is_downloadable': False,
                'is_installed': False,
                'size_mb': 0,
                'package_count': 0
            }
        
        # Check if this exact path is downloadable
        if country_id == child_path:
            children[child_path]['is_downloadable'] = True
            children[child_path]['is_installed'] = child_path in installed_regions
            children[child_path]['size_mb'] = int(valhalla.get('size-compressed', 0)) // (1024*1024)
            children[child_path]['package_count'] = len(packages)
            # Try to get better name from JSON
            json_name = country_data.get('name', '')
            if json_name and '/' in json_name:
                children[child_path]['name'] = json_name.split('/')[-1]
            elif json_name:
                children[child_path]['name'] = json_name
        
        # Check if there are deeper levels (subregions)
        if len(parts) > child_depth:
            children[child_path]['has_subregions'] = True
    
    # Convert to sorted list
    items = sorted(children.values(), key=lambda x: x['name'])
    
    # Calculate breadcrumb path
    breadcrumb = []
    if parent_path:
        parts = parent_path.split('/')
        for i, part in enumerate(parts):
            path = '/'.join(parts[:i+1])
            if i == 0:
                name = CONTINENT_NAMES.get(part, part.replace('-', ' ').title())
            else:
                name = part.replace('-', ' ').title()
            breadcrumb.append({'path': path, 'name': name})
    
    return {
        'success': True,
        'items': items,
        'parent': parent_path,
        'breadcrumb': breadcrumb,
        'count': len(items),
        'installed_count': len([i for i in items if i.get('is_installed')])
    }


def get_available_regions(continent=None):
    """Legacy function - redirects to browse_regions"""
    return browse_regions(continent or "")


def get_installed_tiles():
    """Get list of installed tiles"""
    tiles = []
    tiles_dir = TILES_DIR
    
    if not os.path.isdir(tiles_dir):
        return {'success': True, 'tiles': [], 'count': 0, 'tiles_dir': tiles_dir}
    
    for level in ['0', '1', '2']:
        level_dir = os.path.join(tiles_dir, level)
        if not os.path.isdir(level_dir):
            continue
        
        # 3-level structure: level/xxx/yyy/zzz.gph.gz
        for d1 in os.listdir(level_dir):
            d1_path = os.path.join(level_dir, d1)
            if not os.path.isdir(d1_path):
                continue
            
            for d2 in os.listdir(d1_path):
                d2_path = os.path.join(d1_path, d2)
                if not os.path.isdir(d2_path):
                    continue
                
                for f in os.listdir(d2_path):
                    if f.endswith('.gph.gz') or f.endswith('.gph'):
                        try:
                            tile_id = int(d1) * 1000000 + int(d2) * 1000 + int(f.split('.')[0])
                            tiles.append({'level': int(level), 'id': tile_id})
                        except:
                            pass
    
    return {'success': True, 'tiles': tiles, 'count': len(tiles), 'tiles_dir': tiles_dir}


# Downloaded regions tracking
DOWNLOADED_REGIONS_FILE = os.path.join(TILES_DIR, ".downloaded_regions.json")

def get_downloaded_regions():
    """Get list of downloaded region IDs"""
    try:
        if os.path.exists(DOWNLOADED_REGIONS_FILE):
            with open(DOWNLOADED_REGIONS_FILE, 'r') as f:
                data = json.load(f)
                return data.get('regions', [])
    except:
        pass
    return []

def mark_region_downloaded(region_id):
    """Mark a region as downloaded"""
    regions = get_downloaded_regions()
    if region_id not in regions:
        regions.append(region_id)
    
    try:
        os.makedirs(os.path.dirname(DOWNLOADED_REGIONS_FILE), exist_ok=True)
        with open(DOWNLOADED_REGIONS_FILE, 'w') as f:
            json.dump({'regions': regions}, f)
    except Exception as e:
        print(f"Error saving downloaded regions: {e}")


def start_download(region_id):
    """Start downloading a region via local server"""
    print("[API] start_download called with region_id: %s" % region_id, file=sys.stderr)
    
    if not ensure_local_server():
        print("[API] Could not start local server", file=sys.stderr)
        return {'success': False, 'error': 'Could not start local server'}
    
    try:
        url = ROUTING_BACKENDS['local']['url'] + "/download/" + region_id
        print("[API] Calling URL: %s" % url, file=sys.stderr)
        req = urllib.request.Request(url)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print("[API] Server response: %s" % data, file=sys.stderr)
            return {'success': True, 'status': data.get('status'), 'region': region_id}
    except Exception as e:
        print("[API] Error: %s" % e, file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return {'success': False, 'error': str(e)}


def get_download_status():
    """Get current download status"""
    if not check_local_server():
        return {'success': True, 'downloads': {}, 'server_running': False}
    
    try:
        url = ROUTING_BACKENDS['local']['url'] + "/download_status"
        req = urllib.request.Request(url)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            return {'success': True, 'downloads': data.get('downloads', {}), 'server_running': True}
    except Exception as e:
        return {'success': True, 'downloads': {}, 'server_running': False, 'error': str(e)}


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    try:
        print("[API CLI] Called with args: %s" % sys.argv, file=sys.stderr)
        
        if len(sys.argv) < 2:
            print(json.dumps({
                "error": "Usage: valhalla_api.py <command> [args...]",
                "commands": [
                    "search_location <query>",
                    "search_route <from_lat> <from_lng> <to_lat> <to_lng> [bicycle_type] [use_roads] [use_hills] [backend]",
                    "bicycle_types",
                    "backends",
                    "regions - List available regions for download",
                    "tiles - List installed tiles",
                    "download <region_id> - Start download",
                    "download_status - Check download progress"
                ]
            }))
            sys.exit(1)
        
        cmd = sys.argv[1]
        
        if cmd == "search_location":
            if len(sys.argv) < 3:
                print(json.dumps({"error": "Query required"}))
                sys.exit(1)
            
            query = sys.argv[2]
            result = search_location(query)
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "search_route":
            if len(sys.argv) < 6:
                print(json.dumps({"error": "from_lat, from_lng, to_lat, to_lng required"}))
                sys.exit(1)
            
            from_lat = float(sys.argv[2])
            from_lng = float(sys.argv[3])
            to_lat = float(sys.argv[4])
            to_lng = float(sys.argv[5])
            
            bicycle_type = sys.argv[6] if len(sys.argv) > 6 else 'Mountain'
            use_roads = float(sys.argv[7]) if len(sys.argv) > 7 else 0.5
            use_hills = float(sys.argv[8]) if len(sys.argv) > 8 else 0.5
            backend = sys.argv[9] if len(sys.argv) > 9 else 'valhalla'
            avoid_cars = sys.argv[10].lower() == 'true' if len(sys.argv) > 10 else False
            
            result = search_route(from_lat, from_lng, to_lat, to_lng, 
                                bicycle_type, use_roads, use_hills, backend, avoid_cars)
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "bicycle_types":
            result = get_bicycle_types()
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "backends":
            result = get_routing_backends()
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "browse":
            # Browse regions hierarchically
            parent = sys.argv[2] if len(sys.argv) > 2 else ""
            result = browse_regions(parent)
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "continents":
            # Legacy - same as browse("")
            result = browse_regions("")
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "regions":
            # Legacy - same as browse(continent)
            continent = sys.argv[2] if len(sys.argv) > 2 else ""
            result = browse_regions(continent)
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "tiles":
            result = get_installed_tiles()
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "download":
            print("[API CLI] download command received", file=sys.stderr)
            if len(sys.argv) < 3:
                print(json.dumps({"error": "Region ID required"}))
                sys.exit(1)
            region_id = sys.argv[2]
            print("[API CLI] Downloading region: %s" % region_id, file=sys.stderr)
            result = start_download(region_id)
            print("[API CLI] Download result: %s" % result, file=sys.stderr)
            print(json.dumps(result, ensure_ascii=False))
        
        elif cmd == "download_status":
            result = get_download_status()
            print(json.dumps(result, ensure_ascii=False))
        
        else:
            print(json.dumps({"error": f"Unknown command: {cmd}"}))
            sys.exit(1)
    
    except Exception as e:
        import traceback
        print(json.dumps({
            "success": False, 
            "error": str(e), 
            "traceback": traceback.format_exc()
        }))
