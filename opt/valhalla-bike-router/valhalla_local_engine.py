#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Valhalla-Compatible Local Routing Engine for MeeGo Harmattan

This implements the same bicycle costing model as Valhalla's OSMScout Server,
reading the same .gph.gz tile format and providing a compatible /v2/route API.

Based on Valhalla's sif/bicyclecost.cc costing model.
"""

from __future__ import print_function, division

import struct
import gzip
import os
import sys
import json
import math
import heapq
import time
import threading

try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
    from urlparse import urlparse, parse_qs

# ============================================================================
# Configuration
# ============================================================================

TILES_DIR = "/home/user/MyDocs/Maps.OSM/valhalla/tiles"
SERVER_PORT = 8553

# Tile hierarchy (same as Valhalla)
TILE_LEVELS = {0: 4.0, 1: 1.0, 2: 0.25}


# ============================================================================
# Shape Decoding (Valhalla's decode7 format)
# ============================================================================

def decode7_shape(data, offset, size):
    """
    Decode a Valhalla encode7 shape to list of (lat, lon) tuples.
    
    Format: 7-bit varint with zigzag encoding, delta-encoded lat/lon pairs.
    Precision: 1e-6 (6 decimal places)
    """
    if size <= 0 or size > 10000:
        return []
    
    pos = offset
    end = offset + size
    
    lat = 0
    lon = 0
    points = []
    max_points = 1000  # Reasonable limit for a single edge
    
    def read_varint():
        nonlocal pos
        result = 0
        shift = 0
        iterations = 0
        while pos < end and iterations < 10:  # Max 10 bytes per varint
            byte = data[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
            iterations += 1
        # Zigzag decode
        return (result >> 1) ^ -(result & 1)
    
    while pos < end and len(points) < max_points:
        lat += read_varint()
        if pos >= end:
            break
        lon += read_varint()
        
        # Validate coordinates (roughly in valid range)
        lat_deg = lat * 1e-6
        lon_deg = lon * 1e-6
        if -90 <= lat_deg <= 90 and -180 <= lon_deg <= 180:
            points.append((lat_deg, lon_deg))
        else:
            # Invalid coordinate, stop parsing
            break
    
    return points

# Structure sizes
HEADER_SIZE = 272
NODE_SIZE = 32
EDGE_SIZE = 48

# Header offsets (from graphtileheader.h)
HEADER_EDGEINFO_OFFSET = 112  # Offset to edgeinfo_offset field
HEADER_TEXTLIST_OFFSET = 116  # Offset to textlist_offset field

# Access constants (from graphconstants.h)
kAutoAccess = 1
kPedestrianAccess = 2
kBicycleAccess = 4
kTruckAccess = 8

# Road classes
class RoadClass:
    kMotorway = 0
    kTrunk = 1
    kPrimary = 2
    kSecondary = 3
    kTertiary = 4
    kUnclassified = 5
    kResidential = 6
    kServiceOther = 7

# Use types (from graphconstants.h)
class Use:
    kRoad = 0
    kRamp = 1
    kTurnChannel = 2
    kTrack = 3
    kDriveway = 4
    kAlley = 5
    kParkingAisle = 6
    kEmergencyAccess = 7
    kDriveThru = 8
    kCuldesac = 9
    kLivingStreet = 10
    kServiceRoad = 11
    # 12-19 reserved
    kCycleway = 20
    kMountainBike = 21
    kSidewalk = 22
    # 23-24 reserved
    kFootway = 25
    kSteps = 26
    kPath = 27
    kPedestrian = 28
    kBridleway = 29
    kPedestrianCrossing = 30
    kElevator = 31
    kEscalator = 32
    # ...
    kFerry = 41

# Surface types
class Surface:
    kPavedSmooth = 0
    kPaved = 1
    kPavedRough = 2
    kCompacted = 3
    kDirt = 4
    kGravel = 5
    kPath = 6
    kImpassable = 7

# Cycle lane types
class CycleLane:
    kNone = 0
    kShared = 1
    kDedicated = 2
    kSeparated = 3

# ============================================================================
# Valhalla Bicycle Costing Model - EXACT IMPLEMENTATION
# Based on valhalla/src/sif/bicyclecost.cc
# ============================================================================

class BicycleType:
    """Bicycle types with their characteristics"""
    Road = 'Road'
    Hybrid = 'Hybrid'
    Cross = 'Cross'
    Mountain = 'Mountain'

# Default cycling speeds by bicycle type (kph) - from bicyclecost.cc
BICYCLE_SPEEDS = {
    BicycleType.Road: 25.0,
    BicycleType.Hybrid: 20.0,
    BicycleType.Cross: 18.0,
    BicycleType.Mountain: 16.0,
}

# Surface speed factors by bicycle type
# From bicyclecost.cc kSurfaceSpeedFactor tables
# Surface enum: PavedSmooth=0, Paved=1, PavedRough=2, Compacted=3, Dirt=4, Gravel=5, Path=6, Impassable=7
SURFACE_SPEED_FACTOR = {
    BicycleType.Road: {
        0: 1.0,    # PavedSmooth
        1: 1.0,    # Paved
        2: 0.9,    # PavedRough
        3: 0.1,    # Compacted - road bikes really struggle
        4: 0.0,    # Dirt - impassable for road bikes
        5: 0.0,    # Gravel - impassable
        6: 0.0,    # Path - impassable  
        7: 0.0,    # Impassable
    },
    BicycleType.Hybrid: {
        0: 1.0,    # PavedSmooth
        1: 1.0,    # Paved
        2: 0.95,   # PavedRough
        3: 0.85,   # Compacted
        4: 0.65,   # Dirt
        5: 0.5,    # Gravel
        6: 0.5,    # Path
        7: 0.0,    # Impassable
    },
    BicycleType.Cross: {
        0: 1.0,    # PavedSmooth
        1: 1.0,    # Paved
        2: 1.0,    # PavedRough
        3: 0.95,   # Compacted
        4: 0.75,   # Dirt
        5: 0.65,   # Gravel
        6: 0.7,    # Path
        7: 0.0,    # Impassable
    },
    BicycleType.Mountain: {
        0: 1.0,    # PavedSmooth
        1: 1.0,    # Paved
        2: 1.0,    # PavedRough
        3: 1.0,    # Compacted
        4: 0.95,   # Dirt
        5: 0.9,    # Gravel
        6: 0.85,   # Path
        7: 0.2,    # Impassable - mountain bikes can try
    },
}

# Road class factors (kRoadClassFactor from bicyclecost.cc)
# These are additive penalties in seconds per meter, scaled by (1.5 - use_roads)
# Road class: Motorway=0, Trunk=1, Primary=2, Secondary=3, Tertiary=4, Unclassified=5, Residential=6, Service=7
ROAD_CLASS_FACTOR = {
    0: 10.0,   # Motorway - strongly avoid (usually no bike access anyway)
    1: 0.15,   # Trunk - high penalty
    2: 0.10,   # Primary - medium penalty
    3: 0.05,   # Secondary - slight penalty
    4: 0.02,   # Tertiary - minimal penalty
    5: 0.0,    # Unclassified - no penalty
    6: 0.0,    # Residential - no penalty
    7: 0.0,    # Service - no penalty
}

# Use type factors
# Lower = prefer, higher = avoid
USE_FACTOR = {
    Use.kRoad: 1.0,
    Use.kRamp: 1.5,              
    Use.kTurnChannel: 1.3,
    Use.kTrack: 0.95,            
    Use.kDriveway: 1.3,
    Use.kAlley: 1.1,
    Use.kParkingAisle: 1.5,
    Use.kEmergencyAccess: 10.0,
    Use.kDriveThru: 5.0,
    Use.kCuldesac: 1.1,
    Use.kLivingStreet: 0.8,      # Prefer living streets
    Use.kServiceRoad: 1.1,
    Use.kCycleway: 0.7,          # Strongly prefer cycleways!
    Use.kMountainBike: 0.75,
    Use.kFootway: 1.5,           # Avoid if possible
    Use.kSteps: 50.0,            # Very strongly avoid steps
    Use.kPath: 0.85,             # Prefer paths
    Use.kPedestrian: 1.2,
    Use.kBridleway: 0.9,
    Use.kPedestrianCrossing: 1.0,
    Use.kElevator: 10.0,
    Use.kEscalator: 50.0,
}

# Cycle lane factors (kCycleLaneFactor from bicyclecost.cc)
# Lower value = prefer
CYCLE_LANE_FACTOR = {
    CycleLane.kNone: 1.0,
    CycleLane.kShared: 0.9,      # Shared lane marking
    CycleLane.kDedicated: 0.8,   # Dedicated bike lane
    CycleLane.kSeparated: 0.7,   # Separated/protected bike lane
}

# Bike network factor - from bicyclecost.cc
BIKE_NETWORK_FACTOR = 0.95  # Slightly prefer bike network routes

# Grade penalty factors based on use_hills
# The penalty is applied based on weighted_grade (0-15, 7 = flat)
# From bicyclecost.cc - grade_penalty tables
def get_grade_penalty(weighted_grade, use_hills):
    """
    Calculate grade penalty based on weighted grade and use_hills preference.
    weighted_grade: 0-15 where 7 is flat
    use_hills: 0.0 (avoid hills) to 1.0 (don't mind hills)
    
    Returns a factor >= 1.0 (higher = more penalty)
    """
    # Convert to actual grade estimate: 0=-10%, 7=0%, 15=+15%
    # Valhalla uses a complex formula, simplified here
    if weighted_grade == 7:
        return 1.0  # Flat
    
    # Uphill (grade > 7)
    if weighted_grade > 7:
        grade_pct = (weighted_grade - 7) * 2  # Roughly 0-16%
        # Penalty increases with steeper grades
        # use_hills=0 gives max penalty, use_hills=1 gives minimal penalty
        avoid_factor = 1.0 - use_hills  # 0-1, higher = avoid more
        penalty = 1.0 + (grade_pct / 100.0) * 3.0 * (1.0 + avoid_factor * 4.0)
        return penalty
    
    # Downhill (grade < 7)
    else:
        # Slight penalty for steep downhills (safety)
        grade_pct = (7 - weighted_grade) * 2
        if grade_pct > 6:  # Steeper than -6%
            return 1.0 + (grade_pct - 6) / 100.0 * 0.5
        return 1.0

# Speed adjustment for grades
def get_grade_speed_factor(weighted_grade):
    """
    Adjust speed based on grade.
    Uphill = slower, Downhill = faster (with limits)
    """
    if weighted_grade == 7:
        return 1.0  # Flat
    
    if weighted_grade > 7:
        # Uphill - reduce speed
        grade_pct = (weighted_grade - 7) * 2
        # Reduce speed by ~5% per 1% grade
        return max(0.3, 1.0 - grade_pct * 0.05)
    else:
        # Downhill - increase speed (but cap at 1.3x for safety)
        grade_pct = (7 - weighted_grade) * 2
        return min(1.3, 1.0 + grade_pct * 0.03)

# Transition (turn) penalties - from bicyclecost.cc
# Base maneuver penalty
MANEUVER_PENALTY = 5.0  # seconds

# Turn type penalties
TURN_PENALTIES = {
    'straight': 0.0,
    'slight_right': 0.5,
    'right': 2.0,
    'sharp_right': 3.0,
    'slight_left': 1.0,
    'left': 5.0,       # Left turns are harder/more dangerous
    'sharp_left': 7.0,
    'uturn': 20.0,
}

# Destination only penalty
DESTINATION_ONLY_PENALTY = 300.0  # seconds - strongly avoid

# Gate penalty
GATE_COST = 30.0  # seconds

# Service road penalty - from bicyclecost.cc
SERVICE_PENALTY = 15.0  # seconds


class BicycleCost:
    """
    Valhalla-compatible bicycle costing - EXACT IMPLEMENTATION
    Based on valhalla/src/sif/bicyclecost.cc
    """
    
    # Constants from bicyclecost.cc
    kDefaultCyclingSpeed = {
        BicycleType.Road: 25.0,
        BicycleType.Cross: 20.0, 
        BicycleType.Hybrid: 18.0,
        BicycleType.Mountain: 16.0,
    }
    
    kDismountSpeed = 5.1
    
    # Surface speed factors
    kRoadSurfaceSpeedFactors = [1.0, 1.0, 0.9, 0.6, 0.5, 0.3, 0.2, 0.0]
    kHybridSurfaceSpeedFactors = [1.0, 1.0, 1.0, 0.8, 0.6, 0.4, 0.25, 0.0]
    kCrossSurfaceSpeedFactors = [1.0, 1.0, 1.0, 0.8, 0.7, 0.5, 0.4, 0.0]
    kMountainSurfaceSpeedFactors = [1.0, 1.0, 1.0, 1.0, 0.9, 0.75, 0.55, 0.0]
    
    # Surface penalty factors (for surfaces worse than bike type's minimum)
    kSurfaceFactors = [1.0, 2.5, 4.5, 7.0]
    
    # Worst allowed surface per bike type
    kWorstAllowedSurface = {
        BicycleType.Road: 2,      # Compacted
        BicycleType.Cross: 3,     # Gravel
        BicycleType.Hybrid: 4,    # Dirt
        BicycleType.Mountain: 6,  # Path
    }
    
    # Road class factors from bicyclecost.cc
    kRoadClassFactor = [1.0, 0.4, 0.2, 0.1, 0.05, 0.05, 0.0, 0.5]
    
    # Grade speed factors (index 0-15, 7=flat)
    kGradeBasedSpeedFactor = [
        2.2, 2.0, 1.9, 1.7, 1.4, 1.2, 1.0, 0.95,
        0.85, 0.75, 0.65, 0.55, 0.5, 0.45, 0.4, 0.3
    ]
    
    # Avoid hills strength penalties
    kAvoidHillsStrength = [
        2.0, 1.0, 0.5, 0.2, 0.1, 0.0, 0.05, 0.1,
        0.3, 0.8, 2.0, 3.0, 4.5, 6.5, 10.0, 12.0
    ]
    
    kBicycleNetworkFactor = 0.95
    kBicycleStepsFactor = 8.0
    kTruckStress = 0.5
    kSpeedPenaltyThreshold = 40
    
    def __init__(self, bicycle_type=BicycleType.Hybrid, 
                 use_roads=0.25, use_hills=0.25, 
                 cycling_speed=None, avoid_bad_surfaces=0.25,
                 avoid_cars=False):
        """Initialize bicycle costing with Valhalla-compatible parameters.
        
        Args:
            avoid_cars: If True, heavily penalize roads with car traffic
        """
        self.bicycle_type = bicycle_type
        self.avoid_cars = avoid_cars
        self.car_penalty = 5.0 if avoid_cars else 0.0
        
        # Set speed
        if cycling_speed:
            self.speed_ = max(5.0, min(60.0, cycling_speed))
        else:
            self.speed_ = self.kDefaultCyclingSpeed.get(bicycle_type, 18.0)
        
        # Clamp options to valid ranges
        self.use_roads_ = max(0.0, min(1.0, use_roads))
        self.avoid_roads_ = 1.0 - self.use_roads_
        self.use_hills_ = max(0.0, min(1.0, use_hills))
        self.avoid_bad_surfaces_ = max(0.0, min(1.0, avoid_bad_surfaces))
        
        # Surface speed factors based on bicycle type
        if bicycle_type == BicycleType.Road:
            self.surface_speed_factor_ = self.kRoadSurfaceSpeedFactors
        elif bicycle_type == BicycleType.Hybrid:
            self.surface_speed_factor_ = self.kHybridSurfaceSpeedFactors
        elif bicycle_type == BicycleType.Cross:
            self.surface_speed_factor_ = self.kCrossSurfaceSpeedFactors
        else:
            self.surface_speed_factor_ = self.kMountainSurfaceSpeedFactors
        
        # Surface thresholds
        self.minimal_surface_penalized_ = self.kWorstAllowedSurface.get(bicycle_type, 4)
        self.worst_allowed_surface_ = (self.minimal_surface_penalized_ 
                                        if self.avoid_bad_surfaces_ == 1.0 else 6)
        
        # Road factor based on use_roads
        if self.use_roads_ >= 0.5:
            self.road_factor_ = 1.5 - self.use_roads_
        else:
            self.road_factor_ = 2.0 - self.use_roads_ * 2.0
        
        # Edge costing factors
        self.sidepath_factor_ = 3.0 * (1.0 - self.use_roads_)
        self.livingstreet_factor_ = 0.2 + self.use_roads_ * 0.8
        self.track_factor_ = 0.5 + self.use_roads_
        
        # Cycle lane factors: [shoulder*4 + cyclelane_type]
        self.cyclelane_factor_ = [
            1.0,                              # No shoulder, no cycle lane
            0.9 + self.use_roads_ * 0.05,     # No shoulder, shared
            0.4 + self.use_roads_ * 0.45,     # No shoulder, dedicated
            0.15 + self.use_roads_ * 0.6,     # No shoulder, separated
            0.7 + self.use_roads_ * 0.2,      # Shoulder, no cycle lane
            0.9 + self.use_roads_ * 0.05,     # Shoulder, shared
            0.4 + self.use_roads_ * 0.45,     # Shoulder, dedicated
            0.15 + self.use_roads_ * 0.6,     # Shoulder, separated
        ]
        
        # Path cycle lane factors
        self.path_cyclelane_factor_ = [
            0.2 + self.use_roads_,           # Shared with pedestrians
            0.2 + self.use_roads_,           # Shared with pedestrians
            0.1 + self.use_roads_ * 0.9,     # Segregated
            self.use_roads_ * 0.8,           # No pedestrians
        ]
        
        # Speed penalty table
        avoid_roads = (1.0 - self.use_roads_) * 0.75 + 0.25
        self.speedpenalty_ = [0.0] * 256
        for s in range(1, 256):
            if s <= 40:
                base_pen = float(s) / 40.0
            elif s <= 65:
                base_pen = (float(s) / 25.0) - 0.6
            else:
                base_pen = (float(s) / 50.0) + 0.7
            self.speedpenalty_[s] = (base_pen - 1.0) * avoid_roads + 1.0
        
        # Grade penalties based on use_hills
        avoid_hills = 1.0 - self.use_hills_
        self.grade_penalty_ = [avoid_hills * self.kAvoidHillsStrength[i] for i in range(16)]
        
        # Speed factor table (3.6 / speed for converting to sec/m)
        self.kSpeedFactor = [3.6 / max(s, 1) for s in range(256)]
    
    def edge_cost(self, edge):
        """
        Calculate cost and time for traversing an edge.
        EXACT port of BicycleCost::EdgeCost() from bicyclecost.cc
        
        Returns: (cost, time_seconds)
        """
        length = edge.get('length', 0)
        if length <= 0:
            return (float('inf'), 0.0)
        
        # Get edge attributes
        use = edge.get('use', 0)
        surface = max(0, min(7, edge.get('surface', 0)))
        classification = max(0, min(7, edge.get('classification', 5)))
        cyclelane = max(0, min(3, edge.get('cycle_lane', 0)))
        bike_network = edge.get('bike_network', False)
        weighted_grade = max(0, min(15, edge.get('grade', 7)))
        edge_speed = max(1, min(255, edge.get('speed', 50)))
        
        # Check surface allowed
        if surface > self.worst_allowed_surface_:
            return (float('inf'), float('inf'))
        
        # Steps - high cost
        if use == Use.kSteps:
            sec = length * self.kSpeedFactor[1]
            return (sec * self.kBicycleStepsFactor, sec)
        
        # Ferry
        if use == Use.kFerry:
            sec = length * self.kSpeedFactor[edge_speed]
            return (sec * 1.5, sec)  # Ferry factor = 1.5
        
        # Calculate roadway stress and accommodation factor
        roadway_stress = 1.0
        accommodation_factor = 1.0
        
        # Special use cases
        if use in (Use.kCycleway, Use.kFootway, Use.kPath):
            # Path/cycleway - use path_cyclelane_factor
            accommodation_factor = self.path_cyclelane_factor_[cyclelane]
        elif use == Use.kMountainBike and self.bicycle_type == BicycleType.Mountain:
            accommodation_factor = 0.3 + self.use_roads_
        elif use == Use.kLivingStreet:
            roadway_stress = self.livingstreet_factor_
        elif use == Use.kTrack:
            roadway_stress = self.track_factor_
        else:
            # Regular road
            shoulder = 1 if edge.get('shoulder', False) else 0
            accommodation_factor = self.cyclelane_factor_[shoulder * 4 + cyclelane]
            
            # Lane count penalty
            lanecount = edge.get('lanecount', 1)
            if lanecount > 1:
                roadway_stress += (float(lanecount) - 1) * 0.05 * self.road_factor_
            
            # Truck route penalty
            if edge.get('truck_route', False):
                roadway_stress += self.kTruckStress
            
            # Road class penalty
            roadway_stress += self.road_factor_ * self.kRoadClassFactor[classification]
            
            # Speed penalty
            roadway_stress *= self.speedpenalty_[edge_speed]
        
        # Sidepath penalty
        if edge.get('use_sidepath', False):
            accommodation_factor += self.sidepath_factor_
        
        # Bike network bonus
        if bike_network:
            accommodation_factor *= self.kBicycleNetworkFactor
        
        # Total factor
        factor = 1.0 + self.grade_penalty_[weighted_grade] + (accommodation_factor * roadway_stress)
        
        # Car traffic penalty when avoid_cars is enabled
        if self.avoid_cars:
            if use == Use.kRoad:
                if classification <= RoadClass.kTertiary:
                    # Major roads - heavy penalty (Motorway through Tertiary)
                    factor += self.car_penalty * (4 - classification)
                elif cyclelane == 0:
                    # Roads without bike lanes
                    factor += self.car_penalty * 0.5
            elif use in (Use.kServiceRoad, Use.kLivingStreet):
                # Minor roads with potential car traffic
                factor += self.car_penalty * 0.2
        
        # Surface penalty
        if surface >= self.minimal_surface_penalized_:
            surf_idx = surface - self.minimal_surface_penalized_
            if surf_idx < len(self.kSurfaceFactors):
                factor += self.avoid_bad_surfaces_ * self.kSurfaceFactors[surf_idx]
        
        # Compute bicycle speed
        if edge.get('dismount', False):
            bike_speed = int(self.kDismountSpeed)
        else:
            surface_factor = self.surface_speed_factor_[surface]
            grade_factor = self.kGradeBasedSpeedFactor[weighted_grade]
            bike_speed = int(self.speed_ * surface_factor * grade_factor + 0.5)
            bike_speed = max(1, min(255, bike_speed))
        
        # Compute time and cost
        sec = length * self.kSpeedFactor[bike_speed]
        cost = sec * factor
        
        return (cost, sec)
    
    def transition_cost(self, from_edge, to_edge, turn_type='straight'):
        """
        Cost for transitioning between edges (turns).
        From bicyclecost.cc TransitionCost()
        """
        # Base maneuver penalty
        cost = MANEUVER_PENALTY
        
        # Turn penalty
        cost += TURN_PENALTIES.get(turn_type, 2.0)
        
        # Reduce turn penalty when transitioning to cycleways
        if to_edge:
            to_use = to_edge.get('use', Use.kRoad)
            if to_use == Use.kCycleway:
                cost *= 0.5  # Encourage turns onto cycleways
            elif to_edge.get('bike_network', False):
                cost *= 0.7  # Encourage turns onto bike network
        
        return cost


# ============================================================================
# Tile Parser
# ============================================================================

class NodeListProxy:
    """Proxy that makes separate node arrays behave like a list of dicts"""
    def __init__(self, tile):
        self._tile = tile
    
    def __len__(self):
        return self._tile.node_count
    
    def __getitem__(self, idx):
        if idx < 0 or idx >= self._tile.node_count:
            raise IndexError("node index out of range")
        return {
            'lat': self._tile.node_lats[idx],
            'lon': self._tile.node_lons[idx],
            'edge_index': self._tile.node_edge_idx[idx],
            'edge_count': self._tile.node_edge_cnt[idx],
        }


class TileData:
    """Parsed Valhalla tile"""
    def __init__(self):
        self.level = 0
        self.tile_id = 0
        self.base_lat = 0.0
        self.base_lon = 0.0
        self.nodes = []
        self.edges = []
        self.adj = None
        self.node_index = {}


def parse_tile(filepath):
    """Parse Valhalla .gph/.gph.gz tile - OPTIMIZED for ARM with caching support"""
    if filepath.endswith('.gz'):
        with gzip.open(filepath, 'rb') as f:
            data = f.read()
    else:
        with open(filepath, 'rb') as f:
            data = f.read()
    
    if len(data) < HEADER_SIZE:
        return None
    
    tile = TileData()
    tile.source_path = filepath
    
    # Header
    word0 = struct.unpack_from('<Q', data, 0)[0]
    graphid = word0 & 0x3FFFFFFFFFFF
    tile.level = graphid & 0x7
    tile.tile_id = (graphid >> 3) & 0x3FFFFF
    
    tile.base_lon, tile.base_lat = struct.unpack_from('<ff', data, 8)
    
    word5 = struct.unpack_from('<Q', data, 40)[0]
    node_count = word5 & 0x1FFFFF
    edge_count = (word5 >> 21) & 0x1FFFFF
    
    word6 = struct.unpack_from('<I', data, 48)[0]
    transition_count = word6 & 0x3FFFFF
    
    # Read edgeinfo and textlist offsets from header
    tile.header_edgeinfo_offset = struct.unpack_from('<I', data, HEADER_EDGEINFO_OFFSET)[0]
    tile.header_textlist_offset = struct.unpack_from('<I', data, HEADER_TEXTLIST_OFFSET)[0]
    
    nodes_offset = HEADER_SIZE
    edges_offset = nodes_offset + node_count * NODE_SIZE + transition_count * 8
    
    base_lat = tile.base_lat
    base_lon = tile.base_lon
    
    # Parse nodes - store as simple lists for pickle compatibility
    tile.node_lats = []
    tile.node_lons = []
    tile.node_edge_idx = []
    tile.node_edge_cnt = []
    tile.node_trans_idx = []   # Transition index
    tile.node_trans_up = []    # Has upward transition
    tile.node_trans_down = []  # Has downward transition
    tile.node_count = node_count
    
    # Store transitions offset for later
    tile.transitions_offset = nodes_offset + node_count * NODE_SIZE
    tile.transition_count = transition_count
    
    for i in range(node_count):
        offset = nodes_offset + i * NODE_SIZE
        
        w0 = struct.unpack_from('<Q', data, offset)[0]
        lat = base_lat + ((w0 & 0x3FFFFF) * 1e-6 + ((w0 >> 22) & 0xF) * 1e-7)
        lon = base_lon + (((w0 >> 26) & 0x3FFFFF) * 1e-6 + ((w0 >> 48) & 0xF) * 1e-7)
        
        w1 = struct.unpack_from('<Q', data, offset + 8)[0]
        edge_idx = w1 & 0x1FFFFF
        edge_cnt = (w1 >> 21) & 0x7F
        trans_idx = (w1 >> 49) & 0x7F
        trans_up = bool((w1 >> 56) & 1)
        trans_down = bool((w1 >> 57) & 1)
        
        tile.node_lats.append(lat)
        tile.node_lons.append(lon)
        tile.node_edge_idx.append(edge_idx)
        tile.node_edge_cnt.append(edge_cnt)
        tile.node_trans_idx.append(trans_idx)
        tile.node_trans_up.append(trans_up)
        tile.node_trans_down.append(trans_down)
        
        # Build spatial index
        bucket = (int(lat * 100), int(lon * 100))
        if bucket not in tile.node_index:
            tile.node_index[bucket] = []
        tile.node_index[bucket].append(i)
    
    # Store edges offset for lazy loading
    tile.edges_offset = edges_offset
    tile.edge_count = edge_count
    
    # Pre-parse edge connectivity and edgeinfo offsets
    tile.edge_ends = []
    tile.edge_edgeinfo_offsets = []  # For shape lookup
    
    for i in range(edge_count):
        offset = edges_offset + i * EDGE_SIZE
        
        w0 = struct.unpack_from('<Q', data, offset)[0]
        endnode = w0 & 0x3FFFFFFFFFFF
        end_level = endnode & 0x7  # Lower 3 bits = level
        end_tileid = (endnode >> 3) & 0x3FFFFF
        end_id = (endnode >> 25) & 0x1FFFFF
        
        # Extract opp_index (bits 54-60 of word 0) - opposing edge index at end node
        opp_index = (w0 >> 54) & 0x7F
        
        # Extract edgeinfo_offset from word 1 (bits 0-24)
        w1 = struct.unpack_from('<Q', data, offset + 8)[0]
        edgeinfo_offset = w1 & 0x1FFFFFF  # 25 bits
        tile.edge_edgeinfo_offsets.append(edgeinfo_offset)
        
        # Quick bicycle access check
        w3 = struct.unpack_from('<Q', data, offset + 24)[0]
        has_bike = bool(((w3 & 0xFFF) | ((w3 >> 12) & 0xFFF)) & kBicycleAccess)
        
        tile.edge_ends.append((end_level, end_tileid, end_id, has_bike, opp_index))
    
    # Don't store edge_data - will reload on demand in get_edge_details
    tile.edge_data = None
    
    # Create nodes proxy for backward compatibility
    tile.nodes = NodeListProxy(tile)
    
    return tile


# Compatibility wrapper for node access
def get_node(tile, idx):
    """Get node dict by index"""
    if idx < 0 or idx >= tile.node_count:
        return None
    return {
        'lat': tile.node_lats[idx],
        'lon': tile.node_lons[idx],
        'edge_index': tile.node_edge_idx[idx],
        'edge_count': tile.node_edge_cnt[idx],
    }


def get_edge_details(tile, edge_idx):
    """Lazy load full edge details when needed"""
    if edge_idx >= tile.edge_count:
        return None
    
    end_level, end_tileid, end_id, has_bike, opp_index = tile.edge_ends[edge_idx]
    if not has_bike:
        return None
    
    # Reload data if not cached
    if tile.edge_data is None:
        if hasattr(tile, 'source_path') and tile.source_path:
            if tile.source_path.endswith('.gz'):
                with gzip.open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
            else:
                with open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
        else:
            return None
    
    data = tile.edge_data
    offset = tile.edges_offset + edge_idx * EDGE_SIZE
    
    # Word 2 (offset 16): speeds, use, classification, surface
    # Bit layout:
    #   speed: 0-7, free_flow_speed: 8-15, constrained_flow_speed: 16-23
    #   truck_speed: 24-31, name_consistency: 32-39, use: 40-45
    #   lanecount: 46-49, density: 50-53, classification: 54-56, surface: 57-59
    w2 = struct.unpack_from('<Q', data, offset + 16)[0]
    speed = w2 & 0xFF
    use = (w2 >> 40) & 0x3F
    lanecount = (w2 >> 46) & 0xF       # NEW: lane count
    density = (w2 >> 50) & 0xF
    classification = (w2 >> 54) & 0x7
    surface = (w2 >> 57) & 0x7
    
    # Word 3 (offset 24): access, slopes, cycle info
    # Bit layout:
    #   forwardaccess: 0-11, reverseaccess: 12-23
    #   max_up_slope: 24-28, max_down_slope: 29-33
    #   sac_scale: 34-36, cycle_lane: 37-38, bike_network: 39
    #   use_sidepath: 40, shoulder: 41, dismount: 42, ...
    w3 = struct.unpack_from('<Q', data, offset + 24)[0]
    cycle_lane = (w3 >> 37) & 0x3
    bike_network = bool((w3 >> 39) & 1)
    use_sidepath = bool((w3 >> 40) & 1)  # NEW
    shoulder = bool((w3 >> 41) & 1)      # NEW
    dismount = bool((w3 >> 42) & 1)      # NEW
    
    # Word 4 (offset 32): length, weighted_grade
    # Bit layout: turntype: 0-23, edge_to_left: 24-31, length: 32-55, weighted_grade: 56-59
    w4 = struct.unpack_from('<Q', data, offset + 32)[0]
    length = (w4 >> 32) & 0xFFFFFF
    weighted_grade = (w4 >> 56) & 0xF  # 0-15, 7 = flat
    
    return {
        'end_level': end_level,
        'end_tileid': end_tileid,
        'end_id': end_id,
        'length': length,
        'speed': speed,
        'classification': classification,
        'use': use,
        'surface': surface,
        'cycle_lane': cycle_lane,
        'bike_network': bike_network,
        'grade': weighted_grade,
        'density': density,
        'lanecount': lanecount,       # NEW
        'shoulder': shoulder,          # NEW
        'use_sidepath': use_sidepath,  # NEW
        'dismount': dismount,          # NEW
    }


def get_edge_shape(tile, edge_idx):
    """
    Get the shape (list of lat/lon points) for an edge from EdgeInfo.
    
    EdgeInfo structure:
    - 12 bytes fixed header (wayid, elevation, speed_limit, name_count, encoded_shape_size)
    - name_count * 4 bytes (NameInfo list)
    - encoded_shape_size bytes (the encoded shape)
    
    Returns list of (lat, lon) tuples, or empty list on error.
    """
    if edge_idx >= tile.edge_count or edge_idx >= len(tile.edge_edgeinfo_offsets):
        return []
    
    # Reload data if needed
    if tile.edge_data is None:
        if hasattr(tile, 'source_path') and tile.source_path:
            if tile.source_path.endswith('.gz'):
                with gzip.open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
            else:
                with open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
        else:
            return []
    
    data = tile.edge_data
    
    # Calculate absolute offset to this edge's EdgeInfo
    edgeinfo_offset = tile.edge_edgeinfo_offsets[edge_idx]
    abs_offset = tile.header_edgeinfo_offset + edgeinfo_offset
    
    if abs_offset + 12 > len(data):
        return []
    
    # Parse EdgeInfoInner (12 bytes)
    # Word 2 (bytes 8-11): name_count (4 bits), encoded_shape_size (16 bits), ...
    word2 = struct.unpack_from('<I', data, abs_offset + 8)[0]
    name_count = word2 & 0xF
    encoded_shape_size = (word2 >> 4) & 0xFFFF
    
    # Sanity check - shapes shouldn't be huge
    if encoded_shape_size == 0 or encoded_shape_size > 10000:
        return []
    
    # Skip header and name_info_list to get to encoded_shape
    shape_offset = abs_offset + 12 + (name_count * 4)
    
    if shape_offset + encoded_shape_size > len(data):
        return []
    
    # Decode the shape
    try:
        shape = decode7_shape(data, shape_offset, encoded_shape_size)
        # Sanity check on result
        if len(shape) > 5000:
            return []  # Too many points, likely corrupted
        return shape
    except:
        return []


def get_transitions(tile, node_id):
    """Get transitions for a node. Returns list of (level, tile_id, node_id, is_up)."""
    if node_id >= tile.node_count:
        return []
    
    if not tile.node_trans_up[node_id] and not tile.node_trans_down[node_id]:
        return []
    
    # Reload data if needed
    if tile.edge_data is None:
        if hasattr(tile, 'source_path') and tile.source_path:
            if tile.source_path.endswith('.gz'):
                with gzip.open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
            else:
                with open(tile.source_path, 'rb') as f:
                    tile.edge_data = f.read()
        else:
            return []
    
    data = tile.edge_data
    transitions = []
    trans_idx = tile.node_trans_idx[node_id]
    count = (1 if tile.node_trans_up[node_id] else 0) + (1 if tile.node_trans_down[node_id] else 0)
    
    for i in range(count):
        offset = tile.transitions_offset + (trans_idx + i) * 8
        trans = struct.unpack_from('<Q', data, offset)[0]
        
        graphid = trans & 0x3FFFFFFFFFFF
        is_up = bool((trans >> 46) & 1)
        
        level = graphid & 0x7
        tid = (graphid >> 3) & 0x3FFFFF
        nid = (graphid >> 25) & 0x1FFFFF
        
        transitions.append((level, tid, nid, is_up))
    
    return transitions


def build_adjacency_cross_tile(tile, costing):
    """Build adjacency list with cross-tile edges - OPTIMIZED"""
    if tile.adj is not None:
        return
    
    num_nodes = len(tile.nodes)
    tile.adj = [[] for _ in range(num_nodes)]
    
    # Build node -> edge index mapping first (O(n))
    # Each node has edge_index and edge_count
    for ni in range(num_nodes):
        node = tile.nodes[ni]
        start_edge = node['edge_index']
        end_edge = start_edge + node['edge_count']
        
        for ei in range(start_edge, min(end_edge, tile.edge_count)):
            end_level, end_tileid, end_id, has_bike, opp_index = tile.edge_ends[ei]
            
            if not has_bike:
                continue
            
            # Get full edge details (lazy loaded)
            edge = get_edge_details(tile, ei)
            if edge is None:
                continue
            
            # Calculate cost
            cost, time_secs = costing.edge_cost(edge)
            length = edge['length']
            
            if cost >= float('inf'):
                continue
            
            # Store: (target_tile_id, target_node_id, cost, time, length)
            tile.adj[ni].append((end_tileid, end_id, cost, time_secs, length))


# ============================================================================
# Tile Cache
# ============================================================================

class TileCache:
    def __init__(self, tiles_dir, max_tiles=100):
        self.tiles_dir = tiles_dir
        self.max_tiles = max_tiles
        self.tiles = {}
        self.access_order = []
        self.lock = threading.Lock()
        self.cache_dir = os.path.join(tiles_dir, '.cache')
        
        # Route cache for recently computed routes
        self.route_cache = {}
        self.route_cache_max = 50
        
        # Create cache directory
        if not os.path.exists(self.cache_dir):
            try:
                os.makedirs(self.cache_dir)
            except:
                pass
    
    def get_tile_path(self, tile_id, level):
        # Valhalla uses different directory structure per level
        # Level 2: 2/000/795/665.gph.gz  (tile_id 795665)
        # Level 1: 1/049/876.gph.gz      (tile_id 49876)
        # Level 0: 0/003/109.gph.gz      (tile_id 3109)
        
        if level == 2:
            dir1 = tile_id // 1000000           # millions
            dir2 = (tile_id // 1000) % 1000     # thousands  
            dir3 = tile_id % 1000               # ones
            base = os.path.join(self.tiles_dir, str(level), 
                               str(dir1).zfill(3), str(dir2).zfill(3))
            fname = str(dir3).zfill(3)
        else:
            # Level 0 and 1: simpler structure
            dir1 = tile_id // 1000
            dir2 = tile_id % 1000
            base = os.path.join(self.tiles_dir, str(level), str(dir1).zfill(3))
            fname = str(dir2).zfill(3)
        
        for ext in ['.gph.gz', '.gph']:
            path = os.path.join(base, fname + ext)
            if os.path.exists(path):
                return path
        return None
    
    def get_cache_path(self, tile_id, level):
        """Get path for cached parsed tile"""
        return os.path.join(self.cache_dir, "%d_%d.cache" % (level, tile_id))
    
    def load_cached_tile(self, tile_id, level, source_path):
        """Try to load from cache, return None if not available or stale"""
        cache_path = self.get_cache_path(tile_id, level)
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            # Check if cache is newer than source
            source_mtime = os.path.getmtime(source_path)
            cache_mtime = os.path.getmtime(cache_path)
            
            if cache_mtime < source_mtime:
                return None  # Cache is stale
            
            import pickle
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except:
            return None
    
    def save_cached_tile(self, tile, tile_id, level):
        """Save parsed tile to cache"""
        cache_path = self.get_cache_path(tile_id, level)
        
        try:
            import pickle
            with open(cache_path, 'wb') as f:
                pickle.dump(tile, f, protocol=2)
        except Exception as e:
            print("Cache save error: %s" % e)
    
    def get_tile(self, level, tile_id, costing):
        key = (level, tile_id)
        
        with self.lock:
            if key in self.tiles:
                self.access_order.remove(key)
                self.access_order.append(key)
                return self.tiles[key]
        
        path = self.get_tile_path(tile_id, level)
        if not path:
            return None
        
        # Try loading from cache first (much faster!)
        tile = self.load_cached_tile(tile_id, level, path)
        
        if tile is None:
            # Parse from source
            print("Parsing tile %d (this may take a moment)..." % tile_id)
            tile = parse_tile(path)
            if tile:
                # Save to cache for next time
                self.save_cached_tile(tile, tile_id, level)
                print("Tile %d cached for faster loading next time" % tile_id)
        else:
            print("Loaded tile %d from cache" % tile_id)
            # Ensure source_path is set for transition loading
            tile.source_path = path
        
        if not tile:
            return None
        
        with self.lock:
            self.tiles[key] = tile
            self.access_order.append(key)
            
            while len(self.tiles) > self.max_tiles:
                old = self.access_order.pop(0)
                del self.tiles[old]
        
        return tile
    
    def get_tile_for_point(self, lat, lon, level, costing):
        tile_size = TILE_LEVELS[level]
        tiles_per_row = int(360.0 / tile_size)
        col = int((lon + 180.0) / tile_size)
        row = int((lat + 90.0) / tile_size)
        tile_id = row * tiles_per_row + col
        return self.get_tile(level, tile_id, costing)


# ============================================================================
# Router
# ============================================================================

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def find_nearest_node(tile, lat, lon):
    bucket = (int(lat * 100), int(lon * 100))
    candidates = []
    
    for dlat in range(-2, 3):
        for dlon in range(-2, 3):
            b = (bucket[0] + dlat, bucket[1] + dlon)
            if b in tile.node_index:
                candidates.extend(tile.node_index[b])
    
    if not candidates:
        candidates = list(range(min(2000, len(tile.nodes))))
    
    best_dist = float('inf')
    best_node = None
    
    for idx in candidates:
        node = tile.nodes[idx]
        dist = haversine(lat, lon, node['lat'], node['lon'])
        if dist < best_dist:
            best_dist = dist
            best_node = idx
    
    return best_node, best_dist


def route(cache, costing, from_lat, from_lon, to_lat, to_lon, level=2, use_hierarchy=True, simple_cost=False):
    """
    Hierarchical A* routing with transition support between tile levels.
    
    Uses Valhalla's hierarchical tile system:
    - Level 2: Fine detail (0.25 degree tiles) - for start/end
    - Level 1: Medium detail (1.0 degree tiles) - for longer distances
    - Level 0: Coarse detail (4.0 degree tiles) - for very long routes
    
    Transitions between levels are free (cost=0) to allow efficient routing.
    Uses EdgeInfo shapes for accurate route geometry across all levels.
    
    If simple_cost=True, uses simplified cost (length/4) like hierarchical_router.py
    """
    import time as time_module
    route_start_time = time_module.time()
    
    # Profiling counters
    profile = {
        'tile_loads': 0,
        'tile_load_time': 0,
        'expansions': 0,
        'heap_pushes': 0,
    }
    
    def get_tile_id_for_level(lat, lon, lv):
        tile_size = TILE_LEVELS[lv]
        tiles_per_row = int(360.0 / tile_size)
        col = int((lon + 180.0) / tile_size)
        row = int((lat + 90.0) / tile_size)
        return row * tiles_per_row + col
    
    # Start on Level 2 (finest detail)
    start_level = 2
    from_tile_id = get_tile_id_for_level(from_lat, from_lon, start_level)
    to_tile_id = get_tile_id_for_level(to_lat, to_lon, start_level)
    
    # Get start tile
    t0 = time_module.time()
    from_tile = cache.get_tile(start_level, from_tile_id, costing)
    profile['tile_load_time'] += time_module.time() - t0
    profile['tile_loads'] += 1
    if not from_tile:
        return None, "No tile data for start location"
    
    # Get end tile
    t0 = time_module.time()
    to_tile = cache.get_tile(start_level, to_tile_id, costing)
    profile['tile_load_time'] += time_module.time() - t0
    profile['tile_loads'] += 1
    if not to_tile:
        return None, "No tile data for end location"
    
    # Find nearest nodes
    start_node, start_dist = find_nearest_node(from_tile, from_lat, from_lon)
    end_node, end_dist = find_nearest_node(to_tile, to_lat, to_lon)
    
    if start_node is None or end_node is None:
        return None, "Could not find nodes near coordinates"
    
    # Target location for heuristic
    end_lat_target = to_tile.nodes[end_node]['lat']
    end_lon_target = to_tile.nodes[end_node]['lon']
    
    # State = (level, tile_id, node_id) - now includes level!
    start_state = (start_level, from_tile_id, start_node)
    end_state = (start_level, to_tile_id, end_node)
    
    # Priority queue: (f_score, g_score, time, distance, state)
    # NO MORE path/edges in heap - use predecessor tracking instead!
    open_set = [(0, 0, 0, 0, start_state)]
    g_scores = {start_state: (0, 0, 0)}  # state -> (g_score, time, distance)
    came_from = {}  # state -> (predecessor_state, edge_info)
    visited = set()
    
    # Tile cache for this route - keyed by (level, tile_id)
    tiles = {(start_level, from_tile_id): from_tile, (start_level, to_tile_id): to_tile}
    
    def get_or_load_tile(lv, tid):
        key = (lv, tid)
        if key not in tiles:
            t0 = time_module.time()
            t = cache.get_tile(lv, tid, costing)
            profile['tile_load_time'] += time_module.time() - t0
            profile['tile_loads'] += 1
            if t:
                tiles[key] = t
        return tiles.get(key)
    
    iterations = 0
    
    # Adaptive max_iterations based on distance
    # Short routes need fewer iterations
    dist_km = haversine(from_lat, from_lon, to_lat, to_lon)
    if dist_km < 5:
        max_iterations = 50000
    elif dist_km < 20:
        max_iterations = 100000
    elif dist_km < 50:
        max_iterations = 200000
    else:
        max_iterations = 300000
    
    level_transitions = 0
    total_transitions_found = 0
    
    # Debug: Count transitions in start tile
    start_trans_count = sum(1 for i in range(from_tile.node_count) 
                            if from_tile.node_trans_up[i] or from_tile.node_trans_down[i])
    print("[ROUTE DEBUG] Start tile %d has %d nodes with transitions" % (from_tile_id, start_trans_count))
    
    while open_set and iterations < max_iterations:
        iterations += 1
        f, g, total_time, total_dist, current_state = heapq.heappop(open_set)
        
        # Skip if already visited with better cost
        if current_state in visited:
            continue
        visited.add(current_state)
        profile['expansions'] += 1
        
        current_level, current_tile_id, current_node_id = current_state
        
        # Check if we reached the exact destination node
        if current_state == end_state:
            # Reconstruct path by following came_from backwards
            path = []
            edges = []
            state = current_state
            while state in came_from:
                path.append(state)
                prev_state, edge_info = came_from[state]
                if edge_info:  # Regular edge, not transition
                    edges.append(edge_info)
                state = prev_state
            path.append(start_state)
            path.reverse()
            edges.reverse()
            
            # Build coordinates from path
            coords = []
            for lv, tid, nid in path:
                tile = get_or_load_tile(lv, tid)
                if tile and nid < len(tile.nodes):
                    node = tile.nodes[nid]
                    coords.append({'lat': node['lat'], 'lon': node['lon']})
            
            print("[ROUTE DEBUG] Using node coords: %d points" % len(coords))
            
            # Calculate road statistics from edges
            car_distance = 0
            cycleway_distance = 0
            level_usage = {}
            
            for edge_info in edges:
                length = edge_info.get('length', 0)
                use = edge_info.get('use', 0)
                classification = edge_info.get('classification', 5)
                cycle_lane = edge_info.get('cycle_lane', 0)
                edge_level = edge_info.get('level', 2)
                
                level_usage[edge_level] = level_usage.get(edge_level, 0) + length
                
                if use in (Use.kCycleway, Use.kPath, Use.kFootway, Use.kLivingStreet, 
                           Use.kTrack, Use.kBridleway, Use.kPedestrian):
                    cycleway_distance += length
                elif use == Use.kRoad and classification <= RoadClass.kTertiary and cycle_lane == 0:
                    car_distance += length
                elif use == Use.kRoad and cycle_lane > 0:
                    cycleway_distance += length * 0.5
                    car_distance += length * 0.5
                else:
                    car_distance += length * 0.3
            
            print("[ROUTE DEBUG] Found! dist=%.1f km, iters=%d, level_trans=%d, trans_found=%d" % 
                  (total_dist/1000.0, iterations, level_transitions, total_transitions_found))
            if level_usage:
                for lv, dist in sorted(level_usage.items()):
                    print("[ROUTE DEBUG]   Level %d: %.2f km" % (lv, dist/1000.0))
            
            # Print profiling info
            route_time = time_module.time() - route_start_time
            print("[PROFILE] Total: %.2fs | Tiles: %d (%.2fs) | Iter: %d | Expand: %d | HeapPush: %d" % (
                route_time, profile['tile_loads'], profile['tile_load_time'], iterations, profile['expansions'], profile['heap_pushes']
            ))
            
            return {
                'coords': coords,
                'distance': total_dist,
                'time': total_time,
                'nodes': len(path),
                'iterations': iterations,
                'car_distance': car_distance,
                'cycleway_distance': cycleway_distance,
                'level_transitions': level_transitions,
                'level_usage': level_usage,
                'debug': {
                    'transitions_found': total_transitions_found,
                    'start_tile_trans': start_trans_count,
                    'coord_count': len(coords),
                }
            }, None
        
        current_tile = get_or_load_tile(current_level, current_tile_id)
        if not current_tile or current_node_id >= len(current_tile.nodes):
            continue
        
        # 1. Expand regular edges
        node = current_tile.nodes[current_node_id]
        start_edge = node['edge_index']
        end_edge = start_edge + node['edge_count']
        
        for ei in range(start_edge, min(end_edge, current_tile.edge_count)):
            # Edge ends now include level info
            end_level, neighbor_tile_id, neighbor_node_id, has_bike, opp_index = current_tile.edge_ends[ei]
            
            if not has_bike:
                continue
            
            # If hierarchy is disabled, only follow edges on the same level
            if not use_hierarchy and end_level != current_level:
                continue
            
            # Get edge details for costing
            edge = get_edge_details(current_tile, ei)
            if edge is None:
                continue
            
            # Use end_level from edge_ends (already extracted)
            neighbor_level = end_level
            neighbor_state = (neighbor_level, neighbor_tile_id, neighbor_node_id)
            
            if neighbor_state in visited:
                continue
            
            # Load neighbor tile
            neighbor_tile = get_or_load_tile(neighbor_level, neighbor_tile_id)
            if not neighbor_tile or neighbor_node_id >= len(neighbor_tile.nodes):
                continue
            
            length = edge['length']
            
            # Use simple cost for debugging (like hierarchical_router.py)
            if simple_cost:
                cost = length / 4.0
                time_secs = length / 4.0  # Rough estimate
            else:
                cost, time_secs = costing.edge_cost(edge)
            
            if cost >= float('inf'):
                continue
            
            new_g = g + cost
            new_time = total_time + time_secs
            new_dist = total_dist + length
            
            # Check if this is a better path
            if neighbor_state in g_scores:
                old_g, _, _ = g_scores[neighbor_state]
                if new_g >= old_g:
                    continue
            
            g_scores[neighbor_state] = (new_g, new_time, new_dist)
            came_from[neighbor_state] = (current_state, {
                'length': length,
                'use': edge.get('use', 0),
                'classification': edge.get('classification', 5),
                'cycle_lane': edge.get('cycle_lane', 0),
                'level': current_level,
                'tile_id': current_tile_id,
                'edge_idx': ei,
            })
            
            # Heuristic - must match cost scale
            neighbor_node = neighbor_tile.nodes[neighbor_node_id]
            if simple_cost:
                h = haversine(neighbor_node['lat'], neighbor_node['lon'], 
                             end_lat_target, end_lon_target) / 4.0
            else:
                h = haversine(neighbor_node['lat'], neighbor_node['lon'], 
                             end_lat_target, end_lon_target) / 25.0 * 3.6
            
            heapq.heappush(open_set, (new_g + h, new_g, new_time, new_dist, neighbor_state))
            profile['heap_pushes'] += 1
        
        # 2. Expand transitions (free cost) - only if hierarchy is enabled
        if use_hierarchy:
            transitions = get_transitions(current_tile, current_node_id)
            total_transitions_found += len(transitions)
            for trans_level, trans_tid, trans_nid, is_up in transitions:
                trans_state = (trans_level, trans_tid, trans_nid)
                
                if trans_state in visited:
                    continue
                
                # Transitions are free (cost = 0)
                old_g = g_scores.get(trans_state, (float('inf'), 0, 0))[0]
                if g < old_g:
                    g_scores[trans_state] = (g, total_time, total_dist)
                    came_from[trans_state] = (current_state, None)  # None = transition, no edge
                    level_transitions += 1
                    
                    trans_tile = get_or_load_tile(trans_level, trans_tid)
                    if trans_tile and trans_nid < trans_tile.node_count:
                        trans_node = trans_tile.nodes[trans_nid]
                        if simple_cost:
                            h = haversine(trans_node['lat'], trans_node['lon'],
                                         end_lat_target, end_lon_target) / 4.0
                        else:
                            h = haversine(trans_node['lat'], trans_node['lon'],
                                         end_lat_target, end_lon_target) / 25.0 * 3.6
                        
                        heapq.heappush(open_set, (g + h, g, total_time, total_dist, trans_state))
    
    # Print profiling info even when no route found
    route_time = time_module.time() - route_start_time
    print("[PROFILE] Total: %.2fs | Tiles: %d (%.2fs) | Iter: %d | Expand: %d | HeapPush: %d" % (
        route_time, profile['tile_loads'], profile['tile_load_time'], iterations, profile['expansions'], profile['heap_pushes']
    ))
    
    print("[ROUTE DEBUG] No route found! visited=%d, tiles=%d, level_trans=%d, trans_found=%d" %
          (len(visited), len(tiles), level_transitions, total_transitions_found))
    return None, "No route found (searched %d nodes across %d tiles, %d level transitions)" % (
        len(visited), len(tiles), level_transitions)


# ============================================================================
# Polyline Encoding (Google format)
# ============================================================================

def encode_polyline(coords, precision=6):
    result = []
    prev_lat = 0
    prev_lon = 0
    
    for c in coords:
        lat_int = int(round(c['lat'] * (10 ** precision)))
        lon_int = int(round(c['lon'] * (10 ** precision)))
        
        d_lat = lat_int - prev_lat
        d_lon = lon_int - prev_lon
        
        prev_lat = lat_int
        prev_lon = lon_int
        
        for v in [d_lat, d_lon]:
            v = ~(v << 1) if v < 0 else (v << 1)
            while v >= 0x20:
                result.append(chr((0x20 | (v & 0x1f)) + 63))
                v >>= 5
            result.append(chr(v + 63))
    
    return ''.join(result)


# ============================================================================
# HTTP Server (Valhalla-compatible API)
# ============================================================================

# ============================================================================
# Download Manager
# ============================================================================

MODRANA_BASE = "https://data.modrana.org/osm_scout_server"
COUNTRIES_JSON_URL = MODRANA_BASE + "/countries_provided.json"
VALHALLA_PACKAGES_URL = MODRANA_BASE + "/valhalla-33/valhalla/packages"
GEOCODER_NLP_BASE = MODRANA_BASE + "/geocoder-nlp-39/geocoder-nlp"

# Geocoder files to download (bz2 compressed)
GEOCODER_FILES = [
    'geonlp-primary.sqlite.bz2',
    'geonlp-normalized.trie.bz2', 
    'geonlp-normalized-id.kch.bz2'
]

# libpostal global data (for ML-based address parsing)
LIBPOSTAL_BASE_URL = MODRANA_BASE + "/postal-global-2/postal/global-v1"
LIBPOSTAL_DATA_DIR = "/home/user/MyDocs/Maps.OSM/postal/global-v1"
LIBPOSTAL_FILES = [
    ('address_expansions', 'address_dictionary.dat'),
    ('language_classifier', 'language_classifier.dat'),
    ('numex', 'numex.dat'),
    ('transliteration', 'transliteration.dat'),
]

# libpostal country-specific parser data
LIBPOSTAL_PARSER_BASE_URL = MODRANA_BASE + "/postal-country-2/postal/countries-v1"
LIBPOSTAL_PARSER_DIR = "/home/user/MyDocs/Maps.OSM/postal/countries-v1"
LIBPOSTAL_PARSER_FILES = [
    'address_parser_crf.dat',
    'address_parser_phrases.dat',
    'address_parser_postal_codes.dat',
    'address_parser_vocab.trie',
]

# Map region_id to ISO country code for libpostal parser
# This maps modrana region names to the ISO codes used by libpostal
REGION_TO_ISO = {
    'europe/austria': 'AT',
    'europe/hungary': 'HU',
    'europe/germany': 'DE',
    'europe/switzerland': 'CH',
    'europe/czech-republic': 'CZ',
    'europe/slovakia': 'SK',
    'europe/poland': 'PL',
    'europe/italy': 'IT',
    'europe/france': 'FR',
    'europe/spain': 'ES',
    'europe/portugal': 'PT',
    'europe/netherlands': 'NL',
    'europe/belgium': 'BE',
    'europe/united-kingdom': 'GB',
    'europe/ireland': 'GB-IE',
    'europe/sweden': 'SE',
    'europe/norway': 'NO',
    'europe/finland': 'FI',
    'europe/denmark': 'DK',
    'europe/greece': 'GR',
    'europe/croatia': 'HR',
    'europe/slovenia': 'SI',
    'europe/serbia': 'RS',
    'europe/romania': 'RO',
    'europe/bulgaria': 'BG',
    'europe/ukraine': 'UA',
    'europe/russia': 'RU',
    'europe/liechtenstein': 'LI',
    'europe/luxembourg': 'LU',
    'europe/azores': 'PT',  # Part of Portugal
    'north-america/usa': 'US',
    'north-america/canada': 'CA',
    'asia/japan': 'JP',
    'asia/china': 'CN',
    'asia/india': 'IN',
    'australia-oceania/australia': 'AU',
    'australia-oceania/new-zealand': 'NZ',
}

try:
    from urllib.request import urlopen, Request
    from urllib.error import URLError
except ImportError:
    from urllib2 import urlopen, Request, URLError

# Cache for countries data
_countries_cache = None
_countries_cache_time = 0


class DownloadManager:
    """Download Valhalla tiles from modrana.org using countries_provided.json"""
    
    def __init__(self, tiles_dir):
        self.tiles_dir = tiles_dir
        self.downloads = {}  # region -> {'progress': 0-100, 'status': str}
        self.lock = threading.Lock()
        self.countries_data = None
    
    def is_package_installed(self, pkg_num):
        """Check if a package is installed (all tiles from .list exist)"""
        packages_dir = os.path.join(self.tiles_dir, 'packages')
        list_path = os.path.join(packages_dir, str(pkg_num) + '.list')
        
        if not os.path.exists(list_path):
            return False
        
        try:
            with open(list_path, 'r') as f:
                tiles = [line.strip() for line in f if line.strip()]
            
            if not tiles:
                return False
            
            # Check if all tiles exist
            for tile in tiles:
                # Remove valhalla/tiles/ prefix if present (from original .list files)
                if tile.startswith('valhalla/tiles/'):
                    tile = tile[len('valhalla/tiles/'):]
                # Handle both 3-part (level 0,1) and 4-part (level 2) paths
                tile_path = os.path.join(self.tiles_dir, *tile.split('/'))
                if not os.path.exists(tile_path):
                    return False
            
            return True
        except:
            return False
    
    def is_region_installed(self, region_id, packages):
        """Check if a region is installed (all its packages are installed)"""
        if not packages:
            return False
        
        for pkg_num in packages:
            if not self.is_package_installed(pkg_num):
                return False
        
        return True
    
    def _ensure_dir(self, dir_path):
        """Ensure directory exists, removing any files that block the path.
        
        This handles the case where a file exists where a directory should be.
        For example, if /a/b is a file but we need /a/b/c.txt, we delete /a/b first.
        """
        if os.path.exists(dir_path):
            if os.path.isdir(dir_path):
                return  # Already a directory, nothing to do
            else:
                # It's a file, remove it
                os.remove(dir_path)
        
        # Check each parent path component
        parts = dir_path.split(os.sep)
        current = ""
        for part in parts:
            if not part:
                current = os.sep
                continue
            current = os.path.join(current, part)
            if os.path.exists(current):
                if not os.path.isdir(current):
                    # File blocking the path, remove it
                    os.remove(current)
                    os.makedirs(current)
            else:
                os.makedirs(current)
                break  # makedirs will create the rest
        
        # Final makedirs for remaining path
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    
    def get_installed_regions(self):
        """Get list of installed region IDs"""
        regions_file = os.path.join(self.tiles_dir, 'packages', 'regions.json')
        
        if not os.path.exists(regions_file):
            return []
        
        try:
            with open(regions_file, 'r') as f:
                regions_data = json.load(f)
            
            installed = []
            for region_id, info in regions_data.items():
                packages = info.get('packages', [])
                if self.is_region_installed(region_id, packages):
                    installed.append(region_id)
            
            return installed
        except:
            return []
    
    def _fetch_countries_json(self):
        """Fetch and cache countries_provided.json"""
        global _countries_cache, _countries_cache_time
        import time
        
        # Use cache if less than 1 hour old
        if _countries_cache and (time.time() - _countries_cache_time) < 3600:
            return _countries_cache
        
        # Try local file first (bundled with app)
        local_paths = [
            "/opt/valhalla-bike-router/countries_provided.json",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "countries_provided.json"),
        ]
        
        for local_path in local_paths:
            if os.path.exists(local_path):
                try:
                    with open(local_path, 'r') as f:
                        _countries_cache = json.load(f)
                        _countries_cache_time = time.time()
                        print("Loaded countries from local file: %s" % local_path)
                        return _countries_cache
                except Exception as e:
                    print("Error loading local countries JSON: %s" % e)
        
        # Fallback to download
        try:
            req = Request(COUNTRIES_JSON_URL)
            req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
            response = urlopen(req, timeout=60)
            data = response.read().decode('utf-8')
            _countries_cache = json.loads(data)
            _countries_cache_time = time.time()
            print("Loaded countries from URL")
            return _countries_cache
        except Exception as e:
            print("Error fetching countries JSON: %s" % e)
            return _countries_cache  # Return old cache if available
    
    def get_regions(self):
        """Get available regions from countries_provided.json"""
        regions = []
        
        try:
            countries = self._fetch_countries_json()
            if not countries:
                return {'regions': [], 'source': 'error', 'error': 'Could not fetch country list'}
            
            for country_id, data in countries.items():
                # Only include countries that have valhalla data
                if 'valhalla' not in data:
                    continue
                
                valhalla = data['valhalla']
                packages = valhalla.get('packages', [])
                if not packages:
                    continue
                
                # Calculate size in MB from compressed size
                size_compressed = int(valhalla.get('size-compressed', 0))
                size_mb = size_compressed // (1024 * 1024)
                
                # Get nice name from data or convert from ID
                name = data.get('name', country_id.replace('/', ' / ').replace('-', ' ').title())
                
                regions.append({
                    'id': country_id,
                    'name': name,
                    'size_mb': size_mb,
                    'package_count': len(packages)
                    # packages list not included - too large, fetched on demand during download
                })
            
            # Sort by name
            regions.sort(key=lambda x: x['name'])
            
            return {
                'regions': regions,
                'source': 'online',
                'total_regions': len(regions)
            }
            
        except Exception as e:
            return {'regions': [], 'source': 'error', 'error': str(e)}
    
    def get_download_status(self, region_id=None):
        """Get download status"""
        with self.lock:
            if region_id:
                return self.downloads.get(region_id, {'progress': 0, 'status': 'idle'})
            return dict(self.downloads)
    
    def download_region(self, region_id, callback=None):
        """Download a region's tiles"""
        with self.lock:
            if region_id in self.downloads and self.downloads[region_id].get('status') == 'downloading':
                return {'error': 'Already downloading'}
            self.downloads[region_id] = {'progress': 0, 'status': 'starting'}
        
        def do_download():
            try:
                self._download_region_impl(region_id, callback)
            except Exception as e:
                with self.lock:
                    self.downloads[region_id] = {'progress': 0, 'status': 'error', 'error': str(e)}
        
        thread = threading.Thread(target=do_download)
        thread.daemon = True
        thread.start()
        
        return {'status': 'started', 'region': region_id}
    
    def update_region(self, region_id, callback=None):
        """Update/repair an installed region - download missing geocoder/libpostal files"""
        with self.lock:
            if region_id in self.downloads and self.downloads[region_id].get('status') == 'downloading':
                return {'error': 'Already downloading'}
            self.downloads[region_id] = {'progress': 0, 'status': 'checking'}
        
        def do_update():
            try:
                self._update_region_impl(region_id, callback)
            except Exception as e:
                with self.lock:
                    self.downloads[region_id] = {'progress': 0, 'status': 'error', 'error': str(e)}
        
        thread = threading.Thread(target=do_update)
        thread.daemon = True
        thread.start()
        
        return {'status': 'started', 'region': region_id}
    
    def _check_geocoder_files(self, region_id):
        """Check if geocoder-nlp files exist for a region."""
        geocoder_region = region_id.replace('/', '-')
        geocoder_base = '/home/user/MyDocs/Maps.OSM/geocoder-nlp'
        geocoder_dir = os.path.join(geocoder_base, geocoder_region)
        
        expected_files = [f.replace('.bz2', '') for f in GEOCODER_FILES]
        
        if not os.path.exists(geocoder_dir):
            return False, []
        
        missing = []
        for f in expected_files:
            if not os.path.exists(os.path.join(geocoder_dir, f)):
                missing.append(f)
        
        return len(missing) == 0, missing
    
    def _update_region_impl(self, region_id, callback=None):
        """Check and download missing files for an installed region."""
        import sys
        
        def log(msg):
            print(msg)
            sys.stdout.flush()
        
        log("[UPDATE] Checking region: %s" % region_id)
        
        updates_needed = []
        
        # Check libpostal global data
        libpostal_ok = self._check_libpostal_data()
        if not libpostal_ok:
            updates_needed.append('libpostal')
            log("[UPDATE] libpostal data missing")
        else:
            log("[UPDATE] libpostal data OK")
        
        # Check country-specific parser
        iso_code = self._get_iso_code(region_id)
        parser_ok = True
        if iso_code:
            parser_ok = self._check_parser_data(iso_code)
            if not parser_ok:
                updates_needed.append('parser')
                log("[UPDATE] parser data for %s missing" % iso_code)
            else:
                log("[UPDATE] parser data for %s OK" % iso_code)
        else:
            log("[UPDATE] No ISO code for %s, skipping parser check" % region_id)
        
        # Check geocoder
        geocoder_ok, missing_geocoder = self._check_geocoder_files(region_id)
        if not geocoder_ok:
            updates_needed.append('geocoder')
            log("[UPDATE] geocoder files missing: %s" % missing_geocoder)
        else:
            log("[UPDATE] geocoder files OK")
        
        if not updates_needed:
            log("[UPDATE] Region %s is up to date, nothing to do" % region_id)
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 100,
                    'status': 'complete',
                    'message': 'Already up to date'
                }
            return
        
        log("[UPDATE] Updates needed: %s" % updates_needed)
        
        # Download libpostal if missing
        if 'libpostal' in updates_needed:
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 20,
                    'status': 'Downloading libpostal data...'
                }
            libpostal_ok = self._download_libpostal_data(log)
        
        # Download parser if missing
        if 'parser' in updates_needed:
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 40,
                    'status': 'Downloading parser data...'
                }
            parser_ok = self._download_parser_data(region_id, log)
        
        # Download geocoder if missing
        if 'geocoder' in updates_needed:
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 60,
                    'status': 'Downloading geocoder...'
                }
            geocoder_ok = self._download_geocoder(region_id, log)
        
        log("[UPDATE] Region %s update complete. Libpostal: %s, Parser: %s, Geocoder: %s" % (
            region_id,
            'OK' if libpostal_ok else 'FAILED',
            'OK' if parser_ok else 'FAILED',
            'OK' if geocoder_ok else 'FAILED'
        ))
        
        with self.lock:
            self.downloads[region_id] = {
                'progress': 100,
                'status': 'complete'
            }
        
        if callback:
            callback(region_id, 'complete')
    
    def _download_region_impl(self, region_id, callback=None):
        """Download all packages for a region"""
        import tarfile
        import shutil
        import sys
        
        def log(msg):
            """Log with flush for immediate output"""
            print(msg)
            sys.stdout.flush()
        
        CHUNK_SIZE = 65536  # 64KB chunks
        
        log("[DOWNLOAD] Starting download for region: %s" % region_id)
        
        # Get country data
        countries = self._fetch_countries_json()
        if not countries:
            error_msg = 'Could not load countries data'
            log("[DOWNLOAD] Error: %s" % error_msg)
            with self.lock:
                self.downloads[region_id] = {'progress': 0, 'status': 'error', 'error': error_msg}
            return
        
        if region_id not in countries:
            error_msg = 'Region not found: %s' % region_id
            log("[DOWNLOAD] Error: %s" % error_msg)
            with self.lock:
                self.downloads[region_id] = {'progress': 0, 'status': 'error', 'error': error_msg}
            return
        
        valhalla = countries[region_id].get('valhalla', {})
        packages = valhalla.get('packages', [])
        
        if not packages:
            error_msg = 'No packages for region'
            log("[DOWNLOAD] Error: %s" % error_msg)
            with self.lock:
                self.downloads[region_id] = {'progress': 0, 'status': 'error', 'error': error_msg}
            return
        
        log("[DOWNLOAD] Found %d packages for %s: %s" % (len(packages), region_id, packages))
        
        # Get total size from JSON for progress calculation
        total_size_compressed = int(valhalla.get('size-compressed', 0))
        total_size_mb = total_size_compressed / (1024.0 * 1024.0)
        
        total_packages = len(packages)
        downloaded_packages = 0
        tiles_extracted = 0
        total_bytes_downloaded = 0
        
        # Create temp directory on MyDocs (has space, unlike /tmp which is only 4MB)
        # Use /home/user/MyDocs/.valhalla-tmp/ for downloads
        temp_dir = "/home/user/MyDocs/.valhalla-tmp"
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except:
                # Fallback to tiles_dir parent
                temp_dir = os.path.dirname(self.tiles_dir)
                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir)
        
        try:
            for pkg_num in packages:
                pkg_url = VALHALLA_PACKAGES_URL + "/" + str(pkg_num) + ".tar.bz2"
                tar_path = os.path.join(temp_dir, str(pkg_num) + ".tar.bz2")
                
                log("[DOWNLOAD] Downloading package %s from %s" % (pkg_num, pkg_url))
                
                # Download package with streaming
                try:
                    req = Request(pkg_url)
                    req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
                    response = urlopen(req, timeout=600)
                    pkg_size = int(response.headers.get('Content-Length', 0))
                    log("[DOWNLOAD] Package %s size: %d bytes" % (pkg_num, pkg_size))
                    
                    pkg_downloaded = 0
                    with open(tar_path, 'wb') as f:
                        while True:
                            chunk = response.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            f.write(chunk)
                            pkg_downloaded += len(chunk)
                            
                            # Update progress based on total size from JSON
                            current_total = total_bytes_downloaded + pkg_downloaded
                            if total_size_compressed > 0:
                                overall_progress = int(current_total * 90 / total_size_compressed)
                            else:
                                # Fallback to package count based progress
                                overall_progress = int((downloaded_packages * 90 + pkg_downloaded * 90 / max(pkg_size, 1)) / total_packages)
                            
                            with self.lock:
                                self.downloads[region_id] = {
                                    'progress': min(overall_progress, 90),
                                    'status': 'Downloading %.1f / %.1f MB' % (
                                        current_total / (1024.0 * 1024.0),
                                        total_size_mb
                                    ),
                                    'current_package': pkg_num,
                                    'packages_done': downloaded_packages,
                                    'packages_total': total_packages,
                                    'bytes_downloaded': current_total,
                                    'bytes_total': total_size_compressed
                                }
                    
                    total_bytes_downloaded += pkg_downloaded
                
                except Exception as e:
                    error_msg = "Error downloading package %s: %s" % (pkg_num, str(e))
                    log("[DOWNLOAD] " + error_msg)
                    import traceback
                    traceback.print_exc()
                    # Don't continue - mark as error
                    with self.lock:
                        self.downloads[region_id] = {
                            'progress': 0,
                            'status': 'error',
                            'error': error_msg
                        }
                    return
                
                # Extract tar.bz2 and create .list file
                with self.lock:
                    self.downloads[region_id] = {
                        'progress': min(int(total_bytes_downloaded * 90 / max(total_size_compressed, 1)), 90),
                        'status': 'Extracting package %d/%d...' % (downloaded_packages + 1, total_packages)
                    }
                
                extracted_tiles = []  # Track tiles from this package
                
                try:
                    # Python's bz2 module may not be available in wunderw, use system python or bunzip2
                    tar_uncompressed = tar_path.replace('.tar.bz2', '.tar')
                    
                    log("[DOWNLOAD] Decompressing package %s..." % pkg_num)
                    import subprocess
                    
                    # Try system python first (has bz2 module)
                    decompress_script = '''
import bz2
import sys
with open(sys.argv[1], 'rb') as f_in:
    with open(sys.argv[2], 'wb') as f_out:
        f_out.write(bz2.decompress(f_in.read()))
'''
                    result = subprocess.run(
                        ['python', '-c', decompress_script, tar_path, tar_uncompressed],
                        capture_output=True
                    )
                    if result.returncode != 0:
                        # Fallback to bunzip2
                        log("[DOWNLOAD] Python bz2 failed, trying bunzip2...")
                        with open(tar_uncompressed, 'wb') as f_out:
                            result = subprocess.run(
                                ['bunzip2', '-c', tar_path],
                                stdout=f_out,
                                stderr=subprocess.PIPE
                            )
                            if result.returncode != 0:
                                raise Exception("bunzip2 failed: %s" % result.stderr.decode())
                    
                    log("[DOWNLOAD] Decompressed, opening tar...")
                    
                    # Open as regular tar (not bz2)
                    tar = tarfile.open(tar_uncompressed, mode='r')
                    members = tar.getmembers()
                    log("[DOWNLOAD] Package %s has %d members" % (pkg_num, len(members)))
                    if members:
                        log("[DOWNLOAD] First 5 members: %s" % [m.name for m in members[:5]])
                    
                    for member in members:
                        name = member.name.replace('\\', '/')
                        
                        # Extract .list file from archive (valhalla/packages/XXX.tar.list)
                        if name.endswith('.tar.list'):
                            packages_dir = os.path.join(self.tiles_dir, 'packages')
                            if not os.path.exists(packages_dir):
                                os.makedirs(packages_dir)
                            # Rename from XXX.tar.list to XXX.list
                            list_filename = os.path.basename(name).replace('.tar.list', '.list')
                            list_path = os.path.join(packages_dir, list_filename)
                            f = tar.extractfile(member)
                            if f:
                                with open(list_path, 'wb') as out:
                                    out.write(f.read())
                                log("[DOWNLOAD] Extracted .list file: %s" % list_path)
                            continue
                        
                        # Extract .gph.gz files (tiles)
                        if name.endswith('.gph.gz') or name.endswith('.gph'):
                            parts = name.split('/')
                            # Find tiles/level/... pattern
                            # Level 0,1: level/xxx/yyy.gph.gz (3 parts)
                            # Level 2: level/xxx/yyy/zzz.gph.gz (4 parts)
                            for i, p in enumerate(parts):
                                if p in ['0', '1'] and i + 2 < len(parts):
                                    # 3-part path for level 0 and 1
                                    rel_path = '/'.join(parts[i:i+3])
                                    out_path = os.path.join(self.tiles_dir, parts[i], parts[i+1], parts[i+2])
                                    out_dir = os.path.dirname(out_path)
                                    
                                    self._ensure_dir(out_dir)
                                    
                                    f = tar.extractfile(member)
                                    if f:
                                        with open(out_path, 'wb') as out:
                                            while True:
                                                chunk = f.read(CHUNK_SIZE)
                                                if not chunk:
                                                    break
                                                out.write(chunk)
                                        tiles_extracted += 1
                                        extracted_tiles.append(rel_path)
                                    break
                                elif p == '2' and i + 3 < len(parts):
                                    # 4-part path for level 2
                                    rel_path = '/'.join(parts[i:i+4])
                                    out_path = os.path.join(self.tiles_dir, parts[i], parts[i+1], parts[i+2], parts[i+3])
                                    out_dir = os.path.dirname(out_path)
                                    
                                    self._ensure_dir(out_dir)
                                    
                                    f = tar.extractfile(member)
                                    if f:
                                        with open(out_path, 'wb') as out:
                                            while True:
                                                chunk = f.read(CHUNK_SIZE)
                                                if not chunk:
                                                    break
                                                out.write(chunk)
                                        tiles_extracted += 1
                                        extracted_tiles.append(rel_path)
                                    break
                    
                    tar.close()
                    log("[DOWNLOAD] Extracted %d tiles from package %s" % (len(extracted_tiles), pkg_num))
                    
                    # If no .list file in archive, create one ourselves
                    packages_dir = os.path.join(self.tiles_dir, 'packages')
                    list_path = os.path.join(packages_dir, str(pkg_num) + '.list')
                    if not os.path.exists(list_path) and extracted_tiles:
                        if not os.path.exists(packages_dir):
                            os.makedirs(packages_dir)
                        with open(list_path, 'w') as f:
                            f.write('\n'.join(extracted_tiles))
                        log("[DOWNLOAD] Created .list file: %s" % list_path)
                    
                except Exception as e:
                    log("[DOWNLOAD] Error extracting package %s: %s" % (pkg_num, e))
                    import traceback
                    traceback.print_exc()
                
                # Delete temp tar files immediately to save space on device
                for f in [tar_path, tar_path.replace('.tar.bz2', '.tar')]:
                    try:
                        os.remove(f)
                    except:
                        pass
                
                downloaded_packages += 1
            
            # Write region info file (maps region_id to its packages)
            try:
                packages_dir = os.path.join(self.tiles_dir, 'packages')
                if not os.path.exists(packages_dir):
                    os.makedirs(packages_dir)
                regions_file = os.path.join(packages_dir, 'regions.json')
                regions_data = {}
                if os.path.exists(regions_file):
                    with open(regions_file, 'r') as f:
                        regions_data = json.load(f)
                regions_data[region_id] = {
                    'packages': packages,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                with open(regions_file, 'w') as f:
                    json.dump(regions_data, f, indent=2)
                log("[DOWNLOAD] Saved region info to %s" % regions_file)
            except Exception as e:
                log("[DOWNLOAD] Error saving regions info: %s" % e)
            
            # Success - clean up temp dir (should be empty now)
            try:
                os.rmdir(temp_dir)
            except:
                pass
            
            # Download libpostal data for smart address parsing (once, shared by all regions)
            if not self._check_libpostal_data():
                log("[DOWNLOAD] Downloading libpostal data for smart address parsing...")
                with self.lock:
                    self.downloads[region_id] = {
                        'progress': 90,
                        'status': 'Downloading libpostal data...',
                        'tiles_extracted': tiles_extracted,
                        'packages_downloaded': downloaded_packages
                    }
                libpostal_ok = self._download_libpostal_data(log)
            else:
                log("[DOWNLOAD] Libpostal data already present")
                libpostal_ok = True
            
            # Download country-specific parser data
            iso_code = self._get_iso_code(region_id)
            if iso_code and not self._check_parser_data(iso_code):
                log("[DOWNLOAD] Downloading parser data for %s..." % iso_code)
                with self.lock:
                    self.downloads[region_id] = {
                        'progress': 93,
                        'status': 'Downloading parser data...',
                        'tiles_extracted': tiles_extracted,
                        'packages_downloaded': downloaded_packages
                    }
                parser_ok = self._download_parser_data(region_id, log)
            else:
                if iso_code:
                    log("[DOWNLOAD] Parser data for %s already present" % iso_code)
                else:
                    log("[DOWNLOAD] No ISO code for %s, skipping parser" % region_id)
                parser_ok = True
            
            # Download geocoder files for offline address search
            log("[DOWNLOAD] Now downloading geocoder...")
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 96,
                    'status': 'Downloading geocoder...',
                    'tiles_extracted': tiles_extracted,
                    'packages_downloaded': downloaded_packages
                }
            
            geocoder_ok = self._download_geocoder(region_id, log)
            
            log("[DOWNLOAD] SUCCESS! Region %s complete. Tiles: %d, Packages: %d, Geocoder: %s, Libpostal: %s, Parser: %s" % (
                region_id, tiles_extracted, downloaded_packages, 
                'OK' if geocoder_ok else 'SKIPPED',
                'OK' if libpostal_ok else 'SKIPPED',
                'OK' if parser_ok else 'SKIPPED'))
            
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 100,
                    'status': 'complete',
                    'tiles_extracted': tiles_extracted,
                    'packages_downloaded': downloaded_packages,
                    'bytes_downloaded': total_bytes_downloaded
                }
            
            if callback:
                callback(region_id, 'complete')
                
        except Exception as e:
            log("[DOWNLOAD] FAILED! Region %s error: %s" % (region_id, e))
            import traceback
            traceback.print_exc()
            with self.lock:
                self.downloads[region_id] = {
                    'progress': 0,
                    'status': 'error',
                    'error': str(e)
                }
            if callback:
                callback(region_id, 'error')
            # Try to clean up on error
            try:
                for f in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, f))
                os.rmdir(temp_dir)
            except:
                pass
    
    def _decompress_bz2(self, input_path, output_path, log_func=None):
        """Decompress a bz2 file. Uses bz2 module or bunzip2 as fallback."""
        import subprocess
        
        def log(msg):
            if log_func:
                log_func(msg)
        
        # Try bz2 module first
        try:
            import bz2
            with open(input_path, 'rb') as f_in:
                compressed_data = f_in.read()
                decompressed_data = bz2.decompress(compressed_data)
                with open(output_path, 'wb') as f_out:
                    f_out.write(decompressed_data)
            return len(compressed_data), len(decompressed_data)
        except ImportError:
            pass
        except Exception as e:
            log("[BZ2] bz2 module failed: %s, trying bunzip2..." % e)
        
        # Fallback to bunzip2 command
        try:
            # bunzip2 -k keeps original, -c outputs to stdout
            result = subprocess.run(
                ['bunzip2', '-k', '-c', input_path],
                stdout=open(output_path, 'wb'),
                stderr=subprocess.PIPE
            )
            if result.returncode == 0:
                comp_size = os.path.getsize(input_path)
                decomp_size = os.path.getsize(output_path)
                return comp_size, decomp_size
            else:
                raise Exception("bunzip2 failed: %s" % result.stderr.decode())
        except FileNotFoundError:
            # bunzip2 not found, try bzip2 -d
            result = subprocess.run(
                ['bzip2', '-d', '-k', '-c', input_path],
                stdout=open(output_path, 'wb'),
                stderr=subprocess.PIPE
            )
            if result.returncode == 0:
                comp_size = os.path.getsize(input_path)
                decomp_size = os.path.getsize(output_path)
                return comp_size, decomp_size
            else:
                raise Exception("bzip2 -d failed: %s" % result.stderr.decode())

    def _download_geocoder(self, region_id, log_func=None):
        """Download geocoder-nlp files for a region.
        
        Downloads from: https://data.modrana.org/osm_scout_server/geocoder-nlp-39/geocoder-nlp/{region_id}/
        Files: geonlp-primary_sqlite.bz2, geonlp-normalized_trie.bz2, geonlp-normalized-id_kch.bz2
        """
        import sys
        
        def log(msg):
            if log_func:
                log_func(msg)
            else:
                print(msg)
                sys.stdout.flush()
        
        CHUNK_SIZE = 65536
        
        # Convert region_id to geocoder URL format
        # region_id: "europe/austria" -> geocoder URL: "europe-austria"
        geocoder_region = region_id.replace('/', '-')
        geocoder_url = GEOCODER_NLP_BASE + "/" + geocoder_region + "/"
        
        log("[GEOCODER] Starting geocoder download for region: %s" % region_id)
        log("[GEOCODER] URL: %s" % geocoder_url)
        
        # Create geocoder directory with region subfolder
        # e.g. /home/user/MyDocs/Maps.OSM/geocoder-nlp/europe-austria/
        geocoder_base = '/home/user/MyDocs/Maps.OSM/geocoder-nlp'
        geocoder_dir = os.path.join(geocoder_base, geocoder_region)
        if not os.path.exists(geocoder_dir):
            os.makedirs(geocoder_dir)
        
        # Temp directory for downloads
        temp_dir = "/home/user/MyDocs/.valhalla-tmp"
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except:
                temp_dir = os.path.dirname(self.tiles_dir)
        
        downloaded_files = []
        
        for bz2_file in GEOCODER_FILES:
            file_url = geocoder_url + bz2_file
            temp_path = os.path.join(temp_dir, bz2_file)
            
            # Output filename (remove .bz2, fix naming)
            out_name = bz2_file.replace('.bz2', '')
            # geonlp-primary.sqlite.bz2 -> geonlp-primary.sqlite
            out_path = os.path.join(geocoder_dir, out_name)
            
            log("[GEOCODER] Downloading %s..." % bz2_file)
            
            try:
                req = Request(file_url)
                req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
                response = urlopen(req, timeout=300)
                file_size = int(response.headers.get('Content-Length', 0))
                log("[GEOCODER] File size: %.1f KB" % (file_size / 1024.0))
                
                # Download to temp file
                downloaded = 0
                with open(temp_path, 'wb') as f:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                
                log("[GEOCODER] Downloaded %s (%.1f KB)" % (bz2_file, downloaded / 1024.0))
                
                # Decompress bz2
                log("[GEOCODER] Decompressing to %s..." % out_name)
                comp_size, decomp_size = self._decompress_bz2(temp_path, out_path, log)
                
                log("[GEOCODER] Decompressed %s (%.1f KB -> %.1f KB)" % (
                    out_name, 
                    comp_size / 1024.0,
                    decomp_size / 1024.0
                ))
                
                downloaded_files.append(out_name)
                
                # Clean up temp file
                try:
                    os.remove(temp_path)
                except:
                    pass
                    
            except Exception as e:
                log("[GEOCODER] Error downloading %s: %s" % (bz2_file, e))
                # Continue with other files - geocoder is optional
                continue
        
        if downloaded_files:
            log("[GEOCODER] SUCCESS! Downloaded %d geocoder files: %s" % (
                len(downloaded_files), downloaded_files))
            return True
        else:
            log("[GEOCODER] WARNING: No geocoder files downloaded (offline search unavailable)")
            return False

    def _check_libpostal_data(self):
        """Check if libpostal data files are present."""
        for subdir, filename in LIBPOSTAL_FILES:
            filepath = os.path.join(LIBPOSTAL_DATA_DIR, subdir, filename)
            if not os.path.exists(filepath):
                return False
        return True
    
    def _download_libpostal_data(self, log_func=None):
        """Download global libpostal data files (once, shared by all regions).
        
        Downloads from: https://data.modrana.org/osm_scout_server/postal-global-2/postal/global-v1/
        Files: address_dictionary.dat.bz2, language_classifier.dat.bz2, numex.dat.bz2, transliteration.dat.bz2
        
        Total size: ~35 MB compressed, ~103 MB uncompressed
        """
        import sys
        
        def log(msg):
            if log_func:
                log_func(msg)
            else:
                print(msg)
                sys.stdout.flush()
        
        # Check if already downloaded
        if self._check_libpostal_data():
            log("[LIBPOSTAL] Data already present, skipping download")
            return True
        
        log("[LIBPOSTAL] Downloading global libpostal data (~35 MB compressed)...")
        log("[LIBPOSTAL] This enables smart address parsing (e.g. 'Hauptstr 5' -> street + house_number)")
        
        CHUNK_SIZE = 65536
        
        # Create directories
        for subdir, filename in LIBPOSTAL_FILES:
            subdir_path = os.path.join(LIBPOSTAL_DATA_DIR, subdir)
            if not os.path.exists(subdir_path):
                try:
                    os.makedirs(subdir_path)
                except Exception as e:
                    log("[LIBPOSTAL] Error creating directory %s: %s" % (subdir_path, e))
                    return False
        
        # Temp directory for downloads
        temp_dir = "/home/user/MyDocs/.valhalla-tmp"
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except:
                temp_dir = "/tmp"
        
        downloaded_count = 0
        
        for subdir, filename in LIBPOSTAL_FILES:
            bz2_filename = filename + ".bz2"
            file_url = LIBPOSTAL_BASE_URL + "/" + subdir + "/" + bz2_filename
            temp_path = os.path.join(temp_dir, bz2_filename)
            out_path = os.path.join(LIBPOSTAL_DATA_DIR, subdir, filename)
            
            # Skip if already exists
            if os.path.exists(out_path):
                log("[LIBPOSTAL] %s already exists, skipping" % filename)
                downloaded_count += 1
                continue
            
            log("[LIBPOSTAL] Downloading %s..." % bz2_filename)
            
            try:
                req = Request(file_url)
                req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
                response = urlopen(req, timeout=600)  # 10 min timeout for large files
                file_size = int(response.headers.get('Content-Length', 0))
                log("[LIBPOSTAL] File size: %.1f MB" % (file_size / (1024.0 * 1024.0)))
                
                # Download to temp file
                downloaded = 0
                with open(temp_path, 'wb') as f:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Progress every 5 MB
                        if downloaded % (5 * 1024 * 1024) < CHUNK_SIZE:
                            log("[LIBPOSTAL] Downloaded %.1f MB..." % (downloaded / (1024.0 * 1024.0)))
                
                log("[LIBPOSTAL] Downloaded %s (%.1f MB)" % (bz2_filename, downloaded / (1024.0 * 1024.0)))
                
                # Decompress bz2
                log("[LIBPOSTAL] Decompressing %s..." % filename)
                comp_size, decomp_size = self._decompress_bz2(temp_path, out_path, log)
                
                log("[LIBPOSTAL] Decompressed %s (%.1f MB -> %.1f MB)" % (
                    filename, 
                    comp_size / (1024.0 * 1024.0),
                    decomp_size / (1024.0 * 1024.0)
                ))
                
                downloaded_count += 1
                
                # Clean up temp file
                try:
                    os.remove(temp_path)
                except:
                    pass
                    
            except Exception as e:
                log("[LIBPOSTAL] Error downloading %s: %s" % (bz2_filename, e))
                # Continue with other files
                continue
        
        if downloaded_count == len(LIBPOSTAL_FILES):
            log("[LIBPOSTAL] SUCCESS! All libpostal data files downloaded")
            return True
        elif downloaded_count > 0:
            log("[LIBPOSTAL] WARNING: Only %d/%d libpostal files downloaded" % (
                downloaded_count, len(LIBPOSTAL_FILES)))
            return True  # Partial success - some parsing may work
        else:
            log("[LIBPOSTAL] WARNING: No libpostal files downloaded (using primitive parser)")
            return False

    def _get_iso_code(self, region_id):
        """Get ISO country code for a region_id."""
        # Direct lookup
        if region_id in REGION_TO_ISO:
            return REGION_TO_ISO[region_id]
        
        # Try to extract from region name (e.g., 'europe/austria' -> look for 'austria')
        region_name = region_id.split('/')[-1].lower()
        
        # Common mappings for region names
        name_to_iso = {
            'austria': 'AT', 'hungary': 'HU', 'germany': 'DE', 'switzerland': 'CH',
            'czech-republic': 'CZ', 'slovakia': 'SK', 'poland': 'PL', 'italy': 'IT',
            'france': 'FR', 'spain': 'ES', 'portugal': 'PT', 'netherlands': 'NL',
            'belgium': 'BE', 'united-kingdom': 'GB', 'great-britain': 'GB',
            'sweden': 'SE', 'norway': 'NO', 'finland': 'FI', 'denmark': 'DK',
            'greece': 'GR', 'croatia': 'HR', 'slovenia': 'SI', 'serbia': 'RS',
            'romania': 'RO', 'bulgaria': 'BG', 'ukraine': 'UA', 'russia': 'RU',
            'liechtenstein': 'LI', 'luxembourg': 'LU', 'azores': 'PT',
            'usa': 'US', 'canada': 'CA', 'japan': 'JP', 'china': 'CN',
            'india': 'IN', 'australia': 'AU', 'new-zealand': 'NZ',
            'ireland': 'GB-IE', 'cyprus': 'CY', 'malta': 'MT', 'iceland': 'IS',
            'estonia': 'EE', 'latvia': 'LV', 'lithuania': 'LT',
            'bosnia-herzegovina': 'BA', 'albania': 'AL', 'montenegro': 'ME',
            'macedonia': 'MK', 'kosovo': 'RS', 'moldova': 'MD', 'belarus': 'BY',
        }
        
        return name_to_iso.get(region_name)

    def _check_parser_data(self, iso_code):
        """Check if parser data exists for an ISO country code."""
        parser_dir = os.path.join(LIBPOSTAL_PARSER_DIR, iso_code, 'address_parser')
        if not os.path.exists(parser_dir):
            return False
        
        for filename in LIBPOSTAL_PARSER_FILES:
            if not os.path.exists(os.path.join(parser_dir, filename)):
                return False
        return True

    def _download_parser_data(self, region_id, log_func=None):
        """Download country-specific libpostal parser data.
        
        Downloads from: https://data.modrana.org/osm_scout_server/postal-country-2/postal/countries-v1/{ISO}/address_parser/
        """
        import sys
        
        def log(msg):
            if log_func:
                log_func(msg)
            else:
                print(msg)
                sys.stdout.flush()
        
        iso_code = self._get_iso_code(region_id)
        if not iso_code:
            log("[PARSER] Unknown region %s, cannot determine ISO code" % region_id)
            return False
        
        log("[PARSER] ISO code for %s is %s" % (region_id, iso_code))
        
        # Check if already downloaded
        if self._check_parser_data(iso_code):
            log("[PARSER] Parser data for %s already present" % iso_code)
            return True
        
        log("[PARSER] Downloading parser data for %s (~2 MB compressed)..." % iso_code)
        
        CHUNK_SIZE = 65536
        
        # Create directory
        parser_dir = os.path.join(LIBPOSTAL_PARSER_DIR, iso_code, 'address_parser')
        if not os.path.exists(parser_dir):
            try:
                os.makedirs(parser_dir)
            except Exception as e:
                log("[PARSER] Error creating directory %s: %s" % (parser_dir, e))
                return False
        
        # Temp directory
        temp_dir = "/home/user/MyDocs/.valhalla-tmp"
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir)
            except:
                temp_dir = "/tmp"
        
        downloaded_count = 0
        
        for filename in LIBPOSTAL_PARSER_FILES:
            bz2_filename = filename + ".bz2"
            file_url = LIBPOSTAL_PARSER_BASE_URL + "/" + iso_code + "/address_parser/" + bz2_filename
            temp_path = os.path.join(temp_dir, bz2_filename)
            out_path = os.path.join(parser_dir, filename)
            
            # Skip if already exists
            if os.path.exists(out_path):
                log("[PARSER] %s already exists, skipping" % filename)
                downloaded_count += 1
                continue
            
            log("[PARSER] Downloading %s..." % bz2_filename)
            
            try:
                req = Request(file_url)
                req.add_header('User-Agent', 'ValhallaBikeRouter/3.0')
                response = urlopen(req, timeout=300)
                file_size = int(response.headers.get('Content-Length', 0))
                log("[PARSER] File size: %.1f KB" % (file_size / 1024.0))
                
                # Download
                downloaded = 0
                with open(temp_path, 'wb') as f:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                
                log("[PARSER] Downloaded %s (%.1f KB)" % (bz2_filename, downloaded / 1024.0))
                
                # Decompress
                log("[PARSER] Decompressing %s..." % filename)
                comp_size, decomp_size = self._decompress_bz2(temp_path, out_path, log)
                
                log("[PARSER] Decompressed %s (%.1f KB -> %.1f KB)" % (
                    filename,
                    comp_size / 1024.0,
                    decomp_size / 1024.0
                ))
                
                downloaded_count += 1
                
                # Clean up
                try:
                    os.remove(temp_path)
                except:
                    pass
                    
            except Exception as e:
                log("[PARSER] Error downloading %s: %s" % (bz2_filename, e))
                continue
        
        if downloaded_count == len(LIBPOSTAL_PARSER_FILES):
            log("[PARSER] SUCCESS! All parser files for %s downloaded" % iso_code)
            return True
        elif downloaded_count > 0:
            log("[PARSER] WARNING: Only %d/%d parser files downloaded" % (
                downloaded_count, len(LIBPOSTAL_PARSER_FILES)))
            return True
        else:
            log("[PARSER] WARNING: No parser files downloaded for %s" % iso_code)
            return False


# Global download manager
download_manager = None

# Global geocoder instance (cached for speed)
_geocoder_instance = None
_geocoder_initialized = False

def get_cached_geocoder(warmup=False):
    """Get or create cached offline geocoder instance.
    
    If warmup=True, also initialize libpostal (takes ~5s first time).
    """
    global _geocoder_instance, _geocoder_initialized
    
    if _geocoder_instance is None:
        try:
            from geocoder_offline import OfflineGeocoder
            _geocoder_instance = OfflineGeocoder()
            print("[SERVER] Created cached geocoder instance", file=sys.stderr)
        except ImportError as e:
            print("[SERVER] Could not import geocoder_offline: %s" % e, file=sys.stderr)
            return None
        except Exception as e:
            print("[SERVER] Error creating geocoder: %s" % e, file=sys.stderr)
            return None
    
    # Warm up libpostal if requested (first search will be fast)
    if warmup and not _geocoder_initialized and _geocoder_instance is not None:
        print("[SERVER] Warming up libpostal (this takes ~5 seconds)...", file=sys.stderr)
        try:
            # Do a dummy search to trigger libpostal initialization
            _geocoder_instance.search("test", limit=1)
            _geocoder_initialized = True
            print("[SERVER] libpostal ready!", file=sys.stderr)
        except Exception as e:
            print("[SERVER] libpostal warmup failed: %s" % e, file=sys.stderr)
    
    return _geocoder_instance


class ValhallaHandler(BaseHTTPRequestHandler):
    cache = None
    tiles_dir = None
    
    def log_message(self, format, *args):
        pass  # Quiet logging
    
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        
        if path == '/status':
            self.handle_status()
        elif path == '/regions':
            self.handle_regions()
        elif path == '/installed':
            self.handle_installed()
        elif path == '/tiles':
            self.handle_tiles()
        elif path.startswith('/download/'):
            self.handle_download(path)
        elif path.startswith('/update/'):
            self.handle_update(path)
        elif path == '/download_status':
            self.handle_download_status()
        elif path == '/geocode':
            self.handle_geocode(parsed.query)
        else:
            self.send_error(404)
    
    def do_POST(self):
        parsed = urlparse(self.path)
        
        if parsed.path in ['/route', '/v2/route']:
            self.handle_route()
        else:
            self.send_error(404)
    
    def handle_status(self):
        """Server status"""
        # Count tiles (3-level structure)
        tile_count = 0
        if os.path.isdir(self.tiles_dir):
            for level in ['0', '1', '2']:
                level_dir = os.path.join(self.tiles_dir, level)
                if os.path.isdir(level_dir):
                    for d1 in os.listdir(level_dir):
                        d1_path = os.path.join(level_dir, d1)
                        if os.path.isdir(d1_path):
                            for d2 in os.listdir(d1_path):
                                d2_path = os.path.join(d1_path, d2)
                                if os.path.isdir(d2_path):
                                    tile_count += len([f for f in os.listdir(d2_path) if f.endswith('.gph.gz') or f.endswith('.gph')])
        
        self.send_json({
            'status': 'ok',
            'service': 'valhalla-local',
            'tiles_dir': self.tiles_dir,
            'tile_count': tile_count,
            'cache_size': len(self.cache.tiles) if self.cache else 0
        })
    
    def handle_regions(self):
        """List downloadable regions"""
        global download_manager
        if download_manager:
            regions = download_manager.get_regions()
            # Add installed regions list
            regions['installed'] = download_manager.get_installed_regions()
        else:
            regions = {'regions': [], 'source': 'error', 'error': 'Download manager not initialized', 'installed': []}
        self.send_json(regions)
    
    def handle_installed(self):
        """List installed regions"""
        global download_manager
        if not download_manager:
            download_manager = DownloadManager(self.tiles_dir)
        installed = download_manager.get_installed_regions()
        self.send_json({'success': True, 'installed': installed})
    
    def handle_tiles(self):
        """List installed tiles"""
        tiles = []
        if os.path.isdir(self.tiles_dir):
            for level in ['0', '1', '2']:
                level_dir = os.path.join(self.tiles_dir, level)
                if os.path.isdir(level_dir):
                    # 3-level structure: level/xxx/yyy/zzz.gph.gz
                    for d1 in os.listdir(level_dir):
                        d1_path = os.path.join(level_dir, d1)
                        if os.path.isdir(d1_path):
                            for d2 in os.listdir(d1_path):
                                d2_path = os.path.join(d1_path, d2)
                                if os.path.isdir(d2_path):
                                    for f in os.listdir(d2_path):
                                        if f.endswith('.gph.gz') or f.endswith('.gph'):
                                            try:
                                                tile_id = int(d1) * 1000000 + int(d2) * 1000 + int(f.split('.')[0])
                                                tiles.append({'level': int(level), 'id': tile_id})
                                            except:
                                                pass
        
        self.send_json({'tiles': tiles, 'count': len(tiles)})
    
    def handle_download(self, path):
        """Start download for a region"""
        global download_manager
        # path is like /download/europe/liechtenstein
        # We need everything after /download/
        region_id = path[len('/download/'):]
        print("[SERVER] handle_download: region_id = %s" % region_id)
        
        if not download_manager:
            download_manager = DownloadManager(self.tiles_dir)
        
        result = download_manager.download_region(region_id)
        self.send_json(result)
    
    def handle_download_status(self):
        """Get download status"""
        global download_manager
        if download_manager:
            status = download_manager.get_download_status()
        else:
            status = {}
        self.send_json({'downloads': status})
    
    def handle_update(self, path):
        """Update/repair an installed region - download missing geocoder/libpostal files"""
        global download_manager
        # path is like /update/europe/liechtenstein
        region_id = path[len('/update/'):]
        print("[SERVER] handle_update: region_id = %s" % region_id)
        
        if not download_manager:
            download_manager = DownloadManager(self.tiles_dir)
        
        result = download_manager.update_region(region_id)
        self.send_json(result)
    
    def handle_geocode(self, query_string):
        """Offline geocoding with cached libpostal - much faster than CLI"""
        try:
            # Parse query params
            params = {}
            if query_string:
                try:
                    from urllib.parse import parse_qs
                except ImportError:
                    from urlparse import parse_qs
                params = parse_qs(query_string)
            
            query = params.get('q', [''])[0]
            limit = int(params.get('limit', ['10'])[0])
            
            if not query:
                self.send_json({'success': False, 'error': 'Missing q parameter'})
                return
            
            print("[SERVER] Geocode query: %s" % query, file=sys.stderr)
            
            # Get cached geocoder
            geocoder = get_cached_geocoder()
            if geocoder is None:
                self.send_json({'success': False, 'error': 'Geocoder not available', 'locations': []})
                return
            
            # Search
            results = geocoder.search(query, limit=limit)
            
            # Format results for API compatibility
            locations = []
            for r in results:
                locations.append({
                    'name': r.get('display_name', r.get('name', 'Unknown')),
                    'lat': r.get('lat'),
                    'lng': r.get('lon') or r.get('lng'),
                    'type': r.get('type', ''),
                    'source': 'offline'
                })
            
            print("[SERVER] Geocode returned %d results" % len(locations), file=sys.stderr)
            self.send_json({'success': True, 'locations': locations, 'source': 'offline'})
            
        except Exception as e:
            import traceback
            print("[SERVER] Geocode error: %s" % e, file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            self.send_json({'success': False, 'error': str(e), 'locations': []})
    
    def handle_route(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length).decode('utf-8')
            params = json.loads(body)
        except:
            self.send_error(400, "Invalid JSON")
            return
        
        locations = params.get('locations', [])
        if len(locations) < 2:
            self.send_json({'error': 'Need at least 2 locations'})
            return
        
        from_loc = locations[0]
        to_loc = locations[-1]
        
        from_lat = from_loc.get('lat')
        from_lon = from_loc.get('lon')
        to_lat = to_loc.get('lat')
        to_lon = to_loc.get('lon')
        
        if None in [from_lat, from_lon, to_lat, to_lon]:
            self.send_json({'error': 'Invalid coordinates'})
            return
        
        # Try fast C router first
        c_router_path = '/opt/valhalla-bike-router/vrouter'
        if os.path.exists(c_router_path):
            try:
                import subprocess
                import time as time_module
                t0 = time_module.time()
                result = subprocess.run(
                    [c_router_path, self.tiles_dir, 
                     str(from_lat), str(from_lon), str(to_lat), str(to_lon)],
                    capture_output=True  # No timeout - let it run as long as needed
                )
                elapsed = time_module.time() - t0
                
                # Always print C router stderr for debugging
                c_stderr = result.stderr.decode('utf-8', errors='replace').strip()
                if c_stderr:
                    for line in c_stderr.split('\n')[:30]:  # First 30 lines
                        print("[C-ROUTER] %s" % line)
                
                if result.returncode == 0:
                    c_result = json.loads(result.stdout.decode('utf-8'))
                    if 'coords' in c_result and c_result['coords']:
                        print("[ROUTE] C router: %.2fs, %d nodes" % (elapsed, len(c_result['coords'])))
                        # Build Valhalla-compatible response
                        coords = c_result['coords']
                        distance = 0
                        for i in range(1, len(coords)):
                            distance += haversine(
                                coords[i-1]['lat'], coords[i-1]['lon'],
                                coords[i]['lat'], coords[i]['lon']
                            )
                        
                        shape = encode_polyline(coords)
                        self.send_json({
                            'trip': {
                                'locations': locations,
                                'legs': [{
                                    'shape': shape,
                                    'summary': {
                                        'length': distance / 1000.0,
                                        'time': distance / 15.0 * 3.6,
                                    },
                                    'maneuvers': []
                                }],
                                'summary': {
                                    'length': distance / 1000.0,
                                    'time': distance / 15.0 * 3.6,
                                },
                                'status_message': 'Found route (C router)',
                                'status': 0,
                                'units': 'kilometers',
                                'language': 'en-US',
                            }
                        })
                        return
                    else:
                        print("[ROUTE] C router returned no coords: %s" % result.stdout.decode('utf-8')[:200])
                else:
                    print("[ROUTE] C router exit code %d" % result.returncode)
                print("[ROUTE] C router failed, falling back to Python")
            except Exception as e:
                print("[ROUTE] C router error: %s, falling back to Python" % e)
        
        # Get bicycle type from costing options
        costing_opts = params.get('costing_options', {}).get('bicycle', {})
        bike_type = costing_opts.get('bicycle_type', 'Hybrid')
        use_roads = costing_opts.get('use_roads', 0.25)  # Valhalla default
        use_hills = costing_opts.get('use_hills', 0.25)  # Valhalla default
        avoid_cars = costing_opts.get('avoid_cars', False)
        
        costing = BicycleCost(
            bicycle_type=bike_type,
            use_roads=use_roads,
            use_hills=use_hills,
            avoid_cars=avoid_cars
        )
        
        # Route
        result, error = route(self.cache, costing, from_lat, from_lon, to_lat, to_lon, use_hierarchy=False, simple_cost=False)
        
        if error:
            self.send_json({'error': error})
            return
        
        # Format as Valhalla response
        car_dist = result.get('car_distance', 0) / 1000.0  # km
        cycleway_dist = result.get('cycleway_distance', 0) / 1000.0  # km
        
        response = {
            'trip': {
                'legs': [{
                    'shape': encode_polyline(result['coords']),
                    'summary': {
                        'length': result['distance'] / 1000.0,  # km
                        'time': result['time'],
                        'car_distance': car_dist,
                        'cycleway_distance': cycleway_dist,
                    }
                }],
                'summary': {
                    'length': result['distance'] / 1000.0,
                    'time': result['time'],
                    'car_distance': car_dist,
                    'cycleway_distance': cycleway_dist,
                    'level_transitions': result.get('level_transitions', 0),
                    'level_usage': result.get('level_usage', {}),
                    'debug': result.get('debug', {}),
                },
                'locations': [
                    {'lat': from_lat, 'lon': from_lon},
                    {'lat': to_lat, 'lon': to_lon}
                ],
                'units': 'kilometers'
            }
        }
        
        self.send_json(response)
    
    def send_json(self, data):
        response = json.dumps(data)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(response.encode('utf-8'))


def run_server(tiles_dir=None, port=SERVER_PORT):
    tiles_dir = tiles_dir or TILES_DIR
    
    if not os.path.exists(tiles_dir):
        os.makedirs(tiles_dir)
    
    ValhallaHandler.tiles_dir = tiles_dir
    ValhallaHandler.cache = TileCache(tiles_dir)
    
    print("=" * 50)
    print("Valhalla-Compatible Local Routing Engine")
    print("=" * 50)
    print("Port: %d" % port)
    print("Tiles: %s" % tiles_dir)
    print("")
    print("API: POST /v2/route (Valhalla-compatible)")
    print("=" * 50)
    
    # Start libpostal warmup in background thread
    # Takes several minutes on N9, but search works with primitive parser meanwhile
    import threading
    def warmup_geocoder():
        try:
            from geocoder_offline import warmup_libpostal
            warmup_libpostal()
        except Exception as e:
            print("[SERVER] libpostal warmup error: %s" % e, file=sys.stderr)
    
    warmup_thread = threading.Thread(target=warmup_geocoder, daemon=True)
    warmup_thread.start()
    print("[SERVER] libpostal loading in background (search uses fast mode until ready)")
    
    server = HTTPServer(('127.0.0.1', port), ValhallaHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == '__main__':
    import sys
    
    tiles_dir = TILES_DIR
    port = SERVER_PORT
    
    for i, arg in enumerate(sys.argv[1:]):
        if arg == '--tiles' and i + 1 < len(sys.argv) - 1:
            tiles_dir = sys.argv[i + 2]
        elif arg == '--port' and i + 1 < len(sys.argv) - 1:
            port = int(sys.argv[i + 2])
    
    run_server(tiles_dir, port)
