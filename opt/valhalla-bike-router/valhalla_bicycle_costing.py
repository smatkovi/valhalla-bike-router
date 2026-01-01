#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Valhalla Bicycle Costing - Exact Python port of bicyclecost.cc
Based on: https://github.com/valhalla/valhalla/blob/master/src/sif/bicyclecost.cc
"""

import math

# ============================================================================
# Constants from bicyclecost.cc
# ============================================================================

# Bicycle types
ROAD = 0
CROSS = 1
HYBRID = 2
MOUNTAIN = 3

# Default cycling speed on smooth, flat roads (KPH)
kDefaultCyclingSpeed = [25.0, 20.0, 18.0, 16.0]  # Road, Cross, Hybrid, Mountain

kDismountSpeed = 5.1

# Default options
kDefaultUseRoad = 0.25
kDefaultUseHills = 0.25
kDefaultAvoidBadSurfaces = 0.25

# Surface types
SURFACE_PAVED = 0
SURFACE_PAVED_ROUGH = 1
SURFACE_COMPACTED = 2
SURFACE_GRAVEL = 3
SURFACE_DIRT = 4
SURFACE_MUD = 5
SURFACE_PATH = 6
SURFACE_IMPASSABLE = 7

# Speed factors based on surface types (for each bicycle type)
kRoadSurfaceSpeedFactors = [1.0, 1.0, 0.9, 0.6, 0.5, 0.3, 0.2, 0.0]
kHybridSurfaceSpeedFactors = [1.0, 1.0, 1.0, 0.8, 0.6, 0.4, 0.25, 0.0]
kCrossSurfaceSpeedFactors = [1.0, 1.0, 1.0, 0.8, 0.7, 0.5, 0.4, 0.0]
kMountainSurfaceSpeedFactors = [1.0, 1.0, 1.0, 1.0, 0.9, 0.75, 0.55, 0.0]

# Surface penalty factors
kSurfaceFactors = [1.0, 2.5, 4.5, 7.0]

# Worst allowed surface based on bicycle type
kWorstAllowedSurface = [SURFACE_COMPACTED, SURFACE_GRAVEL, SURFACE_DIRT, SURFACE_PATH]

# Road classification penalty factors
# Motorway, Trunk, Primary, Secondary, Tertiary, Unclassified, Residential, Service
kRoadClassFactor = [1.0, 0.4, 0.2, 0.1, 0.05, 0.05, 0.0, 0.5]

# Speed adjustment factors based on weighted grade (0-15)
# Weighted grade encodes: 0=-10%, 6=-1.5%, 7=0%, 8=1.5%, 15=15%
kGradeBasedSpeedFactor = [
    2.2,   # -10%  - Fast downhill
    2.0,   # -8%
    1.9,   # -6.5%
    1.7,   # -5%
    1.4,   # -3%
    1.2,   # -1.5%
    1.0,   # 0%    - Flat
    0.95,  # 1.5%
    0.85,  # 3%
    0.75,  # 5%
    0.65,  # 6.5%
    0.55,  # 8%
    0.5,   # 10%
    0.45,  # 11.5%
    0.4,   # 13%
    0.3    # 15%   - Steep climb
]

# Avoid hills strength (penalty factors based on grade)
kAvoidHillsStrength = [
    2.0,   # -10%  - Treacherous descent
    1.0,   # -8%
    0.5,   # -6.5%
    0.2,   # -5%
    0.1,   # -3%
    0.0,   # -1.5%
    0.05,  # 0%
    0.1,   # 1.5%
    0.3,   # 3%
    0.8,   # 5%
    2.0,   # 6.5%
    3.0,   # 8%
    4.5,   # 10%
    6.5,   # 11.5%
    10.0,  # 13%
    12.0   # 15%
]

# Bicycle network factor (slight preference)
kBicycleNetworkFactor = 0.95

# Bicycle steps factor
kBicycleStepsFactor = 8.0

# Ferry factor
kDefaultFerryFactor = 1.5

# Speed penalty threshold
kSpeedPenaltyThreshold = 40

# Use types
USE_NONE = 0
USE_CYCLEWAY = 1
USE_FOOTWAY = 4
USE_STEPS = 5
USE_PATH = 6
USE_LIVING_STREET = 7
USE_TRACK = 8
USE_FERRY = 9
USE_MOUNTAIN_BIKE = 24

# Cycle lane types
CYCLELANE_NONE = 0
CYCLELANE_SHARED = 1
CYCLELANE_DEDICATED = 2
CYCLELANE_SEPARATED = 3

# Speed factor lookup (converts kph to seconds per meter)
# kSpeedFactor[speed_kph] = 3.6 / speed_kph
kSpeedFactor = [3.6 / max(s, 1) for s in range(256)]

# Truck stress
kTruckStress = 0.5


class ValhallaBicycleCost:
    """Exact port of Valhalla's BicycleCost class"""
    
    def __init__(self, bicycle_type='hybrid', use_roads=0.25, use_hills=0.25, 
                 avoid_bad_surfaces=0.25, cycling_speed=None):
        """
        Initialize bicycle costing.
        
        Args:
            bicycle_type: 'road', 'cross', 'hybrid', or 'mountain'
            use_roads: 0.0 to 1.0 - willingness to use roads (0=avoid, 1=use freely)
            use_hills: 0.0 to 1.0 - willingness to use hills
            avoid_bad_surfaces: 0.0 to 1.0 - penalty for rough surfaces
            cycling_speed: Override speed in kph (or None for default based on type)
        """
        # Set bicycle type
        if bicycle_type == 'road':
            self.type_ = ROAD
        elif bicycle_type == 'cross':
            self.type_ = CROSS
        elif bicycle_type == 'mountain':
            self.type_ = MOUNTAIN
        else:
            self.type_ = HYBRID
        
        # Set speed
        if cycling_speed is not None:
            self.speed_ = max(5.0, min(60.0, cycling_speed))
        else:
            self.speed_ = kDefaultCyclingSpeed[self.type_]
        
        # Store options
        self.use_roads_ = max(0.0, min(1.0, use_roads))
        self.avoid_roads_ = 1.0 - self.use_roads_
        self.use_hills_ = max(0.0, min(1.0, use_hills))
        self.avoid_bad_surfaces_ = max(0.0, min(1.0, avoid_bad_surfaces))
        
        # Surface factors based on bicycle type
        if self.type_ == ROAD:
            self.surface_speed_factor_ = kRoadSurfaceSpeedFactors
        elif self.type_ == HYBRID:
            self.surface_speed_factor_ = kHybridSurfaceSpeedFactors
        elif self.type_ == CROSS:
            self.surface_speed_factor_ = kCrossSurfaceSpeedFactors
        else:
            self.surface_speed_factor_ = kMountainSurfaceSpeedFactors
        
        # Minimal surface that gets penalized
        self.minimal_surface_penalized_ = kWorstAllowedSurface[self.type_]
        self.worst_allowed_surface_ = (self.minimal_surface_penalized_ 
                                        if self.avoid_bad_surfaces_ == 1.0 
                                        else SURFACE_PATH)
        
        # Road classification factor based on use_roads
        # use_roads >= 0.5: reduce weight difference between road classes
        # use_roads < 0.5: increase differences
        if self.use_roads_ >= 0.5:
            self.road_factor_ = 1.5 - self.use_roads_
        else:
            self.road_factor_ = 2.0 - self.use_roads_ * 2.0
        
        # Edge costing factors
        self.sidepath_factor_ = 3.0 * (1.0 - self.use_roads_)
        self.livingstreet_factor_ = 0.2 + self.use_roads_ * 0.8
        self.track_factor_ = 0.5 + self.use_roads_
        
        # Cycle lane factors: [no_shoulder*4 + cyclelane_type]
        self.cyclelane_factor_ = [
            1.0,                          # No shoulder, no cycle lane
            0.9 + self.use_roads_ * 0.05, # No shoulder, shared cycle lane
            0.4 + self.use_roads_ * 0.45, # No shoulder, dedicated cycle lane
            0.15 + self.use_roads_ * 0.6, # No shoulder, separated cycle lane
            0.7 + self.use_roads_ * 0.2,  # Shoulder, no cycle lane
            0.9 + self.use_roads_ * 0.05, # Shoulder, shared cycle lane
            0.4 + self.use_roads_ * 0.45, # Shoulder, dedicated cycle lane
            0.15 + self.use_roads_ * 0.6, # Shoulder, separated cycle lane
        ]
        
        # Path cycle lane factors (for cycleway/footway/path)
        self.path_cyclelane_factor_ = [
            0.2 + self.use_roads_,         # Share path with pedestrians
            0.2 + self.use_roads_,         # Share path with pedestrians
            0.1 + self.use_roads_ * 0.9,   # Segregated lane from pedestrians
            self.use_roads_ * 0.8,         # No pedestrians allowed on path
        ]
        
        # Speed penalty threshold and factor
        self.speed_penalty_threshold_ = kSpeedPenaltyThreshold + int(self.use_roads_ * 30.0)
        
        # Build speed penalty table
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
        self.grade_penalty_ = [avoid_hills * kAvoidHillsStrength[i] for i in range(16)]
        
        # Ferry factor
        self.ferry_factor_ = kDefaultFerryFactor
    
    def edge_cost(self, edge):
        """
        Calculate cost and time to traverse an edge.
        
        Args:
            edge: dict with edge attributes:
                - length: length in meters
                - speed: speed limit in kph
                - use: road use type
                - surface: surface type (0-7)
                - classification: road class (0-7)
                - cyclelane: cycle lane type (0-3)
                - shoulder: has shoulder (bool)
                - bike_network: part of bike network (bool)
                - grade: weighted grade (0-15, 7=flat)
                - density: traffic density
                - lanecount: number of lanes (optional)
                - truck_route: is truck route (optional)
                - use_sidepath: should use sidepath (optional)
                - dismount: must dismount (optional)
        
        Returns:
            tuple: (cost, time_seconds)
        """
        length = edge.get('length', 0)
        if length <= 0:
            return (0.0, 0.0)
        
        use = edge.get('use', USE_NONE)
        surface = edge.get('surface', 0)
        classification = edge.get('classification', 5)
        cyclelane = edge.get('cyclelane', 0)
        shoulder = edge.get('shoulder', False)
        bike_network = edge.get('bike_network', False)
        weighted_grade = edge.get('grade', 7)  # 7 = flat
        edge_speed = edge.get('speed', 50)
        
        # Clamp values
        surface = max(0, min(7, surface))
        classification = max(0, min(7, classification))
        cyclelane = max(0, min(3, cyclelane))
        weighted_grade = max(0, min(15, weighted_grade))
        edge_speed = max(1, min(255, edge_speed))
        
        # Check if surface is allowed
        if surface > self.worst_allowed_surface_:
            return (float('inf'), float('inf'))
        
        # Steps - high cost
        if use == USE_STEPS:
            sec = length * kSpeedFactor[1]
            return (sec * kBicycleStepsFactor, sec)
        
        # Ferry
        if use == USE_FERRY:
            sec = length * kSpeedFactor[edge_speed]
            return (sec * self.ferry_factor_, sec)
        
        # Roadway stress and accommodation
        roadway_stress = 1.0
        accommodation_factor = 1.0
        
        # Special use cases
        if use in (USE_CYCLEWAY, USE_FOOTWAY, USE_PATH):
            # Differentiate how segregated the path is from pedestrians
            accommodation_factor = self.path_cyclelane_factor_[cyclelane]
        elif use == USE_MOUNTAIN_BIKE and self.type_ == MOUNTAIN:
            accommodation_factor = 0.3 + self.use_roads_
        elif use == USE_LIVING_STREET:
            roadway_stress = self.livingstreet_factor_
        elif use == USE_TRACK:
            roadway_stress = self.track_factor_
        else:
            # Regular road - favor roads with cycle lane and/or shoulder
            shoulder_idx = 4 if shoulder else 0
            accommodation_factor = self.cyclelane_factor_[shoulder_idx + cyclelane]
            
            # Penalize roads with multiple lanes
            lanecount = edge.get('lanecount', 1)
            if lanecount > 1:
                roadway_stress += (float(lanecount) - 1) * 0.05 * self.road_factor_
            
            # Truck routes add stress
            if edge.get('truck_route', False):
                roadway_stress += kTruckStress
            
            # Road classification penalty
            roadway_stress += self.road_factor_ * kRoadClassFactor[classification]
            
            # Multiply by speed penalty
            roadway_stress *= self.speedpenalty_[edge_speed]
        
        # Sidepath penalty
        if edge.get('use_sidepath', False):
            accommodation_factor += self.sidepath_factor_
        
        # Favor bicycle networks
        if bike_network:
            accommodation_factor *= kBicycleNetworkFactor
        
        # Total factor = 1 + grade_penalty + (accommodation * roadway_stress)
        factor = 1.0 + self.grade_penalty_[weighted_grade] + (accommodation_factor * roadway_stress)
        
        # Surface penalty
        if surface >= self.minimal_surface_penalized_:
            surf_idx = surface - self.minimal_surface_penalized_
            if surf_idx < len(kSurfaceFactors):
                factor += self.avoid_bad_surfaces_ * kSurfaceFactors[surf_idx]
        
        # Compute bicycle speed
        if edge.get('dismount', False):
            bike_speed = int(kDismountSpeed)
        else:
            surface_factor = self.surface_speed_factor_[surface]
            grade_factor = kGradeBasedSpeedFactor[weighted_grade]
            bike_speed = int(self.speed_ * surface_factor * grade_factor + 0.5)
            bike_speed = max(1, min(255, bike_speed))
        
        # Compute time and cost
        sec = length * kSpeedFactor[bike_speed]
        cost = sec * factor
        
        return (cost, sec)


# ============================================================================
# Factory function
# ============================================================================

def create_bicycle_cost(bicycle_type='hybrid', **kwargs):
    """Create a ValhallaBicycleCost instance"""
    return ValhallaBicycleCost(bicycle_type=bicycle_type, **kwargs)


# ============================================================================
# Test
# ============================================================================

if __name__ == '__main__':
    # Test with different bicycle types
    for btype in ['road', 'hybrid', 'cross', 'mountain']:
        costing = ValhallaBicycleCost(bicycle_type=btype)
        
        # Test edge: 1km paved residential road, flat
        edge = {
            'length': 1000,
            'speed': 50,
            'use': USE_NONE,
            'surface': SURFACE_PAVED,
            'classification': 6,  # Residential
            'cyclelane': CYCLELANE_NONE,
            'shoulder': False,
            'bike_network': False,
            'grade': 7,  # Flat
        }
        
        cost, time = costing.edge_cost(edge)
        print(f"{btype}: cost={cost:.1f}, time={time:.1f}s ({time/60:.1f}min)")
