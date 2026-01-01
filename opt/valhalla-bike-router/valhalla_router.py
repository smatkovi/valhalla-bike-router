#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Valhalla-style Bidirectional A* Router
Based on valhalla/src/thor/bidirectional_astar.cc

Uses opp_index to traverse edges in reverse direction.
"""

import math
import heapq


def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in meters between two points"""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))


class BidirectionalAStar:
    """
    Bidirectional A* search algorithm.
    Searches from both origin and destination simultaneously.
    Uses opp_index to traverse edges in reverse.
    """
    
    def __init__(self, tile_cache, costing, get_edge_details_func):
        self.cache = tile_cache
        self.costing = costing
        self.get_edge_details = get_edge_details_func
        self.tiles = {}
        
        # Speed for heuristic (m/s) - use max possible bike speed
        self.max_speed_mps = 25.0 / 3.6  # 25 kph in m/s
    
    def load_tile(self, tile_id):
        """Load and cache a tile"""
        if tile_id not in self.tiles:
            tile = self.cache.get_tile(2, tile_id, self.costing)
            if tile:
                self.tiles[tile_id] = tile
        return self.tiles.get(tile_id)
    
    def find_nearest_node(self, lat, lon, tile_id):
        """Find nearest node to a location in a tile"""
        tile = self.load_tile(tile_id)
        if not tile:
            return None
        
        best_dist = float('inf')
        best_node = None
        
        for i in range(tile.node_count):
            d = haversine(lat, lon, tile.node_lats[i], tile.node_lons[i])
            if d < best_dist:
                best_dist = d
                best_node = i
        
        return best_node
    
    def get_node_coords(self, tile_id, node_id):
        """Get coordinates of a node"""
        tile = self.tiles.get(tile_id)
        if tile and node_id < tile.node_count:
            return tile.node_lats[node_id], tile.node_lons[node_id]
        return None, None
    
    def heuristic(self, tile_id, node_id, target_lat, target_lon):
        """A* heuristic - estimated time to reach target."""
        lat, lon = self.get_node_coords(tile_id, node_id)
        if lat is None:
            return float('inf')
        
        dist = haversine(lat, lon, target_lat, target_lon)
        return dist / self.max_speed_mps
    
    def get_outgoing_edges(self, tile_id, node_id):
        """Get all outgoing edges from a node"""
        tile = self.tiles.get(tile_id)
        if not tile or node_id >= tile.node_count:
            return []
        
        edges = []
        edge_idx = tile.node_edge_idx[node_id]
        edge_cnt = tile.node_edge_cnt[node_id]
        
        for i in range(edge_idx, min(edge_idx + edge_cnt, tile.edge_count)):
            end_tileid, end_id, has_bike, opp_index = tile.edge_ends[i]
            if has_bike:
                edges.append((i, end_tileid, end_id, opp_index))
        
        return edges
    
    def get_opposing_edge_at_node(self, tile_id, node_id, opp_local_idx):
        """
        Get the opposing edge at a node given its local index.
        Returns (edge_global_idx, end_tileid, end_nodeid, opp_index) or None
        """
        tile = self.tiles.get(tile_id)
        if not tile or node_id >= tile.node_count:
            return None
        
        edge_idx = tile.node_edge_idx[node_id]
        edge_cnt = tile.node_edge_cnt[node_id]
        
        # opp_local_idx is the local index (0 to edge_cnt-1)
        if opp_local_idx >= edge_cnt:
            return None
        
        global_idx = edge_idx + opp_local_idx
        if global_idx >= tile.edge_count:
            return None
        
        end_tileid, end_id, has_bike, opp_index = tile.edge_ends[global_idx]
        if not has_bike:
            return None
        
        return (global_idx, end_tileid, end_id, opp_index)
    
    def expand_forward(self, tile_id, node_id, current_cost, target_lat, target_lon):
        """
        Expand edges in forward direction from a node.
        Returns list of (new_cost, sort_cost, next_tile_id, next_node_id, edge_idx)
        """
        tile = self.load_tile(tile_id)
        if not tile or node_id >= tile.node_count:
            return []
        
        neighbors = []
        
        for edge_idx, end_tileid, end_id, opp_index in self.get_outgoing_edges(tile_id, node_id):
            # Load destination tile
            dest_tile = self.load_tile(end_tileid)
            if not dest_tile or end_id >= dest_tile.node_count:
                continue
            
            # Get edge cost
            edge = self.get_edge_details(tile, edge_idx)
            if not edge:
                continue
            
            cost, time = self.costing.edge_cost(edge)
            if cost == float('inf'):
                continue
            
            new_cost = current_cost + cost
            
            # Calculate heuristic
            h = self.heuristic(end_tileid, end_id, target_lat, target_lon)
            sort_cost = new_cost + h
            
            neighbors.append((new_cost, sort_cost, end_tileid, end_id, edge_idx))
        
        return neighbors
    
    def expand_reverse(self, tile_id, node_id, current_cost, target_lat, target_lon):
        """
        Expand edges in REVERSE direction from a node.
        
        For each outgoing edge from this node, we find its opposing edge
        (which goes FROM the neighbor TO this node) and use that for expansion.
        This effectively finds all edges that can reach this node.
        """
        tile = self.load_tile(tile_id)
        if not tile or node_id >= tile.node_count:
            return []
        
        neighbors = []
        
        # For each outgoing edge from this node
        for edge_idx, end_tileid, end_id, opp_index in self.get_outgoing_edges(tile_id, node_id):
            # Load the tile containing the neighbor (end node of outgoing edge)
            neighbor_tile = self.load_tile(end_tileid)
            if not neighbor_tile or end_id >= neighbor_tile.node_count:
                continue
            
            # Get the opposing edge at the neighbor node
            # This edge goes FROM neighbor TO current node
            opp_edge_info = self.get_opposing_edge_at_node(end_tileid, end_id, opp_index)
            if not opp_edge_info:
                continue
            
            opp_edge_idx, _, _, _ = opp_edge_info
            
            # Get the cost of the opposing edge
            opp_edge = self.get_edge_details(neighbor_tile, opp_edge_idx)
            if not opp_edge:
                continue
            
            cost, time = self.costing.edge_cost(opp_edge)
            if cost == float('inf'):
                continue
            
            new_cost = current_cost + cost
            
            # Calculate heuristic to origin (target for reverse search)
            h = self.heuristic(end_tileid, end_id, target_lat, target_lon)
            sort_cost = new_cost + h
            
            # In reverse search, we're finding nodes that can reach us
            # So the "neighbor" we're adding is the end_tileid/end_id
            neighbors.append((new_cost, sort_cost, end_tileid, end_id, opp_edge_idx))
        
        return neighbors
    
    def route(self, origin_lat, origin_lon, dest_lat, dest_lon, 
              origin_tile_id, dest_tile_id, max_iterations=500000):
        """
        Find route using bidirectional A*.
        
        Returns: list of (lat, lon) tuples, or None if no route found
        """
        # Find start/end nodes
        origin_node = self.find_nearest_node(origin_lat, origin_lon, origin_tile_id)
        dest_node = self.find_nearest_node(dest_lat, dest_lon, dest_tile_id)
        
        if origin_node is None or dest_node is None:
            print("Could not find start or end node")
            return None
        
        print(f"Routing from tile {origin_tile_id} node {origin_node} to tile {dest_tile_id} node {dest_node}")
        
        # Initialize forward search
        # Priority queue: (sort_cost, cost, tile_id, node_id)
        fwd_pq = []
        fwd_visited = {}  # (tile_id, node_id) -> (cost, pred_tile, pred_node)
        fwd_pred = {}     # (tile_id, node_id) -> (pred_tile, pred_node)
        
        origin_h = self.heuristic(origin_tile_id, origin_node, dest_lat, dest_lon)
        heapq.heappush(fwd_pq, (origin_h, 0, origin_tile_id, origin_node))
        
        # Initialize reverse search
        rev_pq = []
        rev_visited = {}
        rev_pred = {}
        
        dest_h = self.heuristic(dest_tile_id, dest_node, origin_lat, origin_lon)
        heapq.heappush(rev_pq, (dest_h, 0, dest_tile_id, dest_node))
        
        # Best meeting point
        best_cost = float('inf')
        meeting_tile = None
        meeting_node = None
        
        iterations = 0
        fwd_done = False
        rev_done = False
        
        while (fwd_pq or rev_pq) and iterations < max_iterations:
            iterations += 1
            
            # Expand forward
            if fwd_pq and not fwd_done:
                sort_cost, cost, tile_id, node_id = heapq.heappop(fwd_pq)
                
                key = (tile_id, node_id)
                if key in fwd_visited:
                    pass  # Skip, already visited
                else:
                    fwd_visited[key] = cost
                    
                    # Check if we met reverse search
                    if key in rev_visited:
                        total_cost = cost + rev_visited[key]
                        if total_cost < best_cost:
                            best_cost = total_cost
                            meeting_tile, meeting_node = tile_id, node_id
                    
                    # Early termination
                    if sort_cost >= best_cost:
                        fwd_done = True
                    else:
                        # Expand
                        for new_cost, new_sort, next_tile, next_node, _ in \
                                self.expand_forward(tile_id, node_id, cost, dest_lat, dest_lon):
                            next_key = (next_tile, next_node)
                            if next_key not in fwd_visited:
                                heapq.heappush(fwd_pq, (new_sort, new_cost, next_tile, next_node))
                                if next_key not in fwd_pred:
                                    fwd_pred[next_key] = key
            
            # Expand reverse
            if rev_pq and not rev_done:
                sort_cost, cost, tile_id, node_id = heapq.heappop(rev_pq)
                
                key = (tile_id, node_id)
                if key in rev_visited:
                    pass  # Skip
                else:
                    rev_visited[key] = cost
                    
                    # Check if we met forward search
                    if key in fwd_visited:
                        total_cost = cost + fwd_visited[key]
                        if total_cost < best_cost:
                            best_cost = total_cost
                            meeting_tile, meeting_node = tile_id, node_id
                    
                    # Early termination
                    if sort_cost >= best_cost:
                        rev_done = True
                    else:
                        # Expand reverse
                        for new_cost, new_sort, next_tile, next_node, _ in \
                                self.expand_reverse(tile_id, node_id, cost, origin_lat, origin_lon):
                            next_key = (next_tile, next_node)
                            if next_key not in rev_visited:
                                heapq.heappush(rev_pq, (new_sort, new_cost, next_tile, next_node))
                                if next_key not in rev_pred:
                                    rev_pred[next_key] = key
            
            # Progress report
            if iterations % 10000 == 0:
                print(f"  Iter {iterations}: fwd={len(fwd_visited)} rev={len(rev_visited)} best={best_cost:.0f}")
            
            if fwd_done and rev_done:
                break
        
        print(f"Search done: {iterations} iterations, fwd={len(fwd_visited)} rev={len(rev_visited)}")
        
        if meeting_tile is None:
            print("No route found!")
            return None
        
        print(f"Best cost: {best_cost:.0f}, meeting at tile {meeting_tile} node {meeting_node}")
        
        # Reconstruct path
        # Forward path: origin -> meeting
        fwd_path = []
        key = (meeting_tile, meeting_node)
        while key in fwd_pred:
            fwd_path.append(key)
            key = fwd_pred[key]
        fwd_path.append((origin_tile_id, origin_node))
        fwd_path.reverse()
        
        # Reverse path: meeting -> destination
        rev_path = []
        key = (meeting_tile, meeting_node)
        while key in rev_pred:
            key = rev_pred[key]
            rev_path.append(key)
        
        # Combine paths (skip duplicate meeting point)
        full_path = fwd_path + rev_path
        
        # Convert to coordinates
        coord_path = []
        for tile_id, node_id in full_path:
            lat, lon = self.get_node_coords(tile_id, node_id)
            if lat is not None:
                coord_path.append((lat, lon))
        
        print(f"Route has {len(coord_path)} points")
        
        return coord_path


def route_bicycle(cache, costing, get_edge_details, 
                  origin_lat, origin_lon, dest_lat, dest_lon,
                  origin_tile_id, dest_tile_id):
    """
    Convenience function to route between two points.
    """
    router = BidirectionalAStar(cache, costing, get_edge_details)
    return router.route(origin_lat, origin_lon, dest_lat, dest_lon,
                       origin_tile_id, dest_tile_id)
