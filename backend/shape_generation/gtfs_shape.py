import logging
import traceback
import pandas as pd
import numpy as np
from .base_shape import BaseShape
from .misc_shape_classes import Pattern, Valhalla_Point, Valhalla_Request
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points
from parameters.gtfs import GTFS
from typing import Tuple, Dict, Set, List, Type
import math
import tqdm
import requests
import json
import time

logger = logging.getLogger("backendLogger")

class GTFS_Shape(BaseShape):

    def __init__(self, data):
        super().__init__(data)

    def generate_patterns(self):
        
        if not isinstance(self.data, GTFS):
            logger.fatal(f'The data provided for GTFS shape generation must be a GTFS object. ' + \
                    f'Please pass in a GTFS object to create GTFS-based shapes. Exiting...')
            quit()

        gtfs = self.data.validated_data

        #>>> Step 1: Get pattern_dict using the required trips, stop_times and stops tables
        pattern_dict = self.get_patterns_with_requied_gtfs_tables(gtfs['trips'], gtfs['stop_times'], gtfs['stops'])
        
        #>>> Step 2: Use the shapes table in gtfs if it exists to improve quality of stop coordinates
        if 'shapes' in gtfs.keys() and 'shape_id' in gtfs['trips'].columns:
            self.improve_pattern_dict_with_gtfs_shapes(pattern_dict, gtfs['shapes'], gtfs['trips'])
        
        #>>> Step 3: Generate Valhalla input for each pattern
        for index, pattern in pattern_dict.items():
            v_points = [Valhalla_Point(c[1], c[0], pattern.coord_types[i], pattern.radii[i]).point_parameters \
                            for i, c in enumerate(pattern.shape_coords)]
            pattern.v_points = v_points

        return pattern_dict

    def generate_shapes(self):

        VALHALLA_TIMEOUT_TRIES = 6
        # Use map matching to convert the GTFS polylines to matched, encoded polylines

        segment_dict = {}
        skipped_segs = {}
        start_time = time.time()

        for pattern_index, pattern in self.patterns:
            if not isinstance(pattern, Pattern):
                logger.error(f'{pattern_index} is not a valid Pattern object. Skipping...')
                continue

            v_points = pattern.v_points
            coordinate_types = pattern.coord_types
            pattern_segs = len(pattern.stops)-1
            pattern_legs = 0
            start_point = 0

            # Send multiple requests to Valhalla if the response is cut off
            timeout_count = 1
            request_processed = False
            while timeout_count < VALHALLA_TIMEOUT_TRIES:
                try:
                    # Use Valhalla map matching engine to snap shapes to the road network
                    request_data = Valhalla_Request(v_points[start_point:])
                    req = requests.post('http://localhost:8002/trace_route',
                                        data = json.dumps(request_data),
                                        timeout = 60)
                    timeout_count = VALHALLA_TIMEOUT_TRIES+1
                    request_processed = True
                except:
                    print("Valhalla Timeout #", timeout_count)
                    timeout_count += 1
                    
            if not request_processed:
                # Add all segments to skipped_segments
                input_points = [i - start_point for i, x in enumerate(coordinate_types)  \
                                if(x == 'break_through' and i >= start_point)]
                for point_idx, point in enumerate(input_points[:-1]):
                    skipped_segs[(pattern, point_idx)] = v_points[point:input_points[point_idx+1]]
                break

    def get_patterns_with_requied_gtfs_tables(self, trips: pd.DataFrame, stop_times: pd.DataFrame, \
                                            stops: pd.DataFrame) -> Dict[str, Pattern]:
        """Produce a dict of patterns using the required GTFS tables: trips, stops_times and stops.
                In addition, if the shapes table exists, then use the shapes to 

        Args:
            trips: GTFS trips table. Assume having columns: route_id, trip_id, direction_id.
            stop_times: GTFS stop_times table. Assume having columns: trip_id, stop_sequence, stop_id.
            stops: GTFS stops table. Assume having columns: stop_id, stop_lat, stop_lon.

        Returns:
            pattern_dict: <pattern_index: Pattern object>. See Pattern object notes for details.
        """
        
        trip_stop_times = pd.merge(trips, stop_times, on='trip_id', how='inner')
        trip_stop_times.sort_values(by=['trip_id', 'stop_sequence'], inplace=True)

        # dict of trip: list of stop IDs
        trip_stops_dict = trip_stop_times.groupby('trip_id')['stop_id'].agg(list).to_dict()
        
        # dict of trip: list of stop coordinates
        stops = stops[['stop_id','stop_lat','stop_lon']].drop_duplicates()
        stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
        stop_df = pd.merge(trip_stop_times, stops, on='stop_id', how='inner')
        stop_df.sort_values(by=['trip_id', 'stop_sequence'], inplace=True)
        stop_coords_dict = stop_df.groupby('trip_id')['coords'].agg(list).to_dict()

        # dataframe: route_id, trip_id, direction_id
        trips = trip_stop_times[['route_id','trip_id','direction_id']].drop_duplicates()
        
        # Add column of trip hash, which is calculated from the list of stop IDs associated with each trip
        trip_hashes = {}
        for trip, stops in trip_stops_dict.items():
            trip_hashes[trip] = self.get_trip_hash(stops)
        trips['hash'] = trips['trip_id'].map(trip_hashes)

        # Count how many times each route-hash combination appears
        pattern_counts = trips.groupby(['route_id','hash','direction_id']).size().reset_index(name='count')
        
        # dict of route, hash: trip_id
        # Since the returned pattern_df only contains representative trip_id for each hash, this dict serves as
        #   a look up dict for all trip_ids with the same route_id and hash.
        pattern_trip_dict = trips.groupby(['route_id','hash'])['trip_id'].agg(list).to_dict()

        # Create a dataframe for the patterns with count of identical trips, hash, route_ids, direction_id and representative trip id
        trips.drop_duplicates(subset=['route_id','hash', 'direction_id'], inplace=True)
        pattern_counts = pd.merge(pattern_counts[['count','hash','route_id']], trips, on=['hash','route_id'], how='inner')
        pattern_counts.sort_values(by=['route_id','direction_id','count'], ascending=[True, True, False], inplace=True)
        
        # Add pattern index that starts from 0 for each new combination of route_id and direction_id
        pattern_counts['pattern_count'] = pattern_counts.groupby(['route_id','direction_id']).cumcount()
        pattern_counts['pattern_index'] = pattern_counts['route_id'].astype(str)+'-'+pattern_counts['direction_id'].astype(str)+'-'+pattern_counts['pattern_count'].astype(str)
        pattern_counts.drop(columns=['pattern_count'], inplace=True)

        pattern_list = pattern_counts.to_dict('records')
        pattern_dict = {p['pattern_index']:Pattern(p['route_id'], p['direction_id'], trip_stops_dict[p['trip_id']], \
                                                pattern_trip_dict[(p['route_id'], p['hash'])], stop_coords_dict[p['trip_id']]) for p in pattern_list}
        
        return pattern_dict

    

    def improve_pattern_dict_with_gtfs_shapes(self, pattern_dict: Dict[str, Pattern], shapes: pd.DataFrame, trips: pd.DataFrame):
        """Add shape_id to the shape field of Pattern object if a matched shape is found, otherwise leave the object unchanged.
            Update shape_coords with closest bus stop points found in shape, plus any supplemental points to fill in big gaps.
            Update coord_types and radii accordingly.

        Args:
            pattern_dict (Dict[str, Pattern]): dict of Pattern objects
            shapes (pd.DataFrame): GTFS shapes table
            trips (pd.DataFrame): GTFS trips table
        """
        trip_shapes = trips[['trip_id', 'shape_id']].drop_duplicates()
        trip_shape_dict = dict(zip(trip_shapes['trip_id'], trip_shapes['shape_id']))

        shapes = shapes[['shape_id', 'shape_pt_lat', 'shape_pt_lon']].drop_duplicates()

        stop_matching_error_dict = {}
        for index, pattern in tqdm.tqdm(pattern_dict.items(), desc='Preparing stop coordinates'):
            sample_trip = pattern.trips[0]
            if sample_trip in trip_shape_dict:
                shape_id = trip_shape_dict[sample_trip]
                pattern.shape = shape_id

                stop_coords = pattern.stop_coords
                shape_coords = shapes.loc[shapes['shape_id']==shape_id][['shape_pt_lat', 'shape_pt_lon']].copy()
                coordinate_type, coordinate_list, radii = self.locate_stops_in_shapes(shape_coords, stop_coords)

                # Must update shape_coords first, since the coord_types and radii fields must match length of shape_coords.
                pattern.shape_coords = coordinate_list
                pattern.coord_types = coordinate_type
                pattern.radii = radii

                shape_break_throughs = coordinate_type.count('break_through')
                stops_from_gtfs = len(pattern.stops)
                diff = shape_break_throughs-stops_from_gtfs

                if diff != 0:
                    stop_matching_error_dict[index] = diff
        
        for i, e in stop_matching_error_dict.items():
            print("Error: Break Throughs - Stops =", e,"for Pattern ", i)

    # TODO: main source of inefficiency, improve this function
    def locate_stops_in_shapes(self, shape_coords:pd.DataFrame, stop_coords:List[Tuple[float, float]]) -> \
                                Tuple[List[str], List[Tuple[float, float]], List[int]]:
        """Take a dataframe of route shape coordinates and list of bus stop coordinates, then find the 
            route coordinate pair that is closest to each bus stop. Return three lists containing type, shape and radii
            of each point in the new shape.

        Args:
            shape_coords (pd.DataFrame): shape coordinates, assume having columns: shape_pt_lon, shape_pt_lat.
            stop_coords (List[Tuple[float, float]]): list of stop coordinates, each tuple is ordered as (lat, lon).

        Returns:
            Tuple[List[int], List[Tuple[float, float]], List[int]]: three lists of coordinates info after matching stops to shapes
            coordinate_types: list of coordinate types, break_through (at bus stop) and through (between bus stops)
            coordinate_list: list of coordinates
            radii: list of radii
        """
        # TODO: these should be class constants...
        stop_radius = 35 # Radius used to search when matching stop coordinates (meters)
        intermediate_radius = 100 # Radius used to search when matching intermediate coordinates (meters)
        stop_distance_threshold  = 1000 # Stop-to-stop distance threshold for including intermediate coordinates (meters)

        coordinate_types = ["break_through"] * len(stop_coords)
        radii = [stop_radius] * len(stop_coords)
        stop_indices = [0] * len(stop_coords)
        shape_coord_list = shape_coords[['shape_pt_lat','shape_pt_lon']].values.tolist()
        
        last_stop = 0
        coordinate_list = []
        shape_line = LineString([Point(x, y) for x, y in zip(shape_coords.shape_pt_lon, shape_coords.shape_pt_lat)])

        # Get index of shape point closest to each bus stop
        for stop_number, stop in enumerate(stop_coords):
            
            stop_point = Point(stop[1], stop[0])
            new_stop_point = nearest_points(shape_line, stop_point)[0] # index 0 is nearest point on the line
            coordinate_list.append((new_stop_point.y, new_stop_point.x))
            
            benchmark = 10**9
            index = 0
            best_index = 0
            # TODO: it might be unnecessary to go through the entire list. Once the distance starts increasing, the loop could stop.
            #       - opportunity to improve efficiency? If change method, need to compare the encoded polyline.
            for shape_point in shape_coord_list[last_stop:]: # Ensure stops occur sequentially
                test_dist = self.get_distance(shape_point, stop)
                if test_dist+2 < benchmark: # Add 2m to ensure that loop routes don't the later stop 
                    benchmark = test_dist
                    best_index = index + last_stop
                index += 1
            stop_indices[stop_number] = best_index
            last_stop = best_index + 1
        
        added_stop_count = 0
        
        # Add intermediate coordinates if stops are far apart
        for stop_number in range(len(stop_coords)-1):
            current_stop = stop_coords[stop_number]
            next_stop = stop_coords[stop_number + 1]
            current_pos = stop_indices[stop_number]
            next_pos = stop_indices[stop_number + 1]
            
            distance = self.get_distance(current_stop, next_stop)
        
            if distance > stop_distance_threshold:    
                
                coords_to_add = math.floor(distance/stop_distance_threshold )
                num_available_coords = next_pos - current_pos            
                interval = int(num_available_coords / (coords_to_add + 1))

                # If there aren't enough available coords to fill the shape, just add all coords
                # TODO: code redundancy - opportunity to improve efficiency?
                if coords_to_add > num_available_coords:
                    
                    for new_coord in range(num_available_coords):    
                        
                        coordinate_list.insert(stop_number + 1 + added_stop_count, shape_coord_list[current_pos + new_coord])
                        coordinate_types.insert(stop_number + 1 + added_stop_count, 'through')
                        radii.insert(stop_number + 1 + added_stop_count, intermediate_radius)
                        added_stop_count += 1
                    
                else:
                    for new_coord in range(coords_to_add):
                        
                        coordinate_list.insert(stop_number + 1 + added_stop_count, shape_coord_list[current_pos + (interval * new_coord)])
                        coordinate_types.insert(stop_number + 1 + added_stop_count, 'through')
                        radii.insert(stop_number + 1 + added_stop_count, intermediate_radius)
                        added_stop_count += 1
                
        return coordinate_types, coordinate_list, radii

    
    def get_distance(self, start:Tuple[float, float], end:Tuple[float, float]) -> float:
        """get distance (in m) from a pair of lat, long coord tuples

        Args:
            start (Tuple[float, float]): coord of start point
            end (Tuple[float, float]): coord of end point

        Returns:
            float: distance in meters between start and end point
        """
        R = 6372800 # earth radius in m
        lat1, lon1 = start
        lat2, lon2 = end
        
        phi1, phi2 = math.radians(lat1), math.radians(lat2) 
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1) 
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2    
        return round(2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a)),0)
