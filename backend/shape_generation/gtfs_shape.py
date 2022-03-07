import imp
import logging
from tkinter.tix import InputOnly
import traceback
from turtle import shape
from matplotlib.pyplot import flag
import pandas as pd
import numpy as np
from .base_shape import BaseShape, Pattern, Vahalla_Point
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points
from parameters.gtfs import GTFS
from typing import Tuple, Dict, Set, List
import math
import tqdm

logger = logging.getLogger("backendLogger")

class GTFS_Shape(BaseShape):

    def __init__(self, data):
        super().__init__(data)

    def generate_patterns(self):
        if not isinstance(self.data, GTFS):
            logger.fatal(f'The data provided for shape generation is not a GTFS object. Exiting...')
            quit()

        gtfs = self.data.validated_data
        trips = gtfs['trips']
        stop_times = gtfs['stop_times']
        stops = gtfs['stops']

        pattern_dict = self.get_pattern_dict(trips, stop_times, stops)

        # use the shape file in gtfs if it exists to improve quality of stop coordinates
        if 'shapes' in gtfs.keys() and 'shape_id' in trips.columns:
            shapes = gtfs['shapes']
            self.add_gtfs_shapes(pattern_dict, shapes, trips)
        
        for index, pattern in pattern_dict.items():
            coord_list = [Vahalla_Point(c[1], c[0], pattern.coord_types[i], pattern.radii[i]).point_parameters \
                            for i, c in enumerate(pattern.shape_coords)]
            pattern.v_input = coord_list


    def get_pattern_dict(self, trips: pd.DataFrame, stop_times: pd.DataFrame, stops: pd.DataFrame) \
                                    -> Dict[str, Pattern]:
        """Analyze the trips and stop_times tables from GTFS to produce a pattern table and dict of trips.

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
        patterns_dict = {p['pattern_index']:Pattern(p['route_id'], p['direction_id'], trip_stops_dict[p['trip_id']], \
                                                pattern_trip_dict[(p['route_id'], p['hash'])], stop_coords_dict[p['trip_id']]) for p in pattern_list}
        return patterns_dict


    def add_gtfs_shapes(self, pattern_dict: Dict[str, Pattern], shapes: pd.DataFrame, trips: pd.DataFrame):
        """Add shape_id to the shape field of Pattern object if a match is found, otherwise leave the object unchanged.
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
        for index, pattern in tqdm.tqdm(pattern_dict.items(), desc='Preparing coordinates:'):
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
            print("Error: Breaks - Stops =", e,"for Pattern ", i)

    def locate_stops_in_shapes(self, shape_coords:pd.DataFrame, stop_coords:List[Tuple[float, float]]) -> \
                                Tuple[List[str], List[Tuple[float, float]], List[int]]:
        """Takes a set of route shape coordinates and bus stop coordinates, then finds the 
            route coordinate pair that is closest to each bus stop. Returns an array of 
            strings of the same length as the bus stop coordinate input, with 'break_through' 
            for coordinates at bus stops and 'through' for other coordinates.

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

        # Get index of point closest to each bus stop
        # TODO: nested for loops - opportunity to improve efficiency?
        for stop_number, stop in enumerate(stop_coords):
            
            stop_point = Point(stop[1], stop[0])
            new_stop = nearest_points(shape_line, stop_point)[0] # index 0 is nearest point on the line
            coordinate_list.append((new_stop.y, new_stop.x))
            
            benchmark = 10**9
            index = 0
            best_index = 0
            for point in shape_coord_list[last_stop:]: # Ensure stops occur sequentially
                test_dist = self.get_distance(point, stop)
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

    def generate_shapes(self):

        pass


    # @property
    # def gtfs(self):

    #     return self._gtfs


# # main for testing
# def __main__():
#     logger.info(f'Starting shape generation...')
#     print(f'in main method of shape gen')

# def generate_shapes(feed):
#     print(f'running shape gen')
#     logger.info('logging to shape gen logger')

# # shape_gen_test()
# if __name__ == "__main__":
#     __main__()