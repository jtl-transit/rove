import logging
from operator import gt
import pandas as pd
import numpy as np
from .base_shape import BaseShape
from .misc_shape_classes import Pattern, Valhalla_Point, Valhalla_Request
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points
from parameters.gtfs import GTFS
from typing import Tuple, Dict, Set, List, Type
import math
from tqdm.auto import tqdm
import requests
import json
from shapely.ops import nearest_points
from shapely.geometry import LineString, Point
from scipy.spatial import distance
import time
import math
from .helper_functions import get_hash_of_stop_list

logger = logging.getLogger("backendLogger")

# These are configurable parameters for a Valhalla request.
# Constants based on extensive testing by MIT Transit Lab but modifiable for specific needs. 
PARAMETERS = {
    'stop_distance_meter': 1000, # Stop-to-stop distance threshold for including intermediate coordinates (meters)
    'maximum_radius_increase': 100, # Self-defined parameter to limit the search area for matching coordinates (meters)
    'stop_radius': 35, # Radius used to search when matching stop coordinates (meters)
    'intermediate_radius': 100, # Radius used to search when matching intermediate coordinates (meters)
    'radius_increase_step': 10 # Step size used to increase search area when Valhalla cannot find an initial match (meters)
}

# This class provides the functionality to create shapes from a GTFS feed using Valhalla
class GTFS_Shape(BaseShape):

    def __init__(self, gtfs:GTFS, outpath):

        if not isinstance(gtfs, GTFS):
            raise TypeError(f'The data provided for GTFS shape generation is not a valid GTFS object.')

        super().__init__(gtfs, outpath)

    def generate_patterns(self) -> Dict[str, Dict]:
        """Generate a dict of patterns from validated GTFS data. Add a "hash" column to the trips table.

        Raises:
            ValueError: number of unique trip hashes does not match with number of unique sequence of stops

        Returns:
            Dict: Pattern dict - key: pattern hash; value: Segment dict (a segment is a section of road between two transit stops). 
                    Segment dict - key: tuple of stop IDs at the beginning and end of the segment; value: list of coordinates defining the segment.
        """
        logger.info(f'generating patterns with GTFS data...')

        data:GTFS = self.data
        gtfs:Dict = data.validated_data

        # Handle of trips table stored in validated_data (reference semantics). 
        # The handle is used for simplicify of referencing the validated_data trips table, so adding column to the handle
        # changes the referenced object as well. The object is not reassigned other objects later on.
        trips:pd.DataFrame = gtfs['trips']
        stop_times:pd.DataFrame = gtfs['stop_times']
        stops:pd.DataFrame = gtfs['stops']
        
        # Generate a dataframe of trip_id (unique), list of stop_ids, and hash of the stop_ids list
        stop_times = stop_times.sort_values(by=['trip_id', 'stop_sequence'])
        trip_stops = stop_times.groupby('trip_id')['stop_id'].apply(list).reset_index(name='stop_ids')
        trip_stops['hash'] = trip_stops['stop_ids'].apply(lambda x: get_hash_of_stop_list(x))

        # Verify that number of unique hashes indeed match with number of unique sequence of stops
        if trip_stops.astype(str).nunique()['hash'] != trip_stops.astype(str).nunique()['stop_ids']:
            raise ValueError(f'Number of unique hashes should match number of unique lists of ordered stops. '+\
                                'Use debug mode to find out why there is a mismatch. You may need to change the hashing method.')
        else:
            logger.debug(f'Summary of unique trip hashes: \n{trip_stops.astype(str).nunique()}')

        # Add hash column to the trips table
        trip_hash_lookup = trip_stops.set_index('trip_id')['hash'].to_dict()
        trips['hash'] = trips['trip_id'].map(trip_hash_lookup)

        # Generate dict of patterns. 
        # Get a dict of <hash: list of stop ids>
        hash_stops_lookup = trip_stops.set_index('hash')['stop_ids'].to_dict()

        # Get a dict of <stop id: tuple of stop coordinates (lat, lon)>
        stops = stops[['stop_id','stop_lat','stop_lon']].drop_duplicates()
        stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
        stop_coords_lookup = stops.set_index('stop_id')['coords'].to_dict()

        # Get a dict of <hash: segments>
        # e.g. {2188571819865: {('64', '1'):[(lat64, lon64), (lat1, lon1)], ('1', '2'): [(lat1, lon1), (lat2, lon2)]...}...}
        patterns = {hash: {(stop_ids[i], stop_ids[i+1]): [stop_coords_lookup[stop_ids[i]], stop_coords_lookup[stop_ids[i+1]]] \
                                    for i in range(len(stop_ids)-1)} for hash, stop_ids in hash_stops_lookup.items()}

        logger.info(f'gtfs patterns generated')

        if 'shapes' in gtfs.keys():
            patterns = self. __improve_pattern_with_shapes(patterns, gtfs)

        return patterns

    def generate_shapes(self) -> Dict:
        """_summary_

        Returns:
            Dict: Pattern dict - key: pattern hash; value: Segment dict (a segment is a section of road between two transit stops). 
                    Segment dict - key: tuple of stop IDs at the beginning and end of the segment; 
                                    value: segment information (geometry, distance).
        """
        all_matched = {}
        all_skipped = {}

        pbar = tqdm(total=len(self.patterns.keys()), desc='Generating pattern shapes', position=0)
        for p_name, segments in self.patterns.items():
            
            for s_name, coords in segments.items():

                stop_distance = PARAMETERS['stop_distance_meter']
                found_geometry = False
                break_radius = PARAMETERS['stop_radius']
                via_radius = PARAMETERS['intermediate_radius']
                radius_increase = 0
                while radius_increase <= PARAMETERS['maximum_radius_increase'] and not found_geometry:

                    seg_shape = []
                    # Get subset of coordinates based on distance threshold.
                    # Lower bounded at 1 to avoid division by zero.
                    segment_length = self.__get_distance(coords[0], coords[-1])
                    interval_count = max(math.floor(segment_length/stop_distance)+1,1) # min: 1
                    step = math.ceil((len(coords)-1) / interval_count ) # max: len(coords)-1
                    coords_to_use = [coords[i] for i in np.arange(0, len(coords), step)]

                    # build segment shape to be passed to Valhalla
                    for i in range(len(coords_to_use)):
                        if i==0 or i==len(coords_to_use)-1:
                            type='break'
                            radius = break_radius + radius_increase
                        else:
                            type='via'
                            radius = via_radius + radius_increase
                        
                        coord = coords_to_use[i]
                        seg_shape.append(Valhalla_Point(coord[0], coord[1], type, radius).point_parameters())

                    radius_increase = radius_increase + PARAMETERS['radius_increase_step']
                    matched, skipped = Valhalla_Request(s_name, seg_shape).get_trace_route_response()

                    if not skipped:
                        found_geometry = True
                
                # assume the first leg of the matched result corresponds to the segment
                if bool(matched):
                    if p_name not in all_matched:
                        all_matched[p_name] = {}

                    all_matched[p_name][s_name] = matched[s_name][0]

                if bool(skipped):
                    if p_name not in all_skipped:
                        all_skipped[p_name] = {}
                    all_skipped[p_name][s_name] = skipped[s_name]

            pbar.update()

        matched_output = [
                            {
                                **{'hash': p_name,
                                    'seg_index':f'{p_name}-{s_name[0]}-{s_name[1]}',
                                    'stop_pair': s_name}, 
                                **s_info
                            }
                            for p_name, segments in all_matched.items() \
                                for s_name, s_info in segments.items()]

        with open(self.outpath, 'w') as fp:
            json.dump(matched_output, fp)

        skipped_output = [
                            {
                                **{'hash': p_name,
                                    'seg_index':f'{p_name}-{s_name[0]}-{s_name[1]}',
                                    'stop_pair': s_name}, 
                                **s_info
                            }
                            for p_name, segments in all_skipped.items() \
                                for s_name, s_info in segments.items()]
        with open('skipped_shapes.json', 'w') as fp:
            json.dump(skipped_output, fp)
        
        logger.debug(f'Number of patterns matched: {len(all_matched.keys())}. '\
            f'Number of patterns skipped: {len(all_skipped.keys())}')

        return pd.json_normalize(matched_output)


    def __improve_pattern_with_shapes(self, patterns:Dict, gtfs:Dict) -> Dict:
        """Improve the coordinates of each segment in each pattern by replacing the stop coordinates
            with coordinates found in the GTFS shapes table, i.e. in addition to the two stop coordinates
            at both ends of the segment, also add additional intermediate coordinates given by GTFS shapes
            to enrich the segment profile.
        """
        logger.info(f'improving pattern with GTFS shapes table...')

        trips = gtfs['trips']
        shapes = gtfs['shapes'].sort_values(by=['shape_id', 'shape_pt_sequence'])

        # Get dict of <shape_id: list of coordinates>
        shapes['coords'] = list(zip(shapes.shape_pt_lat, shapes.shape_pt_lon))
        shape_coords_lookup = shapes.groupby('shape_id')['coords'].agg(list).to_dict()

        # Get dict of <trip_id: shape_id>
        trip_shape_lookup = trips[['trip_id', 'shape_id']].set_index('trip_id')['shape_id'].to_dict()

        # Get dict of <hash: list of trip_ids>
        hash_trips_lookup = trips.groupby('hash')['trip_id'].agg(list).to_dict()

        for hash, segments in patterns.items():
            
            # Find a representative shape ID and the corresponding list of shape coordinates
            trips = hash_trips_lookup[hash]
            try:
                example_shape = next(trip_shape_lookup[t] for t in trips if t in trip_shape_lookup)
            except StopIteration as err:
                logger.debug(f'{err}: no example shape can be found for hash: {hash}.')
                example_shape = -1
                continue
            
            shape_coords = shape_coords_lookup[example_shape]
            
            # For each segment, find the closest match of start and end stops in the list of shape coordinates.
            #   If more than two matched coordinates in GTFS shapes can be found, then use GTFS shapes.
            #   Otherwise, keep using the stop coordinates from GTFS stops.
            for id_pair, coord_list in segments.items():
                first_stop_match_index = self.__find_nearest_point(shape_coords, coord_list[0])
                last_stop_match_index = self.__find_nearest_point(shape_coords, coord_list[-1])
                
                intermediate_shape_coords = \
                            shape_coords[first_stop_match_index : last_stop_match_index+1]

                if len(intermediate_shape_coords)>2:
                    segments[id_pair] = intermediate_shape_coords

        logger.info(f'patterns improved with GTFS shapes')
        return patterns

    def __find_nearest_point(self, coord_list, coord):
        dist = distance.cdist(np.array(coord_list), np.array([coord]), 'euclidean')
        return dist.argmin()

    def __get_distance(self, start:Tuple[float, float], end:Tuple[float, float]) -> float:
        """Get distance (in m) from a pair of lat, long coord tuples.

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
