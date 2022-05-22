import logging
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
import time

logger = logging.getLogger("backendLogger")

PARAMETERS = {
    'stop_distance_meter': 1000, # Stop-to-stop distance threshold for including intermediate coordinates (meters)
    'maximum_radius_increase': 100,
    'stop_radius': 35, # Radius used to search when matching stop coordinates (meters)
    'intermediate_radius': 100, # Radius used to search when matching intermediate coordinates (meters)
    'radius_increase_step': 10
}

class GTFS_Shape(BaseShape):

    def __init__(self, data):
        super().__init__(data)


    def generate_patterns(self) -> Dict[str, Dict]:

        if not isinstance(self.data, GTFS):
            raise TypeError(f'The data provided for GTFS shape generation is not a valid GTFS object.')
        else:
            return self.data.pattern_dict

    def generate_shapes(self) -> Dict[str, Pattern]:
        
        all_matched = {}
        all_skipped = {}

        pbar = tqdm(total=len(self.patterns.keys()), desc='Generating pattern shapes', \
                                        position=0, leave=True)
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

        logger.info(f'matched shapes')

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