from abc import abstractmethod
import logging
import traceback
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List

from zmq import ROUTER


class Valhalla_Point():

    def __init__(self, 
                lon,
                lat,
                type='break_through', # use 'break-through' for bus stops, 'through' for intermmediate points
                radius=35,
                rank_candidates='true',
                preferred_side='same',
                node_snap_tolerance=0,
                street_side_tolerance=0):
        
        self._point_parameters = {'lon': lon,
                                'lat': lat,
                                'type': type,
                                'radius': radius,
                                'rank_candidates': rank_candidates,
                                'preferred_side': preferred_side,
                                'node_snap_tolerance': node_snap_tolerance,
                                'street_side_tolerance':street_side_tolerance
                                }
        
    @property
    def point_parameters(self):

        return self._point_parameters

class Valhalla_Request():
    turn_penalty_factor = 100000 # Penalizes turns in Valhalla routes. Range 0 - 100,000.
    maneuver_penalty = 43200 # Penalty when a route includes a change from one road to another (seconds). Range 0 - 43,200. 
    
    default_filters = {
                        'attributes': ['edge.id', 'edge.length', 'shape'],
                        'action':'include'
                        }
    default_costing_options = {
                        'bus':{
                            'maneuver_penalty': maneuver_penalty
                            }
                        }
    def __init__(self,
                shape,
                costing='bus',
                shape_match='map_snap',
                fiters=default_filters,
                costing_options=default_costing_options,
                trace_options_turn_penalty_factor=turn_penalty_factor):

        self.request_parameters = {'shape': shape,
                'costing': costing,
                'shape_match': shape_match,
                'filters': fiters,
                'costing_options': costing_options,
                'trace_options.turn_penalty_factor':trace_options_turn_penalty_factor 
                }

class Pattern: # Attributes for each unique pattern of stops that create one or more route variant
    
    def __init__(self, index:str, route:str, direction:int, stops:List[str], trips:List[str], stop_coords:List[Tuple[float, float]]):
        self._index = index
        self._route = route
        self._direction = direction
        self._stops = stops
        self._trips = trips
        self._stop_coords = stop_coords
        self._shape = 0
        self._timepoints = []
        self._shape_coords = stop_coords
        self._coord_types = ['break_through'] * len(stop_coords)
        self._radii = [35] * len(stop_coords)
        self._v_points = [Valhalla_Point(c[1], c[0]).point_parameters for c in stop_coords]
        self._info = {}

    @property
    def info(self):
        return self._info
    
    @info.setter
    def info(self, info:Dict[str, object]) -> Dict[str, object]:
        
        info['route'] = self.route
        info['direction'] = self.direction
        info['stops'] = self.stops
        info['trips'] = self.trips
        info['stop_coords'] = self.stop_coords
        info['shape'] = self.shape
        info['timepoints'] = self.timepoints
        info['shape_coords'] = self.shape_coords
        info['stop_coords'] = self.stop_coords
        info['coord_types'] = self.coord_types
        info['v_points'] = self.v_points

        self._info = info

    @property
    def index(self):
        return self._index

    @property
    def route(self):
        return self._route


    @property
    def shape_coords(self):

        return self._shape_coords

    @shape_coords.setter
    def shape_coords(self, shape_coords:List[Tuple[float, float]]):
        self._shape_coords = shape_coords

    @property
    def coord_types(self):
        return self._coord_types
    
    @coord_types.setter
    def coord_types(self, coord_types:List[str]):
        coord_types = self.__check_length_match_shape_coords(coord_types, 'coord_types')
        # Check that the number of break_throughs match the number of stops
        diff_break_through = coord_types.count('break_through') - len(self.stop_coords)
        if diff_break_through != 0:
            raise ValueError(f'Error specifying coord types. Break Throughs - Stops = {diff_break_through} for pattern {self.index}.')
    
    @property
    def radii(self):
        return self._radii
    
    @radii.setter
    def radii(self, radii:List[int]):
        self._radii = self.__check_length_match_shape_coords(radii, 'radii')

    @property
    def v_points(self):
        return self._v_points
    
    @v_points.setter
    def v_points(self, v_points):
        self._v_points = self.__check_length_match_shape_coords(v_points, 'v_points')
    
    def __check_length_match_shape_coords(self, list:List[object], alias:str) -> List[object]:
        """Ensure that the given list matches the length of shape_coords

        Args:
            list (_type_): _description_
            alias (_type_): _description_

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        if len(list) != len(self.shape_coords):
            raise ValueError(f'Error specifying {alias} of Pattern object. Length of {alias} must match that of shape_coords.')
        else:
            return list

class Segment: # Attributes for each segment which make up a pattern
    def __init__(self, geometry, distance):
        self.geometry = geometry
        self.distance = distance
            
class Corridor: # Attributes for each corridor
    def __init__(self, edges, segments):
        self.edges = edges
        self.segments = segments
        self.passenger_shared = []
        self.stop_shared = []
        
    def get_edges(self):
        return self.edges
    
    def get_segments(self):
        return self.segments
    
    def get_pass_shared(self):
        return self.passenger_shared
    
    def get_stop_shared(self):
        return self.stop_shared