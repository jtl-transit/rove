from abc import abstractmethod
import logging
from unittest import skip
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
import requests
import json
from zmq import ROUTER

logger = logging.getLogger("backendLogger")

class Valhalla_Point():


    def __init__(self, 
                lat,
                lon,
                type,
                radius,
                rank_candidates='true',
                preferred_side='same',
                node_snap_tolerance=0,
                street_side_tolerance=0,):

        self.lat = lat
        self.lon = lon
        self.type = type
        self.radius = radius
        self.rank_candidates = rank_candidates
        self.preferred_side = preferred_side
        self.node_snap_tolerance = node_snap_tolerance
        self.street_side_tolerance = street_side_tolerance

    def point_parameters(self):

        return {'lat': self.lat,
                'lon': self.lon,
                'type': self.type,
                'radius': self.radius,
                'rank_candidates': self.rank_candidates,
                'preferred_side': self.preferred_side,
                'node_snap_tolerance': self.node_snap_tolerance,
                'street_side_tolerance': self.street_side_tolerance
                }


class Valhalla_Request():
    
    def __init__(self,
                shape_name,
                shape, # list of Valhalla_Point
                costing='bus',
                shape_match='map_snap',
                filters={
                        'attributes': ['edge.id', 'edge.length', 'shape'],
                        'action':'include'
                        },
                costing_options={
                        'bus':{
                            'maneuver_penalty': 43200
                            }
                        },
                trace_options_turn_penalty_factor=100000):
        
        self.shape_name = shape_name
        self.shape = shape
        self.costing = costing
        self.shape_match = shape_match
        self.filters = filters
        self.costing_options = costing_options
        self.trace_options_turn_penalty_factor = trace_options_turn_penalty_factor


    def request_parameters(self):

        return {'shape': self.shape,
                'costing': self.costing,
                'shape_match': self.shape_match,
                'filters': self.filters,
                'costing_options': self.costing_options,
                'trace_options.turn_penalty_factor': self.trace_options_turn_penalty_factor 
                }

    def get_trace_route_response(self, timeout=60):
        """_summary_

        Args:
            timeout (int, optional): _description_. Defaults to 60.

        Raises:
            requests.HTTPError: _description_

        Returns:
            matched (Dict): matched dict - key: shape name; value: dict of legs info
                                leg dict - key: leg index; value: geometry (encoded polyline), length
            skipped (Dict): skipped dict - key: shape name; value: list of Valhalla points info
        """
        matched = {}
        skipped = {}

        request_data = self.request_parameters()
        try:
            response = requests.post('http://localhost:8002/trace_route',
                                data = json.dumps(request_data),
                                timeout = timeout)
            result = response.json()
                    
            if 'status_code' in result and result['status_code'] != 200:
                status_code = result['status_code']
                status = result['status']
                raise requests.HTTPError(f'Invalid response from Valhalla. '\
                    f'Status code: {status_code}. Status: {status}.')

        except ConnectionError:
            logger.exception(f'Error connecting to Valhalla service.')
            quit()
        except requests.HTTPError:
            # logger.exception(f'Bad Valhalla request for {self.shape_name}.')
            if self.shape_name not in skipped:
                    skipped[self.shape_name] = {}

            skipped[self.shape_name] = {
                'shape_input': self.shape,
                'result': result
            }

        else:
            for leg_index in range(len(result['trip']['legs'])):
                leg = result['trip']['legs'][0]
                geometry = leg['shape']
                length = leg['summary']['length']
                
                if self.shape_name not in matched:
                    matched[self.shape_name] = {}
                                    
                matched[self.shape_name][leg_index] = {
                    'geometry': geometry,
                    'distance': length
                }

        return matched, skipped

class Pattern: # Attributes for each unique pattern of stops that create one or more route variant
    
    def __init__(self, index:str, route:str, direction:int, stops:List[str], trips:List[str], stop_coords:List[Tuple[float, float]]):
        self._index = index
        # self._route = route
        # self._direction = direction
        self._stops = stops
        # self._trips = trips
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