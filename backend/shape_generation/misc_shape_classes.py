from abc import abstractmethod
import logging
from unittest import skip
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
import requests
import json

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
                trace_options_turn_penalty_factor=100000,
                max_retry=10):
        
        self.max_retry = max_retry
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

    def get_trace_route_response(self, timeout=100):
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
        retry_count = 0
        while retry_count < self.max_retry:
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
                logger.exception(f'Error connecting to Valhalla service. Retry count {retry_count}...')
                retry_count += 1
            except requests.HTTPError:
                # logger.exception(f'Bad Valhalla request for {self.shape_name}.')
                if self.shape_name not in skipped:
                        skipped[self.shape_name] = {}

                skipped[self.shape_name] = {
                    'shape_input': self.shape,
                    'result': result
                }
                break
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
                break
        if retry_count == self.max_retry:
            raise ConnectionError(f'Max retry reached. Unable to proceed.')

        return matched, skipped
