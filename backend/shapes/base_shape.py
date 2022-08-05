"""_summary_
"""

import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from ..helper_functions import check_parent_dir
import math
from tqdm.auto import tqdm
import json
import requests
from time import sleep

logger = logging.getLogger("backendLogger")



class BaseShape():
    """Stores a list of segment dicts, each dict contains keys: pattern (route_id - direction - count of pattern), 
    route_id, direction (0 for outbound or 1 for inbound), seg_index (route_id - first stop_id - second stop_id), 
    stop_pair (list of two stops), geometry (string of the encoded polyline), distance (length of segment in km), 
    mode ('bus').

    :param patterns: dict of patterns
    :type patterns: Dict[str, Dict]
    :param outpath: path to the shape json file
    :type outpath: str
    :param parameters: shape parameters, defaults to PARAMETERS
    :type parameters: Dict[str, int], optional
    :param mode: mode of the transit that the segments are of, defaults to 'bus'
    :type mode: str, optional
    """

    #: parameters that are used to balance the accuracy vs. coverage of shapes returned by Valhalla
    PARAMETERS = {
        'stop_distance_meter': 100, # Stop-to-stop distance threshold for including intermediate coordinates (meters)
        'maximum_radius_increase': 100, # Self-defined parameter to limit the search area for matching coordinates (meters)
        'stop_radius': 35, # Radius used to search when matching stop coordinates (meters)
        'intermediate_radius': 100, # Radius used to search when matching intermediate coordinates (meters)
        'radius_increase_step': 10 # Step size used to increase search area when Valhalla cannot find an initial match (meters)
    }

    def __init__(self, patterns, outpath, parameters=PARAMETERS, mode='bus'):
        """_summary_
        """

        logger.info(f'Generating shapes...')
        self.mode = mode
        self.PARAMETERS = parameters
        self.patterns = self.check_patterns(patterns)
        self.outpath = check_parent_dir(outpath)

        self.shapes = self.generate_shapes()
        logger.info(f'shapes generated')

    def check_patterns(self, patterns:Dict) -> Dict:
        """_summary_

        :param patterns: _description_
        :type patterns: Dict
        :raises TypeError: _description_
        :raises TypeError: _description_
        :raises TypeError: _description_
        :return: _description_
        :rtype: Dict
        """
        
        if not isinstance(patterns, Dict):
            raise TypeError(f'patterns must be given as a dict to be processed for shapes')
        for pattern, segments in patterns.items():
            if not isinstance(segments, Dict):
                raise TypeError(f'segments for pattern: {pattern} must be given as a dict')
            for seg_id, seg_info in segments.items():
                if not isinstance(seg_info, List) or not all([isinstance(coords, Tuple) for coords in seg_info]):
                    raise TypeError(f'info for segment: {seg_id} must be a list of coordinates')
        logger.debug(f'total number of patterns: {len(patterns.keys())}')
        return patterns

    def generate_shapes(self) -> Dict:
        """_summary_

        Returns:
            Dict: Pattern dict - key: pattern; value: Segment dict (a segment is a section of road between two transit stops). 
                    Segment dict - key: tuple of stop IDs at the beginning and end of the segment; 
                                    value: segment information (geometry, distance).
        """

        PARAMETERS = self.PARAMETERS
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
                    coords_to_use = [coords[i] for i in np.unique(np.append(np.arange(0, len(coords), step),[len(coords)-1]))]

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
                                **{'pattern': p_name,
                                    'route_id': f"{'-'.join(p_name.split('-')[0:-2])}",
                                    'direction': int(p_name.split('-')[-2]),
                                    'seg_index':f"{'-'.join(p_name.split('-')[0:-2])}-{s_name[0]}-{s_name[1]}",
                                    'stop_pair': s_name, 
                                    'mode': self.mode},
                                **s_info
                            }
                            for p_name, segments in all_matched.items() \
                                for s_name, s_info in segments.items()]

        with open(self.outpath, 'w') as fp:
            json.dump(matched_output, fp)

        skipped_output = [
                            {
                                **{'pattern': p_name,
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
            except requests.exceptions.ConnectionError:
                sleep(1)
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
