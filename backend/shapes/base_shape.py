import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from backend.helper_functions import check_parent_dir, check_is_file
from backend.data_class.rove_parameters import ROVE_params
import math
from tqdm.auto import tqdm
import json
import requests
from time import sleep
import geopandas as gpd
from shapely.geometry import LineString
import polyline
import stateplane

logger = logging.getLogger("backendLogger")

class BaseShape():
    """Stores a list of segment dicts and output the list to a json file. Each dict contains keys: 
    pattern (route_id - direction - count of pattern), 
    route_id, 
    direction (0 for outbound or 1 for inbound), 
    seg_index (route_id - first stop_id - second stop_id), 
    stop_pair (list of two stops), 
    geometry (string of the encoded polyline), 
    distance (length of segment in km), 
    mode ('bus').

    :param patterns: dict of patterns
    :type patterns: Dict[str, Dict]
    :param outpath: path to the shape json file
    :type outpath: str
    :param parameters: shape parameters, defaults to MAP_MATCHING_PARAMETERS
    :type parameters: Dict[str, int], optional
    :param mode: mode of the transit that the segments are of, defaults to 'bus'
    :type mode: str, optional
    """

    #: parameters that are used in the map matching process to balance the accuracy vs. coverage of shapes returned by Valhalla
    MAP_MATCHING_PARAMETERS = {
        'stop_distance_meter': 100, # Stop-to-stop distance threshold for including intermediate coordinates (meters)
        'maximum_radius_increase': 100, # Self-defined parameter to limit the search area for matching coordinates (meters)
        'stop_radius': 35, # Radius used to search when matching stop coordinates (meters)
        'intermediate_radius': 100, # Radius used to search when matching intermediate coordinates (meters)
        'radius_increase_step': 10 # Step size used to increase search area when Valhalla cannot find an initial match (meters)
        }

    def __init__(self, patterns, params:ROVE_params, check_signal, mode='bus'):

        logger.info(f'Generating shapes...')
        self.params = params
        self.outpath = self.params.output_paths['shapes']
        self.patterns, self.sample_coord = self.__check_patterns(patterns)
        self.mode = mode
        self.shapes = self.generate_segment_shapes()
        # self.shapes = read_shapes(self.params.output_paths['shapes'])
        if check_signal:
            self.shapes = self.check_signal_intersection()
        
        self.generate_shapes_json()

    def generate_shapes_json(self):

        outpath = check_parent_dir(self.outpath)
        with open(outpath, 'w') as fp:
            shapes_json = json.loads(self.shapes.to_json(orient='records'))
            json.dump(shapes_json, fp)

    def __check_patterns(self, patterns:Dict) -> Dict:
        
        if not isinstance(patterns, Dict):
            raise TypeError(f'patterns must be given as a dict to be processed for shapes')
        for pattern, segments in patterns.items():
            if not isinstance(segments, Dict):
                raise TypeError(f'segments for pattern: {pattern} must be given as a dict')
            for seg_id, seg_info in segments.items():
                if not isinstance(seg_info, List) or not all([isinstance(coords, Tuple) for coords in seg_info]):
                    raise TypeError(f'info for segment: {seg_id} must be a list of coordinates')
                else:
                    sample_coord = seg_info[0]
        logger.debug(f'total number of patterns: {len(patterns.keys())}')
        return patterns, sample_coord

    def generate_segment_shapes(self) -> pd.DataFrame:
        """For each segment, find its encoded polyline and distance, then save the data in a json file as well
        as a dataframe.

        :return: a dataframe where each row contains all information of a segment
        :rtype: pd.DataFrame
        """

        PARAMETERS = self.MAP_MATCHING_PARAMETERS
        all_matched = {}
        all_skipped = {}

        # pbar = tqdm(total=len(self.patterns.keys()), desc='Generating pattern shapes', position=0)
        # for p_name, segments in self.patterns.items():
        for p_name, segments in tqdm(self.patterns.items(), desc='Generating pattern shapes', position=0):
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

            # pbar.update()

        matched_output = [
                            {
                                **{'pattern': p_name,
                                    'route_id': f"{'-'.join(p_name.split('-')[0:-2])}",
                                    'direction': int(p_name.split('-')[-2]),
                                    'seg_index':f"{'-'.join(p_name.split('-')[0:-2])}-{s_name[0]}-{s_name[1]}",
                                    'stop_pair': s_name, 
                                    'timepoint_index': f"{p_name}-0",
                                    'mode': self.mode},
                                **s_info
                            }
                            for p_name, segments in all_matched.items() \
                                for s_name, s_info in segments.items()]
        outpath = check_parent_dir(self.outpath)
        with open(outpath, 'w') as fp:
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
        

    def check_signal_intersection(self):

        OSM_PLANE = 'EPSG:4326'
        STATE_PLANE = f'EPSG:{stateplane.identify(self.sample_coord)}'
        logger.info(f'checking intersecting signals')
        signal_inpath = check_is_file(self.params.input_paths['signals'])
        signal_df = gpd.read_file(signal_inpath)
        signal_df['state_point'] = gpd.GeoSeries(signal_df['geometry']).set_crs(OSM_PLANE).to_crs(STATE_PLANE)

        # Add buffer around each traffic signal to catch any segments that pass through the intersection
        buffer_radius = 10 # in ft
        signal_df['buffer_state_point'] = signal_df['state_point'].buffer(buffer_radius)

        sig_gdf = gpd.GeoDataFrame(signal_df[['id', 'buffer_state_point']], geometry='buffer_state_point')
        # signal_df_buffer = signal_df.buffer(buffer_radius)
        # all_signals = signal_df_buffer.unary_union
        
        # Decode polylines and create geodataframe with same projection as signal df
        shapes = self.shapes.copy()
        shapes['linestring'] = shapes['geometry'].apply(lambda x: LineString(polyline.decode(x, geojson = True, precision = 6)))
        shapes['state_linestring'] = gpd.GeoSeries(shapes['linestring']).set_crs(OSM_PLANE).to_crs(STATE_PLANE)
        
        bus_gdf = gpd.GeoDataFrame(shapes[['seg_index', 'state_linestring']], geometry='state_linestring')

        intersect = gpd.sjoin(sig_gdf, bus_gdf, how='left')
        intersect['intersect'] = ~intersect['seg_index'].isnull()
        ig = intersect.groupby('index_right')['intersect'].max().reset_index()
        ig['index'] = ig['index_right'].astype('int')
        ig = ig.set_index('index').drop(columns=['index_right'])

        shapes = shapes.merge(ig, left_index=True, right_index=True, how='left')
        shapes['intersect'] = shapes['intersect'].fillna(False)

        # bus_gdf = gpd.GeoSeries(shapes['linestring']).set_crs(OSM_PLANE).to_crs(STATE_PLANE)
        shapes = shapes.drop(columns=['linestring', 'state_linestring'])

        # For each segment, check if intersects with full signal dataframe
        # shapes['intersect'] = bus_gdf.intersects(all_signals)
        
        return shapes

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
    """Store information of a point that will become part of a list of points passed to the Valhalla trace_attribute 
    service. More detailed documentation: https://valhalla.readthedocs.io/en/latest/api/map-matching/api-reference/#example-trace_attributes-requests.

    :param lat: latitude of the point
    :type lat: float
    :param lon: longitude of the point
    :type lon: float
    :param type: "break" for endpoint of a segment or "via" for intermediate point of a segment
    :type type: str
    :param radius: see Valhalla documentation
    :type radius: int
    :param rank_candidates: see Valhalla documentation, defaults to 'true'
    :type rank_candidates: str, optional
    :param preferred_side: see Valhalla documentation, defaults to 'same'
    :type preferred_side: str, optional
    :param node_snap_tolerance: see Valhalla documentation, defaults to 0
    :type node_snap_tolerance: int, optional
    :param street_side_tolerance: see Valhalla documentation, defaults to 0
    :type street_side_tolerance: int, optional
    """

    def __init__(self, 
                lat:float,
                lon:float,
                type:str,
                radius:int,
                rank_candidates:str='true',
                preferred_side:str='same',
                node_snap_tolerance:int=0,
                street_side_tolerance:int=0,):

        self.lat = lat
        self.lon = lon
        self.type = type
        self.radius = radius
        self.rank_candidates = rank_candidates
        self.preferred_side = preferred_side
        self.node_snap_tolerance = node_snap_tolerance
        self.street_side_tolerance = street_side_tolerance

    def point_parameters(self) -> Dict:
        """Return all information about a point

        :return: a dict of informaiton pertaining to one point
        :rtype: Dict
        """

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
    """Store the request content that can be sent to the Valhalla trace_attribute service. Provide method to send
    the request to Valhalla and save the output from Valhalla. 
    Each request is for one segment only. 
    More detailed documentation: https://valhalla.readthedocs.io/en/latest/api/map-matching/api-reference/#example-trace_attributes-requests.

    :param shape_name: name of the segment
    :type shape_name: str
    :param shape: shape parameters, i.e. a list of Valhalla_Points' point_parameters, each point is a point on the segment
    :type shape: List
    :param costing: see Valhalla documentation, defaults to 'bus'
    :type costing: str, optional
    :param shape_match: see Valhalla documentation, defaults to 'map_snap'
    :type shape_match: str, optional
    :param filters: see Valhalla documentation, defaults to { 'attributes': ['edge.id', 'edge.length', 'shape'], 'action':'include' }
    :type filters: Dict, optional
    :param costing_options: see Valhalla documentation, defaults to { 'bus':{ 'maneuver_penalty': 43200 } }
    :type costing_options: Dict, optional
    :param trace_options_turn_penalty_factor: see Valhalla documentation, defaults to 100000
    :type trace_options_turn_penalty_factor: int, optional
    :param max_retry: see Valhalla documentation, defaults to 10
    :type max_retry: int, optional
    """

    def __init__(self,
                shape_name:str,
                shape:List,
                costing:str='bus',
                shape_match:str='map_snap',
                filters:Dict={
                        'attributes': ['edge.id', 'edge.length', 'shape'],
                        'action':'include'
                        },
                costing_options:Dict={
                        'bus':{
                            'maneuver_penalty': 43200
                            }
                        },
                trace_options_turn_penalty_factor:int=100000,
                max_retry:int=10):

        self.max_retry = max_retry
        self.shape_name = shape_name
        self.shape = shape
        self.costing = costing
        self.shape_match = shape_match
        self.filters = filters
        self.costing_options = costing_options
        self.trace_options_turn_penalty_factor = trace_options_turn_penalty_factor


    def request_parameters(self):
        """Return the request content for one segment

        :return: a dict of request content pertaining to a segment
        :rtype: Dict
        """

        return {'shape': self.shape,
                'costing': self.costing,
                'shape_match': self.shape_match,
                'filters': self.filters,
                'costing_options': self.costing_options,
                'trace_options.turn_penalty_factor': self.trace_options_turn_penalty_factor 
                }

    def get_trace_route_response(self, timeout=100):
        """Retrieve the request content for a segment, then send the request to Valhalla. If a good response is returned, 
        then the geometry (encoded polyline) and distance are saved in the matched dict 
        (e.g. {'seg1_id': {0: {geometry: xxx, distance: 0.5}}, 'seg2_id': {0: {geometry: xxx, distance: 0.5}}}). Otherwise if 
        the response is invalid, then the segment information is stored in the skipped dict 
        (e.g. {'seg1_id': {'shape_input': {request_parameters}, 'result': {400: 'No suitable edges near location'}}}).

        :param timeout: request timeout threshold in seconds, defaults to 100
        :type timeout: int, optional
        :raises requests.HTTPError: the response from Valhalla did not come back with a status code of 200
        :raises ConnectionError: encounters Valhall connection error, will sleep for 1 second and retry
        :return: tuple of matched and skipped dicts
        :rtype: Tuple[Dict, Dict]
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
