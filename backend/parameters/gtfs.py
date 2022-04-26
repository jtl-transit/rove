"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set, Tuple
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback
from .base_data_class import BaseData
from shapely.ops import nearest_points
from shapely.geometry import LineString, Point
from scipy.spatial import distance
import tqdm
import math
from .helper_functions import get_hash_of_stop_list

logger = logging.getLogger("backendLogger")

REQUIRED_DATA_SPEC = {'agency':{

                        }, 
                    'stops':{
                        'stop_id':'str',
                        'stop_lat':'float64',
                        'stop_lon':'float64'
                        }, 
                    'routes':{

                        }, 
                    'trips':{
                        'route_id':'str',
                        'trip_id':'str',
                        'direction_id':'int64'
                        }, 
                    'stop_times':{
                        'trip_id':'str',
                        'stop_id':'str',
                        'stop_sequence':'int64'
                        }}
OPTIONAL_DATA_SPEC = {'shapes':{
                        'shape_id':'str',
                        'shape_pt_lat':'float64',
                        'shape_pt_lon':'float64',
                        'shape_pt_sequence':'int64'}}

class GTFS(BaseData):

    def __init__(self, alias, path, rove_params=None):
        super().__init__(alias, path, rove_params)

        self._pattern_dict = self.__get_pattern_dict()
        if 'shapes' in self.validated_data.keys():
            self. __improve_pattern_with_shapes()

    def load_data(self, path:str)->Dict[str, DataFrame]:
        """Load in GTFS data from a zip file, and retrieve data of the sample date (as stored in rove_params) and 
        route_type (as stored in config). Enforce that required tables are present and not empty, and log (w/o enforcing)
        if optional tables are not present in the feed or empty. Enforce that all spec columns exist for tables in both 
        the required and optional specs. Store the retrieved raw data tables in a dict.

        Returns:
            dict <str, DataFrame>: key: name of GTFS table; value: DataFrames of required and optional GTFS tables.
        """
        rove_params = self.rove_params

        # Retrieve GTFS data for the sample date
        try:
            service_id_list = ptg.read_service_ids_by_date(path)[rove_params.sample_date]
        except KeyError as err:
            logger.exception(traceback.format_exc())
            logger.fatal(f'{err}: Services for sample date {rove_params.sample_date} cannot be found in GTFS.'\
                        f'Please make sure the GTFS data is indeed for {rove_params.month}-{rove_params.year}.')
            quit()

        # Load GTFS feed
        view = {'routes.txt': {'route_type': rove_params.config['route_type']}, 'trips.txt': {'service_id': service_id_list}}
        feed = ptg.load_feed(path, view)

        # Store all required raw tables in a dict, enforce that every table listed in the spec exists and is not empty
        required_data = self.__get_non_empty_gtfs_table(feed, REQUIRED_DATA_SPEC, required=1)

        # Add whichever optional table listed in the spec exists and is not empty
        optional_data = self.__get_non_empty_gtfs_table(feed, OPTIONAL_DATA_SPEC)

        return {**required_data, **optional_data}

    def __get_non_empty_gtfs_table(self, feed:ptg.readers.Feed, table_col_spec:Dict[str,Dict[str,str]], required=0)->Dict[str, DataFrame]:
        """Store in a dict all non-empty GTFS tables from the feed that are listed in the spec. 
        For required tables, each table must exist in the feed and must not be empty, otherwise the program will be halted.
        For optional tables, any table in the spec not in the feed or empty table in the feed is skipped and not stored.
        For tables in any spec, all spec columns must exist if the spec table is not empty.

        Args:
            feed (ptg.readers.Feed): GTFS feed
            table_col_spec (Dict[str,Dict[str,str]]): key: GTFS table name; value: dict of <column name: column dtype>
            requied (int, optional): whether the table_col_spec is required. Defaults to 0.

        Raises:
            ValueError: table is found in the feed, but is empty.
            AttributeError: a table name specified in table_col_spec is not found in GTFS feed.

        Returns:
            Dict[str, DataFrame]: key: name of GTFS table; value: GTFS table stored as DataFrame.
        """
        data = {}
        for table_name, columns in table_col_spec.items():
            try:
                feed_data = getattr(feed, table_name)
                if feed_data.empty:
                    raise ValueError(f'{table_name} data is empty.')
                elif not set(columns.keys()).issubset(feed_data.columns):
                    # not all spec columns are found in raw table => some spec columns are missing
                    missing_columns = set(columns.keys()) - set(feed_data.columns)
                    raise KeyError(f'Table "{table_name}" is missing required columns: {missing_columns}.')
                else:
                    # all spec columns are found in the raw table, so store the raw table
                    data[table_name] = feed_data
            except AttributeError as err:
                if required:
                    logger.fatal(f'{err}: Could not find required table {table_name} from GTFS data. ' + \
                        f'Please double check that the GTFS data you provided to ROVE has this table. Exiting...')
                    quit()
                else:
                    logger.warning(f'{err}: Could not find optional table {table_name} from GTFS data. Skipping...')
            except ValueError as err:
                if required:
                    logger.fatal(f'{err}: Please verify that the GTFS file for {table_name} has valid data.')
                    quit()
                else:
                    logger.warning(f'{err}: The GTFS file for the optional table {table_name} is empty. Skipping...')
        return data
    
    def validate_data(self):
        """Clean up raw data by converting column types to those listed in the spec.

        Raises:
            ValueError: if any one type of the required raw data is empty

        Returns:
            dict <str, DataFrame>: validated and cleaned-up data
        """
        # avoid changing the raw data object
        data = self.raw_data.copy()
        data_dict = {**REQUIRED_DATA_SPEC, **OPTIONAL_DATA_SPEC}

        # convert column types according to the spec
        for table_name, df in data.items():
            columns_dtype_dict = data_dict[table_name]
            cols = list(columns_dtype_dict.keys())
            df[cols] = df[cols].astype(dtype=columns_dtype_dict)
            # try:
            #     df[cols] = df[cols].astype(dtype=columns_dtype_dict)
            # except KeyError as err:
            #     print(err)
        
        # # filter based on stop_times
        # # stop_times = self.filter_table_a_on_unique_b_key(data, 'stop_times', 'trips', ['trip_id'])
        # stop_times = data['stop_times']
        # trips = self.__filter_table_a_on_unique_b_key(data, 'trips', 'stop_times', ['trip_id'])
        # stops = self.__filter_table_a_on_unique_b_key(data, 'stops', 'stop_times', ['stop_id'])

        return data

    def __get_pattern_dict(self)->Dict:
        """Generate a dict of patterns from validated GTFS data. Add a "hash" column to the trips table.

        Raises:
            ValueError: number of unique trip hashes does not match with number of unique sequence of stops

        Returns:
            Dict: Pattern dict - key: pattern hash; value: Segment dict (a segment is a section of road between two transit stops). 
                    Segment dict - key: tuple of stop IDs at the beginning and end of the segment; value: list of coordinates defining the segment.
        """
        logger.info(f'generating patterns with GTFS data...')

        # Handle of trips table stored in validated_data (reference semantics). 
        # The handle is used for simplicify of referencing the validated_data trips table, so adding column to the handle
        # changes the referenced object as well. The object is not reassigned other objects later on.
        trips = self.validated_data['trips']
        
        # Generate a dataframe of trip_id (unique), list of stop_ids, and hash of the stop_ids list
        stop_times = self.validated_data['stop_times'].sort_values(by=['trip_id', 'stop_sequence'])
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
        stops = self.validated_data['stops'][['stop_id','stop_lat','stop_lon']].drop_duplicates()
        stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
        stop_coords_lookup = stops.set_index('stop_id')['coords'].to_dict()

        # Get a dict of <hash: segments>
        # e.g. {2188571819865: {('64', '1'):[(lat64, lon64), (lat1, lon1)], ('1', '2'): [(lat1, lon1), (lat2, lon2)]...}...}
        hash_segments_lookup = {hash: {(stop_ids[i], stop_ids[i+1]): [stop_coords_lookup[stop_ids[i]], stop_coords_lookup[stop_ids[i+1]]] \
                                    for i in range(len(stop_ids)-1)} for hash, stop_ids in hash_stops_lookup.items()}

        logger.info(f'gtfs patterns generated')

        return hash_segments_lookup


    @property
    def pattern_dict(self):
        return self._pattern_dict

    def __improve_pattern_with_shapes(self):
        """Improve the coordinates of each segment in each pattern by replacing the stop coordinates
            with coordinates found in the GTFS shapes table. Also add additional intermediate coordinates
            to enrich the segment profile if the distance of the segment is long.
        """
        logger.info(f'improving pattern with GTFS shapes table...')

        # Stop-to-stop distance threshold for including intermediate coordinates (meters)
        stop_distance_threshold  = 1000

        trips = self.validated_data['trips']
        shapes = self.validated_data['shapes'].sort_values(by=['shape_id', 'shape_pt_sequence'])

        shapes['coords'] = list(zip(shapes.shape_pt_lat, shapes.shape_pt_lon))
        shape_coords_lookup = shapes.groupby('shape_id')['coords'].agg(list).to_dict()

        trip_shape_lookup = trips[['trip_id', 'shape_id']].set_index('trip_id')['shape_id'].to_dict()
        hash_trips_lookup = trips.groupby('hash')['trip_id'].agg(list).to_dict()

        for hash, segments in self.pattern_dict.items():
            
            # Find a representative shape ID and the corresponding list of shape coordinates
            trips = hash_trips_lookup[hash]
            try:
                example_shape = next(trip_shape_lookup[t] for t in trips if t in trip_shape_lookup)
            except StopIteration as err:
                logger.debug(f'{err}: no example shape can be found for hash: {hash}.')
                example_shape = -1
                continue
            
            shape_coords = shape_coords_lookup[example_shape]
            
            # For each segment, find indices of the start and end stops from the list of shape coordinates
            #   Key: id_pair; Value: [index of start stop, index of end stop in shape_coords]
            #   Example: {(64, 1): [0, 7], (1, 2): [7, 13]}
            seg_shape_indices = {id_pair: [self.find_nearest_point(shape_coords, coord_list[0]), \
                                        self.find_nearest_point(shape_coords, coord_list[-1])] \
                                for id_pair, coord_list in segments.items()}
            
            # Add intermediate coordinates if stops on two ends of a segment are far apart
            for id_pair, indicies in seg_shape_indices.items():

                interval_count = max(math.floor(self.get_distance(shape_coords[indicies[0]], shape_coords[indicies[-1]])/stop_distance_threshold),1)
                step = math.ceil((indicies[-1]-indicies[0]) / interval_count )
                ind_list = list(np.arange(indicies[0], indicies[-1], step)) + [indicies[-1]]
                
                c_list = [shape_coords[i] for i in ind_list]
                segments[id_pair] = c_list

        logger.info(f'patterns improved with GTFS shapes')

    def find_nearest_point(self, coord_list, coord):
        dist = distance.cdist(np.array(coord_list), np.array([coord]), 'euclidean')
        return dist.argmin()

    def get_distance(self, start:Tuple[float, float], end:Tuple[float, float]) -> float:
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
