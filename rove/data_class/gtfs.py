"""Data class for GTFS data.
"""

from typing import Dict
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging

from .rove_parameters import ROVE_params
from .base_data_class import BaseData
from copy import deepcopy
from rove.helper_functions import get_hash_of_stop_list, check_dataframe_column
from scipy.spatial import distance


logger = logging.getLogger("backendLogger")

REQUIRED_DATA_SPEC = {
                    'stops':{
                        'stop_id':'string',
                        'stop_code':'string',
                        'stop_lat':'float64',
                        'stop_lon':'float64'
                        }, 
                    'routes':{
                        'route_id':'string',
                        'route_type': 'int64'
                        }, 
                    'trips':{
                        'route_id':'string',
                        'service_id':'string',
                        'trip_id':'string',
                        'direction_id':'int64', # not required by GTFS but required by ROVE
                        }, 
                    'stop_times':{
                        'trip_id':'string',
                        'arrival_time':'int64',
                        'departure_time':'int64',
                        'stop_id':'string',
                        'stop_sequence':'int64',
                        }
                    }
OPTIONAL_DATA_SPEC = {
                    'shapes':{
                        'shape_id':'string',
                        'shape_pt_lat':'float64',
                        'shape_pt_lon':'float64',
                        'shape_pt_sequence':'int64'
                        }
                    }

class GTFS(BaseData):
    """GTFS data class. Stores a validated GTFS stop records table. Add timepoint and branchpoint data to the records table. 
        Also generate and store a dict of route patterns (patterns_dict).

    :param rove_params: a rove_params object that stores information needed throughout the backend
    :type rove_params: ROVE_params
    :param mode: the mode of transit that the GTFS data is for, defaults to 'bus'. The current implementation is developed for bus GTFS data only. 
        Support for other transit modes may be added in the future.
    :type mode: str, optional
    """

    def __init__(self, rove_params:ROVE_params, mode:str='bus'):
        """Instantiate a GTFS data class.
        """

        if mode not in rove_params.config['route_type'].keys():
            raise KeyError(f'Invalid mode: {mode}. Cannot find the corresponding route type value in the config file.')
        self.mode = mode

        super().__init__('gtfs', rove_params)        

        # create the records table that contains all stop events info and trips info
        self.records:pd.DataFrame = self.get_gtfs_records()

        # make sure the 'timepoint' column is valid in the stop_times table
        logger.debug(f'adding timepoints to GTFS data')
        self.add_timepoints()
        check_dataframe_column(self.records, 'timepoint', '0or1')

        # make sure the 'branchpoint' column is valid in the stop_times table
        logger.debug(f'adding branchpoints to GTFS data')
        self.add_branchpoints()
        check_dataframe_column(self.records, 'branchpoint', '0or1')

        # create the patterns dict. Key: pattern, value: segment dict 
        #   segment dict key: tuple of stops defining the segment, value: coordinates of the segment)
        self.patterns_dict = self.generate_patterns()

        if 'shapes' in self.validated_data.keys():
            self.patterns_dict = self. improve_pattern_with_shapes(self.patterns_dict, self.records, self.validated_data)


    def load_data(self, path:str)->Dict[str, DataFrame]:
        """Load in GTFS data from a zip file, and retrieve data of the sample date (as stored in rove_params) and 
            route_type (as stored in config). Enforce that required tables are present and not empty, and log (w/o enforcing)
            if optional tables are not present in the feed or empty. Enforce that all spec columns exist for tables in both 
            the required and optional specs. Store the retrieved raw data tables in a dict.

        :param path: path to the raw data
        :type path: str
        :return: a dict containing raw GTFS data. Key: name of GTFS table; value: DataFrames of required and optional GTFS tables.
        :rtype: Dict[str, DataFrame]
        """

        rove_params = self.rove_params

        # Retrieve GTFS data for the sample date
        try:
            service_id_list = ptg.read_service_ids_by_date(path)[rove_params.sample_date]
        except KeyError as err:
            logger.fatal(f'{err}: Services for sample date {rove_params.sample_date} cannot be found in GTFS.', exc_info=True)
            quit()

        # Load GTFS feed
        view = {'routes.txt': {'route_type': rove_params.config['route_type'][self.mode]}, 'trips.txt': {'service_id': service_id_list}}
        feed = ptg.load_feed(path, view)

        # Store all required raw tables in a dict, enforce that every table listed in the spec exists and is not empty
        required_data = self.__get_non_empty_gtfs_table(feed, REQUIRED_DATA_SPEC, required=True)

        # Add whichever optional table listed in the spec exists and is not empty
        optional_data = self.__get_non_empty_gtfs_table(feed, OPTIONAL_DATA_SPEC)

        return {**required_data, **optional_data}

    def __get_non_empty_gtfs_table(self, feed:ptg.readers.Feed, table_col_spec:Dict[str,Dict[str,str]], required:bool=False)\
                                    ->Dict[str, DataFrame]:
        """Store in a dict all non-empty GTFS tables from the feed that are listed in the spec. 
            For required tables, each table must exist in the feed and must not be empty, otherwise the program will be halted.
            For optional tables, any table in the spec not in the feed or empty table in the feed is skipped and not stored.
            For tables in any spec, all spec columns must exist if the spec table is not empty.

        :raises ValueError: table is found in the feed, but is empty.
        :raises KeyError: table is missing at least one of the required columns
        :return: a dict containing raw GTFS data. Key: name of GTFS table; value: GTFS table stored as DataFrame.
        :rtype: Dict[str, DataFrame]
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
            except AttributeError:
                if required:
                    logger.fatal(f'Could not find required table {table_name} from GTFS data.', exc_info=True)
                    quit()
                else:
                    logger.warning(f'Could not find optional table {table_name} from GTFS data. Skipping...')
            except ValueError:
                if required:
                    logger.fatal(f'The GTFS file for the required table {table_name} is empty.', exc_info=True)
                    quit()
                else:
                    logger.warning(f'The GTFS file for the optional table {table_name} is empty. Skipping...')
        return data
    
    def validate_data(self):
        """Clean up raw data by converting column types to those listed in the spec.

        :return: a dict containing cleaned-up GTFS data. Key: name of GTFS table; value: GTFS table stored as DataFrame.
        :rtype: Dict[str, DataFrame]
        """

        # avoid changing the raw data object
        data:Dict = deepcopy(self.raw_data)
        data_specs = {**REQUIRED_DATA_SPEC, **OPTIONAL_DATA_SPEC}

        # convert column types according to the spec
        for table_name, df in data.items():
            columns_dtype_dict = data_specs[table_name]
            cols = list(columns_dtype_dict.keys())
            df[cols] = df[cols].astype(dtype=columns_dtype_dict)

        return data

    def get_gtfs_records(self) -> pd.DataFrame:
        """Return a dataframe that is the validated GTFS stop_times table left joined by the validated GTFS trips table. 
            Values are sorted by [route_id, trip_id, stop_sequence]. Additional columns are added for the convenience of downstream
            calculations: 
                - 'hour' - the hour that the arrival time is in;
                - 'trip_start_time': start time of the trip that this stop event record is associated with;
                - 'trip_end_time': end time of the trip that this stop event record is associated with.

        :return: the merged dataframe and additional columns
        :rtype: pd.DataFrame
        """

        trips:pd.DataFrame = self.validated_data['trips']
        stop_times:pd.DataFrame = self.validated_data['stop_times']

        gtfs_df = stop_times.merge(trips, on='trip_id', how='left')\
                        .sort_values(by=['route_id', 'trip_id', 'stop_sequence'])
        
        gtfs_df['hour'] = (gtfs_df.groupby('trip_id')['arrival_time'].transform('min'))//3600
        gtfs_df['trip_start_time'] = gtfs_df.groupby('trip_id')['arrival_time'].transform('min')
        gtfs_df['trip_end_time'] = gtfs_df.groupby('trip_id')['arrival_time'].transform('max')

        return gtfs_df

    def add_timepoints(self):
        """Add, or repopulate, the 'timepoint' column in the GTFS records table (created from get_gtfs_records()). 'timepoint' is an optional column in GTFS standards, but
            we require the identification of timepoints in each trip for timepoint-level metric calculations. Therefore, each agency must either supply
            the 'timepoint' info in the 'timepoint' column of the 'stop_times' table in GTFS data, or provide additional data source and extend the standard
            GTFS class and overwrite this method to populate the 'timepoint' column in the GTFS records table. 
        """

        pass

    def add_branchpoints(self):
        """Add the 'branchpoint' and 'tp_bp' columns in the GTFS records table. 'branchpoint' is defined as stops where routes converge or diverge between two timepoints.
            The 'tp_bp' column marks stops that are either a timepoint or a branchpoint. The 'tp_bp' stop pairs are the basis of aggregation for 'timepoint' and 'timepoint-aggregated' metrics.
        """

        records = self.records

        g = deepcopy(records)
        tg = deepcopy(g[g['timepoint']==1])

        # identify timepoint pairs for each stop event
        tg['next_stop'] = tg.groupby(by='trip_id')['stop_id'].shift(-1)
        tg = tg.dropna(subset=['next_stop'])
        tg['tp_stop_pair'] = tg[['stop_id','next_stop']].apply(tuple, axis=1)

        g['tp_stop_pair'] = tg['tp_stop_pair']
        g['tp_stop_pair_filled'] = g.groupby('trip_id')['tp_stop_pair']\
                                            .fillna(method='ffill')

        # get a dataframe of two columns: stop_id and shared routes (set of all routes that use the corresponding stop)
        l=pd.DataFrame(g.groupby('stop_id')['route_id'].agg(set)).rename(columns={'route_id': 'routes'})
        g = g.merge(l, left_on='stop_id', right_index=True, how='left')

        # within a trip_id group, check if the set of shared routes changes from previous and next stop
        g['routes_diff_next'] = g.groupby('trip_id')['routes'].shift(0) - g.groupby('trip_id')['routes'].shift(-1)
        g['routes_diff_prev'] = g.groupby('trip_id')['routes'].shift(0) - g.groupby('trip_id')['routes'].shift()
        g['routes_diff_next_len'] = g['routes_diff_next'].apply(lambda x: len(x) if isinstance(x, set) else 0)
        g['routes_diff_prev_len'] = g['routes_diff_prev'].apply(lambda x: len(x) if isinstance(x, set) else 0)

        # stops that have a different set of routes from adjacent stops and where shared routes appear more than once
        # in adjacent stops (i.e. shared routes not only share this stop, but also the previous or next stop) are labeled as branchpoint
        g['branchpoint'] = (((g['routes_diff_next_len'] + g['routes_diff_prev_len'])>0)\
                             & ~((g['routes_diff_prev']==g['routes_diff_next']) & (g['routes_diff_prev_len']!=0))).astype(int)
        records['branchpoint'] = g['branchpoint']

        # mark both timepoint and branchpoint as tp_bp 
        records['tp_bp'] = ((records['timepoint']==1) | (records['branchpoint']==1)).astype(int)
        # mark the first stop of each trip as tp_bp
        records.loc[records.groupby('trip_id')['tp_bp'].head(1).index, 'tp_bp'] = 1

    def generate_patterns(self) -> Dict[str, Dict]:
        """Generate a dict of patterns from validated GTFS data. Add a "pattern" column to the trips table.

        :raises ValueError: number of unique trip hashes does not match with number of unique sequence of stops
        :return: pattern dict - key: pattern (route_id - direction_id - hash count); value: Segment dict (a segment is a section of road between two transit stops). 
            (Segment dict - key: tuple of first and last stops of the segment; value: list of coordinates defining the segment.)
        :rtype: Dict[str, Dict]
        """

        logger.info(f'generating patterns from GTFS stop coordinates')

        gtfs:Dict = self.validated_data
        records = self.records

        stops:pd.DataFrame = gtfs['stops']
        
        # Generate a dataframe of trip_id (unique), list of stop_ids, and hash of the stop_ids list
        trip_stops = records.groupby('trip_id')['stop_id'].apply(list).reset_index(name='stop_ids')
        trip_stops['hash'] = trip_stops['stop_ids'].apply(lambda x: get_hash_of_stop_list(x))

        # Verify that number of unique hashes indeed match with number of unique sequence of stops
        if trip_stops.astype(str).nunique()['hash'] != trip_stops.astype(str).nunique()['stop_ids']:
            raise ValueError(f'Number of unique hashes should match number of unique lists of ordered stops. '+\
                                'Use debug mode to find out why there is a mismatch. You may need to change the hashing method.')
        else:
            logger.debug(f'Summary of unique trip hashes: \n{trip_stops.astype(str).nunique()}')


        # Add pattern column to the trips table
        trip_hash_lookup = trip_stops.set_index('trip_id')['hash'].to_dict()
        records['hash'] = records['trip_id'].map(trip_hash_lookup)
        records['hash_count'] = (records.drop_duplicates(['route_id', 'direction_id', 'hash'])\
                                    .groupby(['route_id','direction_id']).cumcount()+1).reindex(records.index).ffill()
        records['hash_count'] = records['hash_count'].round().astype(int)
        records['pattern'] = records['route_id'].astype(str) + '-' + records['direction_id'].astype(str) + '-'  + records['hash_count'].astype(str)

        # Generate dict of patterns. 
        # Get a dict of <hash: list of stop ids>
        hash_stops_lookup = trip_stops.set_index('hash')['stop_ids'].to_dict()
        
        records['stop_ids'] = records['hash'].map(hash_stops_lookup)
        # Get a dict of <pattern: list of stop ids>
        pattern_stops_lookup = records.set_index('pattern')['stop_ids'].to_dict()

        # Get a dict of <stop id: tuple of stop coordinates (lat, lon)>
        stops = stops[['stop_id','stop_lat','stop_lon']].drop_duplicates()
        stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
        stop_coords_lookup = stops.set_index('stop_id')['coords'].to_dict()

        # Get a dict of <pattern: segments>
        # e.g. {2188571819865: {('64', '1'):[(lat64, lon64), (lat1, lon1)], ('1', '2'): [(lat1, lon1), (lat2, lon2)]...}...}
        patterns = {pattern: {(stop_ids[i], stop_ids[i+1]): [stop_coords_lookup[stop_ids[i]], stop_coords_lookup[stop_ids[i+1]]] \
                                    for i in range(len(stop_ids)-1)} for pattern, stop_ids in pattern_stops_lookup.items()}

        
        return patterns

    def improve_pattern_with_shapes(self, patterns:Dict, records:pd.DataFrame, gtfs:Dict) -> Dict[str, Dict]:
        """Improve the coordinates of each segment in each pattern by replacing the stop coordinates 
        with coordinates found in the GTFS shapes table, i.e. in addition to the two stop coordinates 
        at both ends of the segment, also add additional intermediate coordinates given by GTFS shapes 
        to enrich the segment profile.

        :param patterns: dict of patterns
        :type patterns: Dict
        :param records: table of validated GTFS stop_times records
        :type records: pd.DataFrame
        :param gtfs: dict of validated GTFS tables
        :type gtfs: Dict
        :return: dict of patterns, where the list of coordinates of each segment is supplemented by the GTFS shapes table
        :rtype: Dict[str, Dict]
        """
        
        logger.info(f'improving patterns with GTFS shapes')

        trips = gtfs['trips']
        shapes = gtfs['shapes'].sort_values(by=['shape_id', 'shape_pt_sequence'])

        # Get dict of <shape_id: list of coordinates>
        shapes['coords'] = list(zip(shapes.shape_pt_lat, shapes.shape_pt_lon))
        shape_coords_lookup = shapes.groupby('shape_id')['coords'].agg(list).to_dict()

        # Get dict of <trip_id: shape_id>
        trip_shape_lookup = trips[['trip_id', 'shape_id']].set_index('trip_id')['shape_id'].to_dict()

        # Get dict of <pattern: list of trip_ids>
        hash_trips_lookup = records[['pattern', 'trip_id']].drop_duplicates()\
                                .groupby('pattern')['trip_id'].agg(list).to_dict()

        for pattern, segments in patterns.items():

            # Find a representative shape ID and the corresponding list of shape coordinates
            trips = hash_trips_lookup[pattern]
            try:
                example_shape = next(trip_shape_lookup[t] for t in trips if t in trip_shape_lookup)
            except StopIteration as err:
                logger.debug(f'{err}: no example shape can be found for pattern: {pattern}.')
                example_shape = -1
                continue
            
            shape_coords = shape_coords_lookup[example_shape]
            last_stop_match_index = 0
            # For each segment, find the closest match of start and end stops in the list of shape coordinates.
            #   If more than two matched coordinates in GTFS shapes can be found, then use GTFS shapes.
            #   Otherwise, keep using the stop coordinates from GTFS stops.
            for id_pair, coord_list in segments.items():
                first_stop_match_index = self.__find_nearest_point(shape_coords, coord_list[0])
                last_stop_match_index = self.__find_nearest_point(shape_coords, coord_list[-1])
                
                intermediate_shape_coords = \
                            shape_coords[first_stop_match_index : last_stop_match_index+1]

                shape_coords = shape_coords[last_stop_match_index:]
                if len(intermediate_shape_coords)>2:
                    segments[id_pair] = intermediate_shape_coords

        return patterns

    def __find_nearest_point(self, coord_list, coord):
        dist = distance.cdist(np.array(coord_list), np.array([coord]), 'euclidean')
        return dist.argmin()

    