"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set, Tuple
from xmlrpc.client import Boolean
from argon2 import Parameters
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
from .base_data_class import BaseData
from copy import deepcopy
import json
from .helper_functions import check_is_file


logger = logging.getLogger("backendLogger")

REQUIRED_DATA_SPEC = {
                    'stops':{
                        'stop_id':'str',
                        'stop_lat':'float64',
                        'stop_lon':'float64'
                        }, 
                    'routes':{
                        'route_id':'str',
                        'route_type': 'int64'
                        }, 
                    'trips':{
                        'route_id':'str',
                        'service_id':'str',
                        'trip_id':'str',
                        'direction_id':'int64',
                        }, 
                    'stop_times':{
                        'trip_id':'str',
                        'arrival_time':'int64',
                        'stop_id':'str',
                        'stop_sequence':'int64',
                        }
                    }
OPTIONAL_DATA_SPEC = {
                    'shapes':{
                        'shape_id':'str',
                        'shape_pt_lat':'float64',
                        'shape_pt_lon':'float64',
                        'shape_pt_sequence':'int64'
                        }
                    }

class GTFS(BaseData):

    def __init__(self, alias, path, rove_params=None):
        super().__init__(alias, path, rove_params)
        self.df = self.get_gtfs_df()
        self.timepoints = self.check_timepoints(self.df)

    def read_shapes(self, path:str):
        
        try:
            in_path = check_is_file(path)
            with open(in_path) as shapes_file:
                shapes_json = json.load(shapes_file)
                shapes = pd.json_normalize(shapes_json)
                return shapes
        except FileNotFoundError:
            logger.exception(f'No shapes file found.')
            return None

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
            logger.fatal(f'{err}: Services for sample date {rove_params.sample_date} cannot be found in GTFS.', exc_info=True)
            quit()

        # Load GTFS feed
        view = {'routes.txt': {'route_type': rove_params.config['route_type']}, 'trips.txt': {'service_id': service_id_list}}
        feed = ptg.load_feed(path, view)

        # Store all required raw tables in a dict, enforce that every table listed in the spec exists and is not empty
        required_data = self.__get_non_empty_gtfs_table(feed, REQUIRED_DATA_SPEC, required=True)

        # Add whichever optional table listed in the spec exists and is not empty
        optional_data = self.__get_non_empty_gtfs_table(feed, OPTIONAL_DATA_SPEC)

        return {**required_data, **optional_data}

    def __get_non_empty_gtfs_table(self, feed:ptg.readers.Feed, table_col_spec:Dict[str,Dict[str,str]], required:Boolean=False)\
                                    ->Dict[str, DataFrame]:
        """Store in a dict all non-empty GTFS tables from the feed that are listed in the spec. 
        For required tables, each table must exist in the feed and must not be empty, otherwise the program will be halted.
        For optional tables, any table in the spec not in the feed or empty table in the feed is skipped and not stored.
        For tables in any spec, all spec columns must exist if the spec table is not empty.

        Args:
            feed (ptg.readers.Feed): GTFS feed
            table_col_spec (Dict[str,Dict[str,str]]): key: GTFS table name; value: dict of <column name: column dtype>
            requied (bool, optional): whether the table_col_spec is required. Defaults to False.

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

        Raises:
            ValueError: if any one type of the required raw data is empty

        Returns:
            dict <str, DataFrame>: validated and cleaned-up data
        """
        # avoid changing the raw data object
        data = deepcopy(self.raw_data)
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

    def get_gtfs_df(self) -> pd.DataFrame:

        trips:pd.DataFrame = self.validated_data['trips']
        stop_times:pd.DataFrame = self.validated_data['stop_times']

        gtfs_df = stop_times.merge(trips, on='trip_id', how='left')\
                        .sort_values(by=['route_id', 'trip_id', 'stop_sequence'])
        return gtfs_df

    def check_timepoints(self, gtfs_df:pd.DataFrame):
        """Check that 'timepoint' column exists, or add the column if it doesn't.

        Args:
            gtfs_df (pd.DataFrame): _description_
            timepoint_column_name (str, optional): _description_. Defaults to 'timepoints'.

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        self.add_timepoints(gtfs_df)
        # key: route_id, value: list of stop_ids of timepoints
        if 'timepoint' not in gtfs_df.columns:
            raise ValueError(f'Cannot find timepoints: timepoint column not in stop_times.')
        
        gtfs_df['timepoint'] = gtfs_df['timepoint'].fillna(0).astype(int)

        if gtfs_df.loc[~gtfs_df['timepoint'].isin([0,1]),'timepoint'].shape[0]>0:
            raise ValueError(f'The timepoint column cannot contain any value other than 0 or 1.')
        num_timepoints = gtfs_df.loc[gtfs_df['timepoint'] == 1,'timepoint'].shape[0]
        logger.debug(f'number of timepoints: {num_timepoints}')
        return gtfs_df
    
    def add_timepoints(self, gtfs_df:pd.DataFrame):
        gtfs_df['timepoint'] = ~gtfs_df['checkpoint_id'].isnull()