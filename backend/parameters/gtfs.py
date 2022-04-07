"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback
from .base_data_class import BaseData
import tqdm

logger = logging.getLogger("backendLogger")

REQUIRED_DATA_DICT = {'agency':{

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
OPTIONAL_DATA_DICT = {'shapes':{}}

class GTFS(BaseData):

    def __init__(self, alias, path, rove_params=None):
        super().__init__(alias, path, rove_params)

    def load_data(self, path:str)->Dict[str, DataFrame]:
        """Load in GTFS data from zip file, and retrieve data of the sample date (as stored in rove_params) and 
        route_type (as stored in config). Enforce that required tables are present and not empty, and log (w/o enforcing)
        if optional tables are not present or empty. Do not enforce data type. Store the retrieved tables in a dict.

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

        # Store all required data in a dict
        required_data = self.get_non_empty_gtfs_table(feed, REQUIRED_DATA_DICT, required=1)

        # Add all optional data if the file exists and is not empty
        optional_data = self.get_non_empty_gtfs_table(feed, OPTIONAL_DATA_DICT)

        return {**required_data, **optional_data}

    def get_non_empty_gtfs_table(self, feed:ptg.readers.Feed, table_name_dict:Dict[str,Dict[str,str]], required=0)->Dict[str, DataFrame]:
        """Get dict of non-empty GTFS tables from the given feed. If the data_set is required, then each table
        must exist in the feed and must not be empty, otherwise the program will be halted. If the data_set is 
        optional, then errors are logged but the program continues to run.

        Args:
            feed (ptg.readers.Feed): GTFS feed
            table_name_dict (Dict[str,Dict[str,str]]): key: GTFS table name; value: dict of <column name: column dtype>
            requied (int, optional): whether the table_name_dict is required. Defaults to 0.

        Raises:
            ValueError: table is found in the feed, but is empty.
            AttributeError: a table name specified in table_name_dict is not found in GTFS feed.

        Returns:
            Dict[str, DataFrame]: key: name of GTFS table; value: GTFS table stored as DataFrame.
        """
        data = {}
        for table_name, columns in table_name_dict.items():
            try:
                feed_data = getattr(feed, table_name)
                if feed_data.empty:
                    raise ValueError(f'{table_name} data is empty.')
                else:
                    if set(columns.keys()).issubset(feed_data.columns):
                        data[table_name] = feed_data
                    else:
                        missing_columns = set(columns.keys()) - set(feed_data.columns)
                        raise KeyError(f'Table "{table_name}" is missing required columns: {missing_columns}.')
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
        """Clean up raw data and make sure that it conforms with the standard format defined in the documentation

        Raises:
            ValueError: if any one type of the required raw data is empty

        Returns:
            dict <str, DataFrame>: validated and cleaned-up data
        """
        # avoid changing the raw data object
        data = self.raw_data.copy()
        data_dict = {**REQUIRED_DATA_DICT, **OPTIONAL_DATA_DICT}

        # convert column types
        for table_name, df in data.items():
            columns_dtype_dict = data_dict[table_name]
            cols = list(columns_dtype_dict.keys())
            if not columns_dtype_dict:
                continue
            else:
                df[cols] = df[cols].astype(dtype=columns_dtype_dict)
        
        # filter based on stop_times
        stop_times = self.filter_table_a_on_unique_b_key(data, 'stop_times', 'trips', ['trip_id'])
        trips = self.filter_table_a_on_unique_b_key(data, 'trips', 'stop_times', ['trip_id'])
        stops = self.filter_table_a_on_unique_b_key(data, 'stops', 'stop_times', ['stop_id'])

        # organize data
        trip_stop_times = pd.merge(trips, stop_times, on='trip_id', how='inner')
        stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
        # Clean up the data
        # clean_up()

        return data

    def filter_table_a_on_unique_b_key(self, data:Dict[str, DataFrame], table_a_name:str, 
                                        table_b_name:str, key:List[str]):
        """Filter table_a leaving records whose key is found in unique values of table_b key.

        Args:
            data (Dict[str, DataFrame]): key: gtfs table name; value: dataframe of gtfs table
            table_a_name (str): name of table to be filtered
            table_b_name (str): name of table that supplies the base for lookup
            key (List[str]): list of key column names that should exist in both table_a and table_b

        Returns:
            DataFrame: filtered dataframe
        """
        data_a = data[table_a_name]
        data_b_key_col = data[table_b_name][key].drop_duplicates()
        
        data_a_filtered = pd.merge(data_a, data_b_key_col, on=key, how='inner')
        if (data_a_filtered.shape[0] > data_a.shape[0]):
            raise ValueError(f'Filtered table "{table_a_name}" should not have more rows than before.')
        logger.debug(f'shape of table "{table_a_name}"'+\
                    f'\n\tbefore filtering:\t{data_a.shape}'+\
                    f'\n\tafter filtering:\t{data_a_filtered.shape}')

        return data_a_filtered

    # TODO: make it more generic... convert route_id to list of route_short_name for any given table
    # Function to convert route_id from GTFS into route_short_name from GTFS, which is useful in some applications
    def convert_route_ids(df, feed):
        feed_routes = feed.routes
        route_dict = dict(zip(feed_routes['route_id'], feed_routes['route_short_name']))
        new_ids = []
        for i in df['route_id'].values.tolist():
            new_ids.append(route_dict[i])
            
        df['route_id'] = new_ids
        
        return df
    
    # Function to convert stop_id from GTFS into stop_code from GTFS, which is needed for WMATA
    def convert_stop_ids(df, feed):
        
        feed_stops = feed.stops
        stops_dict = dict(zip(feed_stops['stop_id'], feed_stops['stop_code']))
        new_ids = []
        for i in df['stop_id'].values.tolist():
            new_ids.append(stops_dict[i])
            
        df['stop_id'] = new_ids
        df = df[~df['stop_id'].isnull()] # Remove any rows with missing values
        
        return df