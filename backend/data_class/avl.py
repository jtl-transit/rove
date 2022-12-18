from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set, Tuple
from xmlrpc.client import Boolean
import pandas as pd
import numpy as np
import logging
import time
from tqdm import tqdm

from backend.data_class.gtfs import GTFS
from backend.data_class.rove_parameters import ROVE_params
from copy import deepcopy
import json
from backend.helper_functions import load_csv_to_dataframe, series_to_datetime, check_is_file, convert_stop_ids


logger = logging.getLogger("backendLogger")


class AVL():
    """Stores a validated AVL data records table with passenger on, off and load values corrected.

    :param rove_params: a rove_params object that stores information needed throughout the backend
    :type rove_params: ROVE_params
    """
    
    #: Required columns and the data types that each column will be converted to in AVL data
    REQUIRED_COL_SPEC = {
        'route':'string',
        'stop_id':'string',
        'stop_time':'int',
        'stop_sequence': 'int64',
        'dwell_time': 'float64',
        'passenger_load': 'int64',
        'passenger_on': 'int64',
        'passenger_off': 'int64',
        'seat_capacity': 'int64',
        'trip_id':'string'
    }

    #: Optional columns in AVL data
    OPTIONAL_COL_SPEC = {

    }

    def __init__(self, rove_params:ROVE_params, bus_gtfs:GTFS):
        """Instantiate an AVL data class.
        """

        #: Alias of the data class, defined as 'avl'.
        alias:str = 'avl'

        #: ROVE_params for the backend, see parameter definition.
        self.rove_params:ROVE_params = rove_params

        #: GTFS records table
        self.gtfs:GTFS = bus_gtfs
        
        logger.info(f'loading {alias} data')
        path = check_is_file(rove_params.input_paths[alias])
        # Raw data read from the given path, see :py:meth:`.AVL.load_data` for details.
        self.raw_data:pd.DataFrame = self.load_data(path)
        
        logger.info(f'validating {alias} data')
        #: Validated data, see :py:meth:`.AVL.validate_data` for details.
        self.validated_data:pd.DataFrame = self.validate_data(bus_gtfs)

        #: AVL records table, see :py:meth:`.AVL.get_avl_records` for details.
        self.records:pd.DataFrame = self.get_avl_records()

        self.correct_passenger_load()

    def load_data(self, path: str) -> pd.DataFrame:
        """Load in AVL data from the given path.

        :param path: file path to raw AVL data
        :type path: str
        :raises ValueError: raw AVL data file is empty
        :return: dataframe of AVL data with all required columns
        :rtype: pd.DataFrame
        """

        id_cols = [col for col, dtype in self.REQUIRED_COL_SPEC.items() if dtype == 'string']
        raw_avl = load_csv_to_dataframe(path, id_cols=id_cols)

        if raw_avl.empty:
            raise ValueError(f'AVL data is empty.')

        if not set(self.REQUIRED_COL_SPEC.keys()).issubset(raw_avl.columns):
            # not all required columns are found in raw table
            missing_columns = set(self.REQUIRED_COL_SPEC.keys()) - set(raw_avl.columns)
            logger.fatal(f'AVL data is missing required columns: {missing_columns}.', exc_info=True)
            quit()
        
        if not set(self.OPTIONAL_COL_SPEC.keys()).issubset(raw_avl.columns):
            # not all optional columns are found in raw table
            missing_columns = set(self.OPTIONAL_COL_SPEC.keys()) - set(raw_avl.columns)
            logger.warning(f'AVL data is missing optional columns: {missing_columns}.')

        return raw_avl

 
    def validate_data(self, gtfs:GTFS) -> pd.DataFrame:
        """Clean up raw data by converting column types to those listed in the spec. Convert dwell_time and stop_time columns 
        to integer seconds if necessary. Filter to keep only AVL records of dates in the date_list in ROVE_params.

        :return: a dataframe of validated AVL data
        :rtype: pd.DataFrame
        """

        data:pd.DataFrame = deepcopy(self.raw_data)

        data['dwell_time'] = self.convert_dwell_time(data['dwell_time'])
        
        data['stop_time'], data['svc_date'] = self.convert_stop_time(data['stop_time'])

        data = deepcopy(data[data['svc_date'].isin(self.rove_params.date_list)])
        if data.empty:
            raise ValueError(f'AVL table is empty after filtering for dates in the date_list.')

        data_specs = {**self.REQUIRED_COL_SPEC, **self.OPTIONAL_COL_SPEC}
        cols = list(data_specs.keys())
        data[cols] = data[cols].astype(dtype=data_specs)
        data = data.rename(columns={'route': 'route_id'})

        gtfs_stop_ids_set = set(gtfs.validated_data['stops']['stop_id'])
        gtfs_trip_ids_set = set(gtfs.validated_data['trips']['trip_id'])

        avl_stop_ids_set = set(data['stop_id'])
        avl_trip_ids_set = set(data['trip_id'])

        matching_stop_ids = gtfs_stop_ids_set & avl_stop_ids_set
        matching_trip_ids = gtfs_trip_ids_set & avl_trip_ids_set
        logger.debug(f'count of AVL stop IDs: {len(avl_stop_ids_set)}, trip IDs: {len(avl_trip_ids_set)}.')
        logger.debug(f'count of matching stop IDs: {len(matching_stop_ids)}, matching trip IDs: {len(matching_trip_ids)}.')

        data = convert_stop_ids('avl', data, 'stop_id', self.gtfs.validated_data['stops'])

        logger.info(f"AVL service date range: {data['svc_date'].min()} to {data['svc_date'].max()}, {data['svc_date'].nunique()} days in total")
               
        return data
    
    def convert_dwell_time(self, data:pd.Series) -> pd.Series:
        """Convert dwell times to integer seconds.

        :param data: the column of dwell_time data
        :type data: pd.Series
        :return: column of dwell times in integer seconds
        :rtype: pd.Series
        """

        return data

    def convert_stop_time(self, data:pd.Series) -> Tuple[pd.Series, pd.Series]:
        """Convert stop times to integer seconds since the beginning of service (defined in config). Also return a
        column of service date (e.g. 01:30 am on March 4 may correspond to the service date of March 3 if service
        span is from 5 am to 3 am the next day).

        :param data: the column of stop_time (time of arrival at a stop) data
        :type data: pd.Series
        :return: column of stop times in integer seconds, and column of service dates
        :rtype: Tuple[pd.Series, pd.Seires]
        """
        
        # stop_time_dt = series_to_datetime(data)
        stop_time_dt = pd.to_datetime(data, infer_datetime_format=True, cache=True)
        stop_time_hour = stop_time_dt.dt.hour
        stop_time_min = stop_time_dt.dt.minute
        stop_time_sec = stop_time_dt.dt.second

        interval_to_second = lambda x: x[0] * 3600 + x[1] * 60

        stop_time_total_seconds = stop_time_hour * 3600 + stop_time_min * 60 + stop_time_sec

        day_start, _ = self.rove_params.config['time_periods']['full']
        day_start_total_seconds = interval_to_second(day_start)
        midnight_total_seconds = interval_to_second([24, 0])

        stop_time_total_seconds_converted = stop_time_total_seconds.where(stop_time_total_seconds > day_start_total_seconds, \
                                            stop_time_total_seconds + midnight_total_seconds)
        
        stop_time_date_converted = (stop_time_dt.dt.date).where(stop_time_total_seconds > day_start_total_seconds, \
                                            (stop_time_dt - pd.DateOffset(1)).dt.date)

        return stop_time_total_seconds_converted, stop_time_date_converted


    def get_avl_records(self) -> pd.DataFrame:
        """Return a dataframe that is the validated AVL table. Values are sorted by ['svc_date', 'route_id', 'trip_id', 'stop_sequence'], 
        and only unique rows of each combination of ['svc_date', 'route_id', 'trip_id', 'stop_sequence'] columns are kept.

        :return: dataframe containing validated and sorted AVL data
        :rtype: pd.DataFrame
        """

        avl_df:pd.DataFrame = deepcopy(self.validated_data)

        avl_df = avl_df.sort_values(['svc_date', 'route_id', 'trip_id', 'stop_sequence'])\
                        .drop_duplicates(['svc_date', 'route_id', 'trip_id', 'stop_sequence'])\
                        .reset_index(drop=True)

        avl_df['trip_start_time'] = avl_df.groupby(by=['svc_date', 'trip_id'])['stop_time'].transform('min')
        avl_df['trip_end_time'] = avl_df.groupby(by=['svc_date', 'trip_id'])['stop_time'].transform('max')

        return avl_df
    
    def correct_passenger_load(self):
        """Enforce that no one alights at the first stop or boards at the last stop, and make sure the passenger_on, passenger_off and
        passenger_load values of each trip add up.
        """

        records = self.records

        p = deepcopy(records)


        start_time = time.time()
        
        # enforce that no one alights at the first stop or boards at the last stop
        head_indices = p.groupby(['svc_date', 'route_id', 'trip_id']).head(1).index
        tail_indices = p.groupby(['svc_date', 'route_id', 'trip_id']).tail(1).index

        p.loc[head_indices, 'passenger_off'] = 0
        p.loc[tail_indices, 'passenger_on'] = 0
        # p['passenger_delta'] = p['passenger_on'] - p['passenger_off']

        # p['passenger_load'] = p.groupby(['svc_date', 'route_id', 'trip_id'])['passenger_delta'].cumsum()
        # p.loc[tail_indices, 'passenger_off'] = p.loc[tail_indices, 'passenger_load']
        # p['passenger_delta'] = p['passenger_on'] - p['passenger_off']

        # p['reset'] = (p['passenger_load']<0).astype(int)
        
        # logger.info(f'correcting passenger load')
        # while 1 in p['reset'].unique():
            
        #     reset_row_index = p[p['reset']==1].first_valid_index()
        #     if reset_row_index in tail_indices:
        #         p.loc[reset_row_index, 'passenger_off'] = p.loc[reset_row_index, 'passenger_load']
        #         p.loc[reset_row_index, 'passenger_delta'] = -p.loc[reset_row_index, 'passenger_off']
        #     else:
        #         p.loc[reset_row_index, 'passenger_off'] = p.loc[reset_row_index-1, 'passenger_load'] \
        #                                                     + p.loc[reset_row_index, 'passenger_on']
        #         p.loc[reset_row_index, 'passenger_delta'] = -p.loc[reset_row_index-1, 'passenger_load']
        #     p['passenger_load'] = p.groupby(['svc_date', 'route_id', 'trip_id'])['passenger_delta'].cumsum()

        #     p['reset'] = (p['passenger_load']<0).astype(int)

        # p.loc[tail_indices, 'passenger_off'] = p.loc[tail_indices, 'passenger_load']
        # p.loc[tail_indices, 'passenger_delta'] = -p.loc[tail_indices, 'passenger_off']

        logger.info(f'finished correcting passenger load in {round((time.time() - start_time), 2)} seconds')
        records[['passenger_off', 'passenger_load']] = p[['passenger_off', 'passenger_load']]
