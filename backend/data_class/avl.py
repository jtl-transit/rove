"""Data class for AVL data.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set, Tuple
from xmlrpc.client import Boolean
import pandas as pd
import numpy as np
import logging
import time
from tqdm import tqdm
from .base_data_class import BaseData
from copy import deepcopy
import json
from backend.helper_functions import load_csv_to_dataframe


logger = logging.getLogger("backendLogger")

REQUIRED_COL_SPEC = {
    'route':'string',
    'stop_id':'string',
    'stop_time':'int64',
    'stop_sequence': 'int64',
    'dwell_time': 'float64',
    'passenger_load': 'int64',
    'passenger_on': 'int64',
    'passenger_off': 'int64',
    'seat_capacity': 'int64',
    'trip_id':'string'
}
OPTIONAL_COL_SPEC = {

}

class AVL(BaseData):
    """AVL data class. Stores a validated AVL data records table with passenger on, off and load values corrected.

    :param rove_params: a rove_params object that stores information needed throughout the backend
    :type rove_params: ROVE_params
    """

    def __init__(self, rove_params):
        """Instantiate an AVL data class.
        """

        super().__init__('avl', rove_params)

        #: dataframe of AVL records
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

        raw_avl = load_csv_to_dataframe(path)

        if raw_avl.empty:
            raise ValueError(f'AVL data is empty.')

        if not set(REQUIRED_COL_SPEC.keys()).issubset(raw_avl.columns):
            # not all required columns are found in raw table
            missing_columns = set(REQUIRED_COL_SPEC.keys()) - set(raw_avl.columns)
            logger.fatal(f'AVL data is missing required columns: {missing_columns}.', exc_info=True)
            quit()
        
        if not set(OPTIONAL_COL_SPEC.keys()).issubset(raw_avl.columns):
            # not all optional columns are found in raw table
            missing_columns = set(OPTIONAL_COL_SPEC.keys()) - set(raw_avl.columns)
            logger.warning(f'AVL data is missing optional columns: {missing_columns}.')

        return raw_avl

 
    def validate_data(self) -> pd.DataFrame:
        """Clean up raw data by converting column types to those listed in the spec. Convert dwell_time and stop_time columns 
        to integer seconds if necessary.

        :return: a dataframe of validated AVL data
        :rtype: pd.DataFrame
        """

        data:pd.DataFrame = deepcopy(self.raw_data)
        
        data['dwell_time'] = self.convert_dwell_time(data['dwell_time'])
        
        data['stop_time'], data['svc_date'] = self.convert_stop_time(data['stop_time'])

        logger.info(f"AVL service date range: {data['svc_date'].min()} to {data['svc_date'].max()}")

        data_specs = {**REQUIRED_COL_SPEC, **OPTIONAL_COL_SPEC}
        cols = list(data_specs.keys())
        data[cols] = data[cols].astype(dtype=data_specs)
               
        return data
    
    def convert_dwell_time(self, data:pd.Series) -> pd.Series:
        """Convert dwell times to integer seconds.

        :param data: the column of dwell_time data
        :type data: pd.Series
        :return: column of dwell times in integer seconds
        :rtype: pd.Series
        """

        pass

    def convert_stop_time(self, data:pd.Series) -> Tuple[pd.Series, pd.Series]:
        """Convert stop times to integer seconds since the beginning of service (defined in config). Also return a
        column of operation date (e.g. 01:30 am on March 4 may correspond to the operation date of March 3 if service
        span is from 5 am to 3 am the next day).

        :param data: the column of stop_time (time of arrival at a stop) data
        :type data: pd.Series
        :return: column of stop times in integer seconds, and column of operation dates
        :rtype: Tuple[pd.Series, pd.Seires]
        """
        
        stop_time_dt = pd.to_datetime(data)
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
        """Return a dataframe that is the validated AVL table. Values are sorted by ['svc_date', 'route', 'trip_id', 'stop_sequence'], 
        and only unique rows of each combination of ['svc_date', 'route', 'trip_id', 'stop_sequence'] columns are kept.

        :return: dataframe containing validated and sorted AVL data
        :rtype: pd.DataFrame
        """

        avl_df:pd.DataFrame = deepcopy(self.validated_data)

        avl_df = avl_df.sort_values(['svc_date', 'route', 'trip_id', 'stop_sequence'])\
                        .drop_duplicates(['svc_date', 'route', 'trip_id', 'stop_sequence'])\
                        .reset_index(drop=True)

        return avl_df
    
    def correct_passenger_load(self):
        """Enforce that no one alights at the first stop or boards at the last stop, and make sure the passenger_on, passenger_off and
        passenger_load values of each trip add up.
        """

        records = self.records

        p = deepcopy(records)


        start_time = time.time()
        
        # enforce that no one alights at the first stop or boards at the last stop
        head_indices = p.groupby(['svc_date', 'route', 'trip_id']).head(1).index
        tail_indices = p.groupby(['svc_date', 'route', 'trip_id']).tail(1).index

        p.loc[head_indices, 'passenger_off'] = 0
        p.loc[tail_indices, 'passenger_on'] = 0
        p['passenger_delta'] = p['passenger_on'] - p['passenger_off']

        p['passenger_load'] = p.groupby(['svc_date', 'route', 'trip_id'])['passenger_delta'].cumsum()
        p.loc[tail_indices, 'passenger_off'] = p.loc[tail_indices, 'passenger_load']
        p['passenger_delta'] = p['passenger_on'] - p['passenger_off']

        p['reset'] = (p['passenger_load']<0).astype(int)
        
        logger.info(f'correcting passenger load')
        while 1 in p['reset'].unique():
            
            reset_row_index = p[p['reset']==1].first_valid_index()
            if reset_row_index in tail_indices:
                p.loc[reset_row_index, 'passenger_off'] = p.loc[reset_row_index, 'passenger_load']
                p.loc[reset_row_index, 'passenger_delta'] = -p.loc[reset_row_index, 'passenger_off']
            else:
                p.loc[reset_row_index, 'passenger_off'] = p.loc[reset_row_index-1, 'passenger_load'] \
                                                            + p.loc[reset_row_index, 'passenger_on']
                p.loc[reset_row_index, 'passenger_delta'] = -p.loc[reset_row_index-1, 'passenger_load']
            p['passenger_load'] = p.groupby(['svc_date', 'route', 'trip_id'])['passenger_delta'].cumsum()

            p['reset'] = (p['passenger_load']<0).astype(int)

        p.loc[tail_indices, 'passenger_off'] = p.loc[tail_indices, 'passenger_load']
        p.loc[tail_indices, 'passenger_delta'] = -p.loc[tail_indices, 'passenger_off']

        logger.info(f'finished correcting passenger load in {round((time.time() - start_time), 2)} seconds')
        records[['passenger_off', 'passenger_load']] = p[['passenger_off', 'passenger_load']]
