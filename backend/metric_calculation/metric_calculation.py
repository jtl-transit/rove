from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from parameters.rove_parameters import ROVE_params
import scipy.stats

logger = logging.getLogger("backendLogger")

FEET_TO_METERS = 0.3048
SPEED_RANGE = [0, 65]
MS_TO_MPH = 3.6/1.6
MAX_HEADWAY = 90

class MetricCalculation():
    
    def __init__(self, rove_params:ROVE_params, shapes:pd.DataFrame, gtfs_records:pd.DataFrame):

        self.gtfs_records = gtfs_records
        self.segments:pd.DataFrame = self.generate_segments(gtfs_records, shapes)
        self.corridors:pd.DataFrame = self.generate_corridors(self.segments)

        self.scheduled_headway(shapes)

    def generate_segments(self, shapes:pd.DataFrame):


        records = deepcopy(self.gtfs_records)

        records['next_stop'] = records.groupby(by='trip_id')['stop_id'].shift(-1)
        records = records.dropna(subset=['next_stop'])
        records['stop_pair'] = list(zip(records.stop_id, records.next_stop))

        # Add shape distance
        records = records.merge(shapes[['pattern_id', 'stop_pair', 'distance']], on=['pattern_id', 'stop_pair'], how='left')

        # Get data structure for segments. Multiindex: route_id, stop_pair, hour
        segments = records[['route_id', 'stop_pair', 'hour', 'distance']].drop_duplicates()\
                            .set_index(['route_id', 'stop_pair', 'hour'])

        return segments
    
    def generate_corridors(self):

        corridors = self.segments.reset_index().set_index(['stop_pair', 'hour'])

        return corridors

    def scheduled_headway(self, method:str='mean'):

        records = deepcopy(self.gtfs_records)

        records['headway'] = records.sort_values(['route_id', 'stop_pair', 'arrival_time'])\
                                .groupby(['route_id', 'stop_pair'])['arrival_time'].diff()

        if method == 'mean':
            func = pd.Series.mean
        elif method == 'mode': # find the first mode if there are multiple
            func = lambda x: scipy.stats.mode(x)[0]
        else:
            raise ValueError(f'Invalid method: {method}.')

        # segments
        self.segments['scheduled_headway'] = records.groupby(['route_id', 'stop_pair', 'hour'])['headway']\
                                                .agg(func) // 60
        # corridors
        self.corridors['scheduled_headway'] = records.groupby(['stop_pair', 'hour'])['headway']\
                                                .agg(func) // 60
        