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
KILOMETER_TO_FT = 3280.84
SPEED_RANGE = [0, 65]
MS_TO_MPH = 3.6/1.6
MAX_HEADWAY = 90

class MetricCalculation():
    
    def __init__(self, rove_params:ROVE_params, shapes:pd.DataFrame, gtfs_records:pd.DataFrame):

        self.gtfs_records = self.prepare_gtfs_records(gtfs_records)
        self.shapes = shapes

        self.SEGMENT_MULTIINDEX = ['route_id', 'pattern_id', 'stop_pair', 'hour']
        self.segments:pd.DataFrame = self.generate_segments(self.gtfs_records)

        self.CORRIDOR_MULTIINDEX = ['pattern_id', 'stop_pair', 'hour']
        self.corridors:pd.DataFrame = self.generate_corridors(self.gtfs_records)

        self.ROUTE_MULTIINDEX = ['pattern_id', 'route_id', 'hour']
        self.routes:pd.DataFrame = self.generate_routes(self.gtfs_records)

        self.tpbp_gtfs_records = self.prepare_gtfs_records(gtfs_records.loc[gtfs_records['tp_bp']==1, :])
        self.tpbp_segments:pd.DataFrame = self.generate_segments(self.tpbp_gtfs_records)

        self.tpbp_corridors:pd.DataFrame = self.generate_corridors(self.tpbp_gtfs_records)

        self.stop_spacing()
        self.scheduled_headway('mean')

    def prepare_gtfs_records(self, records:pd.DataFrame):

        records = deepcopy(records)
        records.loc[:, 'next_stop'] = records.groupby(by='trip_id')['stop_id'].shift(-1)
        records = records.dropna(subset=['next_stop'])
        records.loc[:, 'stop_pair'] = pd.Series(list(zip(records.stop_id, records.next_stop)))

        return records

    def generate_segments(self, records:pd.DataFrame):
        
        logger.info(f'generating segments...')

        # Get data structure for segments. Multiindex: route_id, stop_pair, hour       
        segments = records.groupby(self.SEGMENT_MULTIINDEX)['trip_id'].size().to_frame(name = 'trip_count')

        return segments
    
    def generate_corridors(self, records:pd.DataFrame):

        logger.info(f'generating corridors...')
        corridors = records.groupby(self.CORRIDOR_MULTIINDEX)['trip_id'].size().to_frame(name = 'trip_count')

        return corridors

    def generate_routes(self, records:pd.DataFrame):
        
        logger.info(f'generating routes...')
        routes = records.groupby(self.ROUTE_MULTIINDEX)['trip_id'].size().to_frame(name = 'trip_count')
        return routes

    def stop_spacing(self):
        """Stop spacing in ft. Distance is returned from Valhalla trace route requests in unit of kilometers.
        """
        logger.info(f'calculating stop spacing...')
        # segments
        ## note that merge action can change dataframe index if expanded. So reset then set index to keep the original index.
        records = self.gtfs_records.reset_index()\
                    .merge(self.shapes[['pattern_id', 'stop_pair', 'distance']], on=['pattern_id', 'stop_pair'], how='left')\
                    .set_index('index')
        segments_data = records[self.SEGMENT_MULTIINDEX + ['trip_id', 'distance']].drop_duplicates()
        self.segments['stop_spacing'] = (segments_data.groupby(self.SEGMENT_MULTIINDEX)['distance'].mean() * KILOMETER_TO_FT).round(2)

        # routes
        routes_data = records[self.ROUTE_MULTIINDEX + ['trip_id', 'distance']].drop_duplicates()\
                        .groupby(self.ROUTE_MULTIINDEX + ['trip_id'])['distance'].sum().reset_index()
        self.routes['stop_spacing'] = (routes_data.groupby(self.ROUTE_MULTIINDEX)['distance'].mean() * KILOMETER_TO_FT).round(2)

        # tpbp_segments
        ## calculate the distance between timepoints using the above records dataframe
        ## step 1: label timepoint pairs as tpbp_stop_pair, each tpbp_stop_pair could correspond to multiple consecutive stop_pairs
        ##           stops that don't belong to a tpbp_stop_pair but is a tp_bp (e.g. the last tp_bp of a route) are labeled -1
        records['tpbp_stop_pair'] = self.tpbp_gtfs_records['stop_pair']
        records.loc[(records['tp_bp']==1) & (records['tpbp_stop_pair'].isnull()), 'tpbp_stop_pair'] = -1
        records['tpbp_stop_pair'] = records.groupby('trip_id')['tpbp_stop_pair'].fillna(method='ffill')
        ## step 2: calculate the culmulative distance of each stop along the trip, and keep only the last record of each tpbp_stop_pair
        ##           since the last culmulative distance encompasses the distances of all stop_pairs within the same tpbp_stop_pair
        records['distance_cumsum'] = records.groupby('trip_id')['distance'].cumsum()
        records = records[~records.duplicated(subset=['trip_id', 'tpbp_stop_pair'], keep='last')]
        ## step 3: distance between timepoints = difference between kept tpbp_stop_pair culmulative distances
        records['tpbp_distance'] = records.groupby('trip_id')['distance_cumsum'].diff().fillna(records['distance_cumsum'])

        self.tpbp_gtfs_records = self.tpbp_gtfs_records.reset_index()\
                                .merge(records[['pattern_id', 'tpbp_stop_pair', 'tpbp_distance']].drop_duplicates(), \
                                    left_on=['pattern_id', 'stop_pair'], right_on=['pattern_id', 'tpbp_stop_pair'], how='left')\
                                .set_index('index')

        tpbp_segments_data = self.tpbp_gtfs_records[self.SEGMENT_MULTIINDEX + ['trip_id', 'tpbp_distance']].drop_duplicates()
        self.tpbp_segments['stop_spacing'] = (tpbp_segments_data.groupby(self.SEGMENT_MULTIINDEX)['tpbp_distance'].mean() * KILOMETER_TO_FT).round(2)
        logger.info(f'finished calculating stop spacing')

    def scheduled_headway(self, method:str='mean'):
        """Scheduled headway in minutes. Headway: difference between two consecutive arrival times at the first stop of a stop pair.

        Args:
            method (str, optional): mean - average headway; mode - the first mode value of headways. Defaults to 'mean'.

        Raises:
            ValueError: the provided method is not one of: 'mean', 'mode'.
        """
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
        self.segments['scheduled_headway'] = records.groupby([self.SEGMENT_MULTIINDEX])['headway']\
                                                .agg(func) // 60
        # corridors
        self.corridors['scheduled_headway'] = records.groupby([self.CORRIDOR_MULTIINDEX])['headway']\
                                                .agg(func) // 60
        
    