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
FT_PER_MIN_TO_MPH = 0.0113636
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
        # self.scheduled_headway('mean')
        self.scheduled_running_time()
        # self.revenue_hour()
        self.scheduled_speed()

    def prepare_gtfs_records(self, records:pd.DataFrame):

        records = deepcopy(records).reset_index()
        records.loc[:, 'next_stop'] = records.groupby(by='trip_id')['stop_id'].shift(-1)
        records.loc[:, 'next_stop_arrival_time'] = records.groupby(by='trip_id')['arrival_time'].shift(-1)
        records.loc[:, 'stop_pair'] = pd.Series(list(zip(records.stop_id, records.next_stop)))
        records = records.set_index('index')
        records = records.dropna(subset=['next_stop'])

        return records

    def generate_segments(self, records:pd.DataFrame):
        
        logger.info(f'generating segments...')

        # Get data structure for segments. Multiindex: route_id, stop_pair, hour       
        segments = records.groupby(self.SEGMENT_MULTIINDEX)['trip_id'].size().to_frame(name = 'scheduled_frequency')

        return segments
    
    def generate_corridors(self, records:pd.DataFrame):

        logger.info(f'generating corridors...')
        corridors = records.groupby(self.CORRIDOR_MULTIINDEX)['trip_id'].size().to_frame(name = 'scheduled_frequency')

        return corridors

    def generate_routes(self, records:pd.DataFrame):
        
        logger.info(f'generating routes...')
        routes = records.groupby(self.ROUTE_MULTIINDEX)['trip_id'].size().to_frame(name = 'scheduled_frequency')
        return routes

    def stop_spacing(self):
        """Stop spacing in ft. Distance is returned from Valhalla trace route requests in unit of kilometers.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'calculating stop spacing...')
        # segments
        ## note that merge action can change dataframe index if expanded. So reset then set index to keep the original index.
        records = self.gtfs_records.reset_index()\
                    .merge(self.shapes[['pattern_id', 'stop_pair', 'distance']], on=['pattern_id', 'stop_pair'], how='left')\
                    .set_index('index')
        self.segments['stop_spacing'] = (records.groupby(self.SEGMENT_MULTIINDEX)['distance'].mean() * KILOMETER_TO_FT).round(2)

        # corridors
        self.corridors['stop_spacing'] = (records.groupby(self.CORRIDOR_MULTIINDEX)['distance'].mean() * KILOMETER_TO_FT).round(2)

        # routes
        routes_data = records[self.ROUTE_MULTIINDEX + ['stop_pair', 'trip_id', 'distance']].drop_duplicates()\
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

        self.tpbp_segments['stop_spacing'] = (self.tpbp_gtfs_records.groupby(self.SEGMENT_MULTIINDEX)['tpbp_distance'].mean() * KILOMETER_TO_FT).round(2)
        self.tpbp_corridors['stop_spacing'] = (self.tpbp_gtfs_records.groupby(self.CORRIDOR_MULTIINDEX)['tpbp_distance'].mean() * KILOMETER_TO_FT).round(2)

    def scheduled_headway(self, method:str='mean'):
        """Scheduled headway in minutes. Defined as the difference between two consecutive arrival times at the first stop of a stop pair.
            Levels: segments, corridors
        Args:
            method (str, optional): mean - average headway; mode - the first mode value of headways. Defaults to 'mean'.

        Raises:
            ValueError: the provided method is not one of: 'mean', 'mode'.
        """
        logger.info(f'calculating scheduled headway...')
        records = deepcopy(self.gtfs_records)

        records['headway'] = records.sort_values(['route_id', 'pattern_id', 'stop_pair', 'arrival_time'])\
                                .groupby(['route_id', 'pattern_id', 'stop_pair'])['arrival_time'].diff()

        if method == 'mean':
            func = pd.Series.mean
        elif method == 'mode': # find the first mode if there are multiple
            func = lambda x: scipy.stats.mode(x)[0]
        else:
            raise ValueError(f'Invalid method: {method}.')

        # segments
        self.segments['scheduled_headway'] = records.groupby(self.SEGMENT_MULTIINDEX)['headway']\
                                                .agg(func) // 60
        # corridors
        self.corridors['scheduled_headway'] = records.groupby(self.CORRIDOR_MULTIINDEX)['headway']\
                                                .agg(func) // 60

    def scheduled_running_time(self):
        """Running time in minutes. Defined as the difference between departure time at a stop and arrival time at the next stop.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'calculating scheduled running time...')

        records = deepcopy(self.gtfs_records)
        records['running_time'] = ((records['next_stop_arrival_time'] - records['departure_time']) / 60).round(2)

        # segments
        self.segments['scheduled_running_time'] = records.groupby(self.SEGMENT_MULTIINDEX)['running_time'].mean().round(1)
        # corridors
        self.corridors['scheduled_running_time'] = records.groupby(self.CORRIDOR_MULTIINDEX)['running_time'].mean().round(1)
        # routes
        routes_data = records[self.ROUTE_MULTIINDEX + ['stop_pair', 'trip_id', 'running_time']].drop_duplicates()\
                        .groupby(self.ROUTE_MULTIINDEX + ['trip_id'])['running_time'].sum().reset_index()
        self.routes['scheduled_running_time'] = routes_data.groupby(self.ROUTE_MULTIINDEX)['running_time'].mean().round(1)
        

        tpbp_records = deepcopy(self.tpbp_gtfs_records)
        tpbp_records['running_time'] = ((tpbp_records['next_stop_arrival_time'] - tpbp_records['departure_time']) / 60).round(2)

        # tpbp_segments
        self.tpbp_segments['scheduled_running_time'] = tpbp_records.groupby(self.SEGMENT_MULTIINDEX)['running_time'].mean().round(1)
        # tpbp_corridors
        self.tpbp_corridors['scheduled_running_time'] = tpbp_records.groupby(self.CORRIDOR_MULTIINDEX)['running_time'].mean().round(1)

    def revenue_hour(self):
        """Revenue hours in hr. Defined as the difference between first arrival at first stop and last arrival at last stop.
                Since data is stratified in hours, revenue_hour for each hour is capped at 1 (even though the actual value can be above 1,
                because 'hour' is defined as the starting hour of the trip, and a trip can end in a different hour).
            Levels: routes
        """
        logger.info(f'calculating revenue hour...')

        records = deepcopy(self.gtfs_records)

        records['first_stop_arrival'] = records.groupby('trip_id')['arrival_time'].transform('min')
        records['last_stop_arrival'] = records.groupby('trip_id')['arrival_time'].transform('max')

        # routes
        routes_data = records[self.ROUTE_MULTIINDEX + ['trip_id', 'first_stop_arrival', 'last_stop_arrival']].drop_duplicates()
        routes_data['first_trip_first_stop_arrival'] = routes_data.groupby(self.ROUTE_MULTIINDEX)['first_stop_arrival'].transform('min')
        routes_data['last_trip_last_stop_arrival'] = routes_data.groupby(self.ROUTE_MULTIINDEX)['last_stop_arrival'].transform('max')
        routes_data['revenue_hour'] = ((routes_data['last_trip_last_stop_arrival'] - routes_data['first_trip_first_stop_arrival']) / 3660).round(2)

        self.routes['revenue_hour'] = routes_data[self.ROUTE_MULTIINDEX + ['revenue_hour']].drop_duplicates()\
                                        .groupby(self.ROUTE_MULTIINDEX)['revenue_hour'].sum().round(1).clip(upper=1)
        
    def scheduled_speed(self):
        """Scheduled running speed in mph. Defined as stop spacing divided by running time.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """

        logger.info(f'calculating scheduled speed...')

        self.segments['scheduled_speed'] = ((self.segments['stop_spacing'] / self.segments['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(0)
        self.corridors['scheduled_speed'] = ((self.corridors['stop_spacing'] / self.corridors['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(0)
        self.routes['scheduled_speed'] = ((self.routes['stop_spacing'] / self.routes['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(0)
        self.tpbp_segments['scheduled_speed'] = ((self.tpbp_segments['stop_spacing'] / self.tpbp_segments['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(0)
        self.tpbp_corridors['scheduled_speed'] = ((self.tpbp_corridors['stop_spacing'] / self.tpbp_corridors['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(0)