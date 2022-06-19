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
    
    def __init__(self, shapes:pd.DataFrame, gtfs_records:pd.DataFrame):
        
        logger.info(f'Calculating metrics...')

        self.stop_metrics = self.prepare_gtfs_records(gtfs_records)

        self.tpbp_metrics = self.prepare_gtfs_records(gtfs_records.loc[gtfs_records['tp_bp']==1, :])

        self.ROUTE_METRICS_KEY_COLUMNS = ['pattern_id', 'route_id', 'direction_id', 'trip_id']
        self.route_metrics = self.prepare_route_metrics(gtfs_records)

        self.stop_spacing(shapes)
        self.scheduled_headway()
        self.scheduled_running_time()
        self.scheduled_running_speed()

        logger.info(f'Metrics calculation completed.')

    def prepare_gtfs_records(self, records:pd.DataFrame):
        """Add three columns to gtfs records: next_stop, next_stop_arrival_time, stop_pair while keeping original index.

        Args:
            records (pd.DataFrame): GTFS records

        Returns:
            records: GTFS records with new columns
        """

        records = deepcopy(records).reset_index()
        records.loc[:, 'next_stop'] = records.groupby(by=['trip_id'])['stop_id'].shift(-1)
        records.loc[:, 'next_stop_arrival_time'] = records.groupby(by='trip_id')['arrival_time'].shift(-1)
        records.loc[:, 'stop_pair'] = pd.Series(list(zip(records.stop_id, records.next_stop)))
        records = records.set_index('index')

        return records

    def prepare_route_metrics(self, records:pd.DataFrame):

        route_metrics = records[self.ROUTE_METRICS_KEY_COLUMNS + ['trip_start_time', 'trip_end_time']].drop_duplicates()

        return route_metrics

    def stop_spacing(self, shapes):
        """Stop spacing in ft. Distance is returned from Valhalla trace route requests in unit of kilometers.
        """
        logger.info(f'calculating stop spacing')

        records = self.stop_metrics.reset_index()\
                    .merge(shapes[['pattern_id', 'stop_pair', 'distance']], on=['pattern_id', 'stop_pair'], how='left')\
                    .set_index('index')

        self.stop_metrics['stop_spacing'] = (records['distance'] * KILOMETER_TO_FT).round(2)

        routes_data = self.stop_metrics.groupby(self.ROUTE_METRICS_KEY_COLUMNS)['stop_spacing'].sum().reset_index()
        self.route_metrics = self.route_metrics.merge(routes_data, on=self.ROUTE_METRICS_KEY_COLUMNS, how='left')

        records = deepcopy(self.stop_metrics)
        ## calculate the distance between timepoints using the above records dataframe
        ## step 1: label timepoint pairs as tpbp_stop_pair, each tpbp_stop_pair could correspond to multiple consecutive stop_pairs
        ##           stops that don't belong to a tpbp_stop_pair but is a tp_bp (e.g. the last tp_bp of a route) are labeled -1
        records['tpbp_stop_pair'] = self.tpbp_metrics['stop_pair']
        records.loc[(records['tp_bp']==1) & (records['tpbp_stop_pair'].isnull()), 'tpbp_stop_pair'] = -1
        records['tpbp_stop_pair'] = records.groupby('trip_id')['tpbp_stop_pair'].fillna(method='ffill')
        ## step 2: calculate the culmulative distance of each stop along the trip, and keep only the last record of each tpbp_stop_pair
        ##           since the last culmulative distance encompasses the distances of all stop_pairs within the same tpbp_stop_pair
        records['distance_cumsum'] = records.groupby('trip_id')['stop_spacing'].cumsum()
        records = records[~records.duplicated(subset=['trip_id', 'tpbp_stop_pair'], keep='last')]
        ## step 3: distance between timepoints = difference between kept tpbp_stop_pair culmulative distances
        records['tpbp_distance'] = records.groupby('trip_id')['distance_cumsum'].diff().fillna(records['distance_cumsum'])

        self.tpbp_metrics = self.tpbp_metrics.reset_index()\
                                .merge(records[['pattern_id', 'tpbp_stop_pair', 'tpbp_distance']].drop_duplicates(), \
                                    left_on=['pattern_id', 'stop_pair'], right_on=['pattern_id', 'tpbp_stop_pair'], how='left')\
                                .set_index('index').rename(columns={'tpbp_distance': 'stop_spacing'})

    def scheduled_headway(self):
        """Scheduled headway in minutes. Defined as the difference between two consecutive arrival times at the first stop of a stop pair.
        """
        logger.info(f'calculating scheduled headway')
        
        self.stop_metrics['scheduled_headway'] = self.stop_metrics.groupby(['route_id', 'pattern_id', 'stop_pair'])['arrival_time'].diff()
        self.tpbp_metrics['scheduled_headway'] = self.tpbp_metrics.groupby(['route_id', 'pattern_id', 'stop_pair'])['arrival_time'].diff()


    def scheduled_running_time(self):
        """Running time in minutes. Defined as the difference between departure time at a stop and arrival time at the next stop.
        """
        logger.info(f'calculating scheduled running time')

        self.stop_metrics['scheduled_running_time'] = ((self.stop_metrics['next_stop_arrival_time'] - self.stop_metrics['arrival_time']) // 60).round(2)
        
        routes_data = self.stop_metrics.groupby(self.ROUTE_METRICS_KEY_COLUMNS)['scheduled_running_time'].sum().reset_index()
        self.route_metrics = self.route_metrics.merge(routes_data, on=self.ROUTE_METRICS_KEY_COLUMNS, how='left')

        self.tpbp_metrics['scheduled_running_time'] = ((self.tpbp_metrics['next_stop_arrival_time'] - self.tpbp_metrics['arrival_time']) // 60).round(2)


    def scheduled_running_speed(self):
        """Scheduled running speed in mph. Defined as stop spacing divided by running time.
        """
        logger.info(f'calculating scheduled speed')

        self.stop_metrics['scheduled_running_speed'] = ((self.stop_metrics['stop_spacing'] / self.stop_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.route_metrics['scheduled_running_speed'] = ((self.route_metrics['stop_spacing'] / self.route_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.tpbp_metrics['scheduled_running_speed'] = ((self.tpbp_metrics['stop_spacing'] / self.tpbp_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)
