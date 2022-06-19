from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from parameters.rove_parameters import ROVE_params
import scipy.stats

logger = logging.getLogger("backendLogger")

FT_PER_MIN_TO_MPH = 0.0113636

class MetricAggregation():
    
    def __init__(self, stop_metrics:pd.DataFrame, tpbp_metrics:pd.DataFrame, route_metrics:pd.DataFrame, \
                    start_time:List, end_time:List):

        logger.info(f'Aggregating metrics for time: {start_time} - {end_time}...')
        if not isinstance(start_time, List) or not isinstance(end_time, List) or \
            len(start_time) != 2 or len(end_time) != 2:
            raise TypeError(f'start_time and end_time must both be lists of length 2: [hour, minute].')

        self.start_time = start_time[0]*3600 + start_time[1]*60
        self.end_time = end_time[0]*3600 + end_time[1]*60

        if end_time < start_time:
            raise ValueError(f'Start time must be smaller than end time.')

        self.stop_metrics, self.stop_metrics_time_filtered = self.prepare_metrics(stop_metrics, self.start_time, self.end_time, 'stop')
        self.route_metrics, self.route_metrics_time_filtered = self.prepare_metrics(route_metrics, self.start_time, self.end_time, 'route')
        self.tpbp_metrics, self.tpbp_metrics_time_filtered = self.prepare_metrics(tpbp_metrics, self.start_time, self.end_time, 'tpbp')

        self.SEGMENT_MULTIINDEX = ['route_id', 'pattern_id', 'stop_pair']
        self.CORRIDOR_MULTIINDEX = ['pattern_id', 'stop_pair']
        self.ROUTE_MULTIINDEX = ['pattern_id', 'route_id', 'direction_id']

        self.segments:pd.DataFrame = self.generate_segments(self.stop_metrics)
        self.corridors:pd.DataFrame = self.generate_corridors(self.stop_metrics)
        self.routes:pd.DataFrame = self.generate_routes(self.route_metrics)
        self.tpbp_segments:pd.DataFrame = self.generate_segments(self.tpbp_metrics)
        self.tpbp_corridors:pd.DataFrame = self.generate_corridors(self.tpbp_metrics)

        # not time-dependent
        # self.stop_spacing()

        # time-dependent
        # self.span_of_service()
        # self.revenue_hour()
        # self.scheduled_frequency()
        # self.scheduled_headway('mean')
        # self.scheduled_running_time()
        # self.scheduled_running_speed()
        self.scheduled_poisson_wait_time()

        logger.info(f'Metrics aggregation for time period {start_time} - {end_time} completed.')

    def prepare_metrics(self, metrics:pd.DataFrame, start_time:int, end_time:int, type:str):

        if type != 'route':
            logger.debug(f'{type} metrics size before dropping last stop of trips: {metrics.shape}')
            metrics = metrics.dropna(subset=['next_stop'])

        logger.debug(f'{type} metrics size before time filtering: {metrics.shape}')
        if type == 'route':
            new_metrics = deepcopy(metrics.loc[(metrics['trip_start_time'] >= start_time) & (metrics['trip_start_time'] < end_time), :])
        else:
            new_metrics = deepcopy(metrics.loc[(metrics['arrival_time'] >= start_time) & (metrics['arrival_time'] < end_time), :])
        logger.debug(f'{type} metrics size after time filtering: {new_metrics.shape}')

        return metrics, new_metrics

    def generate_segments(self, records:pd.DataFrame):
        
        logger.info(f'generating segments')

        # Get data structure for segments. Multiindex: route_id, stop_pair, hour       
        segments = records.groupby(self.SEGMENT_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')

        return segments
    
    def generate_corridors(self, records:pd.DataFrame):

        logger.info(f'generating corridors')
        corridors = records.groupby(self.CORRIDOR_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')

        return corridors

    def generate_routes(self, records:pd.DataFrame):
        
        logger.info(f'generating routes')
        routes = records.groupby(self.ROUTE_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')
        return routes

    def stop_spacing(self):
        """Aggregated stop spacing in ft. Defined as the average stop_spacing of all trips on each segments/corridors level, 
                and average of sum of stop_spacing of all stops along a route on the routes level.
                This metric is not time-dependent, so use non-time-filtered metrics for calculations.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'aggregating stop spacing')

        sig_fig = 0

        self.segments['stop_spacing'] = self.stop_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.corridors['stop_spacing'] = self.stop_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.routes['stop_spacing'] = self.route_metrics.groupby(self.ROUTE_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_segments['stop_spacing'] = self.tpbp_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_corridors['stop_spacing'] = self.tpbp_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)

    def span_of_service(self):
        """Aggregated service start/end in sec since epoch. Defined as the first arrival at first stop (service start) and the last arrival 
                at last stop (service end) of all trips on each aggregation level. This metric is not time-dependent, so use non-time-filtered 
                metrics for calculations.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'aggregating span of service')

        self.segments['service_start'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('min')
        self.segments['service_end'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('max')

        self.corridors['service_start'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('min')
        self.corridors['service_end'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('max')

        self.routes['service_start'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['trip_start_time'].agg('min')
        self.routes['service_end'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['trip_end_time'].agg('max')

        self.tpbp_segments['service_start'] = self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('min')
        self.tpbp_segments['service_end'] = self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('max')

        self.tpbp_corridors['service_start'] = self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('min')
        self.tpbp_corridors['service_end'] = self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('max')

    def revenue_hour(self):
        """Aggregated revenue hours in hr. Defined as the difference between service_end and service_start. This metric is not 
                time-dependent, so use non-time-filtered metrics for calculations.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'aggregating revenue hour')

        sig_fig = 1

        self.segments['revenue_hour'] = ((self.segments['service_end'] - self.segments['service_start']) / 3660).round(sig_fig)
        self.corridors['revenue_hour'] = ((self.corridors['service_end'] - self.corridors['service_start']) / 3660).round(sig_fig)
        self.routes['revenue_hour'] = ((self.routes['service_end'] - self.routes['service_start']) / 3660).round(sig_fig)
        self.tpbp_segments['revenue_hour'] = ((self.tpbp_segments['service_end'] - self.tpbp_segments['service_start']) / 3660).round(sig_fig)
        self.tpbp_corridors['revenue_hour'] = ((self.tpbp_corridors['service_end'] - self.tpbp_corridors['service_start']) / 3660).round(sig_fig)

    def scheduled_frequency(self):
        """Aggregated scheduled frequency in trips/hr. Defined as the number of trips divided by service span (revenue hour).
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'aggregating scheduled frequency')

        sig_fig = 1

        self.segments['scheduled_frequency'] = (self.segments['trip_counts'] / self.segments['revenue_hour']).round(sig_fig)
        self.corridors['scheduled_frequency'] = (self.corridors['trip_counts'] / self.corridors['revenue_hour']).round(sig_fig)
        self.routes['scheduled_frequency'] = (self.routes['trip_counts'] / self.routes['revenue_hour']).round(sig_fig)
        self.tpbp_segments['scheduled_frequency'] = (self.tpbp_segments['trip_counts'] / self.tpbp_segments['revenue_hour']).round(sig_fig)
        self.tpbp_corridors['scheduled_frequency'] = (self.tpbp_corridors['trip_counts'] / self.tpbp_corridors['revenue_hour']).round(sig_fig)


    def scheduled_headway(self, method:str='mean'):
        """Aggregated scheduled headway in minutes. Defined as the average or mode of scheduled headway of all trips on each aggregation level.
            Levels: segments, tpbp_segments
        Args:
            method (str, optional): mean - average headway; mode - the first mode value of headways. Defaults to 'mean'.

        Raises:
            ValueError: the provided method is not one of: 'mean', 'mode'.
        """
        logger.info(f'aggregating scheduled headway')

        if method == 'mean':
            func = pd.Series.mean
        elif method == 'mode': # find the first mode if there are multiple
            func = lambda x: scipy.stats.mode(x)[0]
        else:
            raise ValueError(f'Invalid method: {method}.')

        sig_fig = 0

        self.segments['scheduled_headway'] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['scheduled_headway']\
                                                .agg(func) // 60).round(sig_fig)
        self.tpbp_segments['scheduled_headway'] = (self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['scheduled_headway']\
                                                .agg(func) // 60).round(sig_fig)

    def scheduled_running_time(self):
        """Aggregated running time in minutes. Defined as the average scheduled running time of all trips on each segments/corridors level, 
                and average of sum of running time of all stops along a route on the routes level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """
        logger.info(f'aggregating scheduled running time')

        sig_fig = 1

        self.segments['scheduled_running_time'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['scheduled_running_time'].mean().round(sig_fig)
        self.corridors['scheduled_running_time'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)['scheduled_running_time'].mean().round(sig_fig)
        self.routes['scheduled_running_time'] = self.route_metrics_time_filtered\
                                                    .groupby(self.ROUTE_MULTIINDEX)['scheduled_running_time'].mean().round(sig_fig)
        self.tpbp_segments['scheduled_running_time'] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['scheduled_running_time'].mean().round(sig_fig)
        self.tpbp_corridors['scheduled_running_time'] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)['scheduled_running_time'].mean().round(sig_fig)


    def scheduled_running_speed(self):
        """Aggregated scheduled running speed in mph. Defined as the average scheduled running speed of all trips on each segments/corridors level, 
                and average of (sum of stop spacing of all stops) / (sum of running time of all stops) along a route on the routes level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """

        logger.info(f'aggregating scheduled speed')

        sig_fig = 0

        self.segments['scheduled_running_speed'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['scheduled_running_speed'].mean().round(sig_fig)
        self.corridors['scheduled_running_speed'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)['scheduled_running_speed'].mean().round(sig_fig)
        self.routes['scheduled_running_speed'] = self.route_metrics_time_filtered\
                                                    .groupby(self.ROUTE_MULTIINDEX)['scheduled_running_speed'].mean().round(sig_fig)
        self.tpbp_segments['scheduled_running_speed'] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['scheduled_running_speed'].mean().round(sig_fig)
        self.tpbp_corridors['scheduled_running_speed'] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)['scheduled_running_speed'].mean().round(sig_fig)

    def scheduled_poisson_wait_time(self):
        """Aggregated wait time in minuntes. Defined as headway mean / 2 + variance / (2 * mean), assuming passenger arrival follows a Poisson process.
                See Equation 2.66 in Larson, R. C. & Odoni, A. R. (1981) Urban operations research. Englewood Cliffs, N.J: Prentice-Hall.
            Levels: segments
        """
        logger.info(f'aggregating scheduled poisson wait time')

        segments_data = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['scheduled_headway']\
                                                    .agg('mean').to_frame('scheduled_headway_mean')
        segments_data['scheduled_headway_variance'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['scheduled_headway']\
                                                        .agg('var')
        self.segments['scheduled_poisson_wait_time'] = (((segments_data['scheduled_headway_mean'] / 2) \
                                                        + (segments_data['scheduled_headway_variance'] / (2 * segments_data['scheduled_headway_mean']))) / 60).round(1)

    