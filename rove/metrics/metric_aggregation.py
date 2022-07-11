from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List, Callable
import scipy.stats
import inspect

logger = logging.getLogger("backendLogger")

FT_PER_MIN_TO_MPH = 0.0113636

class MetricAggregation():
    
    def __init__(self, stop_metrics:pd.DataFrame, tpbp_metrics:pd.DataFrame, route_metrics:pd.DataFrame, \
                    start_time:List, end_time:List, percentile:int, redValues:dict):

        logger.info(f'aggregating metrics for period: {start_time} - {end_time}, percentile: {percentile}')

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

        self.SEGMENT_MULTIINDEX = ['route_id', 'stop_pair']
        self.CORRIDOR_MULTIINDEX = ['stop_pair']
        self.ROUTE_MULTIINDEX = ['route_id', 'direction_id']

        self.segments:pd.DataFrame = self.generate_segments(self.stop_metrics)
        self.corridors:pd.DataFrame = self.generate_corridors(self.stop_metrics)
        self.routes:pd.DataFrame = self.generate_routes(self.route_metrics)
        self.tpbp_segments:pd.DataFrame = self.generate_segments(self.tpbp_metrics)
        self.tpbp_corridors:pd.DataFrame = self.generate_corridors(self.tpbp_metrics)

        self.redValues = redValues

        # not time-dependent (use non time-filtered data)
        self.stop_spacing()

        # ---- GTFS metrics ----
        # time-dependent (use time-filtered data)
        ## metrics that can't be aggregated by percentile
        self.span_of_service()
        self.revenue_hour()
        self.scheduled_frequency()
        self.headway('percentile', percentile, 'scheduled')
        self.wait_time('scheduled')
        self.running_time(percentile, 'scheduled')
        self.speed(percentile, 'scheduled')

        # ---- AVL metrics ----
        self.headway('percentile', percentile, 'observed')
        self.running_time(percentile, 'observed')
        self.speed(percentile, 'observed', 'without_dwell')
        self.speed(percentile, 'observed', 'with_dwell')
        self.boardings(percentile)
        self.on_time_performance()
        self.crowding()
        self.passenger_load(percentile)
        self.wait_time('observed')
        self.excess_wait_time()
        self.congestion_delay()
        self.productivity()

        self.segments_agg_metrics = self.get_agg_metrics(self.segments.reset_index(), 'segments')
        self.corridors_agg_metrics = self.get_agg_metrics(self.corridors.reset_index(), 'corridors')
        self.routes_agg_metrics = self.get_agg_metrics(self.routes.reset_index(), 'routes')
        self.tpbp_segments_agg_metrics = self.get_agg_metrics(self.tpbp_segments.reset_index(), 'segments')
        self.tpbp_corridors_agg_metrics = self.get_agg_metrics(self.tpbp_corridors.reset_index(), 'corridors')

    def get_agg_metrics(self, metrics_df:pd.DataFrame, data_type:str):

        if 'stop_pair' in metrics_df.columns:
            metrics_df[['first_stop', 'second_stop']] = pd.DataFrame(metrics_df['stop_pair'].tolist(), index=metrics_df.index)
        
        if data_type == 'segments':
            table_rename = {
                'route_id': 'route',
                'stop_pair': 'segment'
            }
            index_cols = ['route_id', 'first_stop', 'second_stop']
        elif data_type == 'corridors':
            table_rename = {
                'stop_pair': 'corridor'
            }
            index_cols = ['first_stop', 'second_stop']
        elif data_type == 'routes':
            table_rename = {
                'route_id': 'route',
                'direction_id': 'direction'
            }
            index_cols = ['route_id', 'direction_id']
        else:
            raise ValueError(f'Invalid metric data_type {data_type}. Must be one of segments, corridors, routes.')
        
        metrics_df['index'] = metrics_df[index_cols].astype(str).apply('-'.join, axis=1)
        if metrics_df.columns.isin(['first_stop', 'second_stop']).any():
            metrics_df = metrics_df.drop(columns=['first_stop', 'second_stop'])
        df = metrics_df.rename(columns=table_rename)
        return df


    def get_percentile(self, p:int, metric_name:str):

        if isinstance(p, int) and p >= 0 and p <= 100:
            if metric_name not in self.redValues:
                raise ValueError(f'{metric_name} is not found in redValues.')
            is_red_value =  self.redValues[metric_name]=='Low'
            p_to_use = (100 - p) if is_red_value else p
            return p_to_use / 100
        else:
            raise ValueError(f'Invalid percentile p={p}. Percentile must be a positive integer in [0, 100].')
        

    def prepare_metrics(self, metrics:pd.DataFrame, start_time:int, end_time:int, data_type:str):

        if data_type != 'route':
            metrics = metrics.dropna(subset=['next_stop'])

        if data_type == 'route':
            new_metrics = deepcopy(metrics.loc[(metrics['trip_start_time'] >= start_time) & (metrics['trip_start_time'] < end_time), :])
        else:
            new_metrics = deepcopy(metrics.loc[(metrics['arrival_time'] >= start_time) & (metrics['arrival_time'] < end_time), :])

        return metrics, new_metrics

    def generate_segments(self, records:pd.DataFrame):
        
        # Get data structure for segments. Multiindex: route_id, stop_pair, hour       
        segments = records.groupby(self.SEGMENT_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')

        return segments
    
    def generate_corridors(self, records:pd.DataFrame):

        corridors = records.groupby(self.CORRIDOR_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')

        return corridors

    def generate_routes(self, records:pd.DataFrame):
        
        routes = records.groupby(self.ROUTE_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'trip_counts')
        return routes

    def stop_spacing(self):
        """Aggregated stop spacing in ft. Defined as the average stop_spacing of all trips on each segments/corridors level, 
                and average of sum of stop_spacing of all stops along a route on the routes level.
                This metric is not time-dependent, so use non-time-filtered metrics for calculations.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """

        sig_fig = 0

        self.segments['stop_spacing'] = self.stop_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.corridors['stop_spacing'] = self.stop_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.routes['stop_spacing'] = self.route_metrics.groupby(self.ROUTE_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_segments['stop_spacing'] = self.tpbp_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_corridors['stop_spacing'] = self.tpbp_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)

    def span_of_service(self):
        """Aggregated service start/end in sec since epoch. Defined as the first arrival at first stop (service start) and the last arrival 
                at last stop (service end) of all trips on each aggregation level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """

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
        """Aggregated revenue hours in hr. Defined as the difference between service_end and service_start.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        """

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

        sig_fig = 1

        self.segments['scheduled_frequency'] = (self.segments['trip_counts'] / self.segments['revenue_hour']).round(sig_fig)
        self.corridors['scheduled_frequency'] = (self.corridors['trip_counts'] / self.corridors['revenue_hour']).round(sig_fig)
        self.routes['scheduled_frequency'] = (self.routes['trip_counts'] / self.routes['revenue_hour']).round(sig_fig)
        self.tpbp_segments['scheduled_frequency'] = (self.tpbp_segments['trip_counts'] / self.tpbp_segments['revenue_hour']).round(sig_fig)
        self.tpbp_corridors['scheduled_frequency'] = (self.tpbp_corridors['trip_counts'] / self.tpbp_corridors['revenue_hour']).round(sig_fig)


    def headway(self, method:str, percentile:int, data_type:str):
        """Aggregated scheduled headway in minutes. Defined as the average or mode of scheduled headway of all trips on each aggregation level.
            Levels: segments, corridors
        Args:
            method (str): mean - average headway; mode - the first mode value of headways; percentile - choose value based on percentile.
            percentile (int): the percentile that metrics are aggregated at.
            data_type (str): e.g. 'scheduled' or 'observed'
        Raises:
            ValueError: the provided method is not one of: 'mean', 'mode'.
        """

        sig_fig = 0

        metric_name = f'{data_type}_headway'

        if method == 'percentile':
            percentile = self.get_percentile(percentile, metric_name)

            self.segments[metric_name] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[metric_name]\
                                                .quantile(percentile)).round(sig_fig)
            self.corridors[metric_name] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)[metric_name]\
                                                    .quantile(percentile)).round(sig_fig)
        else:
            if method == 'mean':
                func = pd.Series.mean
            elif method == 'mode': # find the first mode if there are multiple
                func = lambda x: scipy.stats.mode(x)[0]
            else:
                raise ValueError(f'Invalid method: {method}. Must be one of: percentile, mean, mode.')

            self.segments[metric_name] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[metric_name]\
                                                    .agg(func)).round(sig_fig)
            self.corridors[metric_name] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)[metric_name]\
                                                    .agg(func)).round(sig_fig)


    def running_time(self, percentile:int, data_type:str):
        """Aggregated running time in minutes. Defined as the average scheduled running time of all trips on each segments/corridors level, 
                and average of sum of running time of all stops along a route on the routes level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        Args:
            percentile (int): the percentile that metrics are aggregated at.
            data_type (str): e.g. 'scheduled' or 'observed'
        """
        sig_fig = 1
        metric_name = f'{data_type}_running_time'

        percentile = self.get_percentile(percentile, metric_name)

        self.segments[metric_name] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.corridors[metric_name] = self.stop_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.routes[metric_name] = self.route_metrics_time_filtered\
                                                    .groupby(self.ROUTE_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.tpbp_segments[metric_name] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.tpbp_corridors[metric_name] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)


    def speed(self, percentile:int, data_type:str, dwell:str=''):
        """Aggregated scheduled running speed in mph. Defined as the average scheduled running speed of all trips on each segments/corridors level, 
                and average of (sum of stop spacing of all stops) / (sum of running time of all stops) along a route on the routes level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        Args:
            percentile (int): the percentile that metrics are aggregated at.
            data_type (str): e.g. 'scheduled' or 'observed'
            dwell (str): e.g. 'with_dwell' or 'without_dwell'
        """
       
        sig_fig = 0
        if dwell == '':
            metric_name = f'{data_type}_speed'
        else:
            metric_name = f'{data_type}_speed_{dwell}'

        percentile = self.get_percentile(percentile, metric_name)


        self.segments[metric_name] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.corridors[metric_name] = self.stop_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.routes[metric_name] = self.route_metrics_time_filtered\
                                                    .groupby(self.ROUTE_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.tpbp_segments[metric_name] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)
        self.tpbp_corridors[metric_name] = self.tpbp_metrics_time_filtered\
                                                    .groupby(self.CORRIDOR_MULTIINDEX)[metric_name].quantile(percentile).round(sig_fig)

    def wait_time(self, data_type:str):
        """Aggregated wait time in minuntes. Defined as headway mean / 2 + variance / (2 * mean), assuming passenger arrival follows a Poisson process.
                See Equation 2.66 in Larson, R. C. & Odoni, A. R. (1981) Urban operations research. Englewood Cliffs, N.J: Prentice-Hall.
            Capping wait time values at 300 min (5 hr).
            Levels: segments
        """

        sig_fig = 0
        metric_name = f'{data_type}_wait_time'
        segments_data = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[f'{data_type}_headway']\
                                                    .agg('mean').to_frame(f'{data_type}_headway_mean').fillna(0)
        segments_data[f'{data_type}_headway_variance'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[f'{data_type}_headway']\
                                                        .agg('var')
        self.segments[metric_name] = ((segments_data[f'{data_type}_headway_mean'] / 2) \
                                        + (segments_data[f'{data_type}_headway_variance'] / (2 * segments_data[f'{data_type}_headway_mean'])))\
                                            .clip(upper=300).round(sig_fig)

    def excess_wait_time(self):

        self.segments['excess_wait_time'] = (self.segments['observed_wait_time'] - self.segments['scheduled_wait_time']).clip(lower=0)


    def boardings(self, percentile:int):
        """Aggregated boardings in pax. Defined as the average boardings of all trips on each stop/stop aggregated level,
                and the sum of boardings at all stops averaged over all trips on the route level.
            Levels: segments, corridors, routes, tpbp_segments, tpbp_corridors
        Args:
            percentile (int): the percentile that metrics are aggregated at
        """
        sig_fig = 0

        percentile = self.get_percentile(percentile, 'boardings')

        self.segments['boardings'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.corridors['boardings'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.routes['boardings'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.tpbp_segments['boardings'] = self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.tpbp_corridors['boardings'] = self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)

    def on_time_performance(self):
        """Aggregated on-time performance in seconds or %. Defined as the average arrival delay of all trips on segment level,
                and the percent of late trips averaged over all trips on the route level.
            Levels: segments, routes
        """
        sig_fig = 0

        self.segments['on_time_performance'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)
        self.routes['on_time_performance'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)
    
    def crowding(self):
        """Aggregated crowding in %. Defined as the average crowding of all trips on each stop/stop aggregated level,
                and peak crowding averaged over all trips on the route level.
            Levels: segments, corridors, routes
        """
        sig_fig = 0

        self.segments['crowding'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['crowding'].mean().round(sig_fig)
        self.corridors['boardings'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].mean().round(sig_fig)
        self.routes['crowding'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['crowding'].mean().round(sig_fig)
    
    def passenger_load(self, percentile:int):
        """Aggregated passenger loading in pax. Defined as the average passenger load of all trips on each segments level.
            Levels: segments

        Args:
            percentile (int): the percentile that metrics are aggregated at
        """
        sig_fig = 0
        percentile = self.get_percentile(percentile, 'passenger_load')

        self.segments['passenger_load'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['passenger_load'].quantile(percentile).round(sig_fig)

    def passenger_flow(self):
        """Aggregated passenger flow in pax/hr. Defined as the sum of passenger load divided by the lenghth of time period.
            Levels: segments, corridors
        """
        sig_fig = 0

        period = self.end_time - self.start_time

        self.segments['passenger_flow'] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['passenger_load'].sum() / period).round(sig_fig)
        self.corridors['passenger_flow'] = (self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['passenger_load'].sum() / period).round(sig_fig)

    def congestion_delay(self):
        """Aggregated vehicle- and passenger-weighted congestion delay in min/mile or pax-min/mile. Defined as the sum of congestion
            delays over all trips.
        """
        sig_fig = 0

        self.segments['vehicle_congestion_delay'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['vehicle_congestion_delay'].sum().round(sig_fig)
        self.segments['passenger_congestion_delay'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['passenger_congestion_delay'].sum().round(sig_fig)

    def productivity(self):

        sig_fig = 0

        self.routes['productivity'] = (self.route_metrics.groupby(self.ROUTE_MULTIINDEX)['boardings'].sum() \
                                        / self.routes['revenue_hour']).round(sig_fig)