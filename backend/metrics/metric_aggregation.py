from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List, Callable
import scipy.stats
import pickle
from backend.data_class.rove_parameters import ROVE_params
from backend.metrics.metric_calculation import Metric_Calculation
from backend.helper_functions import check_parent_dir
from tqdm.auto import tqdm

logger = logging.getLogger("backendLogger")

FT_PER_MIN_TO_MPH = 0.0113636
SECONDS_IN_MINUTE = 60
SECONDS_IN_HOUR = 3600
SECONDS_IN_TEN_MINUTES = SECONDS_IN_MINUTE * 10

class Metric_Aggregation():
    
    def __init__(self, metrics:Metric_Calculation, params:ROVE_params):
        logger.info(f'Aggregating metrics...')
        self.gtfs_stop_metrics = deepcopy(metrics.gtfs_stop_metrics)
        self.gtfs_route_metrics = deepcopy(metrics.gtfs_route_metrics) 
        self.gtfs_tpbp_metrics = deepcopy(metrics.gtfs_tpbp_metrics)

        self.data_option = params.data_option
        if 'AVL' in self.data_option:
            self.avl_stop_metrics = deepcopy(metrics.avl_stop_metrics)
            self.avl_route_metrics = deepcopy(metrics.avl_route_metrics) 
            self.avl_tpbp_metrics = deepcopy(metrics.avl_tpbp_metrics)

        self.SEGMENT_MULTIINDEX = ['route_id', 'stop_pair']
        self.CORRIDOR_MULTIINDEX = ['stop_pair']
        self.ROUTE_MULTIINDEX = ['route_id', 'direction_id']

        #: Initial stop-level aggregated metrics table generated from gtfs_stop_metrics, contains unique records of route_id + stop_pair.
        self.segments:pd.DataFrame = self.__generate_segments(self.gtfs_stop_metrics)
        #: Initial stop-aggregated-level aggregated metrics table generated from gtfs_stop_metrics, contains unique records of stop_pair.
        self.corridors:pd.DataFrame = self.__generate_corridors(self.gtfs_stop_metrics)
        #: Initial route-level aggregated metrics table generated from gtfs_route_metrics, contains unique records of route_id + direction_id.
        self.routes:pd.DataFrame = self.__generate_routes(self.gtfs_route_metrics)
        #: Initial timepoint-level aggregated metrics table generated from gtfs_pbp_metrics, contains unique records of route_id + timepoint stop_pair.
        self.tpbp_segments:pd.DataFrame = self.__generate_segments(self.gtfs_tpbp_metrics)
        #: Initial timepoint-aggregated-level aggregated metrics table generated from gtfs_tpbp_metrics, contains unique records of timepoint stop_pair.
        self.tpbp_corridors:pd.DataFrame = self.__generate_corridors(self.gtfs_tpbp_metrics)

        
        self.redValues = params.redValues
        self.percentiles:dict = params.config['percentiles']
        self.time_dict:dict = params.config['time_periods']

        self.aggregate_by_time_periods(params.output_paths['metric_calculation_aggre'])
        self.aggregate_by_10min_intervals(params.output_paths['metric_calculation_aggre_10min'])

    def aggregate_metrics(self, percentile:int):
        """All metrics aggregation methods. Can be overriden by child class to add more methods.

        :param percentile: percentile of metrics that is returned, e.g. 50 -> median, 90 -> worst decile
        :type percentile: int
        """
        # not time-dependent (use non time-filtered data)
        self.stop_spacing()
        self.span_of_service()
        self.revenue_hour()

        # ---- GTFS metrics ----
        # time-dependent (use time-filtered data)
        self.headway(percentile, 'scheduled')
        self.frequency('scheduled')
        self.wait_time('scheduled')
        self.running_time(percentile, 'scheduled')
        self.speed(percentile, 'scheduled')

        # ---- AVL metrics ----
        if 'AVL' in self.data_option:
            self.headway(percentile, 'observed')
            self.frequency('observed')
            self.running_time(percentile, 'observed')
            self.speed(percentile, 'observed', 'without_dwell')
            self.speed(percentile, 'observed', 'with_dwell')
            self.boardings(percentile)
            self.on_time_performance()
            self.crowding()
            self.passenger_load(percentile)
            self.wait_time('observed')
            self.excess_wait_time()
            self.passenger_flow()
            self.congestion_delay()
            self.productivity()
            

    def aggregate_by_start_end_time(self, start_time:List, end_time:List, percentile:int):
        """Given a start_time and end_time, filter each metrics table to keep only stop arrivals within the time window, or 
        trips that depart from the first stop within the time window, then calculate each time-dependent metric using the time-filtered metrics table. 
        A non time-dependent metric is one that does not change with different trips, such as stop spacing. All other metrics are time-dependent, and 
        therefore requries the time-filtered metrics for aggregation.

        :param start_time: the time after which trips/stop events are considered for aggregation, given in a list of [hour, minute], e.g. [3, 0] is 3 am, 
            and [25, 0] is 1 am of the same operation day
        :type start_time: List
        :param end_time: the time before which trips/stop events are considered for aggregation
        :type end_time: List
        :param percentile: percentile of metrics that is returned, e.g. 50 -> median, 90 -> worst decile
        :type percentile: int
        :raises TypeError: start_time or end_time is not provided in a list
        :raises ValueError: end_time is earlier than start_time
        """

        if not isinstance(start_time, List) or not isinstance(end_time, List) or \
            len(start_time) != 2 or len(end_time) != 2:
            raise TypeError(f'start_time and end_time must both be lists of length 2: [hour, minute].')

        start_time = start_time[0]*3600 + start_time[1]*60
        end_time = end_time[0]*3600 + end_time[1]*60

        if end_time < start_time:
            raise ValueError(f'Start time must be smaller than end time.')
        
        self.stop_metrics_time_filtered = self.__get_time_filtered_metrics(self.stop_metrics, start_time, end_time, 'stop')
        self.route_metrics_time_filtered = self.__get_time_filtered_metrics(self.route_metrics, start_time, end_time, 'route')
        self.tpbp_metrics_time_filtered = self.__get_time_filtered_metrics(self.tpbp_metrics, start_time, end_time, 'tpbp')

        # not time-dependent (use non time-filtered data)
        self.stop_spacing()

        if 'AVL' in self.data_option:
            self.avl_stop_metrics_time_filtered = self.__get_time_filtered_metrics(self.avl_stop_metrics, start_time, end_time, 'stop', 'stop_time')
            self.avl_route_metrics_time_filtered = self.__get_time_filtered_metrics(self.avl_route_metrics, start_time, end_time, 'route', 'stop_time')
            self.avl_tpbp_metrics_time_filtered = self.__get_time_filtered_metrics(self.avl_tpbp_metrics, start_time, end_time, 'tpbp', 'stop_time')
        
        self.aggregate_metrics(percentile)
        
        self.segments_agg_metrics = self.__get_agg_metrics(self.segments.reset_index(), 'segments')
        self.corridors_agg_metrics = self.__get_agg_metrics(self.corridors.reset_index(), 'corridors')
        self.routes_agg_metrics = self.__get_agg_metrics(self.routes.reset_index(), 'routes')
        self.tpbp_segments_agg_metrics = self.__get_agg_metrics(self.tpbp_segments.reset_index(), 'segments')
        self.tpbp_corridors_agg_metrics = self.__get_agg_metrics(self.tpbp_corridors.reset_index(), 'corridors')

    def aggregate_by_10min_intervals(self, output_path:str):
        """Generate aggregation output for every 10-min interval of the day and write to a pickled file the results in a dict. 
        Each key is a 10-min interval of the full day (defined in the config file under 'time_periods' -> 'full'), 
        and each element is a dict, whose key is a percentile of aggregation (e.g. 50 or 90), and element is a 
        tuple of five dataframes, each one containing the aggregated metrics of stop, stop-aggregated, route, timepoint, and
        timepoint-aggregated metrics.
        """
        logger.info(f'aggregating metrics for 10-min intervals')
        interval_to_second = lambda x: x[0] * SECONDS_IN_HOUR + x[1] * SECONDS_IN_MINUTE
        second_to_interval = lambda x: (x // SECONDS_IN_HOUR, (x % SECONDS_IN_HOUR) // SECONDS_IN_MINUTE)
        
        day_start, day_end = self.time_dict['full']
        day_start_sec = interval_to_second(day_start)
        day_end_sec = interval_to_second(day_end)

        all_10_min_intervals = []

        for interval_start_second in np.arange(day_start_sec, day_end_sec, SECONDS_IN_TEN_MINUTES):
            interval_end_second = min(day_end_sec, interval_start_second + SECONDS_IN_TEN_MINUTES)
            all_10_min_intervals.append(((second_to_interval(interval_start_second)), (second_to_interval(interval_end_second))))

        agg_metrics_10_min = {}
        for interval in tqdm(all_10_min_intervals, desc='aggregating metrics for 10-min intervals'):
            interval_start, interval_end = interval

            agg_metrics_10_min[interval] = {}

            for agg_method, percentile in self.percentiles.items():
                
                self.aggregate_by_start_end_time(list(interval_start), list(interval_end), percentile)

                agg_metrics_10_min[interval][agg_method] = (
                    self.segments_agg_metrics,
                    self.corridors_agg_metrics,
                    self.routes_agg_metrics,
                    self.tpbp_segments_agg_metrics,
                    self.tpbp_corridors_agg_metrics
                )

        output_path = check_parent_dir(output_path)
        pickle.dump(agg_metrics_10_min, open(output_path, "wb"))

    def aggregate_by_time_periods(self, output_path:str):
        """Generate aggregation output by pre-defined time periods and write to a pickled file the results in a dict. Each 
        key is a string concatenation of "time period name" - "aggregation level" - "percentile", e.g. (am_peak-segment-50), where 
        "segment" means stop level aggregation, corridor means stop-aggregated, segment-timepoints means timepoint, and corridor-timepoints 
        means timepoint-aggregated. Each key is the corresponding aggregated metrics table normalized to the JSON format.
        """
        agg_metrics = {}
        for period_name, period in tqdm(self.time_dict.items(), desc='aggregating metrics for pre-defined time periods'):
            start_time, end_time = period
            for agg_percentile, percentile_value in self.percentiles.items():

                self.aggregate_by_start_end_time(start_time, end_time, percentile_value)

                agg_metrics[f'{period_name}-segment-{agg_percentile}'] = self.segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-{agg_percentile}'] = self.corridors_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-route-{agg_percentile}'] = self.routes_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-segment-timepoints-{agg_percentile}'] = self.tpbp_segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-timepoints-{agg_percentile}'] = self.tpbp_corridors_agg_metrics.to_json(orient='records')

        output_path = check_parent_dir(output_path)
        pickle.dump(agg_metrics, open(output_path, "wb"))


    def __get_agg_metrics(self, metrics_df:pd.DataFrame, data_type:str):

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


    def __get_percentile(self, p:int, metric_name:str):

        if isinstance(p, int) and p >= 0 and p <= 100:
            if metric_name not in self.redValues:
                raise ValueError(f'{metric_name} is not found in redValues.')
            is_red_value =  self.redValues[metric_name]=='Low'
            p_to_use = (100 - p) if is_red_value else p
            return p_to_use / 100
        else:
            raise ValueError(f'Invalid percentile p={p}. Percentile must be a positive integer in [0, 100].')
        

    def __get_time_filtered_metrics(self, metrics:pd.DataFrame, start_time:int, end_time:int, data_type:str):

        if data_type == 'route':
            time_filtered_metrics = deepcopy(metrics.loc[(metrics['trip_start_time'] >= start_time) & (metrics['trip_start_time'] < end_time), :])
        else:
            time_filtered_metrics = deepcopy(metrics.loc[(metrics['arrival_time'] >= start_time) & (metrics['arrival_time'] < end_time), :])

        return time_filtered_metrics

    def __generate_segments(self, records:pd.DataFrame):
        
        # Get data structure for segments. Multiindex: route_id, stop_pair, hour       
        segments = records.groupby(self.SEGMENT_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'sample_size')

        return segments
    
    def __generate_corridors(self, records:pd.DataFrame):

        corridors = records.groupby(self.CORRIDOR_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'sample_size')

        return corridors

    def __generate_routes(self, records:pd.DataFrame):
        
        routes = records.groupby(self.ROUTE_MULTIINDEX)['trip_id'].agg('nunique').to_frame(name = 'sample_size')
        return routes

    def stop_spacing(self):
        """Aggregated stop spacing in ft. This metric is not time-dependent, so use non-time-filtered metrics for calculations.
            
            - stop/stop-aggregated level: stop_spacing of stop pairs averaged over all trips
            - routes level: sum of stop_spacing of all stops along a route averaged over all trips
            - timepoint/timepoint-aggregated level: stop_spacing of timepoint pairs averaged over all trips
        """

        sig_fig = 0

        self.segments['stop_spacing'] = self.stop_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.corridors['stop_spacing'] = self.stop_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.routes['stop_spacing'] = self.route_metrics.groupby(self.ROUTE_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_segments['stop_spacing'] = self.tpbp_metrics.groupby(self.SEGMENT_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)
        self.tpbp_corridors['stop_spacing'] = self.tpbp_metrics.groupby(self.CORRIDOR_MULTIINDEX)['stop_spacing'].mean().round(sig_fig)

    def span_of_service(self):
        """Aggregated service start/end in sec since epoch. 
        
            - all aggregation levels: the first arrival at first stop (service start) and the last arrival 
              at last stop (service end) of all trips
        """

        sig_fig = 1

        self.segments['service_start'] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('min')/3600).round(sig_fig)
        self.segments['service_end'] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('max')/3600).round(sig_fig)

        self.corridors['service_start'] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('min')/3600).round(sig_fig)
        self.corridors['service_end'] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('max')/3600).round(sig_fig)

        self.routes['service_start'] = (self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['trip_start_time'].agg('min')/3600).round(sig_fig)
        self.routes['service_end'] = (self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['trip_end_time'].agg('max')/3600).round(sig_fig)

        self.tpbp_segments['service_start'] = (self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('min')/3600).round(sig_fig)
        self.tpbp_segments['service_end'] = (self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['arrival_time'].agg('max')/3600).round(sig_fig)

        self.tpbp_corridors['service_start'] = (self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('min')/3600).round(sig_fig)
        self.tpbp_corridors['service_end'] = (self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['arrival_time'].agg('max')/3600).round(sig_fig)

    def revenue_hour(self):
        """Aggregated revenue hours in hr. 
        
            - all aggregation levels: the time lapse between service_end and service_start
        """

        sig_fig = 1

        self.segments['revenue_hour'] = (self.segments['service_end'] - self.segments['service_start']).round(sig_fig)
        self.corridors['revenue_hour'] = (self.corridors['service_end'] - self.corridors['service_start']).round(sig_fig)
        self.routes['revenue_hour'] = (self.routes['service_end'] - self.routes['service_start']).round(sig_fig)
        self.tpbp_segments['revenue_hour'] = (self.tpbp_segments['service_end'] - self.tpbp_segments['service_start']).round(sig_fig)
        self.tpbp_corridors['revenue_hour'] = (self.tpbp_corridors['service_end'] - self.tpbp_corridors['service_start']).round(sig_fig)

    def scheduled_frequency(self):
        """Aggregated scheduled frequency in trips/hr. 
        
            - all aggregation levels: the number of trips divided by service span (revenue hour)
        """

        sig_fig = 1

        self.segments['scheduled_frequency'] = (self.segments['sample_size'] / self.segments['revenue_hour']).round(sig_fig)
        self.corridors['scheduled_frequency'] = (self.corridors['sample_size'] / self.corridors['revenue_hour']).round(sig_fig)
        self.routes['scheduled_frequency'] = (self.routes['sample_size'] / self.routes['revenue_hour']).round(sig_fig)
        self.tpbp_segments['scheduled_frequency'] = (self.tpbp_segments['sample_size'] / self.tpbp_segments['revenue_hour']).round(sig_fig)
        self.tpbp_corridors['scheduled_frequency'] = (self.tpbp_corridors['sample_size'] / self.tpbp_corridors['revenue_hour']).round(sig_fig)


    def headway(self, method:str, percentile:int, data_type:str):
        """Aggregated scheduled or observed headway in minutes. 
        
            - all aggregation levels: the average or mode or percentile of headways of all trips

        :param method: mean (average headway), mode (the first mode value of headways), or percentile (choose value based on percentile)
        :type method: str
        :param percentile: the percentile that metrics are aggregated at
        :type percentile: int
        :param data_type: 'scheduled' or 'observed'
        :type data_type: str
        """

        sig_fig = 0

        metric_name = f'{data_type}_headway'

        if method == 'percentile':
            percentile = self.__get_percentile(percentile, metric_name)

            self.segments[metric_name] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[metric_name]\
                                                .quantile(percentile)).round(sig_fig)
            self.corridors[metric_name] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)[metric_name]\
                                                    .quantile(percentile)).round(sig_fig)
        else:
            if method == 'mean':
                func = pd.Series.mean
            elif method == 'mode': # find the first mode if there are multiple
                func = lambda x: scipy.stats.mode(x)[0]

            self.segments[metric_name] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[metric_name]\
                                                    .agg(func)).round(sig_fig)
            self.corridors[metric_name] = (self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)[metric_name]\
                                                    .agg(func)).round(sig_fig)


    def running_time(self, percentile:int, data_type:str):
        """Aggregated scheduled or observed running time in minutes. 
        
            - stop/stop-aggregated level: running time between each stop pair averaged over all trips
            - routes level: sum of running time between all stop pairs along a route averaged over all trips
            - timepoint/timepoint-aggregated level: running time between each timepoint pair averaged over all trips

        :param percentile: the percentile that metrics are aggregated at
        :type percentile: int
        :param data_type: 'scheduled' or 'observed'
        :type data_type: str
        """
        
        sig_fig = 1
        metric_name = f'{data_type}_running_time'

        percentile = self.__get_percentile(percentile, metric_name)

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
        """Aggregated scheduled or observed running speed with or without dwell in mph. 
        
            - stop/stop-aggregated level: running speed between each stop pair averaged over all trips
            - routes level: (sum of stop spacing of all stops) / (sum of running time of all stops) along a route averaged over all trips
            - timepoint/timepoint-aggregated level: running speed between each timepoint pair averaged over all trips

        :param percentile: the percentile that metrics are aggregated at
        :type percentile: int
        :param data_type: 'scheduled' or 'observed'
        :type data_type: str
        :param dwell: 'with_dwell' or '' (empty string means without dwell), defaults to ''
        :type dwell: str, optional
        """
       
        sig_fig = 0
        if dwell == '':
            metric_name = f'{data_type}_speed'
        else:
            metric_name = f'{data_type}_speed_{dwell}'

        percentile = self.__get_percentile(percentile, metric_name)


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
        """Aggregated Poisson wait time in minuntes. Wait time values are capped at 300 min (5 hr).

            - stop level: headway mean / 2 + variance / (2 * mean), assuming passenger arrival follows a Poisson process.
              See Equation 2.66 in Larson, R. C. & Odoni, A. R. (1981) Urban operations research. Englewood Cliffs, N.J: Prentice-Hall.
              
        :param data_type: 'scheduled' or 'observed'
        :type data_type: str
        """

        sig_fig = 0
        wait_time_cap = 300

        metric_name = f'{data_type}_wait_time'
        segments_data = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[f'{data_type}_headway']\
                                                    .agg('mean').to_frame(f'{data_type}_headway_mean').fillna(0)
        segments_data[f'{data_type}_headway_variance'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)[f'{data_type}_headway']\
                                                        .agg('var')
        self.segments[metric_name] = ((segments_data[f'{data_type}_headway_mean'] / 2) \
                                        + (segments_data[f'{data_type}_headway_variance'] / (2 * segments_data[f'{data_type}_headway_mean'])))\
                                            .clip(upper=wait_time_cap).round(sig_fig)

    def excess_wait_time(self):
        """Excess Poisson wait time in minutes. 

            - stop level: observed Poisson wait time - scheduled Poisson wait time
        """
        self.segments['excess_wait_time'] = (self.segments['observed_wait_time'] - self.segments['scheduled_wait_time']).clip(lower=0)


    def boardings(self, percentile:int):
        """Aggregated boardings in pax. 

            - stop/stop-aggregated level: boardings at the first stop of a stop pair averaged over all trips
            - routes level: and the sum of boardings at all stops along a route averaged over all trips
            - timepoint/timepoint-aggregated level: boardings at the first stop of a timepoint pair averaged over all trips

        :param percentile: the percentile that metrics are aggregated at
        :type percentile: int
        """

        sig_fig = 0

        percentile = self.__get_percentile(percentile, 'boardings')

        self.segments['boardings'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.corridors['boardings'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.routes['boardings'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.tpbp_segments['boardings'] = self.tpbp_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)
        self.tpbp_corridors['boardings'] = self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].quantile(percentile).round(sig_fig)

    def on_time_performance(self):
        """Aggregated on-time performance in seconds or %.

            - stop level: arrival delay at the first stop of a stop pair averaged over all trips
            - routes level: percent of on-time arrivals among all stops along a trip averaged over all trips
        """

        sig_fig = 0

        self.segments['on_time_performance'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)
        self.routes['on_time_performance'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)
    
    def crowding(self):
        """Aggregated crowding in %.

            - stop/stop-aggregated level: crowding level between a stop pair averaged over all trips
            - route level: peak crowding level of a trip averaged over all trips
        """

        sig_fig = 0

        self.segments['crowding'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['crowding'].mean().round(sig_fig)
        self.corridors['boardings'] = self.stop_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['boardings'].mean().round(sig_fig)
        self.routes['crowding'] = self.route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['crowding'].mean().round(sig_fig)
    
    def passenger_load(self, percentile:int):
        """Aggregated passenger load in pax.

            - stop level: passenger load between a stop pair averaged over all trips

        :param percentile: the percentile that metrics are aggregated at
        :type percentile: int
        """

        sig_fig = 0
        percentile = self.__get_percentile(percentile, 'passenger_load')

        self.segments['passenger_load'] = self.stop_metrics_time_filtered\
                                                    .groupby(self.SEGMENT_MULTIINDEX)['passenger_load'].quantile(percentile).round(sig_fig)

    def passenger_flow(self):
        """Aggregated passenger flow in pax/hr.

            - stop level: (sum of passenger load) / (lenghth of analysis time period) between a stop pair
        """

        sig_fig = 0

        period = self.end_time - self.start_time

        self.segments['passenger_flow'] = (self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['passenger_load'].sum() / period).round(sig_fig)
        self.corridors['passenger_flow'] = (self.tpbp_metrics_time_filtered.groupby(self.CORRIDOR_MULTIINDEX)['passenger_load'].sum() / period).round(sig_fig)

    def congestion_delay(self):
        """Aggregated vehicle- and passenger-weighted congestion delay in min/mile or pax-min/mile.

            - stop level: vehicle or passenger congestion delays between a stop pair averaged over all trips
        """
        
        sig_fig = 0

        self.segments['vehicle_congestion_delay'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['vehicle_congestion_delay'].mean().round(sig_fig)
        self.segments['passenger_congestion_delay'] = self.stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['passenger_congestion_delay'].mean().round(sig_fig)

    def productivity(self):
        """Productivity in pax/revenue hour. 

            - route level: (sum of passengers that board at all stops of a route) / (revenue hour of the route)
        """

        sig_fig = 0

        self.routes['productivity'] = (self.route_metrics.groupby(self.ROUTE_MULTIINDEX)['boardings'].sum() \
                                        / self.routes['revenue_hour']).round(sig_fig)