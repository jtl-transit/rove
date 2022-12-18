from copy import deepcopy
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger("backendLogger")

FEET_TO_METERS = 0.3048
KILOMETER_TO_FT = 3280.84
FT_PER_MIN_TO_MPH = 0.0113636
FEET_TO_MILES = 0.000189394
MS_TO_MPH = 3.6/1.6
MAX_HEADWAY = 90
MAX_SPEED_MPH = 65
MEAN_SPEED_MPH = 30

class Metric_Calculation():
    """Calculate stop, stop-aggregated, route, timepoint, and timepoint-aggregated level metrics. If AVL data is provided 
    and records for the same trip_id exist across multiple days, then calculate the trip metrics by averaging across all service dates. 
    In other words, the metric calculation module averages metrics for the same trip, so that metrics tables after calculation 
    only contains unique route_id, trip_id and stop_pair combinations. This is the upstream calculation of metric aggregation, which 
    averages metrics of all trips on each aggregation level.

    :param shapes: shapes table from Shape Generation
    :type shapes: pd.DataFrame
    :param gtfs_records: GTFS records table
    :type gtfs_records: pd.DataFrame
    :param avl_records: AVL records table
    :type avl_records: pd.DataFrame
    :param data_option: user-specified data option
    :type data_option: str
    :raises ValueError: 'AVL' is in data_option but the avl_records table is None
    """
    def __init__(self, shapes:pd.DataFrame, gtfs_records:pd.DataFrame, avl_records:pd.DataFrame, data_option:str):
        
        logger.info(f'Calculating metrics...')

        #: Initial stop-level metrics table generated from the GTFS records table.
        self.gtfs_stop_metrics:pd.DataFrame = self.__prepare_stop_event_records(gtfs_records, 'GTFS')

        #: Initial timepoint-level metrics table generated from the GTFS records table.
        self.gtfs_tpbp_metrics = self.__prepare_stop_event_records(gtfs_records.loc[gtfs_records['tp_bp']==1, :], 'GTFS')

        self.GTFS_ROUTE_METRICS_KEY_COLUMNS = ['pattern', 'route_id', 'direction_id', 'trip_id']
        #: Initial route-level metrics table generated from the GTFS records table.
        self.gtfs_route_metrics = self.gtfs_stop_metrics[self.GTFS_ROUTE_METRICS_KEY_COLUMNS + ['trip_start_time', 'trip_end_time']].drop_duplicates()
        # add service_id column
        self.gtfs_route_metrics = self.gtfs_route_metrics.merge(self.gtfs_stop_metrics[['trip_id', 'service_id']].drop_duplicates(), \
                                        on=['trip_id'], how='left')    

        if 'AVL' in data_option:
            if avl_records is not None:
                self.avl_stop_metrics = self.__prepare_stop_event_records(avl_records, 'AVL')
                # add tp_bp column
                self.avl_stop_metrics = self.avl_stop_metrics.merge(self.gtfs_stop_metrics[['route_id', 'stop_id', 'tp_bp', 'timepoint']].drop_duplicates(), \
                                        on=['route_id', 'stop_id'], how='left')

                self.avl_tpbp_metrics = self.__prepare_stop_event_records(self.avl_stop_metrics[self.avl_stop_metrics['tp_bp']==1], 'AVL')

                self.AVL_ROUTE_METRICS_KEY_COLUMNS = ['svc_date', 'trip_id', 'route_id']
                self.avl_route_metrics = self.avl_stop_metrics[self.AVL_ROUTE_METRICS_KEY_COLUMNS + ['trip_start_time', 'trip_end_time']].drop_duplicates()
                # add direction_id column
                self.avl_route_metrics = self.avl_route_metrics.merge(self.gtfs_route_metrics[['route_id', 'trip_id', 'direction_id']].drop_duplicates(), \
                                        on=['route_id', 'trip_id'], how='left')        
            else:
                raise ValueError(f'data_option is {data_option} but the AVL records table is None.')

        # ---- GTFS metrics ----
        self.stop_spacing(shapes)
        self.scheduled_headway()
        self.scheduled_running_time()
        self.scheduled_speed()

        # ---- AVL metrics ----
        if 'AVL' in data_option:
            self.observed_headway()
            self.observed_running_time()
            self.observed_speed_without_dwell()
            self.observed_running_time_with_dwell()
            self.observed_speed_with_dwell()
            self.boardings()
            self.on_time_performance()
            self.passenger_load()
            self.crowding()
            self.congestion_delay()
        logger.info(f'Metrics calculation completed.')

    def __prepare_stop_event_records(self, records:pd.DataFrame, type:str) -> pd.DataFrame:
        """Add three columns to the records table: next_stop, next_stop_arrival_time, stop_pair while keeping original index.

        :param records: GTFS records
        :type records: pd.DataFrame
        :param type: depending on 'GTFS' or 'AVL', a different grouping is used to populate the next stop ID and its arrival time for each stop record
        :type type: str
        :raises ValueError: a type that is not 'GTFS' or 'AVL' is given
        :return: the records table with the three added columns
        :rtype: pd.DataFrame
        """

        if type == 'GTFS':
            groups = ['trip_id']
            arrival_time_col = 'arrival_time'
        elif type == 'AVL':
            groups = ['svc_date', 'trip_id']
            arrival_time_col = 'stop_time'
        else:
            raise ValueError(f"Invalid type {type}, must be one of: 'GTFS', 'AVL'.")

        records = deepcopy(records).reset_index()
        records.loc[:, 'next_stop'] = records.groupby(by=groups)['stop_id'].shift(-1)
        records.loc[:, 'next_stop_arrival_time'] = records.groupby(by=groups)[arrival_time_col].shift(-1)
        records.loc[:, 'stop_pair'] = pd.Series(list(zip(records.stop_id, records.next_stop)))
        records = records.dropna(subset=['next_stop']).set_index('index')

        return records

    def stop_spacing(self, shapes):
        """Stop spacing in ft. Distance is returned from Valhalla trace route requests in unit of kilometers.
        """

        logger.info(f'calculating stop spacing')

        records = self.gtfs_stop_metrics.reset_index()\
                    .merge(shapes[['pattern', 'stop_pair', 'distance']].drop_duplicates(), on=['pattern', 'stop_pair'], how='left')\
                    .set_index('index')

        self.gtfs_stop_metrics['stop_spacing'] = (records['distance'] * KILOMETER_TO_FT).round(2)

        routes_data = self.gtfs_stop_metrics.groupby(self.GTFS_ROUTE_METRICS_KEY_COLUMNS)['stop_spacing'].sum().reset_index()
        self.gtfs_route_metrics = self.gtfs_route_metrics.merge(routes_data, on=self.GTFS_ROUTE_METRICS_KEY_COLUMNS, how='left')

        records = deepcopy(self.gtfs_stop_metrics)
        ## calculate the distance between timepoints using the above records dataframe
        ## step 1: label timepoint pairs as tpbp_stop_pair, each tpbp_stop_pair could correspond to multiple consecutive stop_pairs
        ##           stops that don't belong to a tpbp_stop_pair but is a tp_bp (e.g. the last tp_bp of a route) are labeled -1
        records['tpbp_stop_pair'] = self.gtfs_tpbp_metrics['stop_pair']
        records.loc[(records['tp_bp']==1) & (records['tpbp_stop_pair'].isnull()), 'tpbp_stop_pair'] = -1
        records['tpbp_stop_pair'] = records.groupby('trip_id')['tpbp_stop_pair'].fillna(method='ffill')
        ## step 2: calculate the culmulative distance of each stop along the trip, and keep only the last record of each tpbp_stop_pair
        ##           since the last culmulative distance encompasses the distances of all stop_pairs within the same tpbp_stop_pair
        records['distance_cumsum'] = records.groupby('trip_id')['stop_spacing'].cumsum()
        records = records[~records.duplicated(subset=['trip_id', 'tpbp_stop_pair'], keep='last')]
        ## step 3: distance between timepoints = difference between kept tpbp_stop_pair culmulative distances
        records['tpbp_distance'] = records.groupby('trip_id')['distance_cumsum'].diff().fillna(records['distance_cumsum'])

        self.gtfs_tpbp_metrics = self.gtfs_tpbp_metrics\
                                .merge(records[['pattern', 'tpbp_stop_pair', 'tpbp_distance']].drop_duplicates(), \
                                    left_on=['pattern', 'stop_pair'], right_on=['pattern', 'tpbp_stop_pair'], how='left')\
                                .drop(columns=['tpbp_stop_pair']).rename(columns={'tpbp_distance': 'stop_spacing'})

    def scheduled_headway(self):
        """Scheduled headway in minutes. Defined as the difference between two consecutive scheduled arrivals of a route at the first stop of a stop pair.
        """

        logger.info(f'calculating scheduled headway')
        
        self.gtfs_stop_metrics['scheduled_headway'] = (self.gtfs_stop_metrics.sort_values(['service_id', 'route_id', 'stop_pair', 'arrival_time'])\
                                                    .groupby(['service_id', 'route_id', 'stop_pair'])['arrival_time'].diff())/60
        self.gtfs_route_metrics['scheduled_headway'] = (self.gtfs_route_metrics.sort_values(['service_id', 'route_id', 'direction_id', 'trip_start_time'])\
                                                    .groupby(['service_id', 'route_id', 'direction_id'])['trip_start_time'].diff())/60
        self.gtfs_tpbp_metrics['scheduled_headway'] = (self.gtfs_tpbp_metrics.sort_values(['service_id', 'route_id', 'stop_pair', 'arrival_time'])\
                                                    .groupby(['service_id', 'route_id', 'stop_pair'])['arrival_time'].diff())/60
        

    def scheduled_running_time(self):
        """Running time in minutes. Defined as the difference between the departure time at a stop and arrival time at the next stop.
        """

        logger.info(f'calculating scheduled running time')

        self.gtfs_stop_metrics['scheduled_running_time'] = ((self.gtfs_stop_metrics['next_stop_arrival_time'] - self.gtfs_stop_metrics['departure_time']) / 60).round(2)
        
        routes_data = self.gtfs_stop_metrics.groupby(self.GTFS_ROUTE_METRICS_KEY_COLUMNS)['scheduled_running_time'].sum().reset_index()
        self.gtfs_route_metrics = self.gtfs_route_metrics.merge(routes_data, on=self.GTFS_ROUTE_METRICS_KEY_COLUMNS, how='left')

        tpbp_metrics_temp = self.gtfs_stop_metrics.copy()
        tpbp_metrics_temp['tpbp_group'] = tpbp_metrics_temp.groupby(['trip_id'])['tp_bp'].cumsum()
        tpbp_metrics_temp['tpbp_scheduled_running_time'] = tpbp_metrics_temp.groupby(['trip_id', 'tpbp_group'])['scheduled_running_time'].transform('sum')
        self.gtfs_tpbp_metrics['scheduled_running_time'] = tpbp_metrics_temp['tpbp_scheduled_running_time']

    
    def scheduled_speed(self):
        """Scheduled running speed in mph. Defined as stop spacing divided by running time.
        """

        logger.info(f'calculating scheduled speed')

        self.gtfs_stop_metrics['scheduled_speed'] = ((self.gtfs_stop_metrics['stop_spacing'] / self.gtfs_stop_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.gtfs_route_metrics['scheduled_speed'] = ((self.gtfs_route_metrics['stop_spacing'] / self.gtfs_route_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.gtfs_tpbp_metrics['scheduled_speed'] = ((self.gtfs_tpbp_metrics['stop_spacing'] / self.gtfs_tpbp_metrics['scheduled_running_time']) * FT_PER_MIN_TO_MPH).round(2)

    def observed_headway(self):
        """Observed headway in minutes. Defined as the difference between two consecutive observed arrivals of a route at the first stop of a stop pair on each day, 
        then averaged over all service dates.
        """
        
        logger.info(f'calculating observed headway')

        self.avl_stop_metrics['observed_headway'] = (self.avl_stop_metrics.sort_values(['svc_date', 'route_id', 'stop_pair', 'stop_time'])\
                                                    .groupby(['svc_date', 'route_id', 'stop_pair'])['stop_time'].diff())/60
        self.avl_route_metrics['observed_headway'] = (self.avl_route_metrics.sort_values(['svc_date', 'route_id', 'direction_id', 'trip_start_time'])\
                                                    .groupby(['svc_date', 'route_id', 'direction_id'])['trip_start_time'].diff())/60
        self.avl_tpbp_metrics['observed_headway'] = (self.avl_tpbp_metrics.sort_values(['svc_date', 'route_id', 'stop_pair', 'stop_time'])\
                                                    .groupby(['svc_date', 'route_id', 'stop_pair'])['stop_time'].diff())/60
        

    def observed_running_time(self):
        """Observed running time without dwell in minutes. Defined as the time between departure at a stop and arrival at the next stop averaged over all service dates 
        for each bus trip.
        """

        logger.info(f'calculating observed running time without dwell')

        self.avl_stop_metrics['observed_running_time'] = ((self.avl_stop_metrics['next_stop_arrival_time'] - self.avl_stop_metrics['stop_time'] \
                                                    - self.avl_stop_metrics['dwell_time']).clip(lower=0) / 60).round(2)
        
        routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['observed_running_time'].sum().reset_index()
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data, on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')

        tpbp_metrics_temp = self.avl_stop_metrics.copy()
        tpbp_metrics_temp['tpbp_group'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id'])['tp_bp'].cumsum()
        tpbp_metrics_temp['tpbp_observed_running_time'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id', 'tpbp_group'])['observed_running_time'].transform('sum')
        self.avl_tpbp_metrics['observed_running_time'] = tpbp_metrics_temp['tpbp_observed_running_time']


    def observed_speed_without_dwell(self):
        """Observed running speed without dwell in mph. Defined as stop spacing divided by the observed running time without dwell.
        """

        logger.info(f'calculating observed speed without dwell')

        self.avl_stop_metrics = self.avl_stop_metrics.merge(self.gtfs_stop_metrics[['route_id', 'trip_id', 'stop_pair', 'stop_spacing']].drop_duplicates(), on=['route_id', 'trip_id', 'stop_pair'], how='left')
        self.avl_stop_metrics['observed_speed_without_dwell'] = ((self.avl_stop_metrics['stop_spacing'] / self.avl_stop_metrics['observed_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.avl_route_metrics = self.avl_route_metrics.merge(self.gtfs_route_metrics[['route_id', 'trip_id', 'stop_spacing']].drop_duplicates(), on=['route_id', 'trip_id'], how='left')
        self.avl_route_metrics['observed_speed_without_dwell'] = ((self.avl_route_metrics['stop_spacing'] / self.avl_route_metrics['observed_running_time']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.avl_tpbp_metrics = self.avl_tpbp_metrics.merge(self.gtfs_tpbp_metrics[['route_id', 'trip_id', 'stop_pair', 'stop_spacing']].drop_duplicates(), on=['route_id', 'trip_id', 'stop_pair'], how='left')
        self.avl_tpbp_metrics['observed_speed_without_dwell'] = ((self.avl_tpbp_metrics['stop_spacing'] / self.avl_tpbp_metrics['observed_running_time']) * FT_PER_MIN_TO_MPH).round(2)

    def observed_running_time_with_dwell(self):
        """Observed running time with dwell in minutes. Defined as the time between arrival at a stop and arrival at the next stop averaged over all service dates 
        for each bus trip.
        """

        logger.info(f'calculating observed running time with dwell')

        self.avl_stop_metrics['observed_running_time_with_dwell'] = ((self.avl_stop_metrics['next_stop_arrival_time'] - self.avl_stop_metrics['stop_time']).clip(lower=0) / 60).round(2)
        
        routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['observed_running_time_with_dwell'].sum().reset_index()
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data, on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')

        tpbp_metrics_temp = self.avl_stop_metrics.copy()
        tpbp_metrics_temp['tpbp_group'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id'])['tp_bp'].cumsum()
        tpbp_metrics_temp['tpbp_observed_running_time_with_dwell'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id', 'tpbp_group'])['observed_running_time_with_dwell'].transform('sum')
        self.avl_tpbp_metrics['observed_running_time_with_dwell'] = tpbp_metrics_temp['tpbp_observed_running_time_with_dwell']


    def observed_speed_with_dwell(self):
        """Observed running speed with dwell in mph. Defined as stop spacing divided by the observed running time with dwell.
        """

        logger.info(f'calculating observed speed with dwell')

        self.avl_stop_metrics['observed_speed_with_dwell'] = ((self.avl_stop_metrics['stop_spacing'] / self.avl_stop_metrics['observed_running_time_with_dwell']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.avl_route_metrics['observed_speed_with_dwell'] = ((self.avl_route_metrics['stop_spacing'] / self.avl_route_metrics['observed_running_time_with_dwell']) * FT_PER_MIN_TO_MPH).round(2)
        
        self.avl_tpbp_metrics['observed_speed_with_dwell'] = ((self.avl_tpbp_metrics['stop_spacing'] / self.avl_tpbp_metrics['observed_running_time_with_dwell']) * FT_PER_MIN_TO_MPH).round(2)

    def boardings(self):
        """Boardings in pax. Defined as the number of passengers boarding the bus at each stop averaged over all service dates for each bus trip.
        """

        logger.info(f'calculating boardings')

        self.avl_stop_metrics['boardings'] = self.avl_stop_metrics['passenger_on']
        
        routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['boardings'].sum().reset_index()
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data, on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')

        tpbp_metrics_temp = self.avl_stop_metrics.copy()
        tpbp_metrics_temp['tpbp_group'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id'])['tp_bp'].cumsum()
        tpbp_metrics_temp['tpbp_boardings'] = tpbp_metrics_temp.groupby(['svc_date', 'trip_id', 'tpbp_group'])['boardings'].transform('sum')
        self.avl_tpbp_metrics['boardings'] = tpbp_metrics_temp['tpbp_boardings']


    def on_time_performance(self, no_earlier_than=-1, no_later_than=5, route_metric_bases:str='timepoint'):
        """On time performance in seconds of delay (actual arrival - scheduled arrival) for stop segments, and percentage of stops on time per trip for routes, 
        averaged over all service dates for each bus trip.

        :param no_earlier_than: minutes that a bus can arrive early for to be on time. Must be negative, defaults to -1
        :type no_earlier_than: int, optional
        :param no_later_than: minutes that a bus can arrive late for to be on time. Must be positive, defaults to 5
        :type no_later_than: int, optional
        :raises ValueError: no_earlier_than is positive or no_later_than is negative
        :param route_metric_bases: whether the route on-time performance is calculated by counting number of timepoints on-time
            or number of stops on-time
        :type route_metric_bases: str
        """

        logger.info(f'calculating on time performance')

        if no_earlier_than > 0 or no_later_than < 0:
            raise ValueError(f'no_earlier_than must be a negative value, no_later_than must be a positive value.')
        
        self.avl_stop_metrics = self.avl_stop_metrics.merge(self.gtfs_stop_metrics[['route_id', 'trip_id', 'stop_pair', 'arrival_time']], on=['route_id', 'trip_id', 'stop_pair'], how='left')
        self.avl_stop_metrics['on_time_performance'] = self.avl_stop_metrics['stop_time'] - self.avl_stop_metrics['arrival_time']

        self.avl_stop_metrics['is_on_time'] = ((self.avl_stop_metrics['on_time_performance'] > no_earlier_than * 60) & \
                                                    (self.avl_stop_metrics['on_time_performance'] < no_later_than * 60)).astype(int)
        
        self.avl_tpbp_metrics = self.avl_tpbp_metrics.merge(self.gtfs_tpbp_metrics[['route_id', 'trip_id', 'stop_pair', 'arrival_time']], on=['route_id', 'trip_id', 'stop_pair'], how='left')
        self.avl_tpbp_metrics['on_time_performance'] = self.avl_tpbp_metrics['stop_time'] - self.avl_tpbp_metrics['arrival_time']

        self.avl_tpbp_metrics['is_on_time'] = ((self.avl_tpbp_metrics['on_time_performance'] > no_earlier_than * 60) & \
                                                    (self.avl_tpbp_metrics['on_time_performance'] < no_later_than * 60)).astype(int)

        if route_metric_bases == 'timepoint':
            timepoint_df = self.avl_stop_metrics[self.avl_stop_metrics['timepoint']==1]
            routes_data = timepoint_df.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['is_on_time'].sum().to_frame('on_time_count')
            routes_data['total_stops'] = timepoint_df.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['stop_pair'].count()
        elif route_metric_bases == 'stop':
            routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['is_on_time'].sum().to_frame('on_time_count')
            routes_data['total_stops'] = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['stop_pair'].count()
        else:
            raise ValueError(f"invalid route_metric_bases {route_metric_bases}, must be one of 'timepoint' or 'stop'.")
        routes_data['on_time_performance'] = routes_data['on_time_count'] / routes_data['total_stops'] * 100
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data.reset_index()\
                                    .drop(columns=['on_time_count', 'total_stops']), on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')

    def passenger_load(self):
        """Passenger load in pax. Defined as the number of passengers onboard the bus within each stop pair, averaged over all service dates for each bus trip.
        """

        logger.info(f'calculating passenger load')
        
        routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['passenger_load'].max().reset_index()
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data, on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')


    def crowding(self):
        """Crowding in percentage. Defined as the percent of passenger load over seated capacity for stop metrics, and percent of peak load over seated capacity for route metrics, 
        averaged over all service dates for each bus trip.
        """

        logger.info(f'calculating crowding')

        self.avl_stop_metrics['crowding'] = self.avl_stop_metrics['passenger_load'] / self.avl_stop_metrics['seat_capacity'] * 100
        
        routes_data = self.avl_stop_metrics.groupby(self.AVL_ROUTE_METRICS_KEY_COLUMNS)['crowding'].max().round(0).reset_index()
        self.avl_route_metrics = self.avl_route_metrics.merge(routes_data, on=self.AVL_ROUTE_METRICS_KEY_COLUMNS, how='left')


    def congestion_delay(self):
        """Vehicle congestion delay in min/mile and passenger congestion delay in pax-min/mile.
        """
        
        logger.info(f'calculating congestion delay')

        self.avl_stop_metrics['free_flow_speed'] = self.avl_stop_metrics.groupby(['stop_pair'])['observed_speed_without_dwell'].quantile(0.9)\
                                                    .clip(upper=MAX_SPEED_MPH).fillna(MEAN_SPEED_MPH)
        self.avl_stop_metrics['free_flow_travel_time'] = self.avl_stop_metrics['stop_spacing'] / (self.avl_stop_metrics['free_flow_speed'] / FT_PER_MIN_TO_MPH)
        self.avl_stop_metrics['observed_travel_time'] = self.avl_stop_metrics['stop_spacing'] / (self.avl_stop_metrics['observed_speed_without_dwell'] / FT_PER_MIN_TO_MPH)

        self.avl_stop_metrics['vehicle_congestion_delay'] = (self.avl_stop_metrics['observed_travel_time'] - self.avl_stop_metrics['free_flow_travel_time']) \
                                                / (self.avl_stop_metrics['stop_spacing'] * FEET_TO_MILES)
        
        self.avl_stop_metrics['passenger_congestion_delay'] = self.avl_stop_metrics['vehicle_congestion_delay'] * self.avl_stop_metrics['passenger_load']