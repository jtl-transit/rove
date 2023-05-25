from typing import List
from backend.data_class.rove_parameters import ROVE_params
from backend.metrics.metric_aggregation import Metric_Aggregation
from backend.metrics.metric_calculation import Metric_Calculation
import pandas as pd

class WMATA_Metric_Aggregation(Metric_Aggregation):

    def __init__(self, metrics: Metric_Calculation, params: ROVE_params):
        super().__init__(metrics, params)

    def aggregate_metrics(self, percentile:int):
        super().aggregate_metrics(percentile)

        if 'AVL' in self.data_option:
            self.schedule_sufficiency_index()

    def on_time_performance(self):
        """Aggregated on-time performance in seconds or %.

            - stop level: arrival delay in seconds at the first stop of a stop pair averaged over all trips
            - routes level: percent of on-time arrivals among all stops along a trip averaged over all trips
        """

        sig_fig = 0

        self.segments['on_time_performance_sec'] = self.avl_stop_metrics_time_filtered.groupby(self.SEGMENT_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)
        self.routes['on_time_performance_perc'] = self.avl_route_metrics_time_filtered.groupby(self.ROUTE_MULTIINDEX)['on_time_performance'].mean().round(sig_fig)

        self.metrics_names['on_time_performance_sec'] = 'On Time Performance (sec)'
        self.metrics_names['on_time_performance_perc'] = 'On Time Performance (% of timepoints)'


    def schedule_sufficiency_index(self):
        """Weighted coefficient of standard deviation of running time.

        - corridor level: coefficient of standard deviation of corridor-level running time, weighted by number of observations of each trip_id
        - route level: coefficient of standard deviation of route-level running time, weighted by number of observations of each trip_id
        """
        sig_fig = 2

        def __ssi_calculation(records:pd.DataFrame, by_trips_cols:List) -> pd.DataFrame:
            data_by_trips = records.groupby(by_trips_cols)['observed_running_time_with_dwell'].agg('mean').to_frame(name = 'mean')
            data_by_trips['std'] = records.groupby(by_trips_cols)['observed_running_time_with_dwell'].std()
            data_by_trips['cov'] = data_by_trips['std'] / data_by_trips['mean']
            data_by_trips['count'] = records.groupby(by_trips_cols)['observed_running_time_with_dwell'].count()
            data_by_trips['total_trips'] = data_by_trips.groupby([item for item in by_trips_cols if item not in ['trip_id']])['count'].transform('sum')
            data_by_trips['weight'] = data_by_trips['count'] / data_by_trips['total_trips']
            data_by_trips['weighted_cov'] = data_by_trips['weight'] * data_by_trips['cov']
            return data_by_trips

        segment_by_trips =  __ssi_calculation(self.avl_stop_metrics_time_filtered, ['trip_id', 'route_id', 'stop_pair'])
        self.segments['ssi'] = segment_by_trips.groupby(self.SEGMENT_MULTIINDEX)['weighted_cov'].sum().round(sig_fig)

        corridor_by_trips = __ssi_calculation(self.avl_stop_metrics_time_filtered, ['trip_id', 'stop_pair'])
        self.corridors['ssi'] = corridor_by_trips.groupby(self.CORRIDOR_MULTIINDEX)['weighted_cov'].sum().round(sig_fig)

        route_by_trips = __ssi_calculation(self.avl_route_metrics_time_filtered, ['trip_id', 'route_id', 'direction_id'])         
        self.routes['ssi'] = route_by_trips.groupby(self.ROUTE_MULTIINDEX)['weighted_cov'].sum().round(sig_fig)

        self.metrics_names['ssi'] = 'Run Time Variability'