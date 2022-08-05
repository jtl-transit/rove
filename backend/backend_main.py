# import logging
from data_class import GTFS, MBTA_GTFS, WMATA_GTFS, AVL, MBTA_AVL
from backend.shapes.base_shape import BaseShape
from logger.backend_logger import getLogger
from backend.metrics import MetricCalculation, MetricAggregation
from data_class.rove_parameters import ROVE_params
from helper_functions import read_shapes
import pandas as pd
import numpy as np
import os
import ast
import pickle
from tqdm.auto import tqdm
# from parameters.generic_csv_data import CSV_DATA

# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "10" # MM in string format
YEAR = "2021" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
DATA_OPTION = 'GTFS' # GTFS, GTFS-AVL

SHAPE_GENERATION = True # True/False: whether to generate shapes
METRIC_CAL_AGG = False # True/False: whether to run metric calculation and aggregation

# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__():

    logger.info(f'Starting ROVE backend processes for \n--{AGENCY}, {MONTH}-{YEAR}. '\
                f'\n--Data Option: {DATA_OPTION}.'\
                f'\n--Date Type: {DATE_TYPE}.')

    # -----store parameters-----
    params = ROVE_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION)

    # ------data generation------
    # GTFS
    if AGENCY == 'MBTA':
        bus_gtfs = MBTA_GTFS(params, mode='bus')
    elif AGENCY == 'WMATA':
        bus_gtfs = WMATA_GTFS(params, mode='bus')
    else:
        bus_gtfs = GTFS(params, mode='bus')
    gtfs_records = bus_gtfs.records

    # AVL
    if 'AVL' in DATA_OPTION:
        if AGENCY == 'MBTA':
            avl = MBTA_AVL(params)
        else:
            avl = AVL(params) 
        avl_records = avl.records
    else:
        avl_records = None

    # ------shape generation------ 
    if SHAPE_GENERATION or read_shapes(params.output_paths['shapes']).empty:
        shapes = BaseShape(bus_gtfs.patterns_dict, outpath=params.output_paths['shapes']).shapes
    else:
        shapes = read_shapes(params.output_paths['shapes'])

    # ------metric calculation and aggregation------
    if METRIC_CAL_AGG:
        # metric calculation
        metrics = MetricCalculation(shapes, gtfs_records, avl_records)

        # metric aggregation
        aggregation_stats = {
            'median': 50,
            '90': 90
        }
        redValues = params.redValues

        # metric aggregation by time periods
        time_dict:dict = params.config['time_periods']
        agg_metrics = {}
        for period_name, period in time_dict.items():
            start_time, end_time = period
            for agg_method, percentile in aggregation_stats.items():

                agg = MetricAggregation(metrics.stop_metrics, metrics.tpbp_metrics, metrics.route_metrics, start_time, end_time, percentile, redValues)

                agg_metrics[f'{period_name}-segment-{agg_method}'] = agg.segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-{agg_method}'] = agg.corridors_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-route-{agg_method}'] = agg.routes_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-segment-timepoints-{agg_method}'] = agg.tpbp_segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-timepoints-{agg_method}'] = agg.tpbp_corridors_agg_metrics.to_json(orient='records')

        pickle.dump(agg_metrics, open(params.output_paths['metric_calculation_aggre'], "wb"))

        # metric aggregation by 10-minute interval
        SECONDS_IN_MINUTE = 60
        SECONDS_IN_HOUR = 3600
        SECONDS_IN_TEN_MINUTES = SECONDS_IN_MINUTE * 10

        interval_to_second = lambda x: x[0] * SECONDS_IN_HOUR + x[1] * SECONDS_IN_MINUTE
        second_to_interval = lambda x: (x // SECONDS_IN_HOUR, (x % SECONDS_IN_HOUR) // SECONDS_IN_MINUTE)
        
        day_start, day_end = params.config['time_periods']['full']
        day_start_sec = interval_to_second(day_start)
        day_end_sec = interval_to_second(day_end)

        all_10_min_intervals = []

        for interval_start_second in np.arange(day_start_sec, day_end_sec, SECONDS_IN_TEN_MINUTES):
            interval_end_second = min(day_end_sec, interval_start_second + SECONDS_IN_TEN_MINUTES)
            all_10_min_intervals.append(((second_to_interval(interval_start_second)), (second_to_interval(interval_end_second))))

        agg_metrics_10_min = {}
        for interval in all_10_min_intervals:
            interval_start, interval_end = interval

            agg_metrics_10_min[interval] = {}

            for agg_method, percentile in aggregation_stats.items():

                agg = MetricAggregation(metrics.stop_metrics, metrics.tpbp_metrics, metrics.route_metrics, \
                                        list(interval_start), list(interval_end), percentile, redValues)

                agg_metrics_10_min[interval][agg_method] = (
                    agg.segments_agg_metrics,
                    agg.corridors_agg_metrics,
                    agg.routes_agg_metrics,
                    agg.tpbp_segments_agg_metrics,
                    agg.tpbp_corridors_agg_metrics
                )

        pickle.dump(agg_metrics_10_min, open(params.output_paths['metric_calculation_aggre_10min'], "wb"))

    logger.info(f'ROVE backend process completed')

if __name__ == "__main__":
    __main__()
