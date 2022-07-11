# import logging
from rove.data_class import GTFS, MBTA_GTFS, WMATA_GTFS, AVL, MBTA_AVL
from rove.shape_generation import BaseShape
from logger.backend_logger import getLogger
from rove.metrics import MetricCalculation, MetricAggregation
from data_class.rove_parameters import ROVE_params
from .helper_functions import read_shapes
import pandas as pd
import numpy as np
import os
import ast
import pickle
from tqdm.auto import tqdm
# from parameters.generic_csv_data import CSV_DATA

SUPPORTED_AGENCIES = ['CTA', 'MBTA', 'WMATA']
# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "10" # MM in string format
YEAR = "2021" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
MODE_OPTION = ['shape_generation', 'metric_calculation', 'metric_aggregation']
DATA_OPTION = ['GTFS'] # GTFS, GTFS-AVL, GTFS-AVL-ODX

# SHAPE_GENERATION_OPTION = True # True/False: whether generate shapes
# LINK_SELECTION_OPTION = False # True/False: whether generate input for link selection map in ROVE
# METRIC_CAL_AGG_OPTION = False # True/False: whether run metric calculation and aggregation

# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__():
    # Check that the supplied agency is valid
    if AGENCY not in SUPPORTED_AGENCIES:
        logger.fatal(f'Agency "{AGENCY}" is not supported. Exiting...')
        quit()

    logger.info(f'Starting ROVE backend processes for \n--{AGENCY}, {MONTH}-{YEAR}. '\
                f'\n--Data Options: {DATA_OPTION}.'\
                f'\n--Date Modes: {DATE_TYPE}. \n--Modules: {MODE_OPTION}.')

    # -----parameters-----

    params = ROVE_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION)

    # ------data generation------
    DATA_GENERATION = True
    if DATA_GENERATION:
        if AGENCY == 'MBTA':
            bus_gtfs = MBTA_GTFS('gtfs', params, mode='bus')
        elif AGENCY == 'WMATA':
            bus_gtfs = WMATA_GTFS('gtfs', params, mode='bus')
        else:
            bus_gtfs = GTFS('gtfs', params, mode='bus')
        gtfs_records = bus_gtfs.records
        # gtfs_records.to_csv('mbta_03_2022_gtfs_records.csv')
        gtfs_records.to_csv('wmata_10_2021_gtfs_records.csv')

    else:
        r_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'local', 'wmata_10_2021_gtfs_records.csv'))
        gtfs_records = pd.read_csv(r_path, converters={"stop_pair": ast.literal_eval})
        specs = {
            'stop_id':'string',
            'route_id':'string',
            'trip_id':'string',
            'arrival_time':'int64',
            'trip_start_time':'int64',
            'departure_time':'int64',
            'stop_sequence':'int64',
            'hour': 'int64',
            'pattern': 'string'
        }
        cols = list(specs.keys())
        gtfs_records[cols] = gtfs_records[cols].astype(dtype=specs)
    # timepoints = CSV_DATA(in_path=params.input_paths['timepoints_inpath'])
    # test = CSV_DATA(in_path=params.input_paths['test_inpath'])

    # ------shape generation------ 
    SHAPE_GENERATION = True
    if SHAPE_GENERATION or read_shapes(params.output_paths['shapes']).empty:
        shapes = BaseShape(bus_gtfs.patterns_dict, outpath=params.output_paths['shapes']).shapes
    else:
        shapes = read_shapes(params.output_paths['shapes'])

    # ------metric calculation------
    # data_prep = DataPrep(gtfs)
    AVL_GENERATION = True
    if AVL_GENERATION:
        if AGENCY == 'MBTA':
            avl = MBTA_AVL('avl', params)
        else:
            avl = AVL('avl', params) 
        avl_records = avl.records
        avl_records.to_csv('mbta_03_2022_avl_records.csv')
    else:
        r_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'local', 'mbta_03_2022_avl_records.csv'))
        avl_records = pd.read_csv(r_path)
        specs = {
            'route':'string',
            'stop_id':'string',
            'stop_time':'int64',
            'stop_sequence': 'int64',
            'dwell_time': 'float64',
            'passenger_load': 'int64',
            'passenger_on': 'int64',
            'passenger_off': 'int64',
            'seat_capacity': 'int64',
            'trip_id':'string'
        }
        cols = list(specs.keys())
        avl_records[cols] = avl_records[cols].astype(dtype=specs)


    metrics = MetricCalculation(shapes, gtfs_records, avl_records)

    # ------metric aggregation------
    aggregation_method = {
        'median': 50,
        '90': 90
    }
    redValues = params.redValues

    # ------metric aggregation by time periods------
    METRIC_AGGREGATION_FULL = False
    if METRIC_AGGREGATION_FULL:
        time_dict:dict = params.config['time_periods']
        agg_metrics = {}
        for period_name, period in time_dict.items():
            start_time, end_time = period
            for agg_method, percentile in aggregation_method.items():

                agg = MetricAggregation(metrics.stop_metrics, metrics.tpbp_metrics, metrics.route_metrics, start_time, end_time, percentile, redValues)

                agg_metrics[f'{period_name}-segment-{agg_method}'] = agg.segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-{agg_method}'] = agg.corridors_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-route-{agg_method}'] = agg.routes_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-segment-timepoints-{agg_method}'] = agg.tpbp_segments_agg_metrics.to_json(orient='records')
                agg_metrics[f'{period_name}-corridor-timepoints-{agg_method}'] = agg.tpbp_corridors_agg_metrics.to_json(orient='records')

        pickle.dump(agg_metrics, open(params.output_paths['metric_calculation_aggre'], "wb"))

    # ------metric aggregation by 10-minute interval------
    METRIC_AGGREGATION_10_MIN = False
    aggregation_10min_methods = {
        'median': 50,
        '90_percentile': 90
    }
    if METRIC_AGGREGATION_10_MIN:
        
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

            for agg_method, percentile in aggregation_10min_methods.items():

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
