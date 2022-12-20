# import logging
from backend.data_class.wmata.wmata_avl import WMATA_AVL
from data_class import GTFS, MBTA_GTFS, WMATA_GTFS, AVL, MBTA_AVL
from backend.shapes.base_shape import BaseShape
from logger.backend_logger import getLogger
from backend.metrics import Metric_Calculation, Metric_Aggregation, WMATA_Metric_Calculation
from data_class.rove_parameters import ROVE_params
from helper_functions import read_shapes, write_metrics_to_frontend_config
import argparse
import sys

# from parameters.generic_csv_data import CSV_DATA

# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "MTA_Manhattan" # CTA, MBTA, WMATA
MONTH = "09" # MM in string format
YEAR = "2022" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
DATA_OPTION = 'GTFS' # GTFS, GTFS-AVL

SHAPE_GENERATION = False # True/False: whether to generate shapes
METRIC_CAL_AGG = True # True/False: whether to run metric calculation and aggregation

# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__(args):
    if len(args) > 0:
        parser = argparse.ArgumentParser(description="Do something.")
        parser.add_argument("-a", "--agency", type=str, required=True)
        parser.add_argument("-m", "--month", type=str, required=True)
        parser.add_argument("-y", "--year", type=str, required=True)
        parser.add_argument("-dt", "--date_type", type=str, default='Workday', required=False)
        parser.add_argument("-do", "--data_option", type=str, default='GTFS', required=False)
        parser.add_argument("-sg", "--shape_gen", action='store_true', required=False)
        parser.add_argument("-no-sg", "--no_shape_gen", dest='shape_gen', action='store_false', required=False)
        parser.set_defaults(shape_gen=True)
        parser.add_argument("-ma", "--metric_agg", action='store_true', required=False)
        parser.add_argument("-no-ma", "--no-metric_agg", dest='metric_agg', action='store_false', required=False)
        parser.set_defaults(metric_agg=True)
        parser.add_argument("-sig", "--check_signal", action='store_true', required=False)
        parser.add_argument("-no-sig", "--no-check_signal", dest='check_signal', action='store_false', required=False)
        parser.set_defaults(check_signal=False)
        args = parser.parse_args(args)

        agency = args.agency
        month = args.month
        year = args.year
        date_type = args.date_type
        data_option = args.data_option

        shape_gen = args.shape_gen
        metric_calc_agg = args.metric_agg
        check_signal = args.check_signal
    else:
        agency = AGENCY
        month = MONTH
        year = YEAR
        date_type = DATE_TYPE
        data_option = DATA_OPTION

        shape_gen = SHAPE_GENERATION
        metric_calc_agg = METRIC_CAL_AGG
        check_signal = False

    logger.info(f'Starting ROVE backend processes for {agency}, {month}-{year}, {date_type}, {data_option} mode. ' + \
                f'Shape Generation: {shape_gen}. Metric Calculation and Aggregation: {metric_calc_agg}.')
    
    suffix:str = f'_{agency}_{month}_{year}'

    input_paths = {
            'gtfs': f'data/{agency}/gtfs/GTFS{suffix}.zip',
            'avl': f'data/{agency}/avl/AVL{suffix}.csv',
            'backend_config': f'data/{agency}/config.json',
            'frontend_config': f'frontend/static/inputs/{agency}/config.json',
            'shapes': f'frontend/static/inputs/{agency}/shapes/bus-shapes{suffix}.json',
            'signals': f'frontend/static/inputs/{agency}/backgroundlayers/{agency.lower()}_traffic_signals.geojson',
            'timepoint': f'data/{agency}/agency-specific/timepoints{suffix}.csv', 
            'fsn':  f'data/{agency}/agency-specific/dim_fsn_routes.csv'
        }
    
    output_paths = {
            'shapes': f'frontend/static/inputs/{agency}/shapes/bus-shapes{suffix}.json',
            'timepoints': f'frontend/static/inputs/{agency}/timepoints/timepoints{suffix}.json',
            'stop_name_lookup': f'frontend/static/inputs/{agency}/lookup/lookup{suffix}.json',
            'metric_calculation_aggre': f'data/{agency}/metrics/METRICS{suffix}.p',
            'metric_calculation_aggre_10min': f'data/{agency}/metrics/METRICS_10MIN{suffix}.p'
        }

    # -----store parameters-----
    params = ROVE_params(agency, month, year, date_type, data_option, input_paths, output_paths)

    # ------GTFS data generation------
    if agency == 'MBTA':
        bus_gtfs = MBTA_GTFS(params, mode='bus')
    # elif agency == 'WMATA':
    #     bus_gtfs = WMATA_GTFS(params, mode='bus')
    else:
        bus_gtfs = GTFS(params, mode='bus')
    gtfs_records = bus_gtfs.records

    # AVL
    if 'AVL' in DATA_OPTION:
        if AGENCY == 'MBTA':
            avl = MBTA_AVL(params, bus_gtfs)
        elif AGENCY == 'WMATA':
            avl = WMATA_AVL(params, bus_gtfs)
        else:
            avl = AVL(params, bus_gtfs) 
        avl_records = avl.records
    else:
        avl_records = None

    # ------shape generation------ 
    if shape_gen or read_shapes(params.output_paths['shapes']).empty:
        shapes = BaseShape(bus_gtfs.patterns_dict, params=params, check_signal=check_signal).shapes
    else:
        shapes = read_shapes(params.output_paths['shapes'])

    # ------metric calculation and aggregation------
    if metric_calc_agg:

        # ------AVL data generation------
        if 'AVL' in data_option:
            if agency == 'MBTA':
                avl = MBTA_AVL(params, bus_gtfs)
            elif agency == 'WMATA':
                avl = WMATA_AVL(params, bus_gtfs)
            else:
                avl = AVL(params, bus_gtfs) 
            avl_records = avl.records
        else:
            avl_records = None

        if agency == 'WMATA':
            metrics = WMATA_Metric_Calculation(shapes, gtfs_records, avl_records, params)
            # agg = WMATA_Metric_Aggregation(metrics, params)
        else:
            metrics = Metric_Calculation(shapes, gtfs_records, avl_records, params)
            agg = Metric_Aggregation(metrics, params)

        write_metrics_to_frontend_config(agg.metrics_names, input_paths['frontend_config'])
    logger.info(f'ROVE backend process completed')

if __name__ == "__main__":
    __main__(sys.argv[1:])
