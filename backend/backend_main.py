# import logging
from backend.data_class.wmata.wmata_avl import WMATA_AVL
from data_class import GTFS, MBTA_GTFS, WMATA_GTFS, AVL, MBTA_AVL
from backend.shapes.base_shape import BaseShape
from logger.backend_logger import getLogger
from backend.metrics import Metric_Calculation, Metric_Aggregation, WMATA_Metric_Calculation, WMATA_Metric_Aggregation
from data_class.rove_parameters import ROVE_params
from helper_functions import read_shapes, write_to_frontend_config, string_is_date, string_is_month
import argparse
import sys

# from parameters.generic_csv_data import CSV_DATA

# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "Q2" # MM in string format
YEAR = "2022" # YYYY in string format
START_DATE = '2022-09-11' # YYYY-MM-DD
END_DATE = '2022-12-11' # YYYY-MM-DD
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
DATA_OPTION = 'GTFS' # GTFS, GTFS-AVL

SHAPE_GENERATION = False # True/False: whether to generate shapes
METRIC_CAL_AGG = True # True/False: whether to run metric calculation and aggregation

# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__(args):
    """Main method of ROVE backend - run this file to generate shapes and metrics in the backend. If the file is run in the command line, 
    selected arguments will be required (see details below). Otherwise, if the file is run in an IDE, then use the above variables to change 
    selection of agency, dates, date type, data option, etc.

    :param args: command line arguments needed for the backend.
    "-a" or "--agency": REQUIRED, name of the agency to be analyzed, must be a string with no space. E.g. "WMATA", "MBTA", "MTA_Manhattan".
    "-m" or "--month": REQUIRED, name of the month (or months) to be analyzed, must be a string with not space. E.g. if supplied "01", the backend
        will calculate metrics for the January of the specified year (if no start date end date is given); alternatively, if supplied a non-numeric 
        string, e.g. "Q2", then a "start date" and "end date" must be supplied so the backend will know which time window to calculate the metrics for.
    "-y" or "--year": REQUIRED, name of the year to be analyzed, must be a 4-character string, e.g. "2022", "2023".
    "-sd" or "--start_date": Optionally required, the start date ("YYYY-MM-DD") of the analysis time window. Required only when the given "--month" 
        is not numeric. Can also be used to select a smaller time window than the whole month, when the given "--month" is numeric. E.g. "2022-09-05".
    "-ed" or "--end_date": Optionally required, the end date ("YYYY-MM-DD") of the analysis time window. Used in the same way as "--start_date". 
        E.g. "2022-09-20".
    "-dt" or "--date_type": type of dates the backend analyzes. Must be one of "Workday" (default), "Saturday", "Sunday".
    "-do" or "--data_option": type of analysis. Must be one of "GTFS" (default) or "GTFS-AVL".
    "-sg" or "--shape_gen": perform shape generation (default).
    "-no-sg" or "--no_shape_gen": don't perform shape generation. 
    "-ma" or "--metric_agg": perform metrics aggregation (default).
    "-no-ma" or "--no_metric_agg": don't perform metrics aggregation.
    "-sig" or "--check_signal": check each shape segment and see if it intersects with a traffic signal. This operation may take some time.
    "-no-sig" or "--no_check_signal": don't check whether shape segments intersect with traffic signals (default).
    :type args: _type_
    """
    if len(args) > 0:
        parser = argparse.ArgumentParser(description="Do something.")
        parser.add_argument("-a", "--agency", type=str, required=True)
        parser.add_argument("-m", "--month", type=str, required=True)
        parser.add_argument("-y", "--year", type=str, required=True)
        parser.add_argument("-sd", "--start_date", type=str, required=False)
        parser.add_argument("-ed", "--end_date", type=str, required=False)
        parser.add_argument("-dt", "--date_type", type=str, default='Workday', required=False)
        parser.add_argument("-do", "--data_option", type=str, default='GTFS', required=False)
        parser.add_argument("-sg", "--shape_gen", action='store_true', required=False)
        parser.add_argument("-no-sg", "--no_shape_gen", dest='shape_gen', action='store_false', required=False)
        parser.set_defaults(shape_gen=True)
        parser.add_argument("-ma", "--metric_agg", action='store_true', required=False)
        parser.add_argument("-no-ma", "--no_metric_agg", dest='metric_agg', action='store_false', required=False)
        parser.set_defaults(metric_agg=True)
        parser.add_argument("-sig", "--check_signal", action='store_true', required=False)
        parser.add_argument("-no-sig", "--no_check_signal", dest='check_signal', action='store_false', required=False)
        parser.set_defaults(check_signal=False)
        args = parser.parse_args(args)

        agency = args.agency
        month = args.month
        year = args.year
        start_date = args.start_date
        end_date = args.end_date
        date_type = args.date_type
        data_option = args.data_option

        shape_gen = args.shape_gen
        metric_calc_agg = args.metric_agg
        check_signal = args.check_signal

        if not string_is_month(month) and (not string_is_date(start_date) or not string_is_date(end_date)):
            parser.error(f'-sd (--start_date) and -ed (--end_date) must be valid strig dates (YYYY--MM-DD) '\
                            +f'when -m (--month) is not a valid numeric string between 1 and 12 (received {month}).')

    else:
        agency = AGENCY
        month = MONTH
        year = YEAR
        start_date = START_DATE
        end_date = END_DATE
        date_type = DATE_TYPE
        data_option = DATA_OPTION

        shape_gen = SHAPE_GENERATION
        metric_calc_agg = METRIC_CAL_AGG
        check_signal = False

        if not string_is_month(month) and (not string_is_date(start_date) or not string_is_date(end_date)):
            logger.fatal(f'START_DATE and END_DATE must be valid strig dates (YYYY--MM-DD) '\
                            +f'when MONTH is not a valid numeric string between 1 and 12 (received {month}).')
            quit()

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
    params = ROVE_params(agency, month, year, date_type, data_option, input_paths, output_paths, start_date, end_date)

    # ------GTFS data generation------
    if agency == 'MBTA':
        bus_gtfs = MBTA_GTFS(params, mode='bus', shape_gen=shape_gen)
    elif agency == 'WMATA':
        bus_gtfs = WMATA_GTFS(params, mode='bus', shape_gen=shape_gen)
    else:
        bus_gtfs = GTFS(params, mode='bus', shape_gen=shape_gen)
    gtfs_records = bus_gtfs.records


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
            agg = WMATA_Metric_Aggregation(metrics, params)
        else:
            metrics = Metric_Calculation(shapes, gtfs_records, avl_records, params)
            agg = Metric_Aggregation(metrics, params)

        write_to_frontend_config(agg.metrics_names, params.frontend_config, input_paths['frontend_config'])
    logger.info(f'ROVE backend process completed')

if __name__ == "__main__":
    
    __main__(sys.argv[1:])
