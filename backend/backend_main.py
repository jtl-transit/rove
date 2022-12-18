# import logging
from backend.data_class.wmata.wmata_avl import WMATA_AVL
from backend.data_class.wmata.wmata_parameters import WMATA_params
from backend.metrics.wmata.wmata_metric_aggregation import WMATA_Metric_Aggregation
from backend.metrics.wmata.wmata_metric_calculation import WMATA_Metric_Calculation
from data_class import GTFS, MBTA_GTFS, WMATA_GTFS, AVL, MBTA_AVL
from backend.shapes.base_shape import BaseShape
from logger.backend_logger import getLogger
from backend.metrics import Metric_Calculation, Metric_Aggregation
from data_class.rove_parameters import ROVE_params
from helper_functions import read_shapes

# from parameters.generic_csv_data import CSV_DATA

# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "10" # MM in string format
YEAR = "2021" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
DATA_OPTION = 'GTFS-AVL' # GTFS, GTFS-AVL

SHAPE_GENERATION = False # True/False: whether to generate shapes
METRIC_CAL_AGG = True # True/False: whether to run metric calculation and aggregation

# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__():

    logger.info(f'Starting ROVE backend processes for \n--{AGENCY}, {MONTH}-{YEAR}. '\
                f'\n--Data Option: {DATA_OPTION}.'\
                f'\n--Date Type: {DATE_TYPE}.')

    # -----store parameters-----
    if AGENCY == 'WMATA':
        params = WMATA_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION)
    else:
        params = ROVE_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION)

    # ------GTFS data generation------
    if AGENCY == 'MBTA':
        bus_gtfs = MBTA_GTFS(params, mode='bus')
    elif AGENCY == 'WMATA':
        bus_gtfs = WMATA_GTFS(params, mode='bus')
    else:
        bus_gtfs = GTFS(params, mode='bus')
    gtfs_records = bus_gtfs.records

    # ------shape generation------ 
    if SHAPE_GENERATION or read_shapes(params.output_paths['shapes']).empty:
        shapes = BaseShape(bus_gtfs.patterns_dict, params=params, check_signal=True).shapes
    else:
        shapes = read_shapes(params.output_paths['shapes'])

    # ------metric calculation and aggregation------
    if METRIC_CAL_AGG:

        # ------AVL data generation------
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

        if AGENCY == 'WMATA':
            metrics = WMATA_Metric_Calculation(shapes, gtfs_records, avl_records, params.data_option)
            agg = WMATA_Metric_Aggregation(metrics, params)
        else:
            metrics = Metric_Calculation(shapes, gtfs_records, avl_records, params.data_option)
            agg = Metric_Aggregation(metrics, params)

    logger.info(f'ROVE backend process completed')

if __name__ == "__main__":
    __main__()
