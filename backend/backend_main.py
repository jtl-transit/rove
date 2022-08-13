# import logging
from backend.data_class.wmata.wmata_avl import WMATA_AVL
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
            avl = MBTA_AVL(params, bus_gtfs)
        elif AGENCY == 'WMATA':
            avl = WMATA_AVL(params, bus_gtfs)
        else:
            avl = AVL(params, bus_gtfs) 
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
        metrics = MetricCalculation(shapes, gtfs_records, avl_records, params.data_option)
        agg = MetricAggregation(metrics.stop_metrics, metrics.tpbp_metrics, metrics.route_metrics, params)

    logger.info(f'ROVE backend process completed')

if __name__ == "__main__":
    __main__()
