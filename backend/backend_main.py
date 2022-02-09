import logging
from logger.backend_logger import getLogger
# from shape_generation.shape_generation import shape_gen
from parameters.rove_parameters import ROVE_params
from parameters.gtfs import GTFS
from parameters.generic_csv_data import CSV_DATA
from shape_generation.shape_generation import Shape_Generation

SUPPORTED_AGENCIES = ['CTA', 'MBTA', 'WMATA']
# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "04" # MM in string format
YEAR = "2019" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
MODE_OPTION = ['shape_generation']
DATA_OPTION = ['config', 'GTFS', 'AVL', 'ODX'] # GTFS, GTFS-AVL, GTFS-AVL-ODX
ADDITIONAL_DATA = {
                'timepoints':f'data/{AGENCY}/agency-specific/timepoints_{AGENCY}_{MONTH}_{YEAR}.csv',
                'test':f'data/{AGENCY}/agency-specific/test.csv'
                }
# SHAPE_GENERATION_OPTION = True # True/False: whether generate shapes
# LINK_SELECTION_OPTION = False # True/False: whether generate input for link selection map in ROVE
# METRIC_CAL_AGG_OPTION = False # True/False: whether run metric calculation and aggregation
# --------------------------------END PARAMETERS--------------------------------------

logger = getLogger('backendLogger')

def __main__():
    # Check that the supplied agency is valid
    if AGENCY not in SUPPORTED_AGENCIES:
        logger.fatal(f'Agency "{AGENCY}" is not supported, exiting backend...')
        quit()

    logger.info(f'Starting ROVE backend processes for \n--{AGENCY}, {MONTH}-{YEAR}. '\
                f'\n--Data Options: {DATA_OPTION}. \n--Additional Data: {ADDITIONAL_DATA.keys()}. '\
                f'\n--Date Modes: {DATE_TYPE}. \n--Modules: {MODE_OPTION}.')

    # -----parameters-----
    logger.info(f'Generating parameters...')
    additional_params = {
        'additional_input_paths': ADDITIONAL_DATA,
        'additional_output_paths': {}
    }
    params = ROVE_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION, additional_params=additional_params)
    logger.info(f'parameters generated')

    # ------data generation------
    logger.info(f'Loading input data...')
    gtfs = GTFS(in_path=params.input_paths['gtfs_inpath'], additional_params=params)
    timepoints = CSV_DATA(in_path=params.input_paths['timepoints_inpath'])
    test = CSV_DATA(in_path=params.input_paths['test_inpath'])
    logger.info(f'All data loaded')

    # ------shape generation------
    # logger.info(f'Generating shapes...')
    # shape = Shape_Generation(gtfs)
    # logger.info(f'shapes generatd')
    # # if SHAPE_GENERATION_OPTION:
    # #     sgLogger = getLogger('shapeGenLogger')
    # #     shape_gen(sgLogger)
    # logger.info(f'backend processes completed')

if __name__ == "__main__":
    __main__()
