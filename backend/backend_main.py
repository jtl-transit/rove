import logging
from logger.backend_logger import getLogger
# from shape_generation.shape_generation import shape_gen
from parameters.rove_parameters import ROVE_params
from parameters.data_classes import GTFS
from shape_generation.shape_generation import Shape_Generation

SUPPORTED_AGENCIES = ['CTA', 'MBTA', 'WMATA']
# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATA" # CTA, MBTA, WMATA
MONTH = "04" # MM in string format
YEAR = "2019" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
MODE_OPTION = ['shape_generation']
DATA_OPTION = ['GTFS', 'AVL', 'ODX'] # GTFS, GTFS-AVL, GTFS-AVL-ODX
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

    logger.info(f'Starting ROVE backend processes for {AGENCY}, {MONTH}-{YEAR}. \
                    Data: {DATA_OPTION}. Mode: {DATE_TYPE}, {MODE_OPTION}.')

    # -----parameters-----
    logger.info(f'Generating parameters...')
    params = ROVE_params(AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION)
    logger.info(f'parameters generated')

    # ------data generation------
    logger.info(f'Loading input data...')
    gtfs = GTFS(params.input_paths['gtfs_inpath'], params.sample_date, params.config['route_type'])
    logger.info(f'data loaded')

    # ------shape generation------
    logger.info(f'Generating shapes...')
    shape = Shape_Generation(gtfs)
    logger.info(f'shapes generatd')
    # if SHAPE_GENERATION_OPTION:
    #     sgLogger = getLogger('shapeGenLogger')
    #     shape_gen(sgLogger)
    logger.info(f'backend processes completed')

if __name__ == "__main__":
    __main__()
