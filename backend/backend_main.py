# import logging
from shape_generation.gtfs_shape import GTFS_Shape
from logger.backend_logger import getLogger
# from shape_generation.shape_generation import shape_gen
from parameters.rove_parameters import ROVE_params
from parameters.gtfs import GTFS
# from parameters.generic_csv_data import CSV_DATA

SUPPORTED_AGENCIES = ['CTA', 'MBTA', 'WMATA']
# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "MBTA" # CTA, MBTA, WMATA
MONTH = "03" # MM in string format
YEAR = "2022" # YYYY in string format
DATE_TYPE = "Workday" # Workday, Saturday, Sunday
MODE_OPTION = ['shape_generation']
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
    
    PATHS = {
            'gtfs': f'data/{params.agency}/gtfs/GTFS{params.suffix}.zip',
            # 'avl': f'data/{self.agency}/avl/AVL{self.suffix}.csv',
            # 'odx': f'data/{self.agency}/odx/ODX{self.suffix}.csv',
        }

    # ------data generation------
    gtfs = GTFS('gtfs', PATHS['gtfs'], params)

    # timepoints = CSV_DATA(in_path=params.input_paths['timepoints_inpath'])
    # test = CSV_DATA(in_path=params.input_paths['test_inpath'])

    # ------shape generation------
    shape = GTFS_Shape(gtfs)

if __name__ == "__main__":
    __main__()
