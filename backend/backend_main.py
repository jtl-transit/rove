from logger.backend_logger import getLogger
from shape_generation.shape_generation import shape_gen_test

SUPPORTED_AGENCIES = ['CTA', 'MBTA', 'WMATA']
# -----------------------------------PARAMETERS--------------------------------------
AGENCY = "WMATAE" # CTA, MBTA, WMATA
MONTH = "10" # MM in string format
YEAR = "2021" # YYYY in string format
DATE_OPTION = "Workday" # Workday, Saturday, Sunday
MODE_OPTION = "GTFS" # GTFS, GTFS-AVL, GTFS-AVL-ODX
SHAPE_GENERATION_OPTION = True # True/False: whether generate shapes
LINK_SELECTION_OPTION = False # True/False: whether generate input for link selection map in ROVE
METRIC_CAL_AGG_OPTION = False # True/False: whether run metric calculation and aggregation
# --------------------------------END PARAMETERS--------------------------------------

backendLogger = getLogger('backendLogger')

def run_backend():
    if AGENCY not in SUPPORTED_AGENCIES:
        backendLogger.error(f'Agency "{AGENCY}" is not supported, exiting backend...')
        quit()
    backendLogger.info(f'Starting ROVE backend processes for {AGENCY}')
    # params = 
    if SHAPE_GENERATION_OPTION:
        sgLogger = getLogger('shapeGenLogger')
        shape_gen_test(sgLogger)
    

run_backend()


# print(locals())
# print(globals())
