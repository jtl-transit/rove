import pandas as pd
import partridge as ptg
import json
from .helper_functions import day_list_generation
import logging
logger = logging.getLogger("paramLogger")

class ROVE_params():
    
    def __init__(self, AGENCY, MONTH, YEAR, DATE_OPTION, MODE_OPTION):
        logger.info(f'Constructing parameters for {AGENCY}. Mode: {DATE_OPTION}, {MODE_OPTION}.')
        self.AGENCY = AGENCY
        BUS_TYPE = '3'
        STATIC_PATH = f'bustool/static/inputs/{self.AGENCY}'
        DATA_PATH = f'data/{self.AGENCY}'

        with open(f'backend\parameters\{AGENCY}_param_config.json', 'r') as f:
            config = json.load(f)
        self.date_list = day_list_generation(MONTH, YEAR, DATE_OPTION, config['workalendarPath'])
        
        self.MODE_OPTION = MODE_OPTION
        self.suffix = f'_{AGENCY}_' + MONTH + "_" + YEAR

        if MODE_OPTION == "GTFS":
            self.AVL_DATA = pd.DataFrame([])
            self.ODX_DATA = pd.DataFrame([])
        
        # GTFS Feed
        self.GTFS_IN_PATH = f"{DATA_PATH}/gtfs/GTFS{self.suffix}.zip" # GTFS feed
        # SAMPLE_DATE = self.date_list[max(5, len(self.date_list)-1)]
        # self.SERVICE_ID_LIST = ptg.read_service_ids_by_date(self.GTFS_IN_PATH)[SAMPLE_DATE]
        self.SERVICE_ID_LIST = ptg.read_service_ids_by_date(self.GTFS_IN_PATH)
        VIEW = {'routes.txt': {'route_type': [BUS_TYPE]}, 'trips.txt': {'service_id': set(self.SERVICE_ID_LIST.values())}}
        self.GTFS_FEED = ptg.load_feed(self.GTFS_IN_PATH, VIEW)
        logger.info(f'GTFS feed retrieved.')

        self.GTFS_STOP_TIME = self.GTFS_FEED.stop_times # GTFS stop time
        self.GTFS_TRIP = self.GTFS_FEED.trips # GTFS trip
        self.GTFS_DATA = self.GTFS_STOP_TIME.merge(self.GTFS_TRIP, on='trip_id', how ='left')
        logger.info(f'GTFS data retrieved, size: {self.GTFS_DATA.shape}.')
        self.SHAPES = None

        # pre-defined values
        self.time_periods = config['time_periods']
        self.SPEED_RANGE = config['speed_range']  # mph
        self.percentile_list = config['percentile_list']

        # input paths
        self.signal_inpath = f'{STATIC_PATH}/backgroundlayers/{self.AGENCY}_traffic_signals.geojson'
        self.odx_data_inpath = f'{DATA_PATH}/odx/odx{self.suffix}.csv'

        # output paths
        self.shapes_output_path = f'{STATIC_PATH}/shapes/bus-shapes{self.suffix}.json'
        self.lookup_output_path = f'{STATIC_PATH}/lookup/lookup{self.suffix}.json'

        self.segment_shapes_output_path = f'{STATIC_PATH}/shapes/segment_shapes{self.suffix}.json'
        self.tp_shapes_output_path = f'{STATIC_PATH}/shapes/timepoint_shapes{self.suffix}.json'
        self.segstop_shapes_output_path = f'{STATIC_PATH}/stops/segstopdata{self.suffix}.json'
        self.tpstop_shapes_output_path = f'{STATIC_PATH}/stops/tpstopdata{self.suffix}.json'
        
        self.seg_data_outpath = f'{DATA_PATH}/selectlink/sl-seg-data{self.suffix}.json'
        self.tp_data_outpath = f'{DATA_PATH}/selectlink/sl-tp-data{self.suffix}.json'

        self.metric_calculation_timepoints_outpath = f'{STATIC_PATH}/timepoints/timepoints{self.suffix}.json'
        self.metric_calculation_peak_outpath = f'{STATIC_PATH}/peak/peak{self.suffix}.json'
        self.metric_calculation_aggre_outpath = f'{DATA_PATH}/metrics/METRICS{self.suffix}.p'
        self.metric_calculation_aggre_10min_outpath = f'{DATA_PATH}/metrics/METRICS_10MIN{self.suffix}.p'