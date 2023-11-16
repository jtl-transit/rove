from backend.metrics.metric_calculation import Metric_Calculation
from backend.data_class.rove_parameters import ROVE_params
from backend.helper_functions import check_is_file
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point
import logging

logger = logging.getLogger("backendLogger")

class WMATA_Metric_Calculation(Metric_Calculation):

    def __init__(self, shapes: pd.DataFrame, gtfs_records: pd.DataFrame, avl_records: pd.DataFrame, params: ROVE_params, gtfs_stop_coords: pd.DataFrame):
        super().__init__(shapes, gtfs_records, avl_records, params)
        self.gtfs_stop_coords = gtfs_stop_coords
        self.rove_params:ROVE_params = params
        self.flag_if_in_EFC()

    def on_time_performance(self):
        super().on_time_performance(-2, 7)

    def flag_if_in_EFC(self):
        logger.info(f'flagging stop pairs inside EFC or not')

        path = check_is_file(self.rove_params.input_paths['efc_merged'])

        # Read the merged equity focused community shape file
        EFC_shape = gpd.read_file(path)
        EFC_polygon = EFC_shape.loc[0,'geometry']

        # Create a lookup dictionary, keys are stop_ids and values are 1 (in EFC) or 2 (outside EFC)
        self.gtfs_stop_coords['coords'] = list(zip(self.gtfs_stop_coords.stop_lon, self.gtfs_stop_coords.stop_lat))
        self.gtfs_stop_coords['in_EFC'] = self.gtfs_stop_coords['coords'].apply(lambda x: int(EFC_polygon.contains(Point(x))))
        stop_in_efc_lookup = self.gtfs_stop_coords.set_index('stop_code')['in_EFC'].to_dict()
        
        # Flag the stop pair as 1 if at least one of the two stops is inside EFC
        self.gtfs_stop_metrics['in_efc_1'] = self.gtfs_stop_metrics['stop_id'].map(stop_in_efc_lookup)
        self.gtfs_stop_metrics['in_efc_2'] = self.gtfs_stop_metrics['next_stop'].map(stop_in_efc_lookup)
        self.gtfs_stop_metrics['in_efc'] = self.gtfs_stop_metrics[['in_efc_1', 'in_efc_2']].max(axis = 1)
        self.gtfs_stop_metrics = self.gtfs_stop_metrics.drop(columns=['in_efc_1', 'in_efc_2'])
        