from backend.data_class.gtfs import GTFS
from backend.data_class.rove_parameters import ROVE_params
from backend.helper_functions import convert_stop_ids
from ..avl import AVL
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("backendLogger")

class WMATA_AVL(AVL):

    def __init__(self, rove_params: ROVE_params, bus_gtfs: GTFS):
        super().__init__(rove_params, bus_gtfs)

    def validate_data(self, gtfs: GTFS) -> pd.DataFrame:
        data = super().validate_data(gtfs)
        
        gtfs_stop_ids_set = set(gtfs.validated_data['stops']['stop_id'])
        gtfs_trip_ids_set = set(gtfs.validated_data['trips']['trip_id'])

        avl_stop_ids_set = set(data['stop_id'])
        avl_trip_ids_set = set(data['trip_id'])

        matching_stop_ids = gtfs_stop_ids_set & avl_stop_ids_set
        matching_trip_ids = gtfs_trip_ids_set & avl_trip_ids_set
        logger.debug(f'count of AVL stop IDs: {len(avl_stop_ids_set)}, trip IDs: {len(avl_trip_ids_set)}.')
        logger.debug(f'count of matching stop IDs: {len(matching_stop_ids)}, matching trip IDs: {len(matching_trip_ids)}.')

        data = convert_stop_ids('avl', data, 'stop_id', self.gtfs.validated_data['stops'])

        return data
