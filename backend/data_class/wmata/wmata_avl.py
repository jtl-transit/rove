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
        data = convert_stop_ids('avl', data, 'stop_id', self.gtfs.validated_data['stops'])
        return 
