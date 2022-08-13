from backend.data_class.gtfs import GTFS
from backend.data_class.rove_parameters import ROVE_params
from ..avl import AVL
import pandas as pd


class MBTA_AVL(AVL):

    def __init__(self, rove_params: ROVE_params, bus_gtfs: GTFS):
        super().__init__(rove_params, bus_gtfs)

    def convert_dwell_time(self, data:pd.Series):
        
        dwell_time = (pd.to_timedelta(data).dt.total_seconds())
        return dwell_time.round(1)