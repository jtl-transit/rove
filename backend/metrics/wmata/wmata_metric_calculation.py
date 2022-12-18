from backend.metrics.metric_calculation import Metric_Calculation
from backend.data_class.rove_parameters import ROVE_params
import pandas as pd

class WMATA_Metric_Calculation(Metric_Calculation):

    def __init__(self, shapes: pd.DataFrame, gtfs_records: pd.DataFrame, avl_records: pd.DataFrame, params: ROVE_params):
        super().__init__(shapes, gtfs_records, avl_records, params)

    def on_time_performance(self):
        super().on_time_performance(-2, 7)