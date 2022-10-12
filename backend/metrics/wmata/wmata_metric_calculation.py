from backend.metrics.metric_calculation import Metric_Calculation
import pandas as pd

class WMATA_Metric_Calculation(Metric_Calculation):

    def __init__(self, shapes: pd.DataFrame, gtfs_records: pd.DataFrame, avl_records: pd.DataFrame, data_option: str):
        super().__init__(shapes, gtfs_records, avl_records, data_option)

    def on_time_performance(self):
        super().on_time_performance(-2, 7)