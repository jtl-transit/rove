from .avl import AVL
import pandas as pd

class MBTA_AVL(AVL):

    def __init__(self, alias, rove_params):
        super().__init__(alias, rove_params)

    def convert_dwell_time(self, data:pd.Series):
        
        dwell_time = (pd.to_timedelta(data).dt.total_seconds()) / 60
        return dwell_time.round(1)