from .gtfs import GTFS
import pandas as pd

class MBTA_GTFS(GTFS):

    def __init__(self, alias, path, rove_params):
        super().__init__(alias, path, rove_params)

    def add_timepoints(self, records:pd.DataFrame):
        records['timepoint'] = ~records['checkpoint_id'].isnull()