from ..gtfs import GTFS
import pandas as pd

class MBTA_GTFS(GTFS):

    def __init__(self, alias, rove_params, mode='bus'):
        super().__init__(alias, rove_params, mode)

    def add_timepoints(self):
        records = self.records
        records['timepoint'] = ~records['checkpoint_id'].isnull()