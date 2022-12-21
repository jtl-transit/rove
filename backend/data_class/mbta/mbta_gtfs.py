from ..gtfs import GTFS
import pandas as pd

class MBTA_GTFS(GTFS):

    def __init__(self, rove_params, mode='bus', shape_gen=True):
        super().__init__(rove_params, mode, shape_gen)

    def add_timepoints(self):
        records = self.records
        records['timepoint'] = ~records['checkpoint_id'].isnull()