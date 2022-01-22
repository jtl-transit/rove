"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback
from .data_class import Data

DEFAULT_DATA_SET = {'agency', 'stops', 'routes', 'trips', 'stop_times'}

class GTFS(Data):

    def __init__(self,
                in_path='',
                raw_data=None,
                out_path='',
                required_data_set = DEFAULT_DATA_SET,
                additional_params=None):
        super().__init__(in_path=in_path, raw_data=raw_data, out_path=out_path, 
                        required_data_set = required_data_set, additional_params=additional_params)

    def load_data_from_file(self, in_path):
        params = self.additional_params
        # if 
        data = {}
        service_id_list = ptg.read_service_ids_by_date(in_path)[params.sample_date]

        view = {'routes.txt': {'route_type': params.config['route_type']}, 'trips.txt': {'service_id': service_id_list}}
        feed = ptg.load_feed(in_path, view)

        data['service_id_list'] = service_id_list

        for t in self.required_data_set:
            data[t] = getattr(feed, t)

        return data
    
    def validate_data(self):
        pass

    def save_data(self, out_path):

        pass