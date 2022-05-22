from abc import ABCMeta, abstractmethod
from typing import Dict
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
from .base_data_class import BaseData
import json

REQUIRED_DATA_SET = {'route_type', 'workalendarPath', 'time_periods', 'speed_range', 'percentile_list'}
OPTIONAL_DATA_SET = {}

class Config(BaseData):

    def __init__(self, alias, path, rove_params=None):
        super().__init__(alias, path, rove_params)

    def load_data(self, path: str)->Dict[str, object]:
        with open(path, 'r') as f:
            data = json.load(f)

            if not isinstance(data, dict):
                raise TypeError(f'config file could not be decoded to a valid dict and cannot be processed.')
        return data

    # TODO: add more data enforecement for config data: e.g. enforce time periods not empty?
    def validate_data(self):
        data = self.raw_data

        config_keys = set(data.keys())
        if not REQUIRED_DATA_SET.issubset(config_keys):
            missing_data = REQUIRED_DATA_SET - config_keys
            raise ValueError(f'{missing_data} is missing from config data. Please double check the config file.')

        return data