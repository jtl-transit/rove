
from ast import alias
from .base_data_class import BaseData
import logging
import traceback
import pandas as pd

logger = logging.getLogger("backendLogger")

class CSV_DATA(BaseData):

    def __init__(self, alias, rove_params, required_data_set=None):
        super().__init__(alias, rove_params, required_data_set)
    
    def load_data(self, in_path):
        try:
            in_path = self.rove_params.input_paths[self.alias]
        except KeyError as err:
            logger.fatal(f'{err}, could not find data for {self.alias}')

        try:
            data = pd.read_csv(in_path)
        except pd.errors.EmptyDataError as err:
            # logger.exception(traceback.format_exc())
            logger.warning(f'Data read from {in_path} is empty!')
            data = pd.DataFrame()
        return data

    def validate_data(self):
        pass