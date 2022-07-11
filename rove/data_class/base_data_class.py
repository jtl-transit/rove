"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import logging

from data_class.rove_parameters import ROVE_params
from .helper_functions import check_is_file
import pandas as pd

logger = logging.getLogger("backendLogger")

class BaseData(metaclass=ABCMeta):
    """Base class for all data.
    """
    
    def __init__(self,
                alias:str,
                rove_params:ROVE_params
                ):
        """An abstract data class that stores raw and validated input data for ROVE.

        :param alias: alias of the data, used as key when referencing input paths and logging
        :type alias: str
        :param rove_params: a ROVE_params object that stores data needed throughout the backend.
        :type rove_params: ROVE_params
        """
        logger.info(f'creating a BaseData object for {alias}...')

        self.alias = alias

        self.rove_params = rove_params
        
        # Raw data (read-only) read from the given path.
        logger.info(f'loading {alias} data...')
        path = check_is_file(rove_params.input_paths[alias])
        self.raw_data = self.load_data(path)
        logger.info(f'{alias} data is loaded')
        
        # Validate data (read-only). Set as read-only to prevent user from setting the field manually.
        logger.info(f'validating {alias} data...')
        self.validated_data = self.validate_data()
        logger.info(f'{alias} data is validated')

        logger.info(f'BaseData object created for {alias}')

    @abstractmethod
    def load_data(self, path:str) -> object:
        """Abstract method that reads input data from a file

        :param path: path to the raw data
        :type path: str
        :return raw data
        :rtype a data object
        """
        pass

    @abstractmethod
    def validate_data(self) -> object:
        """Validate that the raw data conforms with a documented standard spec. 
        If the raw data doesn't conform, try to convert it to meet the standard.

        :return: validated data
        :rtype: a data object
        """
        pass
