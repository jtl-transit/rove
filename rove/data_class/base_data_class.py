"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import logging

from parameters.rove_parameters import ROVE_params
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
        """Instantiate a data class.
        Creates a data class that stores raw and validated data.

        Args:
            alias (str): alias of the data, used as key when referencing input paths and logging
            path (str): path to the raw data
            rove_params (ROVE_params, optional): a ROVE_params object that stores data needed throughout the backend. 
                                                Defaults to None. This argument should not be specified when creating
                                                a data class that will be stored in rove_params (e.g. config), to avoid 
                                                circular reference.

        Raises:
            TypeError: if the given ROVE_params is not valid
        """
        logger.info(f'creating a BaseData object for {alias}...')

        self.alias = alias

        # if rove_params is not None:
        #     from .rove_parameters import ROVE_params
        #     if not isinstance(rove_params, ROVE_params):
        #         raise TypeError(f'Not a valid ROVE_params object.')
        #     else:
        #         self.rove_params = rove_params
        # else:
        #     self.rove_params = None
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
    def load_data(self, path:str):
        """Abstract method that reads input data from a file

        Args:
            path (str): path to the raw data
        
        Returns:
            obj: raw data
        """
        pass

    @abstractmethod
    def validate_data(self):
        """Validate that the raw data conforms with a documented standard spec. 
        If the raw data doesn't conform, try to convert it to meet the standard.

        Returns:
            obj: validated data
        """
        pass
