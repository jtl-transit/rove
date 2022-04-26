"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import logging
from .helper_functions import check_is_file

logger = logging.getLogger("backendLogger")

class BaseData(metaclass=ABCMeta):
    """Base class for all data.
    """
    
    def __init__(self,
                alias,
                path,
                rove_params=None
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
        logger.info(f'loading {alias} data...')

        self._alias = alias

        if rove_params is not None:
            try:
                from .rove_parameters import ROVE_params
                if not isinstance(rove_params, ROVE_params):
                    raise TypeError(f'the given rove_params is not a valid ROVE_params object.')
                else:
                    self._rove_params = rove_params
            except ImportError as err:
                logger.fatal(f'{err}: cannot import ROVE_params for data type {alias} due to possible circular reference.')
                quit()
        else:
            self._rove_params = None

        # Raw data (read-only) read from given paths stored in rove_params.
        path = check_is_file(path)
        self._raw_data = self.load_data(path)
        logger.info(f'{alias} data is loaded')
        
        # Validate data (read-only). Set as read-only to prevent user from setting the field manually.
        logger.info(f'validating {alias} data...')
        self._validated_data = self.validate_data()
        logger.info(f'{alias} data is validated')
    
    @property
    def alias(self):

        return self._alias

    @property
    def rove_params(self):

        return self._rove_params
    
    @property
    def raw_data(self):
        """Get raw data.

        Returns:
            obj: raw data
        """
        return self._raw_data

    @property
    def validated_data(self):
        """Get validated data

        Returns:
            obj: validated data
        """
        return self._validated_data

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

    # helpful functions
    def load_csv_data(self, path:str):
        """Read in csv data and return a dataframe

        Args:
            path (str): path to the csv file

        Returns:
            DataFrame: dataframe read from the csv file
        """
        import pandas as pd

        try:
            data = pd.read_csv(path)
        except pd.errors.EmptyDataError as err:
            logger.warning(f'{err}: Data read from {path} is empty!')
            data = pd.DataFrame()
        return data
