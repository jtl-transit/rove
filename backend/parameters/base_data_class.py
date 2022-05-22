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
        logger.info(f'creating a BaseData object for {alias}...')

        self._alias = alias

        if rove_params is not None:
            from .rove_parameters import ROVE_params
            if not isinstance(rove_params, ROVE_params):
                raise TypeError(f'Not a valid ROVE_params object.')
            else:
                self._rove_params = rove_params
        else:
            self._rove_params = None

        # Raw data (read-only) read from the given path.
        logger.info(f'loading {alias} data...')
        path = check_is_file(path)
        self._raw_data = self.load_data(path)
        logger.info(f'{alias} data is loaded')
        
        # Validate data (read-only). Set as read-only to prevent user from setting the field manually.
        logger.info(f'validating {alias} data...')
        self._validated_data = self.validate_data()
        logger.info(f'{alias} data is validated')

        logger.info(f'BaseData object creatd for {alias}')
    
    @property
    def alias(self):
        return self._alias

    @property
    def rove_params(self):
        return self._rove_params
    
    @property
    def raw_data(self):
        return self._raw_data

    @property
    def validated_data(self):
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
    def load_csv_in_dataframe(self, path:str):
        """Read in csv data and return a dataframe

        Args:
            path (str): path to the csv file

        Returns:
            DataFrame: dataframe read from the csv file
        
        Raises:
            ValueError: the given file path does not end with .csv
        """
        import pandas as pd
        
        if not path.endswith('.csv'):
            raise ValueError(f'Not a valid csv file.')

        try:
            data = pd.read_csv(path)
        except pd.errors.EmptyDataError as err:
            logger.warning(f'{err}: Data read from {path} is empty!')
            data = pd.DataFrame()
        return data
