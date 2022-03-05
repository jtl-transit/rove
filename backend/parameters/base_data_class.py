"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import logging
from parameters.rove_parameters import ROVE_params

logger = logging.getLogger("backendLogger")

class BaseData(metaclass=ABCMeta):
    """Base class for all data.
    """
    
    def __init__(self,
                alias,
                rove_params,
                required_data_set=None,
                optional_data_set=None
                ):
        """Instantiate a data class.
        This data object takes an alias name, a rove_params object, and an optional required_data_set,
        and creates a data class that stores raw and validated data.

        Args:
            alias (str): alias of the data, used as key when referencing input paths and logging
            rove_params (ROVE_params): a ROVE_params object that stores data needed throughout the backend
            required_data_set (set): a set of data that's required of this data class.
            optional_data_set (set, optional): a set of data that's optional. Defaults to None.

        Raises:
            TypeError: if the given ROVE_params is not valid
        """
        if not isinstance(rove_params, ROVE_params):
            raise TypeError(f'Not a valid instance of rove_params.')

        self._alias = alias

        self._rove_params = rove_params

        self._required_data_set = required_data_set or set()

        self._optional_data_set = optional_data_set or set()

        logger.info(f'loading {alias} data...')
        # Raw data (read-only) read from given paths stored in rove_params.
        self._raw_data = self.load_data()
        logger.info(f'{alias} data is loaded')

        
        logger.info(f'validating {alias} data...')
        # Validate data (read-only). Set as read-only to prevent user from setting the field manually.
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
    def required_data_set(self):

        return self._required_data_set

    @property
    def optional_data_set(self):

        return self._optional_data_set

    @property
    def validated_data(self):
        """Get validated data

        Returns:
            obj: validated data
        """
        return self._validated_data

    @abstractmethod
    def load_data(self):
        """Abstract method that reads input data from a file

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
    def load_csv_data(self, in_path):
        import pandas as pd

        try:
            in_path = self.rove_params.input_paths[self.alias]
        except KeyError as err:
            logger.fatal(f'{err}, could not find data for {self.alias}')

        try:
            data = pd.read_csv(in_path)
        except pd.errors.EmptyDataError as err:
            logger.warning(f'Data read from {in_path} is empty!')
            data = pd.DataFrame()
        return data
