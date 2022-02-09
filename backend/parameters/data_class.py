"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import logging
from parameters.rove_parameters import ROVE_params

logger = logging.getLogger("backendLogger")

class Data(metaclass=ABCMeta):
    """Base class for all data.
    """
    
    def __init__(self,
                alias,
                rove_params,
                required_data_set=None,
                ):
        """Instantiate a data class.
        This data object takes an input data, and stores validated and optionally processed data.
        The input data can either be from a file, or from another object, but not both.
        If an out_path is specified, then the validated or processed data can be stored to an output folder.

        Args:
            in_path (str, optional): path to the input file of this data. Defaults to ''.
            raw_data (obj): a raw data object used as an input source for this data.
            out_path (str, optional): path to the output file of this data. Defaults to ''.
        """
        if not isinstance(rove_params, ROVE_params):
            raise TypeError(f'Not a valid instance of rove_params.')

        self._alias = alias

        self._rove_params = rove_params

        self._required_data_set = required_data_set or set()

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

    
