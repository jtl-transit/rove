"""Abstract class for all data classes
"""

from abc import ABCMeta, abstractmethod
import os
import shutil
from copy import deepcopy
import logging
# from typing import Set

logger = logging.getLogger("backendLogger")

class Data(metaclass=ABCMeta):
    """Base class for all data.
    """
    
    def __init__(self,
                in_path='',
                raw_data=None,
                out_path='',
                required_data_set = None,
                additional_params=None
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

        # Raw data (read-only)
        if in_path and raw_data is not None:
            # Both in_path and raw_data are given
            raise ValueError(f'Only one of in_path and raw_data can be specified, but not both.')
        elif not in_path and raw_data is None:
            # Neither in_path nor raw_data is given
            raise ValueError(f'Must specify either in_path or raw_data.')

        self._in_path = in_path

        self._out_path = out_path

        self._additional_params = additional_params or {}

        self._required_data_set = required_data_set or set()

        # self._raw_data = raw_data

        if self.in_path:
            # Load data from a given file
            fipath = self.check_is_file(self.in_path)
            self._raw_data = self.load_data_from_file(fipath)
        else:
            # Load data by making a deepcopy of the given raw data object
            self._raw_data = deepcopy(raw_data)

        # Validated data (read-only). Set as read-only to prevent user from setting the field manually.
        self._validated_data = self.validate_data()
        
        # Save data to an output folder if one is specified.
        if out_path:
            fopath = self.check_is_dir(out_path)
            self.save_data(fopath)
    
    @property
    def in_path(self):

        return self._in_path

    @property
    def raw_data(self):
        """Get raw data.

        Returns:
            obj: raw data
        """
        return self._raw_data

    @property
    def additional_params(self):
        """Get additional params

        Returns:
            dict: additional params
        """
        return self._additional_params

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
    def load_data_from_file(self, in_path):
        """Abstract method that reads input data from a file

        Args:
            in_path (str): path to an input file to be stored in the data class
            additional_params (dict): dictionary of any additional parameters needed to load the data
        """
        pass

    @abstractmethod
    def validate_data(self):
        """Validate that the raw data conforms with a documented standard spec. 
        If the raw data doesn't conform, try to convert it to meet the standard.
        """
        pass

    @abstractmethod
    def save_data(self, out_path):
        """Save data to the given 
        """
        pass

    def check_is_file(self, path):
        """Check that the file exists.

        Args:
            path (str): path to a file

        Raises:
            FileNotFoundError: the given path doesn't point to a valid file

        Returns:
            str : path to file
        """
        if os.path.isfile(path):
            return path
        else:
            raise FileNotFoundError('Invalid file: {path}')

    def check_is_dir(self, path, overwrite=True, create_if_none=False):
        """Check that the directory exists.

        Args:
            path (str): path to a directory for output files
            overwrite (bool, optional): whether to overwrite the directory if it exists already.
                                        Defaults to True.
            create_if_none (bool, optional): whether to create the directory if it doesn't exist.
                                            Defaults to False.

        Raises:
            NotADirectoryError: the directory doesn't exist

        Returns:
            str: path to the output directory
        """
        if os.path.isdir(path):
            if overwrite:
                shutil.rmtree(path)
                os.mkdir(path)
                logger.debug('Directory pruned: {path}.')
            return path
        elif create_if_none:
            os.mkdir(path)
            logger.debug('Directory created: {path}.')
        else:
            raise NotADirectoryError('Directory does not exist: {path}')

    
