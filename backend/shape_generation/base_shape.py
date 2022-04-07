from abc import abstractmethod
import logging
import traceback
import pandas as pd
import numpy as np
from .misc_shape_classes import Pattern
from typing import Tuple, Dict, Set, List

logger = logging.getLogger("backendLogger")

class BaseShape():

    def __init__(self, data):
        
        logger.info(f'Generating shapes...')
        self._data = data

        self._patterns = self.generate_patterns()

        self._shapes = self.generate_shapes()
        logger.info(f'shapes generatd')

    @property
    def data(self):

        return self._data

    @property
    def patterns(self):

        return self._patterns

    @abstractmethod
    def generate_patterns(self) -> Dict[str, Pattern]:

        pass

    @property
    def shapes(self):

        return self._shapes

    @abstractmethod
    def generate_shapes(self):

        pass

    # helpful functions
    def get_trip_hash(self, stops:List[str]) -> int:
        """Get hash of a list of stops IDs of a trip
            hashing function: hash = sum(2*index of stop in list)**2 + stop_value**3)
        Args:
            stops: list of stop IDs, 
        Returns:
            hash value of stop
        """
        hash_1 = sum((2*np.arange(1,len(stops)+1))**2)
        hash_2 = 0
        for stop in stops:
            hash_2 += (self.get_stop_value(stop))**3
        hash = hash_1 + hash_2

        return hash

    def get_stop_value(self, stop:str) -> int:
        """Get numerical value of a stop, either the original numerical value or 
            sum of unicode values of all characters
        Args:
            every element must be a string literal
        Returns:
            value of a stop
        """
        try:
            num = int(stop)
        except ValueError as err: # the given stop ID is not a numerical value
            num = sum([ord(x) for x in stop])
        return num

# main for testing
def __main__():
    logger.info(f'Starting shape generation...')
    print(f'in main method of shape gen')


if __name__ == "__main__":
    __main__()

