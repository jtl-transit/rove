from abc import abstractmethod
import logging
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
    def generate_shapes(self) -> Dict:

        pass