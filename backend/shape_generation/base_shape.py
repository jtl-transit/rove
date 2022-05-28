from abc import abstractmethod
import logging
import pandas as pd
import numpy as np
from .misc_shape_classes import Pattern
from typing import Tuple, Dict, Set, List
from parameters.helper_functions import check_parent_dir

logger = logging.getLogger("backendLogger")

class BaseShape():

    def __init__(self, data, outpath):
        
        logger.info(f'Generating shapes...')
        self._data = data
        self._outpath = check_parent_dir(outpath)

        self._patterns = self.generate_patterns()

        self._shapes = self.generate_shapes()
        logger.info(f'shapes generated')

    @property
    def data(self):
        return self._data
    
    @property
    def outpath(self):
        return self._outpath

    @property
    def patterns(self):
        return self._patterns

    @abstractmethod
    def generate_patterns(self) -> Dict:

        pass

    @property
    def shapes(self):
        return self._shapes

    @abstractmethod
    def generate_shapes(self) -> Dict:

        pass