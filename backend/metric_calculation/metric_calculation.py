from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from parameters.gtfs import GTFS
from parameters.rove_parameters import ROVE_params
from shape_generation.base_shape import BaseShape
from .data_preparation import DataPrep

logger = logging.getLogger("backendLogger")

FEET_TO_METERS = 0.3048
SPEED_RANGE = [0, 65]
MS_TO_MPH = 3.6/1.6
MAX_HEADWAY = 90

class MetricCalculation():
    
    def __init__(self, rove_params:ROVE_params, data_prep:DataPrep, shapes, gtfs:GTFS):
        
        if not isinstance(data_prep, DataPrep):
            raise TypeError(f'Not a valid DataPrep object.')

        if not isinstance(rove_params, ROVE_params):
            raise TypeError(f'Not a valid ROVE_params object.')

        if not isinstance(gtfs, GTFS):
            raise TypeError(f'Not a valid GTFS object.')

        