import imp
import logging
import traceback
import pandas as pd
import numpy as np
from .base_shape import BaseShape
from parameters.gtfs import GTFS


logger = logging.getLogger("backendLogger")

class GTFS_Shape(BaseShape):

    def __init__(self, data):
        super().__init__(data)

    def generate_patterns(self):
        if not isinstance(self.data, GTFS):
            logger.fatal(f'The data provided for shape generation is not a GTFS object. Exiting...')
            quit()

        if 'shape' in self.data.validated_data.keys():
            get_patterns_from_gtfs_shapes()
        else:
            get_patterns_from_gtfs_stops()

def get_patterns_from_gtfs_shapes():

    pass

def get_patterns_from_gtfs_stops():

    pass

    # @property
    # def gtfs(self):

    #     return self._gtfs


# # main for testing
# def __main__():
#     logger.info(f'Starting shape generation...')
#     print(f'in main method of shape gen')

# def generate_shapes(feed):
#     print(f'running shape gen')
#     logger.info('logging to shape gen logger')

# # shape_gen_test()
# if __name__ == "__main__":
#     __main__()