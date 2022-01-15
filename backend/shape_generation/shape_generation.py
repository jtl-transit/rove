import logging
import traceback
import pandas as pd
import numpy as np


logger = logging.getLogger("backendLogger")

class Shape_Generation():

    def __init__(self, gtfs):
        
        self._gtfs = gtfs

        self.pattern = pd.DataFrame()

        self.bus_df = pd.DataFrame()
        self.bus_tp_pd = pd.DataFrame()

    @property
    def gtfs(self):

        return self._gtfs

    @property
    def pattern(self):

        return self._pattern

    @pattern.setter
    def pattern(self, pattern):

        if not pattern:
            trips = self.gtfs.trips

        else:
            self._pattern = pattern.copy()


# main for testing
def __main__():
    logger.info(f'Starting shape generation...')
    print(f'in main method of shape gen')

def generate_shapes(feed):
    print(f'running shape gen')
    logger.info('logging to shape gen logger')

# shape_gen_test()
if __name__ == "__main__":
    __main__()