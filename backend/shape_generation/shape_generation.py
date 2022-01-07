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
def get_pattern_hash(stops):
    """Get hash of a stop ID
        hashing function: hash = sum(2*sequence of stop)**2 + stop_value**3)

    Args:
        stops (list(str)): list of stop IDs, 

    Returns:
        int: hash value of stop
    """
    hash = sum((2*np.arange(1,len(stops)+1))**2) + sum(np.array([get_stop_value(s) for s in stops])**3)

    return hash

def get_stop_value(stop):
    """Get numerical value of a stop, either the original numerical value or 
        sum of unicode values of all characters

    Args:
        stop (str): every element must be a string literal

    Returns:
        int: value of a stop
    """
    try:
        num = int(stop)
    except ValueError as err: # the given stop ID is not a numerical value
        num = sum([ord(x) for x in stop])
    except TypeError as err:
        logger.exception(traceback.format_exc())
        logger.fatal(f'stop ID {stop} is of type {type(stop)}, cannot be converted to int. Exiting backend...')
        quit()
    return num

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