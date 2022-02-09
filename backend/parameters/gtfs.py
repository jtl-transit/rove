"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback
from .data_class import Data

REQUIRED_DATA_SET = {'agency', 'stops', 'routes', 'trips', 'stop_times'}

logger = logging.getLogger("backendLogger")

class GTFS(Data):

    def __init__(self, alias, rove_params, required_data_set=REQUIRED_DATA_SET):
        super().__init__(alias, rove_params, required_data_set)

    def load_data(self, in_path):
        """Load GTFS data from zip file

        Args:
            in_path (str): path to the source GTFS file

        Returns:
            dict <str, DataFrame>: dict containing DataFrames of all required data sets
        """
        rove_params = self.rove_params

        data = {}

        # Retrieve GTFS data for the sample date
        try:
            service_id_list = ptg.read_service_ids_by_date(in_path)[rove_params.sample_date]
        except KeyError as err:
            logger.exception(traceback.format_exc())
            logger.fatal(f'Services for sample date {rove_params.sample_date} cannot be found in GTFS.'\
                        f'Please make sure the GTFS data is indeed for {rove_params.month}-{rove_params.year}.')
            quit()

        # Load GTFS feed
        view = {'routes.txt': {'route_type': rove_params.config['route_type']}, 'trips.txt': {'service_id': service_id_list}}
        feed = ptg.load_feed(in_path, view)

        # Store all required data in a dict
        for t in self.required_data_set:
            try:
                data[t] = getattr(feed, t)
            except AttributeError as err:
                logger.fatal(f'Could not find required table {t} from GTFS data. Exiting...')
                quit()
            
        return data
    
    def validate_data(self):
        validated_data = self.raw_data.copy()
        # Verify that all default data sets are not empty
        for n, d in self.raw_data.items():
            if d.empty:
                raise ValueError(f'{n} data is empty. Please verify that the corresponding GTFS file has valid data.')
        
        # Make sure that the 
        return validated_data