"""Data class for GTFS data.
"""

from abc import ABCMeta, abstractmethod
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback
from .base_data_class import BaseData
import tqdm

REQUIRED_DATA_SET = {'agency', 'stops', 'routes', 'trips', 'stop_times'}
OPTIONAL_DATA_SET = {'shapes'}

logger = logging.getLogger("backendLogger")

class GTFS(BaseData):

    def __init__(self, alias, rove_params):
        super().__init__(alias, rove_params, REQUIRED_DATA_SET, OPTIONAL_DATA_SET)

    def load_data(self):
        """Load GTFS data from zip file

        Returns:
            dict <str, DataFrame>: dict containing DataFrames of all required data sets
        """
        rove_params = self.rove_params

        data = {}

        in_path = rove_params.input_paths[self.alias]

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
        for t in tqdm.tqdm(self.required_data_set, desc=f'Loading required {self.alias} data: '):
            try:
                feed_data = getattr(feed, t)
                if feed_data.empty:
                    raise ValueError(f'{t} data is empty.')
                data[t] = feed_data
            except AttributeError as err:
                logger.fatal(f'Could not find required table {t} from GTFS data. Exiting...')
                quit()
            except ValueError as err:
                logger.fatal(f'Please verify that the GTFS file for {t} has valid data.')
                quit()

        # Add all optional data if the file exists and is not empty
        for t in tqdm.tqdm(self.optional_data_set, desc=f'Loading optional {self.alias} data: '):
            try:
                feed_data = getattr(feed, t)
                if feed_data.empty:
                    raise ValueError(f'{t} data is empty.')
                data[t] = feed_data
            except AttributeError as err:
                logger.warning(f'Could not find optional table {t} from GTFS data. Skipping...')
            except ValueError as err:
                logger.warning(f'The GTFS file for the optional table {t} is empty. Skipping...')

        return data
    
    def validate_data(self):
        """Clean up raw data and make sure that it conforms with the standard format defined in the documentation

        Raises:
            ValueError: if any one type of the required raw data is empty

        Returns:
            dict <str, DataFrame>: validated and cleaned-up data
        """
        # avoid changing the raw data object
        validated_data = self.raw_data.copy()

        # Verify that all default data sets are not empty
        for n, d in self.raw_data.items():
            if d.empty:
                raise ValueError(f'{n} data is empty. Please verify that the corresponding GTFS file has valid data.')
        
        # Clean up the data
        # clean_up()

        return validated_data

    # TODO: make it more generic... convert route_id to list of route_short_name for any given table
    # Function to convert route_id from GTFS into route_short_name from GTFS, which is useful in some applications
    def convert_route_ids(df, feed):
        feed_routes = feed.routes
        route_dict = dict(zip(feed_routes['route_id'], feed_routes['route_short_name']))
        new_ids = []
        for i in df['route_id'].values.tolist():
            new_ids.append(route_dict[i])
            
        df['route_id'] = new_ids
        
        return df
    
    # Function to convert stop_id from GTFS into stop_code from GTFS, which is needed for WMATA
    def convert_stop_ids(df, feed):
        
        feed_stops = feed.stops
        stops_dict = dict(zip(feed_stops['stop_id'], feed_stops['stop_code']))
        new_ids = []
        for i in df['stop_id'].values.tolist():
            new_ids.append(stops_dict[i])
            
        df['stop_id'] = new_ids
        df = df[~df['stop_id'].isnull()] # Remove any rows with missing values
        
        return df