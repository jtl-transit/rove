"""Data classes for basic data used in rove:
    GTFS, AVL and ODX
"""
from abc import ABCMeta, abstractmethod
from pandas.core.frame import DataFrame
import partridge as ptg
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger("backendLogger")

class GTFS():
    """Stores GTFS data of a sample date and view
    """
    def __init__(self, in_path, sample_date, route_type):
        """Instantiate the GTFS data class

        Args:
            in_path (string): path to the GTFS zip file
            sample_date (datetime): sample date for analysis
            route_type (list of strings): type of transit to analyze
        
        Attributes:
            See notes.
        """

        self.in_path = in_path
        self.sample_date = sample_date
        
        # list of service IDs of the sample date
        self.service_id_list = None

        # GTFS feed and geo feed of a view, read-only
        
        view = {'routes.txt': {'route_type': route_type}, 'trips.txt': {'service_id': self.service_id_list}}
        self._feed = ptg.load_feed(self.in_path, view)
        self._geo_feed = ptg.load_geo_feed(self.in_path, view)

        # DataFrame : result of inner joining trips and stop_events tables on trip_id
        self.stop_times_trips = pd.DataFrame()

        # DataFrame : timepoints
        self.timepoints = pd.DataFrame()

        # dict <str: list(str)> : lookup dict of trip_ids: list of stop_ids
        self.trip_stops_dict = dict()

        # dict <str: list(tuple(float, float))> : lookup dict of trip_ids: list of stop coordinates (lat, lon)
        self.trip_coords_dict = dict()

        # dict <tuple(str, int, int): 
        self.pattern_dict = dict()
    
    @property
    def service_id_list(self):
        """Get list of service IDs

        Returns:
            list (int): list of service IDs
        """
        return self._service_id_list

    @service_id_list.setter
    def service_id_list(self, ids):
        """Set list of service IDs, either user-defined or retrieved from GTFS data

        Args:
            ids (list): list of service IDs
        """
        if not ids:
            self._service_id_list = ptg.read_service_ids_by_date(self.in_path)[self.sample_date]
        else:
            self._service_id_list = ids
    @property
    def feed(self):

        return self._feed

    @property
    def geo_feed(self):

        return self._geo_feed

    @property
    def stop_times_trips(self):
        """Get joined stop_times and trips

        Returns:
            DataFrame: joined stop_times and trips
        """
        return self._stop_times_trips

    @stop_times_trips.setter
    def stop_times_trips(self, df):
        """Set stop_times_trips as user-defined or inner joining stop_times and trips on trip_id

        Args:
            df (DataFrame): user-defined stop_times_trips DataFrame
        """
        if not isinstance(df, DataFrame):
            raise TypeError('stop_times_trips must be given as a DataFrame')

        if df.empty:
            feed = self.feed
            try:
                self._stop_times_trips = pd.merge(feed.trips, feed.stop_times, on='trip_id', how='inner')
                self._stop_times_trips.sort_values(by=['trip_id', 'stop_sequence'],inplace=True)
            except KeyError as err:
                logger.exception(traceback.format_exc())
                logger.fatal(f'Error joining trips and stop_times. Exiting...')
                quit()
            else:
                logger.debug(f'Stop times: \n{get_data_stats(feed.stop_times)}')
                logger.debug(f'Trips: \n{get_data_stats(feed.trips)}')
        else:
            self._stop_times_trips = df.copy()
        logger.debug(f'stop_times_trips: \n{get_data_stats(self._stop_times_trips)}')

        
    @property
    def timepoints(self):
        """Get the dataframe of timepoints

        Returns:
            DataFrame: [stop ID, route ID] of all timepoints
        """
        return self._timepoints

    @timepoints.setter
    def timepoints(self, timepoints):
        """Set timepoints, either user-defined or identified from the GTFS data

        Assumption:
            the stop_times table in GTFS data contains the optional column 'timepoint'
            (If this is not true for the agency, then inherite the class and override this method.) 

        Args:
            timepoints (DataFrame): dataframe with two columns, unique [stop ID, route ID], of timepoints

        Raises:
            TypeError: if timepoints are not given in a DataFrame
        """
        if not isinstance(timepoints, DataFrame):
            raise TypeError('timepoints must be given as a DataFrame')

        if timepoints.empty:
            try:
                all_stops = self.stop_times_trips[['trip_id', 'stop_id', 'stop_sequence', 'timepoint', 'route_id']]
            except KeyError as err:
                logger.exception(traceback.format_exc())
                logger.fatal(f'Error encountered retrieving columns from stop_times_trips. Exiting...')
                quit()
            else:
                logger.debug(f'stop_times_trips: \n{get_data_stats(all_stops)}')
            
            tp_only = all_stops[~all_stops['timepoint'].isnull()]
            tp_only = tp_only[['stop_id', 'route_id']]
            tp_only['stop_id'] = tp_only['stop_id'].astype(int)
            tp_only = tp_only.drop_duplicates()

            logger.debug(f'Timepoints: \n{get_data_stats(tp_only)}')

            self._timepoints = tp_only
        else:
            self._timepoints = timepoints
        
    @property
    def trip_stops_dict(self):

        return self._trip_stops_dict

    @trip_stops_dict.setter
    def trip_stops_dict(self, trip_stops_dict):

        if not isinstance(trip_stops_dict, dict):
            raise TypeError('trip_stops_dict must be given as a dict')

        if not trip_stops_dict:
            self._trip_stops_dict = self.stop_times_trips.groupby('trip_id')['stop_id'].agg(list).to_dict()
        else:
            self._trip_stops_dict = trip_stops_dict

        logger.debug(f'trip_stops_dict generated for {len(self._trip_stops_dict.keys())} trips')

    @property
    def trip_coords_dict(self):

        return self._trip_coords_dict
    
    @trip_coords_dict.setter
    def trip_coords_dict(self, trip_coords_dict):
        if not isinstance(trip_coords_dict, dict):
            raise TypeError('trip_coords_dict must be given as a dict')

        if not trip_coords_dict:
            feed = self.feed
            stops = feed.stops[['stop_id','stop_lat','stop_lon']].copy()
            stops['coords'] = list(zip(stops.stop_lat, stops.stop_lon))
            stops = stops[['stop_id','coords']].drop_duplicates()
            logger.debug(f'stops: \n{get_data_stats(stops)}')

            stop_times_trips_stops = pd.merge(self.stop_times_trips, stops, on='stop_id', how='inner')
            self._trip_coords_dict = stop_times_trips_stops.groupby('trip_id')['coords'].agg(list).to_dict()
        else:
            self._trip_coords_dict = trip_coords_dict

        logger.debug(f'trip_coords_dict generated for {len(self._trip_coords_dict.keys())} trips')
    
    @property
    def pattern_dict(self):

        return self._pattern_dict

    @pattern_dict.setter
    def pattern_dict(self, pattern_dict):
        """Get pattern dict. Key: pattern (route-direction-count). Value: Pattern object

        Args:
            pattern_dict ([type]): [description]

        Raises:
            TypeError: [description]
        """
        if not isinstance(pattern_dict, dict):
            raise TypeError('pattern_dict must be given as a dict')

        if not pattern_dict:
            trips = self.stop_times_trips[['trip_id', 'route_id', 'direction_id']].drop_duplicates()
            trips = trips[trips['trip_id'].isin(self.trip_stops_dict.keys())]
            trips['hash'] = trips['trip_id'].apply(lambda r: get_pattern_hash(self.trip_stops_dict[r]))

            # # Count how many times each route-hash combination appears
            # pattern_counts = trips.groupby(['route_id','hash','direction_id']).size().reset_index(name='count')
            
            route_dir_pattern_trips_dict = trips.groupby(['route_id', 'direction_id', 'hash'])['trip_id'].agg(list).to_dict()
            trips.drop_duplicates(subset=['route_id', 'direction_id','hash'], inplace=True)

            # Get the trip_ids associated with each route-hash combination as a list of lists
            trip_dict = trips.groupby(['route_id','hash'])['trip_id'].agg(list).to_dict()
        else:
            self._pattern_dict = pattern_dict
    
    # def load(self, view):

    #     self.feed = ptg.load_feed(self.in_path, view)
    #     self.geo_feed = ptg.load_geo_feed(self.in_path, view)

def get_pattern_hash(stops):
    """Get hash of a list of stop IDs
        hashing function: hash = sum(2*sequence of stop)**2 + stop_value**3)

    Args:
        stops (list(str)): list of stop IDs, 

    Returns:
        int: hash value of stop
    """
    # TODO: verify that the hashing function does indeed generate unique hash for different stop combinations
    convert_stop_value = lambda stop: int(stop) if str(stop).isnumeric() else sum([ord(c) for c in stop])
    hash = sum((2*np.arange(1,len(stops)+1))**2) + sum(np.array([convert_stop_value(s) for s in stops])**3)

    return hash


def get_data_stats(df):
    """Get count, min and max of all dataframe columns

    Args:
        df (DataFrame): any dataframe

    Returns:
        DataFrame: summary data where rows are columns of the given dataframe, 
                    and columns are head, tail, nunique, count, min and max of each column
    """
    if not isinstance(df, DataFrame):
        raise TypeError(f'Cannot get statistics of a non-DataFrame object.')
    head = df.head(1)
    tail = df.tail(1)
    desc_list = ['count','min', 'max']
    desc = df.describe(include="all")
    desc = desc[desc.index.isin(desc_list)]
    u = df.nunique().to_frame('unique').T
    stats = pd.concat([head,tail,u,desc]).T
    return f'shape: {df.shape}\nstats:\n{stats}'