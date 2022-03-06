import imp
import logging
import traceback
import pandas as pd
import numpy as np
from .base_shape import BaseShape
from parameters.gtfs import GTFS
from typing import Tuple, Dict, Set, List

logger = logging.getLogger("backendLogger")

class GTFS_Shape(BaseShape):

    def __init__(self, data):
        super().__init__(data)

    def generate_patterns(self):
        if not isinstance(self.data, GTFS):
            logger.fatal(f'The data provided for shape generation is not a GTFS object. Exiting...')
            quit()
        
        gtfs = self.data.validated_data
        trips = gtfs['trips']
        stop_times = gtfs['stop_times']
        stops = gtfs['stops']

        patterns, trip_dict = self.get_pattern_df_and_trip_dict(trips, stop_times)

        # use the shape file in gtfs
        if 'shapes' in self.data.validated_data.keys():
            self.generate_patterns_from_gtfs_shapes()
        else: # no shape file in gtfs, use stop coordinates instead
            self.generate_patterns_from_gtfs_stops()

    def get_pattern_df_and_trip_dict(self, trips: pd.DataFrame, stop_times: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
        """Analyze the trips and stop_times tables from GTFS to produce a pattern table and dict of trips.

        Args:
            trips: GTFS trips table. Assume having columns: route_id, trip_id, direction_id.
            stop_times: GTFS stop_times table. Assume having columns: trip_id, stop_sequence, stop_id.

        Returns:
            patterns: Columns: count (count of trips with the same pattern), hash (hash of pattern), route_id, 
                                    (representative) trip_id, direction_id, pattern_index
            trip_dict: <trip_id: list of stop_ids>
        """

        trip_stop_times = pd.merge(trips, stop_times, on='trip_id', how='inner')
        trip_stop_times.sort_values(by=['trip_id', 'stop_sequence'], inplace=True)

        # dict of trip: list of stop IDs
        trip_stops_dict = trip_stop_times.groupby('trip_id')['stop_id'].agg(list).to_dict()
        
        # dataframe: route_id, trip_id, direction_id
        trips = trip_stop_times[['route_id','trip_id','direction_id']].drop_duplicates()
        
        # Add column of trip hash, which is calculated from the list of stop IDs associated with each trip
        trip_hashes = {}
        for trip, stops in trip_stops_dict.items():
            trip_hashes[trip] = self.get_trip_hash(stops)
        trips['hash'] = trips['trip_id'].map(trip_hashes)

        # Count how many times each route-hash combination appears
        pattern_counts = trips.groupby(['route_id','hash','direction_id']).size().reset_index(name='count')
        
        # dict of route, hash: trip_id
        # Since the returned pattern_df only contains representative trip_id for each hash, this dict serves as
        #   a look up dict for all trip_ids with the same route_id and hash.
        trip_dict = trips.groupby(['route_id','hash'])['trip_id'].agg(list).to_dict()

        # Create a dataframe for the patterns with count of identical trips, hash, route_ids, direction_id and representative trip id
        trips.drop_duplicates(subset=['route_id','hash', 'direction_id'], inplace=True)
        pattern_counts = pd.merge(pattern_counts[['count','hash','route_id']], trips, on=['hash','route_id'], how='inner')
        pattern_counts.sort_values(by=['route_id','direction_id','count'], ascending=[True, True, False], inplace=True)
        
        # Add pattern index that starts from 0 for each new combination of route_id and direction_id
        pattern_counts['pattern_count'] = pattern_counts.groupby(['route_id','direction_id']).cumcount()
        pattern_counts['pattern_index'] = pattern_counts['route_id'].astype(str)+'-'+pattern_counts['direction_id'].astype(str)+'-'+pattern_counts['pattern_count'].astype(str)
        pattern_counts.drop(columns=['pattern_count'], inplace=True)

        return pattern_counts, trip_dict

    def generate_patterns_from_gtfs_shapes(self):
        pass

    def generate_patterns_from_gtfs_stops(self):

        pass

    def generate_shapes(self):

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