from abc import abstractmethod
from copy import deepcopy
import logging
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List
from parameters.gtfs import GTFS
from parameters.rove_parameters import ROVE_params
from shape_generation.base_shape import BaseShape

logger = logging.getLogger("backendLogger")

class DataPrep():

    def __init__(self, gtfs:GTFS, avl=None, odx=None):
        
        logger.info(f'Generating metrics...')

        if not isinstance(gtfs, GTFS):
            raise TypeError(f'Not a valid GTFS dataframe.')
        
        self.gtfs_df = self.gtfs_data_cleaning(gtfs)
        self.gtfs_trip_dict, self.gtfs_stop_dict, self.gtfs_route_dict = self.gtfs_aggregrate(self.gtfs_df)

        self.segments = self.generate_segments(self.gtfs_df)

        self.corridors = self.generate_corridors(self.segments)
        
        self.timepoints_segments, self.timepoints_corridors, self.time_branchpoints = \
                    self.process_timepoints(self.gtfs_df)
    

        self.avl_df = self.avl_data_cleaning(avl)
        self.odx_df = self.odx_data_cleaning(odx)

        
        # self._segment_metric = self.initialize_segment_metric()
        # self._corridor_metric = self.initialize_corridor_metric()
        # self._route_metric = self.initialize_route_metric()

        logger.info(f'metrics generated')

    def gtfs_data_cleaning(self, gtfs:GTFS):

        gtfs_df = deepcopy(gtfs.df)
        gtfs_df['shape_dist_traveled'] = 0
        gtfs_df['arrival_time'] = pd.to_datetime(gtfs_df['arrival_time'], unit='s')

        gtfs_df = gtfs_df[
            ['arrival_time', 'stop_id', 'stop_sequence', 'shape_dist_traveled', 'trip_id', 'route_id',
            'direction_id', 'timepoint']]\
                .sort_values(by=['route_id', 'trip_id', 'stop_sequence'])\
                .drop_duplicates(subset=['stop_id', 'trip_id'])

        return gtfs_df
    
    def gtfs_aggregrate(self, gtfs_df:pd.DataFrame):
        
        gtfs_df_copy = deepcopy(gtfs_df)

        # Dict for trips
        # Key: trip_id, Value: List of stop event for this bus trip
        gtfs_df_copy['list'] = gtfs_df_copy.loc[:,~gtfs_df.columns.isin(['trip_id'])].values.tolist()
        gtfs_trip_dict = gtfs_df_copy.groupby('trip_id')['list']\
                            .apply(list)\
                            .to_dict()

        # Dict for stops
        # Key: stop_id; Value: list of trip_ids
        gtfs_stop_dict = gtfs_df_copy.drop_duplicates(subset=['stop_id', 'trip_id'])\
                            .groupby('stop_id')['trip_id']\
                            .apply(list)\
                            .to_dict()

        # Dict for routes
        # Key: route_id; Value: list of trip_ids
        gtfs_route_dict = gtfs_df_copy.drop_duplicates(subset=['route_id', 'trip_id'])\
                            .groupby('route_id')['trip_id']\
                            .apply(list)\
                            .to_dict()

        return (gtfs_trip_dict, gtfs_stop_dict, gtfs_route_dict)

    def generate_segments(self, gtfs_df:pd.DataFrame):
        
        # key: route_id, value: list of stop pairs
        gtfs_df_copy = deepcopy(gtfs_df)

        gtfs_df_copy['next_stop'] = gtfs_df_copy.groupby(by='trip_id')['stop_id'].shift(-1)
        gtfs_df_copy = gtfs_df_copy.dropna(subset=['next_stop'])
        gtfs_df_copy['stop_pair'] = gtfs_df_copy[['stop_id','next_stop']].apply(tuple, axis=1)

        segments = gtfs_df_copy.drop_duplicates(subset=['route_id', 'stop_pair'])\
                            .groupby('route_id')['stop_pair']\
                            .apply(list)\
                            .to_dict()

        return segments
    
    def generate_corridors(self, segments:Dict):
        
        # key: stop pair, value: list of routes (more than 1 route)
        route_segment_df = pd.DataFrame.from_dict(segments, orient='index')\
                            .stack()\
                            .reset_index()\
                            .drop(columns=['level_1'])\
                            .rename(columns={'level_0':'route_id', 0:'stop_pair'})\

        route_segment_df['count'] = route_segment_df.groupby('stop_pair')['route_id'].transform('count')
        corridors = route_segment_df.loc[route_segment_df['count']>1, ['stop_pair', 'route_id']]\
                            .groupby('stop_pair')['route_id']\
                            .apply(list)\
                            .to_dict()

        return corridors

    def process_timepoints(self, gtfs_df:pd.DataFrame):
        g = deepcopy(gtfs_df)
        tg = deepcopy(g[g['timepoint']==1])

        # identify timepoint pairs for each stop event
        tg['next_stop'] = tg.groupby(by='trip_id')['stop_id'].shift(-1)
        tg = tg.dropna(subset=['next_stop'])
        tg['tp_stop_pair'] = tg[['stop_id','next_stop']].apply(tuple, axis=1)

        g['tp_stop_pair'] = tg['tp_stop_pair']
        g['tp_stop_pair_filled'] = g.groupby('trip_id')['tp_stop_pair']\
                                            .fillna(method='ffill')

        # get a dataframe of two columns: stop_id and shared routes (set of all routes that use the corresponding stop)
        l=pd.DataFrame(g.groupby('stop_id')['route_id'].agg(set)).rename(columns={'route_id': 'routes'})
        g = g.merge(l, left_on='stop_id', right_index=True, how='left')

        # within a trip_id group, check if the set of shared routes changes from previous and next stop
        g['routes_diff_next'] = g.groupby('trip_id')['routes'].shift(0) - g.groupby('trip_id')['routes'].shift(-1)
        g['routes_diff_prev'] = g.groupby('trip_id')['routes'].shift(0) - g.groupby('trip_id')['routes'].shift()
        g['routes_diff_next_len'] = g['routes_diff_next'].apply(lambda x: len(x) if isinstance(x, set) else 0)
        g['routes_diff_prev_len'] = g['routes_diff_prev'].apply(lambda x: len(x) if isinstance(x, set) else 0)

        # stops that have a different set of routes from adjacent stops and where shared routes appear more than once
        # in adjacent stops are labeled as branchpoint
        g['branchpoint'] = (((g['routes_diff_next_len'] + g['routes_diff_prev_len'])>0)\
                             & ~((g['routes_diff_prev']==g['routes_diff_next']) & (g['routes_diff_prev_len']!=0))).astype(int)

        # create new column tp_bp: 1 if it is either timepoint or branchpoint, 0 otherwise
        g['tp_bp'] = ((g['branchpoint']==1) | (g['timepoint']==1)).astype(int)

        tp_bp = g.loc[g['tp_bp']==1, ['route_id', 'stop_id']]\
                    .drop_duplicates()\
                    .groupby('route_id')['stop_id']\
                    .apply(list)\
                    .to_dict()

        tp_bp_segments = self.generate_segments(g[g['tp_bp']==1])
        tp_bp_corridors = self.generate_corridors(tp_bp_segments)
        return tp_bp_segments, tp_bp_corridors, tp_bp

    def avl_data_cleaning(self, avl):
        pass

    def odx_data_cleaning(self, odx):
        pass