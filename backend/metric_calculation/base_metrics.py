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

class BaseMetrics():

    def __init__(self, rove_params:ROVE_params, shapes:BaseShape, gtfs:GTFS, avl=None, odx=None):
        
        logger.info(f'Generating metrics...')

        if not isinstance(rove_params, ROVE_params):
            raise TypeError(f'Not a valid ROVE_params object.')
        self._rove_params = rove_params

        if not isinstance(gtfs, GTFS):
            raise TypeError(f'Not a valid GTFS object.')
        
        self.shapes = shapes

        self.gtfs_df = self.gtfs_data_cleaning(gtfs)
        self.gtfs_trip_dict, self.gtfs_stop_dict, self.gtfs_route_dict = self.gtfs_aggregrate(self.gtfs_df)

        self.avl_df = self.avl_data_cleaning(avl)
        self.odx_df = self.odx_data_cleaning(odx)

        
        self._segment_metric = self.initialize_segment_metric()
        self._corridor_metric = self.initialize_corridor_metric()
        self._route_metric = self.initialize_route_metric()

        logger.info(f'metrics generated')

    def gtfs_data_cleaning(self, gtfs:GTFS):

        trips = gtfs.validated_data['trips']
        stop_times = gtfs.validated_data['stop_times']

        data = stop_times.merge(trips, on='trip_id', how='left')

        data['shape_dist_traveled'] = 0
        data['arrival_time'] = pd.to_datetime(data['arrival_time'], unit='s')

        gtfs_data = data[
            ['arrival_time', 'stop_id', 'stop_sequence', 'shape_dist_traveled', 'trip_id', 'route_id',
            'direction_id']]\
                .sort_values(by=['route_id', 'trip_id', 'stop_sequence'])\
                .drop_duplicates(subset=['stop_id', 'trip_id'])

        return gtfs_data
    
    def gtfs_aggregrate(self, gtfs_df:pd.DataFrame):
        
        gtfs_df_copy = deepcopy(gtfs_df)

        # Dict for trips
        # Key: trip_id, Value: List of stop event for this bus trip
        gtfs_df_copy['list'] = gtfs_df_copy[gtfs_df.columns.difference(['trip_id'])].values.tolist()
        gtfs_trip_dict = gtfs_df_copy.groupby('trip_id')['list']\
                            .apply(list)\
                            .to_dict()

        # Dict for stops
        # Key: stop_id; Value: list of trip_ids
        gtfs_stop_dict = gtfs_df_copy.groupby('stop_id')['trip_id']\
                            .apply(list)\
                            .to_dict()

        # Dict for routes
        # Key: route_id; Value: list of trip_ids
        gtfs_route_dict = gtfs_df_copy.groupby('route_id')['trip_id']\
                            .apply(list)\
                            .to_dict()

        return (gtfs_trip_dict, gtfs_stop_dict, gtfs_route_dict)

    def avl_data_cleaning(self, avl):
        pass

    def odx_data_cleaning(self, odx):
        pass

    @property
    def segment_metric(self):
        return self._segment_metric

    @property
    def corridor_metric(self):
        return self._corridor_metric
    
    @property
    def route_metric(self):
        return self._route_metric

    @property
    def rove_params(self):
        return self._rove_params
