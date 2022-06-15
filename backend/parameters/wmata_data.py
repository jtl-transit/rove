
import pandas as pd
from local.gtfs_class import GTFS


class WMATA_GTFS(GTFS):

    def __init__(self, in_path, sample_date, route_type):
        super().__init__(in_path, sample_date, route_type)

    @GTFS.timepoints.setter
    def timepoints(self, timepoints):
        
        feed = self.feed
        # Get relevant tables from GTFS feed: trips, routes and stop sequences
        feed_stop_events = feed.stop_times[['trip_id', 'stop_id', 'stop_sequence']]
        feed_trips = feed.trips[['route_id','trip_id','direction_id']]
        all_stops = pd.merge(feed_trips, feed_stop_events, on='trip_id', how='inner')
        all_stops = all_stops.sort_values(by=['trip_id', 'stop_sequence'])
        print(all_stops.shape)
        tpdf = timepoints[['route', 'stopid', 'assoc_tpid']].drop_duplicates().dropna(how='any')
        tpdf = tpdf.astype(str)
        all_stops_tp = pd.merge(all_stops, tpdf, \
                                left_on=['route_id', 'stop_id'], right_on=['route', 'stopid'], \
                                    how='inner')
        print(all_stops_tp.shape)
        tp_only = all_stops_tp[~all_stops_tp['assoc_tpid'].isnull()]
        tp_only = tp_only[['stop_id', 'route_id']]
        tp_only['stop_id'] = tp_only['stop_id'].astype(int)
        tp_only = tp_only.drop_duplicates()

        self._timepoints = tp_only.groupby(['route_id'])['stop_id'].agg(list).to_dict()