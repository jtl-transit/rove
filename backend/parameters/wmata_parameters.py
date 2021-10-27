from .rove_parameters import ROVE_params
import pandas as pd

class WMATA_params(ROVE_params):
    def __init__(self, MONTH, YEAR, DATE_OPTION, MODE_OPTION):
        super().__init__("WMATA", MONTH, YEAR, DATE_OPTION, MODE_OPTION)
        self.TIMEPOINTS = {}
        # self.identify_timepoints()
    
    def identify_timepoints(self):
        feed = self.GTFS_FEED
        # Get relevant tables from GTFS feed: trips, routes and stop sequences
        feed_stop_events = feed.stop_times[['trip_id', 'stop_id', 'stop_sequence', 'checkpoint_id']]
        feed_trips = feed.trips[['route_id','trip_id','direction_id']]
        all_stops = pd.merge(feed_trips, feed_stop_events, on='trip_id', how='inner')
        all_stops = all_stops.sort_values(by=['trip_id', 'stop_sequence'])
        
        tp_only = all_stops[~all_stops['checkpoint_id'].isnull()]
        tp_only = tp_only[['stop_id', 'route_id']]
        tp_only['stop_id'] = tp_only['stop_id'].astype(int)
        tp_only = tp_only.drop_duplicates()

        self.TIMEPOINTS = tp_only.groupby(['route_id'])['stop_id'].agg(list).to_dict()