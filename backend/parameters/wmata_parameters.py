from .rove_parameters import ROVE_params
import pandas as pd
import logging
logger = logging.getLogger("paramLogger")

class WMATA_params(ROVE_params):
    def __init__(self, MONTH, YEAR, DATE_OPTION, MODE_OPTION):
        super().__init__("WMATA", MONTH, YEAR, DATE_OPTION, MODE_OPTION)
        self.TIMEPOINTS = {}
        try:
            self.identify_timepoints()
            logger.info(f'Timepoints table successfully generated.')
            logger.info(f'Timepoints table has length {len(self.TIMEPOINTS)}.')
        except Exception as e:
            logger.error(e)
            logger.info(f'Timepoints table generation failed.')
    def identify_timepoints(self):
        logger.info(f'Start generating timepoints table...')
        feed = self.GTFS_FEED
        # Get relevant tables from GTFS feed: trips, routes and stop sequences
        feed_stop_events = feed.stop_times[['trip_id', 'stop_id', 'stop_sequence', 'timepoint']]
        feed_trips = feed.trips[['route_id','trip_id','direction_id']]
        all_stops = pd.merge(feed_trips, feed_stop_events, on='trip_id', how='inner')
        logger.info(f'Found {all_stops.shape[0]} matching stop events.')
        all_stops = all_stops.sort_values(by=['trip_id', 'stop_sequence'])
        
        tp_only = all_stops[all_stops['timepoint']==1]
        tp_only = tp_only[['stop_id', 'route_id']]
        tp_only['stop_id'] = tp_only['stop_id'].astype(int)
        tp_only = tp_only.drop_duplicates()

        self.TIMEPOINTS = tp_only.groupby(['route_id'])['stop_id'].agg(list).to_dict()