from numpy import dtype
from ..gtfs import GTFS
import pandas as pd
from backend.helper_functions import convert_stop_ids, load_csv_to_dataframe
import json
import logging

logger = logging.getLogger("backendLogger")
class WMATA_GTFS(GTFS):

    def __init__(self, rove_params, mode='bus', shape_gen=True):
        super().__init__(rove_params, mode, shape_gen)

        self.generate_route_types_by_fsn()

    def validate_data(self):
        data = super().validate_data()
        gtfs_stops = data['stops'].copy()
        data['stops'] = convert_stop_ids('validated GTFS stops', data['stops'], 'stop_id', gtfs_stops, 'stop_code')
        data['stop_times'] = convert_stop_ids('validated GTFS stop_times', data['stop_times'], 'stop_id', gtfs_stops, 'stop_code')
        return data
        
    def generate_route_types_by_fsn(self):
        """Modify the routeTypes object in frontend_config JSON file to include Frequent Service Network (FSN) routes and categories.
        """
        fsn_inpath = self.rove_params.input_paths['fsn']
        fsn_df = load_csv_to_dataframe(fsn_inpath, id_cols=['route'])

        fsn_active = fsn_df[fsn_df['active']==1].groupby('signid').get_group(fsn_df['signid'].max())
        fsn_lookup = fsn_active.groupby('category')['route'].apply(list).to_dict()

        with open(self.rove_params.input_paths['frontend_config'], 'r+') as f:
            data = json.load(f)
            data['routeTypes'] = fsn_lookup # <--- add `id` value.
            f.seek(0)        # <--- should reset file position to the beginning.
            json.dump(data, f)
            f.truncate()     # remove remaining part

    def add_timepoints(self):
        logger.info(f'adding timepoint to GTFS records')
        tp_df_col_types = {
            'route': 'string',
            'stopid': 'string',
            'reg_id': 'string'
        }
        id_cols = list(tp_df_col_types.keys())
        timepont_inpath = self.rove_params.input_paths['timepoint']
        timepoint_df = load_csv_to_dataframe(timepont_inpath, id_cols=id_cols)
        timepoint_df[id_cols] = timepoint_df[id_cols].astype(tp_df_col_types)
        
        timepoint_df = convert_stop_ids('wmata timepoint data', timepoint_df, 'reg_id', self.validated_data['stops'])
        
        timepoint_stop_lookup = timepoint_df[['route', 'reg_id', 'assoc_tpid']].drop_duplicates(subset=['route', 'reg_id'])
        self.records = self.records.merge(timepoint_stop_lookup, left_on=['route_id', 'stop_id'], right_on=['route','reg_id'], how='left')
        self.records['timepoint'] = ~self.records['assoc_tpid'].isnull()
        self.records.drop(columns=['route','reg_id','assoc_tpid'], inplace=True)
