from numpy import dtype
from ..gtfs import GTFS
import pandas as pd
from backend.helper_functions import convert_stop_ids, load_csv_to_dataframe

class WMATA_GTFS(GTFS):

    def __init__(self, rove_params, mode='bus'):
        super().__init__(rove_params, mode)

    def add_timepoints(self):
        tp_df_col_types = {
            'route': 'string',
            'stopid': 'string',
            'reg_id': 'string'
        }
        id_cols = list(tp_df_col_types.keys())
        timepont_inpath = f'data/{self.rove_params.agency}/agency-specific/timepoints{self.rove_params.suffix}.csv'
        timepoint_df = load_csv_to_dataframe(timepont_inpath, id_cols=id_cols)
        timepoint_df[id_cols] = timepoint_df[id_cols].astype(tp_df_col_types)
        
        timepoint_df = convert_stop_ids('wmata timepoint data', timepoint_df, 'reg_id', self.validated_data['stops'])
        
        timepoint_stop_lookup = timepoint_df[['route', 'reg_id', 'assoc_tpid']].drop_duplicates(subset=['route', 'reg_id'])
        self.records = self.records.merge(timepoint_stop_lookup, left_on=['route_id', 'stop_id'], right_on=['route','reg_id'], how='left')
        self.records['timepoint'] = ~self.records['assoc_tpid'].isnull()
        self.records.drop(columns=['route','reg_id','assoc_tpid'], inplace=True)

    # If WMATA input data uses stop_ids that don't match GTFS stop_id, convert them
    def substitute_stop_id_with_stop_code(self, df:pd.DataFrame, gtfs_stops:pd.DataFrame, stop_id_col_name:str) -> pd.DataFrame:
        
        stop_id_code = gtfs_stops[['stop_id', 'stop_code']].drop_duplicates()
        df = df.merge(stop_id_code, left_on=stop_id_col_name, right_on='stop_code', how='left').drop(columns=['stop_code',stop_id_col_name])
        df = df.rename(columns={'stop_id': stop_id_col_name})
        return df
