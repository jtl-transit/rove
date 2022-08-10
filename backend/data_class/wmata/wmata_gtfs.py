from numpy import dtype
from ..gtfs import GTFS
import pandas as pd

class WMATA_GTFS(GTFS):

    def __init__(self, rove_params, mode='bus'):
        super().__init__(rove_params, mode)

    def add_timepoints(self):
        timepoint_df = pd.read_csv(f'data/{self.rove_params.agency}/agency-specific/timepoints{self.rove_params.suffix}.csv')
        timepoint_df.columns = timepoint_df.columns.str.lower()
        tp_df_col_types = {
            'route': 'string',
            'reg_id': 'string'
        }
        tp_df_cols = list(tp_df_col_types.keys())
        timepoint_df[tp_df_cols] = timepoint_df[tp_df_cols].astype(dtype=tp_df_col_types)
        timepoint_df = self.substitute_stop_id_with_stop_code(timepoint_df, self.validated_data['stops'], 'reg_id')[['route','reg_id','assoc_tpid']]\
                        .drop_duplicates(subset=['route','reg_id']).dropna(how='any')
        
        self.records = self.records.merge(timepoint_df, left_on=['route_id', 'stop_id'], right_on=['route','reg_id'], how='left')
        self.records['timepoint'] = ~self.records['reg_id'].isnull()
        self.records.drop(columns=['route','reg_id','assoc_tpid'], inplace=True)

    # If WMATA input data uses stop_ids that don't match GTFS stop_id, convert them
    def substitute_stop_id_with_stop_code(self, df:pd.DataFrame, gtfs_stops:pd.DataFrame, stop_id_col_name:str) -> pd.DataFrame:
        
        stop_id_code = gtfs_stops[['stop_id', 'stop_code']].drop_duplicates()
        df = df.merge(stop_id_code, left_on=stop_id_col_name, right_on='stop_code', how='left').drop(columns=['stop_code',stop_id_col_name])
        df = df.rename(columns={'stop_id': stop_id_col_name})
        return df
