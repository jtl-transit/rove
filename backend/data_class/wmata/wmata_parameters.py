

from backend.data_class.rove_parameters import ROVE_params


class WMATA_params(ROVE_params):

    def __init__(self, agency: str, month: str, year: str, date_type: str, data_option: str):
        super().__init__(agency, month, year, date_type, data_option)

        self.input_paths.update({
            'timepoint': f'data/{self.agency}/agency-specific/timepoints{self.suffix}.csv', 
            'fsn':  f'data/{self.agency}/agency-specific/dim_fsn_routes.csv'
        })