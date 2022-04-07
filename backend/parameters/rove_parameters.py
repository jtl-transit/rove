from abc import ABCMeta, abstractmethod
import datetime
from typing import List
from .helper_functions import day_list_generation
import logging
import traceback
from .config import Config

logger = logging.getLogger("backendLogger")

class ROVE_params(object, metaclass=ABCMeta):
    """Data structure that stores all parameters needed throughout the backend.
    """
    def __init__(self,
                AGENCY='',
                MONTH='',
                YEAR='',
                DATE_TYPE='',
                DATA_OPTION=[],
                additional_params={}):
        """Instantiate rove parameters.
        """
        logger.info(f'Generating parameters...')
        # str : analyzed transit agency
        self._agency = AGENCY

        # str : analysis month, year and date type
        self._month = MONTH
        self._year = YEAR
        self._date_type = DATE_TYPE

        # list (str) : list of input data used for backend calculations
        self._data_option = DATA_OPTION

        # dict <str, any> : any additional parameters.
        #       Example of additional parameters that can be specified: 
        #       --> additional_input_paths : list of file paths to additional input files other than the default ones
        #       --> additional_output_paths: list of paths to additional output files
        if not isinstance(additional_params, dict):
            raise TypeError(f'additional_params must be a dict.')
        self._additional_params = additional_params or {}

        # dict <str, str> : dict of paths to input and output data
        # self._input_paths = self._additional_params.get('additional_input_paths', {})
        self._output_paths = self._additional_params.get('additional_output_paths', {})

        # dict <str, any> : agency-specific configuration parameters 
        #                   (e.g. time periods, speed range, percentile list, additional files, etc.)
        self._config = Config('config', f'data/{self.agency}/config/{self.agency}_param_config.json').validated_data

        # list (datetime) : list of dates of given month, year, agency
        self._date_list = self.generate_date_list()

        # date : sample date for analysis
        self._sample_date = self.generate_sample_date()
        logger.info(f'parameters generated')

    @property
    def agency(self):
        return self._agency
    
    @property
    def month(self):
        return self._month
    
    @property
    def year(self):
        return self._year

    @property
    def date_type(self):
        return self._date_type
    
    @property
    def data_option(self):
        return self._data_option
    
    @property
    def additional_params(self):
        return self._additional_params

    @property
    def suffix(self):
        """suffix used in file names (e.g. WMATA_02_2019)

        Returns:
            str: concatenate agency_month_year 
        """
        return f'_{self.agency}_{self.month}_{self.year}'

    @property
    def config(self):
        return self._config

    @property
    def output_paths(self):
        return self._output_paths

    @output_paths.setter
    def output_paths(self, additional_output_paths):
        """Set paths to output data

        Args:
            additional_output_paths (dict <str, str>, optional): additional paths to output data. Defaults to {}.
        """
        suffix = f'_{self.agency}_{self.month}_{self.year}'

        # TODO: Pick output paths based on analysis type (shape generation, or link selection, or journey visualization)
        # Output data paths
        default_paths = {
            'shapes_outpath': f'bustool/static/inputs/{self.agency}/shapes/bus-shapes{suffix}.json',
            'lookup_outpath': f'bustool/static/inputs/{self.agency}/lookup/lookup{suffix}.json',
            'segment_shapes_outpath': f'bustool/static/inputs/{self.agency}/shapes/segment_shapes{suffix}.json',
            'tp_shapes_outpath': f'bustool/static/inputs/{self.agency}/shapes/timepoint_shapes{suffix}.json',
            'segstop_shapes_outpath': f'bustool/static/inputs/{self.agency}/stops/segstopdata{suffix}.json',
            'tpstop_shapes_outpath': f'bustool/static/inputs/{self.agency}/stops/tpstopdata{suffix}.json',
            'seg_data_outpath': f'data/{self.agency}/selectlink/sl-seg-data{suffix}.json',
            'tp_data_outpath': f'data/{self.agency}/selectlink/sl-tp-data{suffix}.json',
            'metric_calculation_timepoints_outpath': f'bustool/static/inputs/{self.agency}/timepoints/timepoints{suffix}.json',
            'metric_calculation_peak_outpath': f'bustool/static/inputs/{self.agency}/peak/peak{suffix}.json',
            'metric_calculation_aggre_outpath': f'data/{self.agency}/metrics/METRICS{suffix}.p',
            'metric_calculation_aggre_10min_outpath': f'data/{self.agency}/metrics/METRICS_10MIN{suffix}.p'
        }

        if not isinstance(additional_output_paths, dict):
            raise TypeError('additional_output_paths must be a dict')

        output_paths = {**default_paths, **additional_output_paths}
        self._output_paths = output_paths

        logger.debug(f'{len(output_paths.keys())} output file paths are set.')

    @property
    def date_list(self):
        return self._date_list

    def generate_date_list(self)->List[datetime.datetime]:
        """Generate list of dates of date_type in the given month and year.

        Raises:
            KeyError: No workalendarPath is found in config.

        Returns:
            List[datetime.datetime]: List of dates.
        """

        if 'workalendarPath' not in self.config:
            raise KeyError('can not find workalendarPath in the config file')
        else:
            try:
                workalendar_path = self.config['workalendarPath']
                date_list = day_list_generation(self.month, self.year, self.date_type, workalendar_path)
            except ValueError as err:
                logger.exception(traceback.format_exc())
                logger.fatal(f'Error generating date list: {err}. Exiting backend...')
                quit()

        logger.debug(f'date list generated: {len(date_list)} {self.date_type} days in {self.month}-{self.year}.')
        return date_list

    @property
    def sample_date(self):
        return self._sample_date

    def generate_sample_date(self)->datetime.datetime:
        """Get a sample date. If date_type is 'Workday', then return the last Wednesday of the month, otherwise the last date
        found in the date_list. If the date_list is empty, then return today's date.

        Returns:
            datetime.datetime: A sample date.
        """
        if self.date_list:
            if self.date_type=='Workday':
                from calendar import WEDNESDAY
                sample_date = self.date_list[max(5, len(self.date_list) - (self.date_list[-1].weekday() - WEDNESDAY) % 7 - 1)]
            else:
                sample_date = self.date_list[max(5, len(self.date_list)-1)]
        else:
            sample_date = datetime.date.today()
        return sample_date
