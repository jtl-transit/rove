from abc import ABCMeta, abstractmethod
import datetime
from typing import List
from rove.helper_functions import day_list_generation
import logging
import json

logger = logging.getLogger("backendLogger")

class ROVE_params(object, metaclass=ABCMeta):
    """Data structure that stores all parameters needed throughout the backend.
    """

    def __init__(self,
                AGENCY='',
                MONTH='',
                YEAR='',
                DATE_TYPE='',
                DATA_OPTION=[]):
        """Instantiate rove parameters.
        """

        logger.info(f'Generating parameters...')
        # str : analyzed transit agency
        self.agency = AGENCY

        # str : analysis month, year and date type
        self.month = MONTH
        self.year = YEAR
        self.date_type = DATE_TYPE

        self.suffix = f'_{self.agency}_{self.month}_{self.year}'

        # list (str) : list of input data used for backend calculations
        self.data_option = DATA_OPTION

        # dict <str, any> : any additional parameters.
        #       Example of additional parameters that can be specified: 
        #       --> additional_input_paths : list of file paths to additional input files other than the default ones
        #       --> additional_output_paths: list of paths to additional output files

        # dict <str, str> : dict of paths to input and output data
        # self._input_paths = self._additional_params.get('additional_input_paths', {})
        # self.output_paths = self.additional_params.get('additional_output_paths', {})
        self.input_paths = self.get_input_paths()
        self.output_paths = self.get_output_paths()
        
        # dict <str, any> : agency-specific configuration parameters 
        #                   (e.g. time periods, speed range, percentile list, additional files, etc.)
        with open(self.input_paths['frontend_config']) as json_file:
            config = json.load(json_file)
            self.redValues = config['redValues']
        
        with open(self.input_paths['backend_config']) as json_file:
            self.config = json.load(json_file)
            
        # self.config = Config('config', f'data/{self.agency}/config/{self.agency}_param_config.json').validated_data

        # list (datetime) : list of dates of given month, year, agency
        self.date_list = self.__generate_date_list()

        # date : sample date for analysis
        self.sample_date = self.__generate_sample_date()
        logger.info(f'Sample date: {self.sample_date}')
        logger.info(f'parameters generated')

    def get_input_paths(self):
        """Set paths to input data
        """

        return {
            'gtfs': f'data/{self.agency}/gtfs/GTFS{self.suffix}.zip',
            'avl': f'data/{self.agency}/avl/AVL{self.suffix}.csv',
            'odx': f'data/{self.agency}/odx/ODX{self.suffix}.csv',
            'backend_config': f'data/{self.agency}/config/{self.agency}_param_config.json',
            'frontend_config': f'frontend/static/inputs/{self.agency}/config.json',
            'shapes': f'frontend/static/inputs/{self.agency}/shapes/bus-shapes{self.suffix}.json'
        }

    def get_output_paths(self):
        """Set paths to output data
        """

        return {
            'shapes': f'frontend/static/inputs/{self.agency}/shapes/bus-shapes{self.suffix}.json',
            'metric_calculation_timepoints': f'frontend/static/inputs/{self.agency}/timepoints/timepoints{self.suffix}.json',
            'metric_calculation_peak': f'frontend/static/inputs/{self.agency}/peak/peak{self.suffix}.json',
            'metric_calculation_aggre': f'data/{self.agency}/metrics/METRICS{self.suffix}.p',
            'metric_calculation_aggre_10min': f'data/{self.agency}/metrics/METRICS_10MIN{self.suffix}.p'
        }

    def __generate_date_list(self)->List[datetime.datetime]:
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
            # except ValueError as err:
            except ValueError:
                logger.fatal(f'Error generating date list.', exc_info=True)
                quit()

        logger.debug(f'date list generated: {len(date_list)} {self.date_type} days in {self.month}-{self.year}.')
        return date_list

    def __generate_sample_date(self)->datetime.datetime:
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
