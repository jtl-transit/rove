from abc import ABCMeta, abstractmethod
from calendar import WEDNESDAY
import datetime
from typing import Dict, List
from backend.helper_functions import day_list_generation
import logging
import json

logger = logging.getLogger("backendLogger")

class ROVE_params(object, metaclass=ABCMeta):
    """Data structure that stores all parameters needed throughout the backend.
    
    :param agency: name of the analyzed agency
    :type agency: str
    :param month: 2-character string of the analyzed month, e.g. 03 for March
    :type month: str
    :param year: 4-character string of the analyzed year, e.g. 2022
    :type year: str
    :param date_type: type of dates to be analyzed. One of "Workday", "Saturday", "Sunday"
    :type date_type: str
    :param data_option: list of input data options. One of 'GTFS', 'GTFS-AVL'
    :type data_option: list
    """

    def __init__(self,
                agency:str,
                month:str,
                year:str,
                date_type:str,
                data_option:str):
        """Instantiate rove parameters.
        """

        logger.info(f'generating parameters')
        #: Analyzed transit agency, see parameter definition.
        self.agency:str = agency

        if not month.isnumeric() or len(month) != 2 or int(month) < 1 or int(month) > 12:
            raise ValueError(f"month must be a 2-character stringified numeric value between 1 and 12, e.g. '02', '12'.")
        #: Analyzed month, see parameter definition.
        self.month:str = month

        if not year.isnumeric() or len(year) != 4:
            raise ValueError(f"year must be a 4-character stringified numeric value, e.g. '2022'.")
        #: Analyzed year, see parameter definition.
        self.year:str = year

        SUPPORTED_DATE_TYPES = ['Workday', 'Saturday', 'Sunday']
        if date_type not in SUPPORTED_DATE_TYPES:
            raise ValueError(f"Invalid date_type: {date_type}, must be one of: {SUPPORTED_DATE_TYPES}.")
        #: Analyzed date option, see parameter definition.
        self.date_type:str = date_type

        SUPPORTED_DATA_OPTIONS = ['GTFS', 'GTFS-AVL']
        if data_option not in SUPPORTED_DATA_OPTIONS:
            raise ValueError(f"Invalid data_option: {data_option}, must be one of: {SUPPORTED_DATA_OPTIONS}.")
        #: Analyzed data option, see parameter definition.
        self.data_option:str = data_option

        #: Suffix used in input and output file names, string concatenation in the form of "<agency>_<month>_<year>", e.g. "MBTA_02_2021".
        self.suffix:str = f'_{self.agency}_{self.month}_{self.year}'

        #: Dict of paths to input data, i.e. gtfs, avl, backend_config, frontend_config, shapes file (if shape generation has been run previously).
        self.input_paths:Dict[str, str] = self.__get_input_paths()

        #: Dict of paths to output data, i.e., shapes file, timepoints lookup, stop name lookup, aggregated metrics by time periods, 
        #: aggregated metrics by 10-min intervals.
        self.output_paths:Dict[str, str] = self.__get_output_paths()
        
        with open(self.input_paths['frontend_config']) as json_file:
            config = json.load(json_file)
            #: A dict serving as the lookup for "redValues", i.e. whether the visualization of a metric value is red when the value is high or low. 
            #: This information is required in the frontend_config JSON file, where an object named "redValues" must exist and consist of name-value pairs
            #: of each metric to be calculated, where the value must be "High" or "Low". e.g. "scheduled_frequency" : "Low" means that the scheduled frequency 
            #: of stop pairs/routes will be colored red if the value is low and blue if high; whereas "High" means high values are colored red and low values blue.
            self.redValues:Dict[str, str] = config['redValues']
        
        with open(self.input_paths['backend_config']) as json_file:
            #: agency-specific configuration parameters for the backend (backend_config), e.g., time periods, speed range, percentile list, additional files, etc. 
            #: (Although there are two config files (frontend_config and backend_config), this attribute storing backend_config data is called "config" for simplicity, 
            #: because frontend_config is only used in the backend to retrieve redValues as described above, and all other reference to "config" in the backend 
            #: is using backend_config.)
            self.config:Dict[str, object] = json.load(json_file)
            
        #: List of dates of the given date_type in the given month and year of the agency.
        self.date_list:List[datetime.datetime] = self.generate_date_list()

        #: sample date for analysis
        # self.sample_date:datetime.datetime = self.__generate_sample_date()
        # logger.info(f'Sample date: {self.sample_date}')

    def __get_input_paths(self):
        """Get predefined paths to input data.

        :return: dict of paths to input data. Key: alias of input data; value: path to the data file.
        :rtype: dict
        """

        return {
            'gtfs': f'data/{self.agency}/gtfs/GTFS{self.suffix}.zip',
            'avl': f'data/{self.agency}/avl/AVL{self.suffix}.csv',
            'backend_config': f'data/{self.agency}/config/{self.agency}_backend_config.json',
            'frontend_config': f'frontend/static/inputs/{self.agency}/config.json',
            'shapes': f'frontend/static/inputs/{self.agency}/shapes/bus-shapes{self.suffix}.json'
        }

    def __get_output_paths(self):
        """Get predefined paths to output data.

        :return: dict of paths to output data. Key: alias of output data; value: path to the data file.
        :rtype: dict
        """

        return {
            'shapes': f'frontend/static/inputs/{self.agency}/shapes/bus-shapes{self.suffix}.json',
            'timepoints': f'frontend/static/inputs/{self.agency}/timepoints/timepoints{self.suffix}.json',
            'stop_name_lookup': f'frontend/static/inputs/{self.agency}/lookup/lookup{self.suffix}.json',
            'metric_calculation_aggre': f'data/{self.agency}/metrics/METRICS{self.suffix}.p',
            'metric_calculation_aggre_10min': f'data/{self.agency}/metrics/METRICS_10MIN{self.suffix}.p'
        }

    def generate_date_list(self)->List[datetime.datetime]:
        """Generate a list of dates of date_type in the given month and year. For example, if the user specified to analyze "MBTA", "02", "2021", "Workday" as the 
        agency, month, year and date_type, then this method will return a list of datetime objects that are the workdays (no weekend or holiday) in Feb 2021 in Massachusetts. 
        A "workalendarPath" object must exist in the backend_config JSON file for the method to know which state/region to lookup the holiday calendar for, and its value must
        be the workalendar class for the region that the agency operates in. E.g. for the MBTA, this name-value pair is specified in the backend_config file: 
        "workalendarPath": "workalendar.usa.massachusetts.Massachusetts". For details on how to find the correct workalendar class for your region, refer to 
        https://workalendar.github.io/workalendar/iso-registry.html.

        :raises KeyError: No workalendarPath is found in config.
        :return: List of dates.
        :rtype: List[datetime.datetime]
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

    def __generate_sample_date(self, dow:int=WEDNESDAY)->datetime.datetime:
        """Get a sample date from self.date_list. If date_type is 'Workday', then return the last specified day of week found in date_list, otherwise the 
        last date found in the date_list.

        :param dow: day of week in integer (e.g. MONDAY = 0, WEDNESDAY = 2), defaults to WEDNESDAY
        :type dow: int, optional
        :raises ValueError: self.date_list is empty
        :return: a sample date
        :rtype: datetime.datetime
        """
        
        if self.date_list:
            if self.date_type=='Workday':
                last_dow_in_month = self.date_list[-1] - datetime.timedelta((datetime.date(2022, 5, 31).weekday()-dow) % 7)
                sample_date = last_dow_in_month if last_dow_in_month in self.date_list else self.date_list[-1]
            else:
                sample_date = self.date_list[-1]
        else:
            raise ValueError(f'No date can be found, date_list is empty.')
        return sample_date
