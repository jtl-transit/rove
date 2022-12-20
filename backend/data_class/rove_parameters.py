from abc import ABCMeta, abstractmethod
from calendar import WEDNESDAY
import datetime
from typing import List, Dict
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
                data_option:str,
                input_paths:Dict,
                output_paths:Dict):
        """Instantiate rove parameters.
        """

        logger.info(f'Generating ROVE parameters...')
        #: Analyzed transit agency, see parameter definition.
        self.agency:str = agency

        # str : analysis month, year, date type and data option
        if not month.isnumeric() or len(month) != 2 or int(month) < 1 or int(month) > 12:
            raise ValueError(f"month must be a 2-character stringified numeric value between 1 and 12, e.g. '02', '12'.")
        self.month = month

        if not year.isnumeric() or len(year) != 4:
            raise ValueError(f"year must be a 4-character stringified numeric value, e.g. '2022'.")
        self.year = year

        SUPPORTED_DATE_TYPES = ['Workday', 'Saturday', 'Sunday']
        if date_type not in SUPPORTED_DATE_TYPES:
            raise ValueError(f"Invalid date_type: {date_type}, must be one of: {SUPPORTED_DATE_TYPES}.")
        self.date_type = date_type

        SUPPORTED_DATA_OPTIONS = ['GTFS', 'GTFS-AVL']
        if data_option not in SUPPORTED_DATA_OPTIONS:
            raise ValueError(f"Invalid data_option: {data_option}, must be one of: {SUPPORTED_DATA_OPTIONS}.")
        self.data_option = data_option

        self.suffix = f'_{self.agency}_{self.month}_{self.year}'

        #: Dict of paths to input data, i.e. gtfs, avl, backend_config, frontend_config, shapes file (if shape generation has been run previously).
        self.input_paths:Dict[str, str] = input_paths

        #: Dict of paths to output data, i.e., shapes file, timepoints lookup, stop name lookup, aggregated metrics by time periods, 
        #: aggregated metrics by 10-min intervals.
        self.output_paths:Dict[str, str] = output_paths
        
        # dict <str, any> : agency-specific configuration parameters 
        #                   (e.g. time periods, speed range, percentile list, additional files, etc.)
        with open(self.input_paths['frontend_config']) as json_file:
            frontend_config = json.load(json_file)
            self.frontend_config = frontend_config
            self.redValues = frontend_config['redValues']
        
        with open(self.input_paths['backend_config']) as json_file:
            self.backend_config = json.load(json_file)
            
        # list (datetime) : list of dates of given month, year, agency
        self.date_list = self.__generate_date_list()

        # date : sample date for analysis
        self.sample_date = self.__generate_sample_date()
        logger.info(f'Sample date: {self.sample_date}')
        logger.info(f'parameters generated')


    def __generate_date_list(self)->List[datetime.datetime]:
        """Generate list of dates of date_type in the given month and year.

        :raises KeyError: No workalendarPath is found in config.
        :return: List of dates.
        :rtype: List[datetime.datetime]
        """

        if 'workalendarPath' not in self.backend_config:
            raise KeyError('can not find workalendarPath in the config file')
        else:
            try:
                workalendar_path = self.backend_config['workalendarPath']
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
