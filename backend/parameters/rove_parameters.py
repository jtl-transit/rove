from abc import ABCMeta, abstractmethod
import datetime
from numpy import add, empty
from numpy.lib.arraysetops import isin
import pandas as pd
import partridge as ptg
import json
import os
from .helper_functions import day_list_generation
import logging
import traceback

logger = logging.getLogger("backendLogger")

class ROVE_params(object, metaclass=ABCMeta):
    """Data structure that stores all parameters needed throughout the backend.
    """
    def __init__(self, AGENCY, MONTH, YEAR, DATE_TYPE, DATA_OPTION):
        """Instantiate rove parameters.
        """
        
        # str : analyzed transit agency
        self.agency = AGENCY

        # str : analysis month, year and date type
        self.month = MONTH
        self.year = YEAR
        self.date_option = DATE_TYPE

        # list (str) : list of input data used for backend calculations
        self.data_option = DATA_OPTION
        
        # str : suffix used in file names
        self.suffix = f'_{AGENCY}_{MONTH}_{YEAR}'

        # dict <str, str> : dict of paths to input and output data
        self.input_paths = dict()
        self.output_paths = dict()
        
        # dict <str, any> : agency-specific configuration parameters 
        #                   (e.g. time periods, speed range, percentile list, additional files, etc.)
        self.config = dict()

        # list (datetime) : list of dates of given month, year, agency
        self.date_list = []

        # date : sample date for analysis
        self.sample_date = datetime.date.today()

        

        # dict <str, data class> : dict of input data (e.g. GTFS, AVL, ODX)
        self.data = dict()

        # self.__load_required_data()
        # self.__load_additional_data()

        # self.__get_timepoints()

    @property
    def input_paths(self):
        """Get input paths

        Returns:
            dict<str, str> : dict of input paths that includes required and additional files
        """
        return self._input_paths

    @input_paths.setter
    def input_paths(self, additional_input_paths):
        """Set required and optional files needed for backend processes after checking validity

        Args:
            additional_input_paths (dict <str, str>, optional): additional input files. Defaults to {}.
        """
        required_input_paths = {
            'gtfs_inpath': f'data/{self.agency}/gtfs/GTFS{self.suffix}.zip',
            'avl_inpath': f'data/{self.agency}/avl/AVL{self.suffix}.csv',
            'odx_inpath': f'data/{self.agency}/odx/ODX{self.suffix}.csv',
            'config_inpath': f'data/{self.agency}/config/{self.agency}_param_config.json'
        }

        

        if not isinstance(additional_input_paths, dict):
            raise TypeError('additional_input_paths must be a dict')

        req_keys = required_input_paths.keys()
        logger.debug(f'{len(req_keys)} required files: {req_keys}')

        add_keys = additional_input_paths.keys()
        logger.debug(f'{len(add_keys)} additional files: {add_keys}')
            
        input_paths = {**required_input_paths, **additional_input_paths}
        
        # Check that all files are valid
        for name, path in input_paths.items():
            try:
                check_is_file(path)
            except FileNotFoundError as e:
                logger.exception(traceback.format_exc())
                logger.fatal(f'Cannot find file {name} at {path}. Exiting...')
                quit()

        self._input_paths = input_paths
        logger.debug(f'All input files are in place.')

    @property
    def output_paths(self):
        """Get output paths

        Returns:
            dict<str, str> : dict of paths to output files
        """
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

        # if not isinstance(additional_output_paths, dict):
        #     raise TypeError('additional_output_paths must be a dict')

        output_paths = {**default_paths, **additional_output_paths}
        self._output_paths = output_paths

        logger.debug(f'{len(output_paths.keys())} output file paths are set.')

    @property
    def config(self):
        """Get config parameters

        Returns:
            dict <str, any>: dict of config parameters
        """
        return self._config

    @config.setter
    def config(self, config):
        """Set config parameters as defined by user or in the config file

        Args:
            config (dict, optional): dict of config parameters. Defaults to {}.

        Raises:
            AssertionError: if config is not given as a dict
            KeyError: if using config file but the config file path is not in input_paths
        """
        if not isinstance(config, dict):
            raise TypeError('config must be a dict')

        if not config:
            if 'config_inpath' not in self.input_paths:
                raise KeyError('can not find config_inpath in input_paths')
            else:
                config_path = self.input_paths['config_inpath']
                with open(config_path, 'r') as f:
                    self._config = json.load(f)
        else:
            self._config = config

        logger.debug(f'config loaded: {self._config.keys()}')

    @property
    def date_list(self):
        """Get list of dates

        Returns:
            list (datetime): list of dates
        """
        return self._date_list

    @date_list.setter
    def date_list(self, date_list):
        """Get list of dates
        """
        if not isinstance(date_list, list):
            raise TypeError('date_list must be a list')
        
        if not date_list:
            if 'workalendarPath' not in self.config:
                raise KeyError('can not find workalendarPath in input_paths')
            else:
                try:
                    workalendar_path = self.config['workalendarPath']
                    self._date_list = day_list_generation(self.month, self.year, self.date_option, workalendar_path)
                except ValueError as err:
                    logger.exception(traceback.format_exc())
                    logger.fatal(f'ValueError generating date list: {err}. Exiting backend...')
                    quit()
        else:
            if not all(isinstance(d, datetime) for d in date_list):
                raise TypeError('elements in date_list must all be datetime')
            else:
                self._date_list = date_list

        logger.debug(f'date list generated: {len(self.date_list)} {self.date_option} days in {self.month}-{self.year}.')

    @property
    def sample_date(self):

        return self._sample_date

    @sample_date.setter
    def sample_date(self, date):

        if self.date_list:
            if self.date_option=='Workday':
                from calendar import WEDNESDAY
                self._sample_date = self.date_list[max(5, len(self.date_list) - (self.date_list[-1].weekday() - WEDNESDAY) % 7 - 1)]
            else:
                self._sample_date = self.date_list[max(5, len(self.date_list)-1)]
        else:
            self._sample_date = date

    # def __load_required_data(self):
    #     logger.info(f'Loading required data...')
        
    #     # TODO: force to specify which day of the week, because agencies have differnet patterns
    #     # Choose a Wednesday as sample workday or the last day from the date list
    #     if self.date_option=='Workday':
    #         from calendar import WEDNESDAY
    #         gtfs_sample_date = self.date_list[max(5, len(self.date_list) - (self.date_list[-1].weekday() - WEDNESDAY) % 7 - 1)]
    #     else:
    #         gtfs_sample_date = self.date_list[max(5, len(self.date_list)-1)]
    #     logger.debug(f'gtfs sample date is: {gtfs_sample_date}')

    #     gtfs_inpath = self.required_files['gtfs_inpath']
    #     gtfs_route_type = self.config['route_type']
    #     self.data['GTFS'] = GTFS(gtfs_inpath, gtfs_sample_date, gtfs_route_type)

        

    # def __load_additional_data(self):
    #     logger.info(f'Loading additional data...')
    #     if 'additional_data' in self.config and self.config['additional_data'] is not empty:
    #         for name, path  

    #     # # Read in required files
    #     # self.__read_requied_files()

    #     # # Read in additional files
    #     # additional_files = config['additional_files']
    #     # self.__read_additional_files(additional_files)

    #     # # Read in GTFS data
    #     # route_type = config['route_type']
    #     # gtfs_in_path = f"data/MBTA/gtfs/GTFS{self.suffix}.zip"

    #     # try:
    #     #     gtfs = GTFS(gtfs_in_path, sample_date, route_type)
    #     #     self.gtfs_data = gtfs.gtfs_data
    #     # except NotImplementedError as e:
    #     #     logger.info(f'Error encountered while generating GTFS data')
    #     #     logger.fatal(f'Method not implemented. {e}. Exiing backend...')
    #     #     quit()

    

def check_is_file(path):
    """Check that the file exists

    Args:
        path (str): path to a file

    Raises:
        FileNotFoundError: if the given path doesn't point to a valid file

    Returns:
        str : path to file
    """
    if os.path.isfile(path):
        return path
    else:
        raise FileNotFoundError