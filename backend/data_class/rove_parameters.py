from abc import ABCMeta, abstractmethod
import calendar
import datetime
from typing import Dict, List
from backend.helper_functions import day_list_generation, check_is_file, check_parent_dir, string_is_month, string_is_date,\
                                    write_to_backend_config
import logging
import json
import partridge as ptg
from geopy.geocoders import Nominatim
from pycountry import subdivisions

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
                output_paths:Dict,
                start_date:str='',
                end_date:str=''):
                                 
                                   
        """Instantiate rove parameters.
        """

        logger.info(f'Generating ROVE parameters...')
        #: Analyzed transit agency, see parameter definition.
        self.agency:str = agency

        #: Analyzed month, see parameter definition.
        self.month:str = month
        if not string_is_month(month):
            # raise ValueError(f"month must be a 2-character stringified numeric value between 1 and 12, e.g. '02', '12'.")
            self.month_name:str = month
        else:
            self.month_name:str = calendar.month_name[int(month)]

        if not year.isnumeric() or len(year) != 4:
            raise ValueError(f"year must be a 4-character stringified numeric value, e.g. '2022'.")
        #: Analyzed year, see parameter definition.
        self.year:str = year

        self.start_date:str = start_date
        self.end_date:str = end_date

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
        self.input_paths:Dict[str, str] = input_paths

        #: Dict of paths to output data, i.e., shapes file, timepoints lookup, stop name lookup, aggregated metrics by time periods, 
        #: aggregated metrics by 10-min intervals.
        self.output_paths:Dict[str, str] = output_paths           

        self.frontend_config = self.get_frontend_config(self.input_paths['frontend_config'])
        #: A dict serving as the lookup for "redValues", i.e. whether the visualization of a metric value is red when the value is high or low. 
        #: This information is required in the frontend_config JSON file, where an object named "redValues" must exist and consist of name-value pairs
        #: of each metric to be calculated, where the value must be "High" or "Low". e.g. "scheduled_frequency" : "Low" means that the scheduled frequency 
        #: of stop pairs/routes will be colored red if the value is low and blue if high; whereas "High" means high values are colored red and low values blue.
        self.redValues:Dict[str, str] = self.frontend_config['redValues']
        
        #: agency-specific configuration parameters for the backend (backend_config), e.g., time periods, speed range, percentile list, additional files, etc. 
        #: (Although there are two config files (frontend_config and backend_config), this attribute storing backend_config data is called "config" for simplicity, 
        #: because frontend_config is only used in the backend to retrieve redValues as described above, and all other reference to "config" in the backend 
        #: is using backend_config.)
        self.backend_config = self.get_backend_config(self.input_paths['backend_config'])
            
        self.iso3166_code = self.get_iso3166_code()

        #: List of dates of the given date_type in the given month and year of the agency.
        self.date_list:List[datetime.datetime] = self.generate_date_list()

        #: sample date for analysis
        # self.sample_date:datetime.datetime = self.__generate_sample_date()
        # logger.info(f'Sample date: {self.sample_date}')
    def get_iso3166_code(self):
        if 'iso3166_code' in self.backend_config:
            readin_code = self.backend_config['iso3166_code']
            if subdivisions.get(country_code=readin_code) is not None or subdivisions.get(code=readin_code):
                return readin_code

        gtfs_path = self.input_paths['gtfs']
        _date, service_ids = ptg.read_busiest_date(gtfs_path)
        view = {
            'routes.txt': {'route_type': self.backend_config['route_type']['bus']}, 
            'trips.txt': {'service_id': service_ids}
            }
        feed = ptg.load_feed(gtfs_path, view)
        stops = feed.stops
        try:
            sample_stop_lat = stops.loc[0, 'stop_lat']
            sample_stop_lon = stops.loc[0, 'stop_lon']
            geolocator = Nominatim(user_agent="geoapiExercises")
            loc = geolocator.reverse(str(sample_stop_lat) + "," + str(sample_stop_lon))
            add = loc.raw['address']
            country_code = add.get('country_code').upper()
            iso_key_name = [k for k in add.keys() if 'ISO3166' in k]
            if len(iso_key_name) > 0:
                iso3166_code = add.get(iso_key_name[0]).upper()
            else:
                iso3166_code = country_code
            self.backend_config['iso3166_code'] = iso3166_code
            write_to_backend_config(self.backend_config, self.input_paths['backend_config'])
            return iso3166_code
        except KeyError:
            logger.fatal(f'Unable to infer ISO3166 code based on a sample stop coordinate from GTFS. Please check that '\
                + f'GTFS stops.txt file contains valid stop_lat and stop_lon columns.')
            quit()

    def get_backend_config(self, fpath:str):
        init_bconfig = {
            "speed_range": {
                "min": 0,
                "max": 65
            },
            "route_type": {
                "bus": ["3"]
            }
        }

        try:
            fpath = check_is_file(fpath)
            with open(fpath) as json_file:
                bconfig = json.load(json_file)
        except FileNotFoundError as e:
            logger.warning(f'Backend config file not found. Creating a default backend config file.')
            check_parent_dir(fpath)
            bconfig = init_bconfig
        
        return bconfig

    def get_frontend_config(self, fpath:str):
        default_directionLabels = {
            "0": "NB/EB",
            "1": "SB/WB"
        }
        default_timePeriods = {
            "1": "full",
            "2": "am_early",
            "3": "am_peak",
            "4": "midday",
            "5": "pm_peak",
            "6": "early_night",
            "7": "late_night"
        }
        default_periodNames = {
            "full": "Full Day (24 hrs)",
            "am_early": "AM Early (4AM-6AM)",
            "am_peak": "AM Peak (6AM-9AM)",
            "midday": "Mid-Day (9AM-3PM)",
            "pm_peak": "PM Peak (3PM-7PM)",
            "early_night": "Early Night (7PM-11PM)",
            "late_night": "Late Night (11PM-3AM)"
        }
        default_periodRanges = {
            "full": [3, 27],
            "am_early": [4, 6],
            "am_peak": [6, 9],
            "peak_of_am_peak": [8, 9],
            "midday": [9, 15],
            "pm_peak": [15, 19],
            "peak_of_pm_peak": [17, 18],
            "early_night": [19, 23],
            "late_night": [23, 27]
        }
        default_redValues = {
            "scheduled_headway": "High",
            "observed_headway": "High",
            "scheduled_running_time": "High",
            "observed_running_time": "High",
            "excess_wait_time": "High",
            "crowding": "High",
            "boardings": "High",
            "on_time_performance": "High",
            "scheduled_wait_time": "High",
            "observed_wait_time": "High",
            "passenger_flow": "High",
            "passenger_load": "High",
            "vehicle_congestion_delay": "High",
            "passenger_congestion_delay": "High",
        }
        init_fconfig = {
            'transitFileProp': {},
            'vizFileProp': {},
            'URL_prefix': '',
            'units': {},
            'redValues': default_redValues,
            'directionLabels': default_directionLabels,
            'backgroundLayerProp': {},
            'timePeriods': default_timePeriods,
            'periodNames': default_periodNames,
            'periodRanges': default_periodRanges,
            'altRouteIDs': {},
            'garageAssignments': {},
            'routeTypes': {}
        }

        try:
            fpath = check_is_file(fpath)
            with open(fpath) as json_file:
                fconfig = json.load(json_file)
        except FileNotFoundError as e:
            logger.warning(f'Frontend config file not found. Creating a default frontend config file.')
            check_parent_dir(fpath)
            fconfig = init_fconfig

        this_transitFileProp = {
            'name': f'{self.agency} {self.month_name} {self.year} ({self.data_option})',
            'full_data_filename': self.output_paths['metric_calculation_aggre_10min'],
            'aggre_data_filename': self.output_paths['metric_calculation_aggre'],
            'shapes_file': self.output_paths['shapes'],
            'lookup_table': self.output_paths['stop_name_lookup'],
            'timepoints': self.output_paths['timepoints'],
            'peak_directions': f'{self.agency}/peak/peak_{self.agency}_{self.month}_{self.year}.json'
        }
        # remove substring in the file path before agency name
        this_transitFileProp = {name: path[path.find(self.agency):] for name, path in this_transitFileProp.items()}
        fconfig = self.get_transitFileProp_or_vizFileProp('transitFileProp', fconfig, this_transitFileProp)

        this_vizFileProp = {
            "name": f'{self.agency} {self.month_name} {self.year} ({self.data_option})',
            "stop_shapes": f"{self.agency}/shapes/segment_shapes_{self.agency}_{self.month}_{self.year}.json",
            "tp_shapes": f"{self.agency}/shapes/timepoint_shapes_{self.agency}_{self.month}_{self.year}.json"
        }
        this_vizFileProp = {name: path[path.find(self.agency):] for name, path in this_vizFileProp.items()}
        fconfig = self.get_transitFileProp_or_vizFileProp('vizFileProp', fconfig, this_vizFileProp)

        return fconfig

    def get_transitFileProp_or_vizFileProp(self, name:str, fconfig:Dict, this_sub_dict:Dict):
        try:
            tf_keys = fconfig[name].keys()
            tf_name_order_dict = {v['name']: k for k, v in fconfig[name].items()}
            if this_sub_dict['name'] in tf_name_order_dict.keys():
                transitFileProp_id = tf_name_order_dict[this_sub_dict['name']]
            else:
                transitFileProp_id = str(max([int(i) for i in tf_keys if i.isnumeric()]) + 1)
            fconfig[name][transitFileProp_id] = this_sub_dict
        except:
            fconfig[name] = {0: this_sub_dict}
        return fconfig

    def generate_date_list(self)->List[datetime.datetime]:
        """Generate a list of dates of date_type between the start_date and end_date or in the given month and year. For example, if the user specified to 
        analyze "MBTA", "02", "2021", "Workday" as the agency, month, year and date_type and did not specify a start_date or end_date, then this method will 
        return a list of datetime objects that are the workdays (no weekend or holiday) in Feb 2021 in the state/country represented by the iso3166_code; 
        otherwise, if a start_date and end_date are specified, then this method will return a list of datetime objects between the start date and end date, but the 
        A "workalendarPath" object must exist in the backend_config JSON file for the method to know which state/region to lookup the holiday calendar for, and its value must
        be the workalendar class for the region that the agency operates in. E.g. for the MBTA, this name-value pair is specified in the backend_config file: 
        "workalendarPath": "workalendar.usa.massachusetts.Massachusetts". For details on how to find the correct workalendar class for your region, refer to 
        https://workalendar.github.io/workalendar/iso-registry.html.
        :raises KeyError: No workalendarPath is found in config.
        :return: List of dates.
        :rtype: List[datetime.datetime]
        """
        
        if string_is_date(self.start_date) and string_is_date(self.end_date):
            start_date = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
            end_date = datetime.datetime.strptime(self.end_date, '%Y-%m-%d')
            delta = end_date - start_date   # returns timedelta
            date_list = []
            for i in range(delta.days + 1):
                day = start_date + datetime.timedelta(days=i)
                date_list.append(day.date())
        else:
            month = int(self.month)
            year = int(self.year)
            num_days = calendar.monthrange(year, month)[1]
            date_list = [datetime.date(year, month, day) for day in range(1, num_days+1)]

        filtered_date_list = day_list_generation(date_list, self.date_type, self.iso3166_code)

        logger.debug(f'date list generated: {len(filtered_date_list)} {self.date_type} days in {self.month}-{self.year}.')
        return filtered_date_list
