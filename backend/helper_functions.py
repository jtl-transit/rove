"""
This is the script containing functions
for running the parameters for different transit agencies.

@author: Xiaotong Guo & Nick Caros (MIT JTL-Transit Lab)
"""

import datetime
import os
import shutil
import logging
import workalendar.usa
from pathlib import Path
from typing import Dict, List
import pandas as pd
import json
import numpy as np

logger = logging.getLogger("backendLogger")

def day_list_generation(MONTH, YEAR, DATE_TYPE, workalendarPath):
    """Generate list of dates of the given month, year and option

    Args:
        MONTH (str): A month (MM) between 1 and 12
        YEAR (str): A year (YYYY)
        DATE_TYPE (str): one of "Workday", "Saturday", "Sunday"
        workalendarPath (str): the path to the workalendar data of the analyzed agency

    Returns:
        list(datetime): list of dates

    Raises:
        ValueError: if MONTH is out of [1, 12] range or if DATE_TYPE is not one of "Workday", "Saturday", "Sunday"
    """

    MONTH = int(MONTH)
    YEAR = int(YEAR)

    date_type_list = ["Workday", "Saturday", "Sunday"]
    if MONTH<1 or MONTH >12:
        raise ValueError('MONTH must be between 1 and 12.')
    elif DATE_TYPE not in date_type_list:
        raise ValueError(f'date_type must be one of {date_type_list}.')
        
    NEXT_YEAR = (datetime.date(YEAR, MONTH, 1) + datetime.timedelta(days=31)).year
    NEXT_MONTH = (datetime.date(YEAR, MONTH, 1) + datetime.timedelta(days=31)).month
    total_number_days = (datetime.date(NEXT_YEAR, NEXT_MONTH, 1) - datetime.date(YEAR, MONTH, 1)).days

    ALL_DAYS = []
    for i in range(1, total_number_days+1):
        ALL_DAYS.append(datetime.date(YEAR, MONTH, i))
        holiday_calendar = eval(workalendarPath + '()')
        HOLIDAYS = [i[0] for i in holiday_calendar.holidays()]

    WORKDAYS = []
    SATURDAYS = []
    SUNDAYS = []
    for day in ALL_DAYS:
        if day in HOLIDAYS:
            continue
        elif day.isoweekday() == 6:
            SATURDAYS.append(day)
        elif day.isoweekday() == 7:
            SUNDAYS.append(day)
        else:
            WORKDAYS.append(day)

    if DATE_TYPE == "Workday":
        return WORKDAYS
    elif DATE_TYPE == "Saturday":
        return SATURDAYS
    elif DATE_TYPE == "Sunday":
        return SUNDAYS

def check_is_file(path, extension=None):
    """Check that the file exists.

    Args:
        path (str): path to a file

    Raises:
        FileNotFoundError: the given path doesn't point to a valid file

    Returns:
        str : path to file
    """

    if os.path.isfile(path):
        if extension and not path.endswith(extension):
            raise ValueError(f'Not a valid {extension} file.')
        return path
    else:
        raise FileNotFoundError(f'Invalid file: {path}. Please double check a valid file is located at the specified location.')

def check_parent_dir(path):
    
    parent = Path(path).parent
    if os.path.isdir(parent):
        logger.debug(f'Parent directory already exists: {parent}.')
    else:
        os.mkdir(parent)
        logger.debug(f'Created parent directory: {parent}.')
    return path

def check_is_dir(path, overwrite=False, create_if_none=False):
    """Check that the directory exists.

    Args:
        path (str): path to a directory for output files
        overwrite (bool, optional): whether to overwrite the directory if it exists already.
                                    Defaults to True.
        create_if_none (bool, optional): whether to create the directory if it doesn't exist.
                                        Defaults to False.

    Raises:
        NotADirectoryError: the directory doesn't exist

    Returns:
        str: path to the output directory
    """

    if os.path.isdir(path):
        if overwrite:
            shutil.rmtree(path)
            os.mkdir(path)
            logger.debug('Directory pruned: {path}.')
        return path
    elif create_if_none:
        os.mkdir(path)
        logger.debug('Directory created: {path}.')
    else:
        raise NotADirectoryError('Directory does not exist: {path}')



def get_hash_of_stop_list(stops:List[str]) -> int:
    """Get hash of a list of stops IDs of a trip using the following formula:
        hashing function: hash = sum((index of stop in list * stop_value)**3).
        Note that this method could potentially lead to duplicate hashes for:
            e.g. lists of stops that are in the same set (i.e. have same length and unique stops) but ordered differently.
        Therefore, it is important to order the stops by stop_sequence before using this function.

    Args:
        stops: list of stop IDs, 
    Returns:
        string of hash value of the list of stops. Use string to avoid using large integers.
    """

    # hash_1 = sum((2*np.arange(1,len(stops)+1))**2)
    hash_2 = 0
    for i in range(len(stops)):
        hash_2 += ((i+1) * get_stop_value(stops[i]))**3
    # hash = hash_1 + hash_2

    return str(hash_2)

def get_stop_value(stop:str) -> int:
    """Get numerical value of a stop, either the original numerical value or 
        sum of unicode values of all characters
    Args:
        every element must be a string literal
    Returns:
        value of a stop
    """

    try:
        num = int(stop)
    except ValueError as err: # the given stop ID is not a numerical value
        num = sum([ord(x) for x in stop])
    return num

def read_shapes(path:str):
        
    try:
        in_path = check_is_file(path)
        with open(in_path) as shapes_file:
            shapes_json = json.load(shapes_file)
            shapes = pd.json_normalize(shapes_json)
            shapes['stop_pair'] = shapes.apply(lambda x: tuple(x.stop_pair), axis=1)
            specs = {
                'pattern':'string',
                'distance':'float64'
            }
            cols = list(specs.keys())
            shapes[cols] = shapes[cols].astype(dtype=specs)
            return shapes
    except FileNotFoundError:
        logger.exception(f'No shapes file found.')
        return None

def load_csv_to_dataframe(path:str, id_cols=[]):
        """Read in csv data and return a dataframe

        Args:
            path (str): path to the csv file

        Returns:
            DataFrame: dataframe read from the csv file
        
        Raises:
            ValueError: the given file path does not end with .csv
        """
        
        in_path = check_is_file(path, '.csv')
        try:
            data = pd.read_csv(in_path)
            data.columns = data.columns.str.lower()
            if id_cols:
                data = data.dropna(subset=id_cols)
                for col in id_cols:
                    numeric_rows=data[[col]].applymap(lambda x: isinstance(x, (int, float)))[col]
                    data.loc[numeric_rows, col]=data.copy().loc[numeric_rows, col].astype(np.int64)
        except pd.errors.EmptyDataError as err:
            logger.warning(f'{err}: Data read from {in_path} is empty!')
            data = pd.DataFrame()
        return data

def check_dataframe_column(df:pd.DataFrame, column_name, criteria='0or1'):
    """Check that column_name column exists in gtfs_table_name and satisfys the criteria.
        Criteria: 0or1 - only values 0 or 1 exists in the column.
                (other criteria can be added)
    Args:
        column_name (str): column to be checked
        criteria (str): type of criteria to be used. Defaults to '0or1'.

    Raises:
        ValueError: either gtfs_table_name is not stored in validated_data or 
                    column_name is not in the GTFS table
    """

    # check that the records table has column_name column
    if column_name not in df.columns:
        raise ValueError(f'{column_name} column not in dataframe.')

    valid_criteria = ['0or1']

    if criteria == '0or1':
        # fill in NaN values in column with 0
        df[column_name] = df[column_name].fillna(0).astype(int)
        # check that only values 1 and 0 exist in column_name column
        if df.loc[~df[column_name].isin([0,1]),column_name].shape[0]>0:
            raise ValueError(f'The {column_name} column cannot contain any value other than 0 or 1.')
        num_ones = df.loc[df[column_name] == 1,column_name].shape[0]
        logger.debug(f'number of {column_name}: {num_ones}')
    
    else:
        raise ValueError(f'Invalid criteria. Select from: {valid_criteria}.')

def series_to_datetime(date_pd_series:pd.Series, format:str=None):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates = {date:pd.to_datetime(date, format=format) for date in date_pd_series.unique()}
    return date_pd_series.map(dates)

def convert_stop_ids(raw_data_alias:str, raw_data:pd.DataFrame, raw_data_stop_col:str, gtfs_stops:pd.DataFrame, use_column='stop_id'):
    
    # table with two columns: xstop_id_gtfs, xstop_code_gtfs
    gtfs_stop_ids_lookup = gtfs_stops[['stop_id', 'stop_code']].drop_duplicates().rename(columns={'stop_id': 'xstop_id_gtfs', 'stop_code': 'xstop_code_gtfs'}).reset_index(drop=True)

    raw_data = raw_data.copy()
    logger.debug(f'total number of {raw_data_alias} records: {raw_data.shape[0]}')
    raw_data_merge_gtfs_stop_id = pd.merge(raw_data, gtfs_stop_ids_lookup, left_on=raw_data_stop_col, right_on='xstop_id_gtfs', how='inner')
    raw_data_gtfs_stop_id_match_count = raw_data_merge_gtfs_stop_id.shape[0]
    logger.debug(f'count of {raw_data_alias} records with matched gtfs stop_id: {raw_data_gtfs_stop_id_match_count}')

    raw_data_merge_gtfs_stop_code = pd.merge(raw_data, gtfs_stop_ids_lookup, left_on=raw_data_stop_col, right_on='xstop_code_gtfs', how='inner')
    raw_data_gtfs_stop_code_match_count = raw_data_merge_gtfs_stop_code.shape[0]
    logger.debug(f'count of {raw_data_alias} records with matched gtfs stop_code: {raw_data_gtfs_stop_code_match_count}')

    if raw_data_gtfs_stop_id_match_count == 0 and raw_data_gtfs_stop_code_match_count == 0:
        raise ValueError(f'no matched stop_id can be found')
    else:
        if raw_data_gtfs_stop_id_match_count > raw_data_gtfs_stop_code_match_count:
            raw_table = raw_data_merge_gtfs_stop_id
            matched_col = 'GTFS stop_id'
        else:
            raw_table = raw_data_merge_gtfs_stop_code
            matched_col = 'GTFS stop_code'

        if use_column == 'stop_id':
            raw_table[raw_data_stop_col] = raw_table['xstop_id_gtfs']
        elif use_column == 'stop_code':
            raw_table[raw_data_stop_col] = raw_table['xstop_code_gtfs']
        raw_table = raw_table.drop(columns=['xstop_id_gtfs', 'xstop_id_gtfs'])
        logger.debug(f'{raw_data_stop_col} in {raw_data_alias} records matches with {matched_col}, replaced with matching GTFS {use_column}')

    return raw_table.dropna(subset=[raw_data_stop_col]).reset_index(drop=True)


def convert_trip_ids(raw_data_alias:str, raw_data:pd.DataFrame, raw_data_trip_col:str, gtfs_trips:pd.DataFrame):
    """Convert raw data trip_id to GTFS trip_id or scheduled_trip_id depending on which columns
        has more matched values.
    Args:
        raw_data (DataFrame): raw AVL data, assume having column: trip_id
        feed (GTFS Feed): contains trips table
    Raises:
        DataError: neither GTFS trip_id nor scheduled_trip_id has more than 90% matches with AVL trip_id
    Returns:
        DataFrame: AVL data with trip_id converted if needed
    """
    if 'scheduled_trip_id' not in gtfs_trips.columns:
        logger.debug(f'the gtfs trips table does not contain scheduled_trip_id, nothing to convert from')
        return raw_data
    else:
        trips = gtfs_trips.copy()
        gtfs_trip_ids = trips['trip_id'].dropna().drop_duplicates().reset_index(drop=True)
        
        raw_data = raw_data.copy()
        logger.debug(f'total number of {raw_data_alias} records: {raw_data.shape[0]}')

        raw_data_merge_gtfs_trip_id = pd.merge(raw_data, gtfs_trip_ids, left_on=raw_data_trip_col, right_on='trip_id', how='inner')
        raw_data_gtfs_trip_match_count = raw_data_merge_gtfs_trip_id.shape[0]
        logger.debug(f'count of {raw_data_alias} records with matched gtfs trip_id: {raw_data_gtfs_trip_match_count}')

        gtfs_trip_ids_lookup = gtfs_trips[['trip_id', 'scheduled_trip_id']].dropna().drop_duplicates().rename(columns={'trip_id': 'xtrip_id_gtfs'}).reset_index(drop=True)
        gtfs_trip_ids_lookup['scheduled_trip_id'] = gtfs_trip_ids_lookup['scheduled_trip_id'].astype(int).astype('string')

        raw_data_merge_gtfs_sch_trip_id = pd.merge(raw_data, gtfs_trip_ids_lookup, left_on=raw_data_trip_col, right_on='scheduled_trip_id', how='inner')
        raw_data_gtfs_sch_trip_match_count = raw_data_merge_gtfs_sch_trip_id.shape[0]
        logger.debug(f'count of {raw_data_alias} records with matched gtfs scheduled_trip_id: {raw_data_gtfs_sch_trip_match_count}')

        if raw_data_gtfs_trip_match_count == 0 and raw_data_gtfs_sch_trip_match_count == 0:
            raise ValueError(f'no matched trip_id can be found')
        if raw_data_gtfs_trip_match_count > raw_data_gtfs_sch_trip_match_count:
            raw_data_merge_gtfs_trip_id[raw_data_trip_col] = raw_data_merge_gtfs_trip_id['trip_id']
            if raw_data_trip_col != 'trip_id':
                raw_data_merge_gtfs_trip_id = raw_data_merge_gtfs_trip_id.drop(columns=['trip_id'])
            return_data = raw_data_merge_gtfs_trip_id
            logger.debug(f'{raw_data_trip_col} matches with trip_id, replaced with matching GTFS trip_id')
        else: 
            raw_data_merge_gtfs_sch_trip_id[raw_data_trip_col] = raw_data_merge_gtfs_sch_trip_id['xtrip_id_gtfs']
            if raw_data_trip_col != 'scheduled_trip_id':
                raw_data_merge_gtfs_sch_trip_id = raw_data_merge_gtfs_sch_trip_id.drop(columns=['scheduled_trip_id', 'xtrip_id_gtfs'])
            return_data = raw_data_merge_gtfs_sch_trip_id
            logger.debug(f'{raw_data_trip_col} matches with scheduled_trip_id, replaced with matching GTFS trip_id')

    return return_data.dropna(subset=[raw_data_trip_col]).reset_index(drop=True)

def write_metrics_to_frontend_config(metric_names:Dict, path):

    fpath = check_is_file(path)
    metrics_list = list(metric_names.keys())
    metrics = {
        k: {
            'label': v,
            'order': metrics_list.index(k)
        }
        for k, v in metric_names.items()
    }
    with open(fpath, 'r+') as f:
        config = json.load(f)
        config['units'] = metrics # <--- add `id` value.
        f.seek(0)        # <--- should reset file position to the beginning.
        json.dump(config, f)
        f.truncate()     # remove remaining part