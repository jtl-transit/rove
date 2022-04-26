"""
This is the script containing functions
for running the parameters for different transit agencies.

@author: Xiaotong Guo & Nick Caros (MIT JTL-Transit Lab)
"""

import datetime
from typing import List
import workalendar.usa
import partridge as ptg
import pandas as pd
import random
import numpy as np
import os
import shutil
import logging

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

def check_is_file(path):
    """Check that the file exists.

    Args:
        path (str): path to a file

    Raises:
        FileNotFoundError: the given path doesn't point to a valid file

    Returns:
        str : path to file
    """
    if os.path.isfile(path):
        return path
    else:
        raise FileNotFoundError(f'Invalid file: {path}. Please double check a valid file is located at the specified location.')

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
        hashing function: hash = sum(2*index of stop in list)**2 + stop_value**3).
        Note that this method could potentially lead to duplicate hashes for:
            e.g. lists of stops that are in the same set (i.e. have same length and unique stops) but ordered differently.
        Therefore, it is important to order the stops by stop_sequence before using this function.

    Args:
        stops: list of stop IDs, 
    Returns:
        hash value of the list of stops
    """
    hash_1 = sum((2*np.arange(1,len(stops)+1))**2)
    hash_2 = 0
    for stop in stops:
        hash_2 += (get_stop_value(stop))**3
    hash = hash_1 + hash_2

    return hash

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