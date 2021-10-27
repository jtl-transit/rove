"""
This is the script containing functions
for running the parameters for different transit agencies.

@author: Xiaotong Guo & Nick Caros (MIT JTL-Transit Lab)
"""

import datetime
import workalendar.usa
import partridge as ptg
import pandas as pd
import random
import numpy as np

def day_list_generation(MONTH, YEAR, DATE_OPTION, workalendarPath):
    """
    Function for generating day list for workdays, saturdays and sundays
    """

    MONTH = int(MONTH)
    YEAR = int(YEAR)

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

    if DATE_OPTION == "Workday":
        return WORKDAYS
    elif DATE_OPTION == "Saturday":
        return SATURDAYS
    elif DATE_OPTION == "Sunday":
        return SUNDAYS

