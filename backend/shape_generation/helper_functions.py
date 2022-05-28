from typing import List
import partridge as ptg
import pandas as pd
import numpy as np


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