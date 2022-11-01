"""

This program takes two time periods and finds the difference in passenger
flows between those periods for each stop/timepoint in a bus network. 

Nick Caros, July 2021

"""

import json

def paxflow_difference(filepath_lookup, base_file, comp_file, request_level):

    diff_dict = {}    

    with open(r'data/selectlink/' + filepath_lookup[base_file][request_level + '_data']) as data_file:
        data = data_file.read()
        base_data = json.loads(data)
        
    with open(r'data/selectlink/' + filepath_lookup[comp_file][request_level + '_data']) as data_file:
        data = data_file.read()
        comp_data = json.loads(data)
        
    # Loop over base data and find corresponding comp data
    for init_segment in base_data:
        diff_dict[init_segment] = {}
        
        for mode in base_data[init_segment]:
            diff_dict[init_segment][mode] = {}
            
            for next_segment in base_data[init_segment][mode]:
                
                base_val = base_data[init_segment][mode][next_segment]
                try:
                    comp_val = comp_data[init_segment][mode][next_segment]
                    diff = comp_val - base_val
                except KeyError:
                    diff = base_val * -1
                    
                diff_dict[init_segment][mode][next_segment] = diff
    
    # Then check for any pax flows in comp data that was not in base data
    for init_segment in comp_data:
        
        if init_segment not in diff_dict:
            diff_dict[init_segment] = {}
            
        for mode in comp_data[init_segment]:
            
            if mode not in diff_dict[init_segment]:
                diff_dict[init_segment][mode] = {}

            for next_segment in comp_data[init_segment][mode]:
                
                if next_segment not in diff_dict[init_segment][mode]:

                    diff_dict[init_segment][mode][next_segment] = comp_data[init_segment][mode][next_segment]
                
    return diff_dict