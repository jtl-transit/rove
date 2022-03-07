from abc import abstractmethod
import logging
import traceback
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Set, List

logger = logging.getLogger("backendLogger")

class BaseShape():

    def __init__(self, data):
        
        self._data = data

        self._patterns = self.generate_patterns()

        self._shapes = pd.DataFrame()

    @property
    def data(self):

        return self._data

    @property
    def patterns(self):

        return self._patterns

    @abstractmethod
    def generate_patterns(self):

        pass

    @property
    def shapes(self):

        return self._shapes

    @abstractmethod
    def generate_shapes(self):

        pass

    # helpful functions
    def get_trip_hash(self, stops:List[str]) -> int:
        """Get hash of a list of stops IDs of a trip
            hashing function: hash = sum(2*index of stop in list)**2 + stop_value**3)
        Args:
            stops: list of stop IDs, 
        Returns:
            hash value of stop
        """
        hash_1 = sum((2*np.arange(1,len(stops)+1))**2)
        hash_2 = 0
        for stop in stops:
            hash_2 += (self.get_stop_value(stop))**3
        hash = hash_1 + hash_2

        return hash

    def get_stop_value(self, stop:str) -> int:
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
        except TypeError as err:
            logger.exception(traceback.format_exc())
            logger.fatal(f'stop ID {stop} is of type {type(stop)}, cannot be converted to int. Exiting backend...')
            quit()
        return num

# main for testing
def __main__():
    logger.info(f'Starting shape generation...')
    print(f'in main method of shape gen')


if __name__ == "__main__":
    __main__()

class Vahalla_Point():

    def __init__(self, 
                lon,
                lat,
                type='break_through',
                radius=35,
                rank_candidates='true',
                preferred_side='same',
                node_snap_tolerance=0,
                street_side_tolerance=0):
        
        self.point_parameters = {
            {'lon': lon,
            'lat': lat,
            'type': type,
            'radius': radius,
            'rank_candidates': rank_candidates,
            'preferred_side': preferred_side,
            'node_snap_tolerance': node_snap_tolerance,
            'street_side_tolerance':street_side_tolerance
            }
        }

class Vahalla_Request():
    turn_penalty_factor = 100000 # Penalizes turns in Valhalla routes. Range 0 - 100,000.
    maneuver_penalty = 43200 # Penalty when a route includes a change from one road to another (seconds). Range 0 - 43,200. 
    
    default_filters = {
                        'attributes': ['edge.id', 'edge.length', 'shape'],
                        'action':'include'
                        }
    default_costing_options = {
                        'bus':{
                            'maneuver_penalty': maneuver_penalty
                            }
                        }
    def __init__(self,
                shape=None,
                costing='bus',
                shape_match='map_snap',
                fiters=default_filters,
                costing_options=default_costing_options,
                trace_options_turn_penalty_factor=turn_penalty_factor):

        self.request_parameters = {'shape': shape,
                'costing': costing,
                'shape_match': shape_match,
                'filters': fiters,
                'costing_options': costing_options,
                'trace_options.turn_penalty_factor':trace_options_turn_penalty_factor 
                }

class Pattern: # Attributes for each unique pattern of stops that create one or more route variant
    
    def __init__(self, route:str, direction:int, stops:List[str], trips:List[str], stop_coords:List[Tuple[float, float]]):
        self.route = route
        self.direction = direction
        self.stops = stops
        self.trips = trips
        self.stop_coords = stop_coords
        self.shape = 0
        self.timepoints = []
        self._shape_coords = stop_coords
        self._v_input = Vahalla_Point(0,0)
        self._coord_types = ['break_through'] * len(stop_coords)
        self._radii = [35] * len(stop_coords)


    @property
    def shape_coords(self):

        return self._shape_coords

    @shape_coords.setter
    def shape_coords(self, shape_coords:List[Tuple[float, float]]):
        self._shape_coords = shape_coords

    @property
    def coord_types(self):
        return self._coord_types
    
    @coord_types.setter
    def coord_types(self, coord_types:List[str]):
        if len(coord_types) != len(self.shape_coords):
            raise ValueError(f'Error specifying coord_types of Pattern object. Length of coord_types must match that of shape_coords.')
        else:
            self._coord_types = coord_types
    
    @property
    def radii(self):
        return self._radii
    
    @radii.setter
    def radii(self, radii:List[int]):
        if len(radii) != len(self.radii):
            raise ValueError(f'Error specifying radii of Pattern object. Length of radii must match that of shape_coords.')
        else:
            self._radii = radii

    @property
    def v_input(self):
        return self._v_input
    
    @v_input.setter
    def v_input(self, v_input):

        self._v_input = v_input

class Segment: # Attributes for each segment which make up a pattern
    def __init__(self, geometry, distance):
        self.geometry = geometry
        self.distance = distance
            
class Corridor: # Attributes for each corridor
    def __init__(self, edges, segments):
        self.edges = edges
        self.segments = segments
        self.passenger_shared = []
        self.stop_shared = []
        
    def get_edges(self):
        return self.edges
    
    def get_segments(self):
        return self.segments
    
    def get_pass_shared(self):
        return self.passenger_shared
    
    def get_stop_shared(self):
        return self.stop_shared