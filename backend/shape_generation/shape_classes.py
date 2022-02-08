
class Point():

    def __init__(self, 
                point={'lon': None,
                        'lat': None,
                        'type': None,
                        'radius': None,
                        'rank_candidates': 'true',
                        'preferred_side': 'same',
                        'node_snap_tolerance': 0,
                        'street_side_tolerance':0
                        }):
        self.point = point

class Vahalla_Request():
    turn_penalty_factor = 100000 # Penalizes turns in Valhalla routes. Range 0 - 100,000.
    stop_radius = 35 # Radius used to search when matching stop coordinates (meters)
    intermediate_radius = 100 # Radius used to search when matching intermediate coordinates (meters)
    
    stop_distance_threshold  = 1000 # Stop-to-stop distance threshold for including intermediate coordinates (meters)
    maneuver_penalty = 43200 # Penalty when a route includes a change from one road to another (seconds). Range 0 - 43,200. 
    def __init__(self,
                request={'shape': None,
                          'costing': 'bus',
                          'shape_match': 'map_snap',
                          'filters':{
                              'attributes': ['edge.id', 'edge.length', 'shape'],
                              'action':'include'
                              },
                          'costing_options':{
                              'bus':{
                                  'maneuver_penalty': maneuver_penalty
                                  }
                              },
                          'trace_options.turn_penalty_factor': turn_penalty_factor
                          }):
        self.request = request

class Pattern: # Attributes for each unique pattern of stops that create one or more route variant
    def __init__(self, route, direction, stops, trips, stop_coords, shape, timepoints):
        self.route = route
        self.direction = direction
        self.stops = stops
        self.trips = trips
        self.stop_coords = stop_coords
        self.shape = shape
        self.timepoints = timepoints
        self.shape_coords = 0
        self.v_input = 0
        self.coord_types = 0
        self.radii = 0

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