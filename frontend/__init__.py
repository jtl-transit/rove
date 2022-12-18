import os
import json
from flask import Flask, render_template, session
from . import load

def create_app(agency, test_config=None):

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY='dev')

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    app.register_blueprint(load.bp)

    with open(r'frontend/static/inputs/'+agency+'/config.json') as data_file:
        data = data_file.read()
        config = json.loads(data)

    try:
        with open(r'frontend/static/inputs/'+agency+'/presets.json') as data_file:
            data = data_file.read()
            presets = json.loads(data)
    except:
        presets = 0

    transit_files = config['transitFileProp']
    viz_files = config['vizFileProp']
    url_prefix = config['URL_prefix']
    units = config['units']
    red_values = config['redValues']
    direction_labels = config['directionLabels']
    background_layers = config['backgroundLayerProp']
    time_periods = config['timePeriods']
    period_names = config['periodNames']
    period_ranges = config['periodRanges']
    alt_route_ids = config['altRouteIDs']
    garage_assignments = config['garageAssignments']
    route_types = config['routeTypes']
    background_files = config['backgroundLayerProp']

    # Initialize the map
    @app.route(url_prefix + "/")
    def index():

        # Add some information as a session variable for use in other routes
        session['viz_files'] = viz_files
        session['background_files'] = background_files
        session['agency'] = agency
        session['transit_files'] = transit_files

        # Initialize the map
        return render_template("index.html",
                               transit_files = transit_files,
                               viz_files = viz_files,
                               units = units,
                               red_values = red_values,
                               direction_labels = direction_labels,
                               background_layers = background_layers,
                               time_periods = time_periods,
                               period_names = period_names,
                               period_ranges = period_ranges,
                               alt_route_ids = alt_route_ids,
                               garage_assignments = garage_assignments,
                               route_types = route_types,
                               presets = presets
                               )

    return app