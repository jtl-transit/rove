from flask import (Blueprint, redirect, request, url_for, jsonify, session, Response, Flask)
from frontend.auxiliary_functions.dynamic_filter import dynamic_filter_process
from frontend.auxiliary_functions.calculate_difference import paxflow_difference
import json
import pandas as pd
import cProfile
import pstats
import io

bp = Blueprint('load', __name__, url_prefix='/load')

# route for performance metrics data request (MOST LIKELY THIS FUNCTION)
@bp.route("/load_data", methods = ["GET", "POST", "PUT"])
def load_tables():

	# This is used to update data based on time filter
    if request.method == 'PUT':

        profiler = cProfile.Profile()
        profiler.enable()

        # Get agency from session variable
        transit_files = session['transit_files']

        # Get period from ajax post request
        request_file = request.json['file']
        period_id = request.json['predefined']

        # Get timepoint-segment correspondence
        tp_filepath = 'frontend/static/inputs/' + transit_files[request_file]['timepoints']
        with open(tp_filepath) as data_file:
            data = data_file.read()
            tp_lookup = json.loads(data)
        response = {}

        # If custom range selected, run full aggregation script
        if period_id == 0:
            custom_range = request.json['custom_range']
            start_time = tuple(custom_range[0])
            end_time = tuple(custom_range[1])

            data_dict = pd.read_pickle(r'data/' + transit_files[request_file]['full_data_filename'])
            metrics = dynamic_filter_process(data_dict, start_time, end_time)

            response['seg_median'] = metrics['segment-median']
            response['seg_ninety'] = metrics['segment-90']
            response['rte_median'] = metrics['route-median']
            response['rte_ninety'] = metrics['route-90']
            response['cor_median'] = metrics['corridor-median']
            response['cor_ninety'] = metrics['corridor-90']
            response['tp_seg_median'] = metrics['segment-timepoints-median']
            response['tp_seg_ninety'] = metrics['segment-timepoints-90']
            response['tp_cor_median'] = metrics['corridor-timepoints-median']
            response['tp_cor_ninety'] = metrics['corridor-timepoints-90']
            response['timepoint_lookup'] = tp_lookup

        else: # Otherwise just get pre-calculated metrics from pickle file
            metrics = pd.read_pickle(r'data/' + transit_files[request_file]['aggre_data_filename'])

            response['seg_median'] = metrics[str(period_id)+'-segment-median']
            response['seg_ninety'] = metrics[str(period_id)+'-segment-90']
            response['rte_median'] = metrics[str(period_id)+'-route-median']
            response['rte_ninety'] = metrics[str(period_id)+'-route-90']
            response['cor_median'] = metrics[str(period_id)+'-corridor-median']
            response['cor_ninety'] = metrics[str(period_id)+'-corridor-90']
            response['tp_seg_median'] = metrics[str(period_id)+'-segment-timepoints-median']
            response['tp_seg_ninety'] = metrics[str(period_id)+'-segment-timepoints-90']
            response['tp_cor_median'] = metrics[str(period_id)+'-corridor-timepoints-median']
            response['tp_cor_ninety'] = metrics[str(period_id)+'-corridor-timepoints-90']
            response['timepoint_lookup'] = tp_lookup
        
        profiler.disable()  # Stop profiling after the function logic
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')

        return jsonify(response)

    return redirect(url_for("index"))

# route for journey visualization single period data request
@bp.route("/load_viz_data", methods = ["GET", "POST", "PUT"])
def load_viz_data():

	# This is used to update data based on time filter
    if request.method == 'PUT':

        # Get agency from session variable
        filepath_lookup = session['viz_files']

        # Get specifications from ajax post request
        request_file = request.json['file']
        request_level = request.json['level']

        with open(r'data/' + filepath_lookup[request_file][request_level + '_data']) as data_file:
            data = data_file.read()
            segments = json.loads(data)

        return Response(json.dumps(segments), mimetype='application/json; charset=utf-8')

    return redirect(url_for("index"))

# route for journey visualization comparison mode data request
@bp.route("/load_viz_data_comparison", methods = ["GET", "POST", "PUT"])
def load_viz_data_comparison():

	# This is used to update data based on time filter
    if request.method == 'PUT':

        # Get agency from session variable
        filepath_lookup = session['viz_files']

        # Get specifications from ajax post request
        base_file = request.json['base_period']
        comp_file = request.json['comp_period']
        request_level = request.json['level']

        difference = paxflow_difference(filepath_lookup, base_file, comp_file, request_level)

        session['base_location'] = r'data/' + filepath_lookup[base_file][request_level + '_data']
        session['comp_location'] = r'data/' + filepath_lookup[comp_file][request_level + '_data']

        return Response(json.dumps(difference), mimetype='application/json; charset=utf-8')

    return redirect(url_for("index"))


# route for adding map overlays
@bp.route("/load_sublayer", methods = ["GET", "POST", "PUT"])
def load_sublayer():

    if request.method == 'PUT':

        layer_num = request.json
        filepaths = session['background_files']
        filename = filepaths[layer_num]['filename']
        path = 'frontend/static/inputs/' + str(filename)
        with open(path) as f:
            layer = json.load(f)

        return jsonify(layer)

    return redirect(url_for("index"))

# route for adding shape files
@bp.route("/load_shapes", methods = ["GET", "POST", "PUT"])
def load_shapes():

    if request.method == 'PUT':

        layer_num = request.json

        file_info = session['transit_files']
        filename = file_info[layer_num]['shapes_file']
        path = 'frontend/static/inputs/' + str(filename)

        with open(path) as f:
            layer = json.load(f)

        return jsonify(layer)

    return redirect(url_for("index"))

# route for adding bus path shapes
@bp.route("/load_viz_shapes", methods = ["GET", "POST", "PUT"])
def load_viz_shapes():

    if request.method == 'PUT':

        request_file = request.json['file']
        request_level = request.json['level']
        request_type = request.json['type']

        file_info = session['viz_files']

        filename = file_info[request_file][request_level + '_' + request_type]
        path = 'frontend/static/inputs/' + str(filename)

        with open(path) as f:
            layer = json.load(f)

        return jsonify(layer)

    return redirect(url_for("index"))

# route for returning lookup table
@bp.route("/load_lookup", methods = ["GET", "POST", "PUT"])
def load_lookup():

    if request.method == 'PUT':

        layer_num = request.json

        file_info = session['transit_files']
        filename = file_info[layer_num]['lookup_table']
        if len(filename) == 0:
            return '0'

        else:
            path = 'frontend/static/inputs/' + str(filename)

            with open(path) as f:
                layer = json.load(f)

            return jsonify(layer)

    return redirect(url_for("index"))

# Route for loading peak directions
@bp.route("/load_peak", methods = ["GET", "POST", "PUT"])
def load_peak():

    if request.method == 'PUT':

        layer_num = request.json
        # When comparing two time periods, load the peak direction of the second time period
        if len(layer_num) > 1:
            layer_num = layer_num[1]
        file_info = session['transit_files']
        filename = file_info[layer_num]['peak_directions']

        try:
            path = 'frontend/static/inputs/' + str(filename)
            with open(path) as data_file:
                data = data_file.read()
                peak_directions = json.loads(data)
            
            return jsonify(peak_directions)

        except:
            return '0'

    return redirect(url_for("index"))

# Route for passing one period of data in journey visualization comparison mode
@bp.route("/load_period_data", methods = ["GET", "POST", "PUT"])
def load_period_data():

    if request.method == 'PUT':

        level = request.json['level']
        layers = request.json['segment']

        response_dict = {}
        base_file = session['base_location']
        comp_file = session['comp_location']

        with open(base_file) as data_file:
            data = data_file.read()
            base_data = json.loads(data)

        with open(comp_file) as data_file:
            data = data_file.read()
            comp_data = json.loads(data)

        for layer in layers:
            layer_dict = {}
            if level == 'all': # Merge upstream and downstream
                try:
                    base_upstream = base_data[layer]['upstream']
                except KeyError:
                    base_upstream = {}
                try:
                    base_downstream = base_data[layer]['downstream']
                except KeyError:
                    base_downstream = {}

                layer_dict['base'] = {**base_upstream, **base_downstream}

                try:
                    comp_upstream = comp_data[layer]['upstream']
                except KeyError:
                    comp_upstream = {}
                try:
                    comp_downstream = comp_data[layer]['downstream']
                except KeyError:
                    comp_downstream = {}

                layer_dict['comp'] = {**comp_upstream, **comp_downstream}

            else:
                try:
                    layer_dict['base'] = base_data[layer][level]
                except KeyError:
                    layer_dict['base'] = {}
                try:
                    layer_dict['comp'] = comp_data[layer][level]
                except KeyError:
                    layer_dict['comp'] = {}

            response_dict[layer] = layer_dict

        return jsonify(response_dict)

    return redirect(url_for("index"))
