####################################
Backend
####################################

Introduction
============
The main use of the ROVE backend is to generate shapes and metrics files from raw transit data. This documentation provides detailed explanations of 
every module in the backend, its input, functions and output. The purpose of this documentation is to familiarize the reader with the design and workflow of ROVE, 
so that one can adapt the tool for their own use.

Instructions
============
As the package has not been published yet, it CANNOT be installed as a standalone package. The suggested method 
for now is to download the code base and run the scipts in a conda environment.

All backend processes start in `backend_main.py`. First, the user needs to specify a few parameters as shown below. ``AGENCY`` 
is the name of the agency that the user is analyzing for. This is also the name of the directories that the input data should be stored in, and where output data will 
be saved to. ``MONTH`` and ``YEAR`` are 2- and 4-character strings of the month and year to be analyzed, e.g. to analyze metrics for Feb 2021, "02" and "2021" should 
be used. ``DATE_TYPE`` is the type of day that the user wants to analyze, namely Weekday (which excludes weekends and holidays), Saturday or Sunday. ``DATA_OPTION`` is 
the concatenated string of the input data that will be used to generate metrics. Currently, only GTFS and AVL data are supported, so the option is either 'GTFS' or 'GTFS-AVL'.

Then, the user will specify which backend module to use, either ``SHAPE_GENERATION`` or ``METRIC_CAL_AGG`` or both. The ability to choose which module to use is useful 
when testing out the backend.

.. code-block:: python

   AGENCY = "WMATA" # CTA, MBTA, WMATA, etc...
   MONTH = "02" # MM in string format
   YEAR = "2021" # YYYY in string format
   DATE_TYPE = "Workday" # Workday, Saturday, Sunday
   DATA_OPTION = 'GTFS-AVL' # GTFS, GTFS-AVL

   SHAPE_GENERATION = True # True/False: whether to generate shapes
   METRIC_CAL_AGG = False # True/False: whether to run metric calculation and aggregation

Workflow
============
The following descriptions aim at providing the reader with details of the workflow of the backend.

Parameter Storing
------------
First, the parameters specified above are passed to and stored in a :py:class:`.ROVE_params` object. These parameters, 
along with others generated within the class object (e.g. list of analysis dates, paths to input and output files, config parameters, etc.), are used 
throughout the backend. Users can create a child class by inheriting :py:class:`.ROVE_params` and use customized attributes or class methods, such as customized 
:py:attr:`.input_paths` for where the input files are stored (be careful with changing the :py:attr:`.output_paths` attribute, since that might impact file loading on 
the frontend), or a customized :py:meth:`.generate_date_list` method that defines how the date list is selected.

Loading and Validation of Input Data
------------
Then, depending on the ``DATA_OPTION``, the backend processes the GTFS and optinally AVL data using the :py:class:`.GTFS` and 
:py:class:`.AVL` objects. Each data class contains methods that are responsible for loading the raw data from a file path, as well as validating the loaded raw data 
to make sure the data table(s) and columns meet the specifications described in :ref:`Input Data Requirements <intput_data_spec>`. 

In the :py:class:`.GTFS` object, two of the most important attributes are :py:attr:`.GTFS.records` 
(a joined GTFS stop_times and trips table with some extra columns) that is used for the calculation and aggregation of scheduled metrics, and 
:py:attr:`.GTFS.patterns_dict` that is used for shape generation. Similarly, the most important attribute in :py:class:`.AVL` is 
:py:attr:`.AVL.records` that is used for the calculation and aggregation of observed metrics.

Shape Generation
------------
Next, the backend enters the Shape Generation module using the class :py:class:`.BaseShape`. A :py:attr:`.GTFS.patterns_dict` and an output path to the 
:ref:`shapes JSON file <shapes_json>` are used to initialize a :py:class:`.BaseShape` object, which contains an attribute :py:attr:`.shapes` that is a data table containing all stop-pair 
shapes information. Note that the attribtue :py:attr:`.shapes` stores exactly the same information as the output shapes JSON file, but in a DataFrame format. 

Metric Calculation and Aggregation
------------
The :py:attr:`.shapes`, :py:attr:`.GTFS.records`, :py:attr:`.AVL.records` and :py:attr:`.data_option` from above are used to generate calculated metrics stored in a 
:py:class:`.MetricCalculation` object. Specifically, :py:attr:`.shapes` is used to provide information on stop spacing. :py:attr:`.GTFS.records` and :py:attr:`.AVL.records` are 
used to generate scheduled and observed metrics, respectively. :py:attr:`.data_option` is used to decide which metrics to calculated, depending on the data option chosen. In 
this module, metrics from each trip are calculated on the stop, timepoint and route levels, and are averaged over all service dates for the same trip if the 'GTFS-AVL' option 
is selected and multiple days' AVL data is provided.

These metrics are then processed in the Metric Aggregation module, where metrics of different trips for the same stop pair, timepoint pair, or route are averaged. Metrics are 
aggregated on stop, stop-aggregated, timepoint, timepoint-aggregated and route levels (different level have a different set of metrics, see :py:class:`.MetricAggregation` for details.)

.. _intput_data_spec:

Input Data Requirements
============
The current implementation of ROVE supports two data sources, ``GTFS`` (GTFS static data) and ``AVL`` data. 

.. warning::
   Note that for ROVE to process either data source, they must follow the requirements outlined below. Data that does not comply 
   with the requirements will likely not work in ROVE, and may result in errors or wrong metric calculations.

GTFS
------------
The GTFS data must be a zipped file (.zip) containing GTFS tables in separate text files (.txt). The GTFS zipped file must locate in the ``backend\data\<agency>\gtfs\``
folder, and named ``GTFS_<AGENCY>_<MONTH>_<YEAR>.zip``, e.g. ``GTFS_MBTA_02_2021.zip``. GTFS data should follow the `Reference for static GTFS data <https://developers.google.com/transit/gtfs/reference/>`_. As documented in :py:class:`.GTFS`, 
by default, ROVE requires that the zipped GTFS data file contains the following data tables and columns.

+-----------------+--------------+
| Table           |  Columns     |
+=================+==============+
| stops.txt       |  stop_id     |
|                 +--------------+
|                 |  stop_code   |
|                 +--------------+
|                 |  stop_name   |
|                 +--------------+
|                 |  stop_lat    |
|                 +--------------+
|                 |  stop_lon    |
+-----------------+--------------+
| routes.txt      |  route_id    |
|                 +--------------+
|                 |  route_type  |
+-----------------+--------------+
| trips.txt       |  route_id    |
|                 +--------------+
|                 |  service_id  |
|                 +--------------+
|                 |  trip_id     |
|                 +--------------+
|                 | direction_id |
|                 +--------------+
|                 | direction_id |
+-----------------+--------------+
| stop_tims.txt   |  trip_id     |
|                 +--------------+
|                 | arrival_time |
|                 +--------------+
|                 |departure_time|
|                 +--------------+
|                 |stop_id       |
|                 +--------------+
|                 |stop_sequence |
+-----------------+--------------+

AVL
------------
If the user wishes to calcualte observed metrics, then an AVL data table must be supplied, and the ``DATA_OPTION`` must be specified as ``GTFS-AVL``. 
The AVL data must be a comma-separated values file (.csv) containing a combinaiton of stop-level Automatic Passenger Counter (APC) and Automatic Vehicle Location (AVL) 
records. The AVL file must locate in the ``backend\data\<agency>\avl\`` 
folder, and named ``AVL_<AGENCY>_<MONTH>_<YEAR>.csv``, e.g. ``AVL_MBTA_02_2021.csv``. Since different transit agency uses different systems and devices to record 
AVL data, ROVE requires that the input AVL data must follow a standard format that contains specific columns detailed as follows.

==============  =====
Column          Definition
==============  =====
route           route ID, must be consistent with GTFS route_id
stop_id         stop ID, must be consistent with GTFS stop_id (preferred) or stop_code
stop_time       date and time of of the stop event
stop_sequence   sequence of the stop in a trip
dwell_time      dwell time at the stop in integer seconds
passenger_load  number of passengers on the bus after leaving the stop
passenger_on    number of passengers that boarded the bus at the stop
passenger_off   number of passengers that alighted the bus at the stop
seat_capacity   number of seats on the bus
trip_id         trip ID, must be consistent with GTFS trip_id
==============  =====

Backend Configuration File
------------
The backend config data must be a JSON file (.json) containing agency-specific parameters listed below. The backend config file must locate in the ``backend\data\<agency>\`` 
folder, and named ``config.json`` (not to be confused with the frontend config file which is named the same but stored in the frontend directory). 

==============  =====
Name            Definition
==============  =====
time_periods    a lookup of time period and the corresponding beginning and end time of the period, used in :py.class:`.MetricAggregation`
speed_range     minimum and maximum speeds that bound the calculated speeds, used in :py.class:`.MetricAggregation`
workalendarPath a workalendar calendar class for the region that the transit agency operates in, see :py:meth:`.generate_date_list` for details
route_type      a lookup of transit mode and list of GTFS route type values, see :py:attr:`.GTFS.mode` for details
==============  =====

An example of the backend config JSON file is given below (the format of the sample snippet is condensed to save space).

.. code-block:: JSON

   {
      "time_periods": {
         "full": [
            [3, 0],
            [27, 0]
         ],
         "am_peak": [
            [5, 0],
            [9, 0]
         ],
         "midday": [
            [9, 0],
            [15, 0]
         ],
         "pm_peak": [
            [15, 0],
            [19, 0]
         ]
      },
      "speed_range": {
         "min": 0,
         "max": 65
      },
      "workalendarPath": "workalendar.usa.massachusetts.Massachusetts",
      "route_type": {
         "bus": [
            "3"
         ]
      }
   }

Output Data Formats
============

.. _shapes_json:
Shapes JSON File
------------
The Shape Generation module outputs a JSON file that contains geometry information of each stop pair of each route pattern found in 
the GTFS data. The JSON file is saved in the ``frontend/static/inputs/<agency>/shapes/`` directory, and named 
``bus-shapes_<AGENCY>_<MONTH>_<YEAR>.json``. A sample snippet of the shapes JSON file is shown here.

.. code-block:: JSON
   
   {
      {
         "geometry": "onrvoAfurqfCvB_e@yQkC}KiAiKaBkH{A{I}CmI{J??SU",
         "route_id": "1",
         "direction": 0,
         "seg_index": "1-64-1",
         "stop_pair": [
            "64",
            "1"
         ],
         "distance": 0.14,
         "pattern": "1-0-1",
         "mode": "bus",
         "timepoint_index": "1-0-1-1"
         },
      {
         "geometry": "cvtvoAbqpqfCyBkCwRoTyVkZwJqL{S}QsDsD??_@]",
         "route_id": "1",
         "direction": 0,
         "seg_index": "1-1-2",
         "stop_pair": [
            "1",
            "2"
         ],
         "distance": 0.173,
         "pattern": "1-0-1",
         "mode": "bus",
         "timepoint_index": "1-0-1-1"
      },
   }

A quick reference of some name fields in the shapes JSON file is given in the table below. See :py:class:`BaseShape` 
for detailed definition of all name fields. Users can use `Vahalla's online tool <http://valhalla.github.io/demos/polyline/?unescape=true&polyline6=true#>`_ 
to verify the geometry generated by Valhalla and stored by ROVE.

+-----------------+------------------------------------------------------------------------------------------------------------------------------------+
| Name            |  Definition                                                                                                                        |
+=================+====================================================================================================================================+
| pattern         |  string concatenation of "<route_id>-<direction_id>-<pattern_count (ordered number of unique patterns of a route and direction)>"  |
+-----------------+------------------------------------------------------------------------------------------------------------------------------------+
| segment_index   |  concatenation of "<route_id>-<stop ID of the first stop in the stop pair>-<stop ID of the second stop in the stop pair>"          |
+-----------------+------------------------------------------------------------------------------------------------------------------------------------+
| geometry        |  encoded polyline of the stop pair (six digits, as specified by `Valhalla <https://valhalla.readthedocs.io/en/latest/decoding/>`_) |
+-----------------+------------------------------------------------------------------------------------------------------------------------------------+


Timepoints Lookup JSON File
------------
A timepoint lookup file is saved from :py:class:`GTFS`, after the static GTFS data is validated. 
The JSON file is saved in the ``frontend/static/inputs/<agency>/timepoints/`` directory, and named 
``timepoints_<AGENCY>_<MONTH>_<YEAR>.json``. A sample snippet of the shapes JSON file is shown here. This lookup table is used 
by the frontend to visualize timepoint-level metrics using stop-pair geometries.

.. code-block:: JSON

   {
      "1-62-63": [
         62,
         64
      ],
      "1-63-64": [
         62,
         64
      ],
   }

For each name-value pair stored in the JSON file, the name is the segment_index of the stop pair, and the value is a list 
of two values, namely the first and last stop of the timepoint pair that this stop pair belongs to.

Stop Name JSON File
------------
A stop name lookup file is saved from :py:class:`GTFS`. 
The JSON file is saved in the ``frontend/static/inputs/<agency>/lookup/`` directory, and named 
``lookup_<AGENCY>_<MONTH>_<YEAR>.json``. A sample snippet of the shapes JSON file is shown here.

.. code-block:: JSON

   {
      "62": {
         "stop_name": "Washington St @ Williams St",
         "municipality": "Boston"
      },
      "63": {
         "stop_name": "Washington St @ Ruggles St",
         "municipality": "Boston"
      }
   }

Aggregated Metric Files
------------
The aggregated metrics are saved in the ``data/<agency>/metrics/`` directory. Two separate pickle files are saved from 
the :py:class:`MetricAggregation` module. The file that stores aggregated metrics by time periods is ``METRICS_<AGENCY>_<MONTH>_<YEAR>.p``. 
The file that stores aggregated metrics by 10-min time intervals is ``METRICS_10MIN_<AGENCY>_<MONTH>_<YEAR>.p``. 

Details of how each file is generated can be found in the documentation of functions :py:meth:`aggregate_by_time_periods` 
and :py:meth:`aggregate_by_10min_intervals`. In short, before pickling, the time-period metrics file is a dict of JSON files, 
where each key is the type of metric in the form of "<time period name>-<aggregation level>-<percentile>", e.g. "am_peak-segment-50" or 
"full-corridor-90", and each value is the corresponding metrics DataFrame normalized to JSON format. On the other hand, the 10-min metrics file 
is a nested dict, the format of which is shown belw in the snippet containing metrics for two 10-min intervals (6:00 am to 6:10 am, and 
6:10 am to 6:20 am).

.. code-block:: JSON

   {
      ((6, 0), (6, 10)): {
         "median": (stop-level metrics, stop-aggregated-level metrics, route-level metrics, timepoint-level metrics, timepoint-aggregated-level metrics),
         "90": (stop-level metrics, stop-aggregated-level metrics, route-level metrics, timepoint-level metrics, timepoint-aggregated-level metrics)
      },
      ((6, 10), (6, 20)): {
         "median": (..),
         "90": (..)
      }
   }

Modules
============

.. toctree::
   :maxdepth: 4

   backend.data_class
   backend.shapes
   backend.metrics
