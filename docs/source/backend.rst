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

All backend processes start in :doc:`../../backend/backend_main`. First, the user needs to specify a few parameters as shown below. ``AGENCY`` 
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

First, the parameters specified above are passed to and stored in a :py:class:`.ROVE_params` object. These parameters, 
along with others generated within the class object (e.g. list of analysis dates, paths to input and output files, config parameters, etc.), are used 
throughout the backend. Users can create a child class by inheriting :py:class:`.ROVE_params` and use customized attributes or class methods, such as customized 
:py:attr:`.input_paths` for where the input files are stored (be careful with changing the :py:attr:`.output_paths` attribute, since that might impact file loading on 
the frontend), or customized :py:meth:`.generate_date_list` method that defines how the date list is selected.

Then, depending on the ``DATA_OPTION``, the backend processes the GTFS and optinally AVL data using the :py:class:`.GTFS` and 
:py:class:`.AVL` objects. In the :py:class:`.GTFS` object, two of the most important attributes are :py:attr:`.GTFS.records` 
(a joined GTFS stop_times and trips table with some extra columns) that is used for the calculation and aggregation of scheduled metrics, and 
:py:attr:`.GTFS.patterns_dict` that is used for shape generation. Similarly, the most important attribute in :py:class:`.AVL` is 
:py:attr:`.AVL.records` that is used for the calculation and aggregation of observed metrics.

Next, the backend enters the Shape Generation module using the class :py:class:`.BaseShape`. A :py:attr:`.GTFS.patterns_dict` and an output path to the 
shapes JSON file are used to initialize a :py:class:`.BaseShape` object, which contains an attribute :py:attr:`.shapes` that is a data table containing all stop-pair 
shapes information. Note that the data table stored in :py:attr:`.shapes` stores exactly the same information as in the output shapes JSON file, but in a table format. 
An example of an object stored in the shapes JSON file is shown below. The ``pattern`` of a stop-to-stop segment (the segment between a stop pair) is the string 
concatenation of "<route_id>-<direction_id>-<pattern_count (ordered number of unique patterns of a route and direction)>". The ``segment_index`` is the concatenation of 
"<route_id>-<stop ID of the first stop in the stop pair>-<stop ID of the second stop in the stop pair>". The ``geometry`` is the encoded polyline of the stop pair 
(six digits, as specified by Valhalla here: https://valhalla.readthedocs.io/en/latest/decoding/). 

.. code-block:: JSON

	{
		"pattern": "10A-1-1",
		"route_id": "10A",
		"direction": 1,
		"seg_index": "10A-3625-3641",
		"stop_pair": [
			"3625",
			"3641"
		],
		"timepoint_index": "10A-1-1-0",
		"mode": "bus",
		"geometry": "kgcciApna~qCo@pK{Eny@",
		"distance": 0.099
	}


Input Data Requirements
============

GTFS
------------
The current implementation of ROVE supports two 

AVL
------------
avl data Requirements

Output Data Requirements
============

Shapes JSON
------------

Modules
============

.. toctree::
   :maxdepth: 4

   backend.data_class
   backend.shapes
   backend.metrics
