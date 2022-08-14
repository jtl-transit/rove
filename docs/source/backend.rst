Backend
============

Introduction
-----------
The main use of the ROVE backend is to generate shapes and metrics files from raw transit data. This documentation provides detailed explanations of 
every module in the backend, its input, functions and output. The purpose of this documentation is to familiarize the reader with the design and workflow of ROVE, 
so that one can adapt the tool for their own use.

Instructions
-----------
As the package has not been published yet, it CANNOT be installed as a standalone package. The suggested method 
for now is to download the code base and run the scipts in a conda environment.

All backend processes start in :doc:`../../backend_main`. First, the user needs to specify a few parameters as shown below. ``AGENCY`` 
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
-----------
The following descriptions aim at providing the reader with details of the workflow of the backend.

First, the parameters specified above are passed to and stored in a :py:class:`backend.data_class.rove_parameters.ROVE_params` object. These parameters, 
along with others generated within the class object, are used throughout the backend.

Modules
-----------

.. toctree::
   :maxdepth: 4

   backend.data_class
   backend.shapes
   backend.metrics
