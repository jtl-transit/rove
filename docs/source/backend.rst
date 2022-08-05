Backend
============

Introduction
-----------
The main use of the ROVE backend is to generate shapes and metrics files from raw transit data. This documentation provides detailed explanations of 
every module in the backend, its input, functions and output. The purpose of this documentation is to familiarize the reader with the design and workflow of ROVE, 
so that one can adapt the tool for their own use.

Modules
-----------

.. toctree::
   :maxdepth: 4

   backend.data_class
   backend.shapes
   backend.metrics

Example
-----------
As the package has not been published yet, it CANNOT be installed as a standalone package. The suggested method for now is to download the code base and run the scipts in a conda environment.

.. code-block:: python

   AGENCY = "WMATA" # CTA, MBTA, WMATA
   MONTH = "10" # MM in string format
   YEAR = "2021" # YYYY in string format
   DATE_TYPE = "Workday" # Workday, Saturday, Sunday
   DATA_OPTION = 'GTFS-AVL' # GTFS, GTFS-AVL

   SHAPE_GENERATION = True # True/False: whether generate shapes
   METRIC_CAL_AGG = False # True/False: whether run metric calculation and aggregation