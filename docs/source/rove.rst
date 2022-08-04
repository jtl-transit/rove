ROVE Backend
============

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   rove.data_class
   rove.shapes
   rove.metrics

Example
-----------
As the package has not been published yet, it CANNOT be install as a standalone package. The suggested method for now is to download the code base and run the scipts in a conda environment.

.. code-block:: python

   AGENCY = "WMATA" # CTA, MBTA, WMATA
   MONTH = "10" # MM in string format
   YEAR = "2021" # YYYY in string format
   DATE_TYPE = "Workday" # Workday, Saturday, Sunday
   DATA_OPTION = 'GTFS-AVL' # GTFS, GTFS-AVL

   SHAPE_GENERATION = True # True/False: whether generate shapes
   METRIC_CAL_AGG = False # True/False: whether run metric calculation and aggregation