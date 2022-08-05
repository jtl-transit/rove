Shape Generation
===================

base\_shape module
------------------------------

.. automodule:: backend.shapes.base_shape
   :members:
   :undoc-members:
   :show-inheritance:

   .. autoattribute:: backend.shapes.base_shape.PARAMETERS

   .. code-block:: python

      PARAMETERS = {
         'stop_distance_meter': 100, # Stop-to-stop distance threshold for including intermediate coordinates (meters)
         'maximum_radius_increase': 100, # Self-defined parameter to limit the search area for matching coordinates (meters)
         'stop_radius': 35, # Radius used to search when matching stop coordinates (meters)
         'intermediate_radius': 100, # Radius used to search when matching intermediate coordinates (meters)
         'radius_increase_step': 10 # Step size used to increase search area when Valhalla cannot find an initial match (meters)
      }