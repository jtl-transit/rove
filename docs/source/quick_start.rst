Quick Start
=====
Step 1. Download the ROVE Source Code
------------
Visit the `ROVE repo on Github <https://github.com/jtl-transit/rove>`_ to download the source code.

Step 2. Set up Conda Environment
------------
If you don't have conda or miniconda already, you can `download miniconda here <https://docs.conda.io/en/latest/miniconda.html>`_. 
Run the executable after it's downloaded. You might need to have admin rights for this installation.

In miniconda, change directory to the ROVE folder, where the source code is located (e.g. the Downloads folder). 
Then create the virtual environment for ROVE.

.. code-block:: console
   
   (base) cd C:/Users/Name/Downloads/rove
   (base) conda env create -f environment.yml
   (base) conda activate rove
   (rove) 

Step 3. Prepare GTFS Data
------------
Navigate to ``rove\data\``, then create a folder named ``<agency>`` without space or special characters, 
e.g. ``KCM`` for King County Metro. Then in the ``rove\data\<agency>`` directory, create a folder named ``gtfs``, 
then paste in a zip file of static GTFS data to this directory. The name of the GTFS file should be ``GTFS_<AGENCY>_<MONTH>_<YEAR>.zip``.

The zip file of GTFS data should conform with the :ref:`specification <intput_data_spec>`.

Below is an example of the folder structure after adding GTFS files to the relevant directory.

::

   rove
   ├── backend
   ├── data
   │   └── KCM
   |       └── gtfs
   |           ├── GTFS_KCM_12_2022.zip
   |           ├── GTFS_KCM_04_2023.zip
   |           └── GTFS_KCM_Q1_2023.zip
   └── frontend


Set APP and ENV variables and spin up the Flask web app. 

(Linux and macOS)

.. code-block:: console

   export FLASK_APP='frontend:create_app("AGENCY_NAME")'
   export FLASK_ENV=development
   flask run


(Windows CMD)

.. code-block:: console
   
   set FLASK_APP=frontend:create_app("AGENCY_NAME")
   set FLASK_ENV=development
   flask run

(Windows Powershell)

.. code-block:: console
   
   $env:FLASK_APP="frontend:create_app(""AGENCY_NAME"")"
   $env:FLASK_ENV="development"
   flask run

Note that to run the Flask app on a specific host and port, one can use the handles 
``-h`` and ``-p`` (e.g. :code:`flask run -h 10.xxx.xxx.xxx -p 50xx` will run the app on host 10.xxx.xxx.xxx and port 50xx).