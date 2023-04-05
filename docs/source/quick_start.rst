Quick Start
#####################

Using ROVE for the First Time
============

Step 1. Download the ROVE Source Code
------------
Visit the `ROVE repo on Github <https://github.com/jtl-transit/rove>`_ to download the source code.

Step 2. Set up Conda Environment
------------
If you don't have conda or miniconda already, you can `download miniconda here <https://docs.conda.io/en/latest/miniconda.html>`_. 
Run the executable after it's downloaded. You might need to have admin rights for this installation.

Follow the commands below in miniconda. These commands will change the directory to the ROVE folder
where the source code is located (e.g. the Downloads folder), create a virtual environment for ROVE, 
and build the rove backend as a package within the virtual environment.

.. code-block:: console
   
   (base) cd C:/Users/Name/Downloads/rove
   (base) conda env create -f environment.yml
   (base) conda activate rove
   (rove) conda develop .

Step 3. Prepare GTFS Data
------------
Navigate to ``rove\data\``, then create a folder named ``<agency>`` without space or special characters, 
e.g. ``KCM`` for King County Metro. Then in the ``rove\data\<agency>`` directory, create a folder named ``gtfs``, 
then paste in a zip file of static GTFS data to this directory. The name of the GTFS file should be ``GTFS_<AGENCY>_<MONTH>_<YEAR>.zip``.

The zip file of GTFS data must conform with the :ref:`specification <intput_data_spec>`, otherwise errors will be raised 
during the backend calculation process.

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

Step 4. Run the Backend Process
------------
The backend process can be started in the miniconda command window from the root directory of ROVE. 
Enter the following commands, make sure that the ``rove`` virtual environment is activated.

The example below generates performance metrics using the monthly GTFS data from King County Metro (i.e. ``GTFS_KCM_04_2023.zip``).

.. code-block:: console
   
   (base) conda activate rove
   (rove) python backend/backend_main.py -a "KCM" -m "04" -y "2023"

The ROVE backend is now running. You will see a screen printout of logs as the backend process is going through 
data validation, shape generation, and metric calculation and aggregation.


Step 5. Spin up the Frontend
------------

Set the APP and ENV variables for Flask as follows. By default, the app is run on port 5000 at localhost. 
Spin up the Flask web app by copying and pasting the default url in the browser ``http://127.0.0.1:5000/``.

(Windows Powershell)

.. code-block:: console
   
   $env:FLASK_APP="frontend:create_app(""KCM"")"
   $env:FLASK_ENV="development"
   flask run

(Linux and macOS)

.. code-block:: console

   export FLASK_APP='frontend:create_app("KCM")'
   export FLASK_ENV=development
   flask run

(Windows CMD)

.. code-block:: console
   
   set FLASK_APP=frontend:create_app("KCM")
   set FLASK_ENV=development
   flask run

Note that to run the Flask app on a specific host and port, one can use the handles 
``-h`` and ``-p`` (e.g. :code:`flask run -h 10.xxx.xxx.xxx -p 50xx` will run the app on host 10.xxx.xxx.xxx and port 50xx).

You should now see the ROVE app loaded in the broswer.

Quick Guide to the UI
------------

.. _advanced_usage:

Advanced Usage
============

Command Line Arguments
------------