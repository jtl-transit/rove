Installation
=====

Backend Setup
------------

Install and activate the ROVE virtual environment, then import dependencies via ``requirements_back.txt``.

.. code-block:: console
   
   conda create --name @ENV_NAME python=3.7
   conda activate @ENV_NAME
   (.venv) cd @Directory_of_tool
   (.venv) pip install -r requirements_back.txt

Frontend Setup
----------------

Assuming the ROVE virtual environment already exists (if not, follow the first line in the Backend Setup snippet above), 
import dependencies via ``requirements_front.txt``.

.. code-block:: console

   conda activate @ENV_NAME
   (.venv) cd @Directory_of_tool
   (.venv) pip install -r requirements_front.txt

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
(-h and -p, e.g. :code:`flask run -h 10.xxx.xxx.xxx -p 50xx` will run the app on host 10.xxx.xxx.xxx and port 50xx).