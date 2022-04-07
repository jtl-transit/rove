Installation
=====

Back-End Setup
------------

Install virtual environment and activate. 
Import dependencies via ``requirements_back.txt``:

.. code-block:: console
   
   conda create --name @ENV_NAME python=3.7
   conda activate @ENV_NAME
   (.venv) cd @Directory_of_tool
   (.venv) pip install -r requirements_back.txt

Front-End Setup
----------------

Install virtual environment and activate. 
Import dependencies via ``requirements_front.txt``:

.. code-block:: console
   
   >>>conda create --name @ENV_NAME python=3.7
   >>>conda activate @ENV_NAME
   >>>(.venv) cd @Directory_of_tool
   >>>(.venv) pip install -r requirements_front.txt

Set APP and ENV variables (Windows CMD):
   >>> set FLASK_APP=frontend:create_app("AGENCY_NAME")
   >>> set FLASK_ENV=development
   >>> flask run

