# Instructions for adding a new transit agency

### 0. Example Scenario

MTA_Manhattan---

There are five main steps to adding a new transit agency and generating GTFS-based metrics. They are listed below and then described in detail in the text that follows.

1. Clone this repository to the machine that will be used to run ROVE.
2. Install the required software packages and Python libraries. 
3. Create a configuration file using the template provided.
4. Save a standard GTFS feed in the designated folder.
5. Run the metric generation program for the new agency and desired time period. 

### 1. Clone the repository

Instructions on how to clone a repository from GitHub are available [here.](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)

### 2. Install the requirements

These instructions assume you have already installed Anaconda. An Anaconda distribution can be downloaded [here.]( https://www.anaconda.com/products/distribution)

Open the Anaconda Prompt command line interface. Install a virtual environment using Anaconda and activate it (replace @ENV_NAME with the name of your virtual environment):

```
conda create --name @ENV_NAME python=3.7
conda activate @ENV_NAME
```

For example:

```
conda create --name rove python=3.7
conda activate rove
```

Import dependencies via requirements_back.txt (replace @ROVE_DIRECTORY with the location of your cloned repo from step 1):

```
cd @ROVE_DIRECTORY
pip install -r requirements.txt
```

### 3. Create a configuration file

The configuration file is the main location of all agency-specific information that ROVE needs to create and visualize performance metrics. 
There are a large number of fields that must be filled in, such as the location of the service area and the time periods used for analysis. 
Copy the [template provided](data/templates/config/template_param_config.json) to a new folder `data/@AGENCYNAME/config/` and enter your agency-specific information according to the conventions for each field below:

- `directionLabels`: Used to display directions within ROVE. A dictionary of string keys and string values, where the keys represent the GTFS `direction_id` field and the values represent the desired labels that will appear in ROVE.
- `backgroundLayerProp`: Used to include static GIS layers in ROVE if desired. The keys of the higher level dictionary are consecutive numbers (as strings) starting at zero. The lower level dictionary has a "name" field and a "filename" field. "name" is the label that will appear in ROVE, and "filename" is the location of the geoJSON file that contains the GIS layer information.
- `timePeriods`: The pre-set time periods for which performance metrics will be calculated. The keys are consecutive numbers starting at 1, and the values are a string used to identify the period.
- `periodNames`: The labels for the time periods defined above. The keys are the string identifiers defined above, and the values are the description of the time period that will appear in ROVE.
- `periodRanges`: The start and end point of the preset time periods. The keys are the string identifiers defined above, and the values are a list of two numbers representing the start and end point of a period in numerical hours. Fractional values are accepted (i.e. \[16.5, 18.5\] for 4:30 PM - 6:30 PM). 
- `altRouteIDs`: An optional parameter that converts some or all GTFS route IDs into a different value to be shown in ROVE. The keys are the GTFS `route_id` field and the values are the converted route IDs.
- `garageAssignments`: An optional parameter that allows for groups of routes to be filtered by garage. The keys are the garage name that will appear in ROVE, and each value is a list of GTFS `route_id` fields that are assigned to that garage.
- `routeTypes`: An optional parameter that allows for groups of routes to be filtered by the route type (e.g. express route, local route). The keys are the route type label that will appear in ROVE, and each value is a list of GTFS `route_id` fields that are assigned to that garage.

- `transitFileProp`: Not needed at this time. This field will be auto-completed once the metrics are generated. 
- `vizFileProp`: Not needed at this time. This field will be auto-completed if the required metrics are generated.
- `URL_prefix`: Not needed at this time. This field can be used to apply a generic URL prefix throughout the backend to make it easier to deploy the tool on an agency server. 

Once the new configuration file is complete, rename the file "@AGENCYNAME_param_config.json". 

### 4. Save a GTFS feed in the designated folder

Save a standard GTFS feed (in .zip format) representing the schedule for which you would like to generate performance metrics in the following folder: `/data/@AGENCYNAME/gtfs/`. The name of the GTFS feed file must be in the following format GTFS_AGENCYNAME_MONTH_YEAR.zip where MONTH and YEAR are numeric values (e.g. GTFS_MBTA_05_2022 for the May 2022 schedule).

### 5. Run the metric generation program

Return to the command line interface. 
Run the `run_backend` function from backend_main.py to generate the performance metrics.
The function takes four input arguments: 
- AGENCY: the name of the agency used in the prior steps
- MONTH: a two digit number representing the month for which performance metrics are being generated
- YEAR: a four digit number representing the year for which performance metrics are being generate
- DATE_OPTION: either "Workday", "Saturday" or "Sunday, depending on the date to be used in calculated the schedule metrics.

So to calculate the metrics for the MBTA for weekdays in May 2022, the following code would be entered:
```
from backend_main import run_backend
run_backend(MBTA, 05, 2022, "Workday")
```

This project is licensed under the terms of the GPLv2 license.
