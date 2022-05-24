## Instructions for adding a new transit agency

There are five main steps to adding a new transit agency and generating GTFS-based metrics. They are listed below and then described in detail in the text that follows.

1. Clone this repository to the machine that will be used to run ROVE.
2. Install the necessary software packages and Python libraries. 
3. Create a configuration file using the template provided.
4. Place a standard GTFS feed in the designated folder.
5. Run the metric generation program for the new agency and desired time period. 

### 1. Clone the repository to the machine that will be used to run ROVE

Instructions on how to clone a repository from GitHub are available [here.](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)

### 2. Install the necessary software packages and Python libraries

These instructions assume you have already installed Anaconda. An Anaconda distribution can be downloaded [here.]( https://www.anaconda.com/products/distribution)

Install a virtual environment using Anaconda and activate it (replace @ENV_NAME with the name of your virtual environment):
```
conda create --name @ENV_NAME python=3.7
conda activate @ENV_NAME
```

Import dependencies via requirements_back.txt (replace @ROVE_DIRECTORY with the location of your cloned repo from step 1):

```
cd @ROVE_DIRECTORY
pip install -r requirements_back.txt
```

