# Identifying unregistered houses of multiple occupation.

This python package has been put together to help London Boroughs use data
analytics to identify unregistered houses of multiple occupation (HMOs)
within their borough.

Some familarity with conda environments and using python for data analysis
(e.g. pandas, geopandas, matplotlib, scikit-learn, jupyter notebooks) is needed.

## Envionment Setup

After cloning this repo, a conda environment with all the necessary
dependencies can be created using the `environment.yml` file included by
running the following:

```
conda env create -f environment.yml
```

The environment can then be activated:

```
conda activate hmo_identifier
```

## Project Structure

### data 

This directory will contain all the data related to the project from the raw
data to modelling results. For more information see the [data README.](data/raw/README.md)

### hmo_identifier


This directory contains python modules for collecting, processing and
modelling the data. The modules can be imported in the same way as other
python packages, e.g.

``` python
from hmo_identifier.data import open
```

#### data

Functions for fetching various datasets, which correspond to the details given
in the [data README](data/raw/README.md). 

#### process

Functions for processing data to get it into a useable format. Mainly relate
to address matching and other data matching.


### notebooks

These jupyter notebooks can be run in order to go through the whole process
from collecting raw data, generating features and running models.


## Adjustment for your set up and data

The package includes code for collecting data for the London Borough of Camden
as an example. The functions for collecting open data that is available for
the whole of London include a `borough` parameter to specify which borough to
collect. The functions in `gla` will need to be replaced with functions to
collect your in-house copy of the gazatteer and UK buildings data (which can
be shared by the GLA). The functions in `local` currently collect open data 
available through [Camden's Open Data Portal](https://opendata.camden.gov.uk/)
and will need to be replaced with functions to collect locally available data
relevant to your borough.

Any change you make to the raw data will need to be reflected in later stages.

## Future developments

* Modelling
 



