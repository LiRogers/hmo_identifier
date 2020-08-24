# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:47:45 2020

@author: lirogers
"""

import pandas as pd


def hmo_register() -> pd.DataFrame:
    """
    Fetch the Camden HMO Register from
    (here)[https://opendata.camden.gov.uk/Housing/HMO-Licensing-Register/x43g-c2rf].
    Will need to be adjusted for your own borough.
    

    Returns
    -------
    df : pd.DataFrame
        HMO register as a pandas dataframe.

    """
    url = "https://opendata.camden.gov.uk/api/views/x43g-c2rf/rows.csv?accessType=DOWNLOAD"
    df = pd.read_csv(url)
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    return df


def social_housing() -> pd.DataFrame:
    """
    Fetch data on Camden Housing stock from 
    (here)[https://opendata.camden.gov.uk/Housing/Camden-Housing-Stock/pkzy-2qkt].
    Will need to be adjusted for your own borough.

    Returns
    -------
    df : pd.DataFrame
        Details of social housing as a pandas dataframe.

    """
    url = "https://opendata.camden.gov.uk/api/views/pkzy-2qkt/rows.csv?accessType=DOWNLOAD"
    df = pd.read_csv(url)
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    return df


if __name__ == "__main__":

    df = hmo_register()
    file = "data/raw/local/hmo_register.csv"
    print("Saving file", file)
    df.to_csv(file, index=False)
    df = social_housing()
    file = "data/raw/local/social_housing.csv"
    print("Saving file", file)
    df.to_csv(file, index=False)
