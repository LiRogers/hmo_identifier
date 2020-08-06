# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:47:45 2020

@author: lirogers
"""

import pandas as pd


def hmo_register():
    """
    

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    url = "https://opendata.camden.gov.uk/api/views/x43g-c2rf/rows.csv?accessType=DOWNLOAD"
    df = pd.read_csv(url)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    
    return df


def social_housing():
    """
    

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    url = "https://opendata.camden.gov.uk/api/views/pkzy-2qkt/rows.csv?accessType=DOWNLOAD"
    df = pd.read_csv(url)
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    
    return df


if __name__ == "__main__":
    
    df = hmo_register()
    df.to_csv("data/raw/local/hmo_register.csv", index=False)
    df = social_housing()
    df.to_csv("data/raw/local/social_housing.csv", index=False)
    
