# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 14:51:01 2020

@author: lirogers
"""
import geopandas as gpd
import pandas as pd
from typing import Union


def by_uprn(ref: Union[pd.DataFrame, gpd.GeoDataFrame],
            add: Union[pd.DataFrame, gpd.GeoDataFrame],
            name: str) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    

    Parameters
    ----------
    ref : Union[pd.DataFrame, gpd.GeoDataFrame]
        Reference dataset - usually address base or gazatteer.
        Must have uprn column.
    add : Union[pd.DataFrame, gpd.GeoDataFrame]
        New dataset to add (must also contain uprn column).
    name : str
        Name to append to add columns.

    Returns
    -------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]
        ref with add left joined by UPRN

    """

    add = add.copy()
    # add name label to all columns - so we know where they came from
    add.columns = [f"{col}_{name}" for col in add.columns]
    add.columns = add.columns.str.replace(f"uprn_{name}", "uprn")
    df = ref.merge(add, how='left', on='uprn',
                   indicator=f"merge_{name}")
    return df


def by_geog(ref: Union[pd.DataFrame, gpd.GeoDataFrame],
            add: Union[pd.DataFrame, gpd.GeoDataFrame],
            name: str) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Merge a reference and additional DataFrame by reference geography.

    Parameters
    ----------
    ref : Union[pd.DataFrame, gpd.GeoDataFrame]
        Reference dataset - usually address base or gazatteer.
        Must include at least one reference geography column.
    add : Union[pd.DataFrame, gpd.GeoDataFrame]
        New dataset to add 
        (must contain at least one reference geography column in common with ref).
    name : str
        Name to append to add columns.

    Returns
    -------
    df : Union[pd.DataFrame, gpd.GeoDataFrame]
        ref with add left joined by reference geography.

    """
    geog_regex = f"^(oa|lsoa|msoa|lad|ward)(cd|nm)_{name}"
    add = add.copy()
    # add name label to all columns - so we know where they came from
    add.columns = [f"{col}_{name}" for col in add.columns]
    add.columns = add.columns.str.replace(geog_regex, "\\1\\2")
    df = ref.merge(add, how='left', indicator=f"merge_{name}")

    return df


def by_buffer(ref: gpd.GeoDataFrame, add: gpd.GeoDataFrame,
              name: str, buffer: float,
              sum_cols: list=['median', 'mean', 'max', 'min', 'sum']) -> gpd.GeoDataFrame:

    """
    Merge a reference and additional geo dataframe by summarising features of
    additional within a buffer of points in reference

    Parameters
    ----------
    ref : gpd.GeoDataFrame
        Reference spatial dataset - usually address base or gazatteer.
    add : gpd.GeoDataFrame
        New spatial dataset to add.
    name : str
        Name to append to add columns.
    buffer : float
        Buffer around points in reference to summarise add in.
    sum_cols : TYPE, optional
        What summary variables to produce.
        The default is ['median', 'mean', 'max', 'min', 'sum']: list.

    Returns
    -------
    df : gpd.GeoDataFrame
        ref with a summary of add left joined.

    """

    add = add.copy()
    add_cols = add.columns
    add_agg = {col: sum_cols for col in add_cols if col != 'geometry'}
    ref = ref.copy()
    ref_buff = ref[['uprn', 'geometry']].assign(geometry=ref.buffer(buffer))
    ref_join = (gpd.sjoin(ref_buff, add, how='left')
                .drop(columns=['index_right', 'geometry']))
    ref_sum = (ref_join
               .groupby("uprn")
               .agg(add_agg)
               .reset_index())
    ref_sum.columns = ["_".join([a, b]) for (a, b) in ref_sum.columns]
    ref_sum.columns = ref_sum.columns.str.replace("_$", "")

    ref_sum.columns = [f"{col}_{name}" for col in ref_sum.columns]
    ref_sum.columns = ref_sum.columns.str.replace(f"uprn_{name}", "uprn")
    df = ref.merge(ref_sum)

    return df
