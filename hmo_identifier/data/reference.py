# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 14:48:21 2020
Import reference geographical data sets
@author: lirogers
"""


import pandas as pd
import requests
import geopandas as gpd
from shapely import wkt
from typing import Union
import sys


# %% Main ONS Geography Linked Data Query Function
def query_ons(
    parent: str = "E12000007",
    code_filter: str = "",
    inc_uri: bool = False,
    inc_parent: bool = False,
    inc_geom: bool = False,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Query the ONS Geography Linked Data to find geographies within parent areas.

    Uses SPARQL. More details here: http://statistics.data.gov.uk/sparql

    Parameters
    ----------
    parent : str, optional
        ode of parent geography to search within.
        The default is "E12000007" (London Region).
    code_filter : str, optional
        First 3 characters of the geography level required,
        e.g. "E09" for London borough, "E00" for census output areas.
        The default is "" (returns all available child geographies).
    inc_uri : bool, optional
         Include the geography URI (unique linked data url).
         The default is False.
    inc_parent : bool, optional
        Include details of parent geography. The default is False.
    inc_geom : bool, optional
        Include geometry of geography. Will return a geopandas geodataframe
        if set to True The default is False.

    Returns
    -------
    df : pd.DataFrame or gpd.GeoDataFrame
        A dataset containing details of the requested geographies.

    """
    url = "http://statistics.data.gov.uk/sparql.json"
    # Set up SPARQL query
    if code_filter != "":
        code_filter = f"?uri sedef:code seid:{code_filter}."
    if inc_geom:
        geouri_query = "?uri geo:hasGeometry ?geouri."
        geom_query = "?geouri geo:asWKT ?geometry."
    else:
        geouri_query = ""
        geom_query = ""
    query = f"""
    PREFIX sgdef: <http://statistics.data.gov.uk/def/statistical-geography#>
    PREFIX sedef: <http://statistics.data.gov.uk/def/statistical-entity#>
    PREFIX seid: <http://statistics.data.gov.uk/id/statistical-entity/>
    PREFIX sgid: <http://statistics.data.gov.uk/id/statistical-geography/>
    PREFIX not: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ont: <http://publishmydata.com/def/ontology/foi/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    SELECT *
    WHERE {{
      ?uri ont:within sgid:{parent}.
      {code_filter}
      ?uri not:notation ?code.
      ?uri sgdef:status ?status.
      {geouri_query}
      {geom_query}
      OPTIONAL {{ ?uri ont:displayName ?name }}
      OPTIONAL {{ ?uri sgdef:parentcode ?parent }}
    }}
    """
    # Request and reformat data
    r = requests.get(url, params={"query": query})
    r.raise_for_status()
    data = r.json()["results"]["bindings"]
    data = {i: data[i] for i in range(0, len(data))}
    df = pd.DataFrame.from_dict(data, orient="index")
    empt_dict = {"type": pd.np.NAN, "value": pd.np.NAN}
    if "parent" not in df.columns:
        df = df.assign(parent=pd.np.NAN)
    df["parent"] = df["parent"].apply(lambda x: x if x == x else empt_dict)
    df = df.apply(lambda x: [d.get("value") for d in x], axis=0)
    df = df.loc[df.status == "live", :]
    df = df.sort_values(by="code").drop(columns=["status"]).reset_index(drop=True)
    if "name" not in df.columns:
        df = df.assign(name=df.code)
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("name")))
    cols.insert(0, cols.pop(cols.index("code")))
    df = df.reindex(columns=cols)
    # Adjust for requested columns
    if inc_uri is False:
        df = df.drop(columns=["uri"])

    if inc_parent:
        if "parent" in df.columns:
            # parent is returned as the full URI - extract code
            df.parent = df.parent.str.extract("/(E[0-9]{8}$)")
    else:
        df = df.drop(columns=["parent"])

    if inc_geom:
        df.geometry = df.geometry.apply(wkt.loads)
        df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        df = df.drop(columns=["geouri"])
        cols = df.columns.tolist()
        cols.insert(len(cols), cols.pop(cols.index("geometry")))
        df = df.reindex(columns=cols)

    return df


# %% London Boroughs
def london_boroughs(
    borough: str = None, inc_geom: bool = False
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Fetch reference data on London Boroughs from ONS Geography Linked Data.

    Parameters
    ----------
    borough : str, optional
        London borough name. The default is None (all boroughs returned).
    inc_geom : bool, optional
        Include geometry of borough(s). Will return a geopandas GeoDataFrame
        if set to True The default is False.

    Returns
    -------
    df : pd.DataFrame or gpd.GeoDataFrame
        A dataset containing details of the requested borough(s).

    """
    df = query_ons(parent="E12000007", code_filter="E09", inc_geom=inc_geom)
    df = df.rename(columns={"code": "ladcd", "name": "ladnm"})
    if borough is not None:
        df = df.loc[df.ladnm == borough, :]
    return df


# %% London Wards
def london_wards(
    borough: str = None, combined: bool = False, inc_geom: bool = False
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Fetch reference data for London Wards from ONS Geography Linked Data.

    Parameters
    ----------
    borough : str, optional
        London borough name. The default is None (all boroughs returned).
    combined : bool, optional
        Census combined or standard wards. The default is False.
    inc_geom : bool, optional
        Include geometry of the wards. Will return a GeoDataFrame if set
        to True. The default is False.

    Returns
    -------
    df : pd.DataFrame or gpd.GeoDataFrame
        A dataset containing details of the requested wards.

    """
    boroughs = london_boroughs()
    if borough is not None:
        boroughs = boroughs.loc[boroughs.ladnm == borough, :]
    if combined:
        code_filter = "E36"
    else:
        code_filter = "E05"

    df = pd.concat(
        [
            query_ons(
                parent=row.ladcd, code_filter=code_filter, inc_geom=inc_geom
            ).assign(ladcd=row.ladcd, ladnm=row.ladnm)
            for i, row in boroughs.iterrows()
        ]
    )

    if combined:
        df = df.rename(columns={"code": "wardcmcd", "name": "wardcmnm"})
    else:
        df = df.rename(columns={"code": "wardcd", "name": "wardnm"})

    return df


# %% London Output Areas (All levels)
def london_output_areas(
    borough: str = None, inc_geom: bool = False
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Fetch reference data for London Output Areas from ONS Geography Linked Data.

    Parameters
    ----------
    borough : str, optional
        London borough name. The default is None.
    inc_geom : bool, optional
        Include geometry of the Output Areas. Will return a GeoDataFrame
        if set to True. The default is False.

    Returns
    -------
    output_areas : pd.DataFrame or gpd.GeoDataFrame
        A dataset containing details of the requested u

    """
    boroughs = london_boroughs()
    if borough is not None:
        boroughs = boroughs.loc[boroughs.ladnm == borough, :]
    borough_codes = boroughs.ladcd.tolist()
    oas = pd.DataFrame()
    for code in borough_codes:
        oa = query_ons(
            parent=code, code_filter="E00", inc_parent=True, inc_geom=inc_geom
        )
        oas = oas.append(oa, sort=True)
    oas = oas.rename(columns={"code": "oacd", "name": "oanm", "parent": "lsoacd"})
    lsoas = query_ons(code_filter="E01", inc_parent=True)
    lsoas = lsoas.rename(
        columns={"code": "lsoacd", "name": "lsoanm", "parent": "msoacd"}
    )
    msoas = query_ons(code_filter="E02")
    msoas = msoas.assign(
        ladnm=msoas.name.str.extract("^([A-Za-z ]+) [0-9]+", expand=False)
    ).rename(columns={"code": "msoacd", "name": "msoanm"})
    output_areas = oas.merge(lsoas).merge(msoas).merge(boroughs)
    if inc_geom:
        cols = output_areas.columns.tolist()
        cols.insert(len(cols), cols.pop(cols.index("geometry")))
        output_areas = output_areas.reindex(columns=cols)

    return output_areas


# %% Main
if __name__ == "__main__":

    if (len(sys.argv) == 1) | (sys.argv[1] is None):
        borough = None
    else:
        borough = sys.argv[1]

    boroughs = london_boroughs(borough=borough, inc_geom=True)
    boroughs.to_file("data/raw/reference/boroughs.shp")
    wards = london_wards(borough=borough, inc_geom=True)
    wards.to_file("data/raw/reference/wards.shp")
    oas = london_output_areas(borough=borough, inc_geom=True)
    oas.to_file("data/raw/reference/output_areas.shp")
