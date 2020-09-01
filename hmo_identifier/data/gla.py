# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 10:57:32 2020

@author: lirogers
"""

import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import os
import psycopg2
import sys
from hmo_identifier.data import reference


# %% AddressBase/Gazatteer
def gazateer(
    table_name: str,
    dbname: str,
    user: str,
    password: str,
    host: str,
    borough: str = None,
) -> pd.DataFrame:
    """
    Fetch AddressBase/Gazetteer data.
    This is set up for fetching AddressBase from the GLA Postgres Database,
    and will need to be adjusted for your own set up.

    Parameters
    ----------
    table_name : str
        Gazatteer table name in the database.
    dbname : str
        Database name.
    user : str
        Database user name used to authenticate.
    password : str
        Database password used to authenticate.
    host : str
        Database host address.
    borough : str, optional
        London borough to filter table by.
        If none provided the whole of London will be returned.
        The default is None.

    Returns
    -------
    df : pandas.DataFrame
        Gazatteer data.

    """

    cols = [
        "uprn",
        "udprn",
        "class",
        "parent_uprn",
        "class_desc",
        "primary_code",
        "secondary_code",
        "tertiary_code",
        "quaternary_code",
        "primary_desc",
        "secondary_desc",
        "tertiary_desc",
        "quaternary_desc",
        "x_coordinate",
        "y_coordinate",
        "sub_building_name",
        "building_name",
        "building_number",
        "sao_start_number",
        "sao_start_suffix",
        "sao_end_number",
        "sao_end_suffix",
        "sao_text",
        "pao_start_number",
        "pao_start_suffix",
        "pao_end_number",
        "pao_end_suffix",
        "pao_text",
        "street_description",
        "dependent_thoroughfare",
        "thoroughfare",
        "double_dependent_locality",
        "dependent_locality",
        "post_town",
        "town_name",
        "postcode_locator",
    ]
    if borough == None:
        query = f"SELECT {', '.join(cols)} FROM {table_name} WHERE class LIKE 'R%'"
    else:
        query = f"SELECT {', '.join(cols)} FROM {table_name} WHERE administrative_area='{borough.upper()}' AND class LIKE 'R%'"
    con = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    df = pd.read_sql(sql=query, con=con)

    return df


# %% UK Buildings


def uk_buildings(ukb_file: str, ukb_link_file: str, borough: str = None) -> dict:
    """
    Fetch UK Buildings Data
    This is set up for fetching the data from a geodatabase in your
    local system.
    
    More information on UK Buildings data can be found
    (here)[https://www.geomni.co.uk/ukbuildings].
    

    Parameters
    ----------
    ukb_file : str
        file path of UK buildings data (geodatabase or similar).
    ukb_link_file : str
        file path of the UK buildings link file
        (contains lookup between UK buildings unique property number and UPRN)
    borough : str, optional
        London borough to filter table by.
        If none provided the whole of London will be returned.
        The default is None.
    Returns
    -------
    dict
         A dictionary containing the UK Buildings data in 'data' and a lookup
        between UPRN and UBN in 'link_data'.

    """

    boroughs = reference.london_boroughs(borough=borough, inc_geom=True)
    poly_27700 = boroughs.to_crs(27700).unary_union

    df = gpd.read_file(filename=ukb_file, bbox=boroughs)
    df = df.loc[df.geometry.intersects(poly_27700), :]
    df = df.assign(
        ubn=df.unique_building_number.astype(str),
        upn=df.unique_property_number.astype(str),
    ).drop(columns=["unique_building_number", "unique_property_number", "geometry"])

    to_remove = [
        "res_",
        "residential_",
        "_building",
        "_identifier",
        "class",
        "_of",
        "known_",
    ]
    for string in to_remove:
        df.columns = df.columns.str.replace(string, "")

    link_file = pd.read_csv(
        ukb_link_file,
        usecols=["upn", "ubn", "uprn", "udprn"],
        dtype={"upn": str, "ubn": str, "uprn": str, "udprn": str},
    )
    link_file = link_file.merge(df[["ubn"]], how="inner")

    ukb = {"data": df, "link_data": link_file}

    return ukb


# %% main

if __name__ == "__main__":

    if (len(sys.argv) == 1) | (sys.argv[1] == None):
        borough = None
    else:
        borough = sys.argv[1]

    load_dotenv()
    dbname = os.getenv("DATABASE")
    user = os.getenv("USER")
    password = os.getenv("KEY")
    host = os.getenv("HOST")
    table_name = "addbase_ldn_pending"
    gaz = gazateer(
        table_name=table_name,
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        borough=borough,
    )
    file = "data/raw/local/gazatteer.csv"
    print("Saving file", file)
    gaz.to_csv(file, index=False)

    # This will need adjusted for your local file location
    ukb = uk_buildings(
        ukb_file="F:/project_folders/GIS/UK_Map/201910/GLA/GLA/UKBuildings_Edition_7_GLA.gdb",
        ukb_link_file="F:/project_folders/GIS/UK_Map/201910/OSAB_UKBUILDINGS_NN_LINK_FILE_190822.csv",
        borough=borough,
    )
    file = "data/raw/local/uk_buildings.csv"
    print("Saving file", file)
    ukb["data"].to_csv(file, index=False)
    file = "data/raw/local/uk_buildings_link.csv"
    print("Saving file", file)
    ukb["link_data"].to_csv(file, index=False)
