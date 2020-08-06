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
def gazateer(table_name, dbname, user, password, host, borough = None):

    cols = ['uprn', 'udprn', 'class', 'parent_uprn', 'class_desc',
       'primary_code', 'secondary_code', 'tertiary_code', 'quaternary_code',
       'primary_desc', 'secondary_desc', 'tertiary_desc', 'quaternary_desc',
       'x_coordinate', 'y_coordinate',
       "sub_building_name", "building_name", "building_number",
                "sao_start_number", "sao_start_suffix", "sao_end_number",
                "sao_end_suffix", "sao_text",
                "pao_start_number", "pao_start_suffix", "pao_end_number",
                "pao_end_suffix", "pao_text",
                "street_description",
                "dependent_thoroughfare",
                "thoroughfare",
                "double_dependent_locality",
                "dependent_locality", "post_town", 'town_name',
                "postcode_locator"]
    if borough == None:
        query = f"SELECT {', '.join(cols)} FROM {table_name} WHERE class LIKE 'R%'"
    else:
        query = f"SELECT {', '.join(cols)} FROM {table_name} WHERE administrative_area='{borough.upper()}' AND class LIKE 'R%'"
    con = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)
    df = pd.read_sql(sql=query, con=con)
    
    return df

# %% UK Buildings

def uk_buildings(borough=None):
    
    boroughs = reference.london_boroughs(borough=borough, inc_geom=True)
    poly_27700 = boroughs.to_crs(27700).unary_union
    
    ukb_file = "F:/project_folders/GIS/UK_Map/201910/GLA/GLA/UKBuildings_Edition_7_GLA.gdb"
    df = gpd.read_file(filename=ukb_file, bbox=boroughs)
    df = df.loc[df.geometry.intersects(poly_27700),:]
    df = (df
       .assign(ubn = df.unique_building_number.astype(str),
               upn = df.unique_property_number.astype(str))
       .drop(columns=['unique_building_number', 'unique_property_number']))
    to_remove = ["res_", "residential_", "_building", "_identifier",
                 "class", "_of", "known_"]
    for string in to_remove:
        df.columns = df.columns.str.replace(string, "")
    to_replace = {
        "property_number": "pn",
        "building_number": "bn",
        "residential": "res",
        "building": "bld",
        "number": "no",
        "dwelling": "dwell",
        "bedroom": "bed",
        "reception": "recep",
        "text$": "t",
        "modelled": "mod",
        "source": "src",
        "code": "cd",
        "_+": "_",
        "date": "dt",
        "geographic": "geo",
        "element": "el",
        "count": "cnt",
        "primary": "pri",
        "revision": "rev",
        "mapping": "map",
        "block": "blk",
        "wet_room": "wetroom",
        "floor": "flo",
        "type": "tp",
        "entity": "ent"
        }    
    for key, value in to_replace.items():
        df.columns = df.columns.str.replace(key, value)
    
    ukb_link_file = "F:/project_folders/GIS/UK_Map/201910/OSAB_UKBUILDINGS_NN_LINK_FILE_190822.csv"
    link_file = pd.read_csv(ukb_link_file,
                            usecols = ['upn', 'ubn', 'uprn', 'udprn'],
                            dtype={"upn": str,
                                   "ubn": str,
                                   "uprn": str,
                                   "udprn": str})
    link_file = link_file.merge(df[['ubn']], how = 'inner')
    
    ukb = {'data': df,
           'link_data': link_file}
    
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
    gaz = gazateer(table_name=table_name, 
                   dbname=dbname, user=user, password=password, host=host,
                   borough=borough)
    gaz.to_csv("data/raw/local/gazatteer.csv", index=False)
    
    ukb = uk_buildings(borough = borough)
    
    ukb['data'].to_file('data/raw/local/uk_buildings.shp')
    ukb['link_data'].to_csv("data/raw/local/uk_buildings_link.csv", index=False)
    
