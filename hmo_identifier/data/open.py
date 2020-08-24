# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 13:38:33 2020
Functions to fetch open data
@author: lirogers
"""

import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
from hmo_identifier.data import utils, reference
import geopandas as gpd
import shapely
from pandas.io.json import json_normalize
import datetime
import dateutil
import time
import sys
import io
from dotenv import load_dotenv
import os


# %% Airbnb data

def airbnb(borough: str = None) -> pd.DataFrame:
    """
    Fetch data on AirBnB listings in London.
    
    This fetches data from Inside Airbnb.
    More information here:http://insideairbnb.com

    Parameters
    ----------
    borough : str, optional
         London borough name. The default is None (all boroughs returned).

    Returns
    -------
    df : pd.DataFrame
        A dataframe of AirBnB listings.

    """
    inside_airbnb = "http://insideairbnb.com/get-the-data.html"
    req = requests.get(inside_airbnb)
    req.raise_for_status()
    html_page = req.content
    soup = BeautifulSoup(html_page)
    links = [link.get('href') for link in
             soup.findAll('a',
                          href=re.compile("london/.+listings\.csv\.gz"))]
    most_recent_link = list(sorted(set(links)))[-1]
    df = pd.read_csv(most_recent_link, compression='gzip',
                     dtype = {'weekly_price': str,
                              'monthly_price': str,
                              'license': str,
                              'jurisdiction_names': str})
    if borough is not None:
         borough_clean = utils.clean_borough_names(borough)
         df = df.loc[
             df.neighbourhood_cleansed.apply(
                 utils.clean_borough_names
                 ) == borough_clean,
             :].reset_index(drop=True)

    return df

# %% Census data

def fetch_census(table: str, borough: str = None) -> pd.DataFrame:
    """
    Fetch a census table.

    Parameters
    ----------
    table : str
        Census table to fetch.
        Options are 'hhold_comp_bedrooms', 'hhold_comp_occ_rating' or 'tenure_occ_rating'
    borough : str, optional
        London borough name. The default is None (all boroughs returned).

    Returns
    -------
    df : pd.DataFrame
        A dataframe of census data from the relevant table for the requested areas.

    """
    if borough is not None:
        borough_name = utils.match_borough_name(borough)
        oas = reference.london_output_areas(borough=borough_name)
    else :
        oas = reference.london_output_areas()
    census_urls = {
        "hhold_comp_bedrooms": "https://www.nomisweb.co.uk/api/v01/dataset/nm_861_1.bulk.csv",
        "hhold_comp_occ_rating": "https://www.nomisweb.co.uk/api/v01/dataset/nm_865_1.bulk.csv",
        "tenure_occ_rating": "https://www.nomisweb.co.uk/api/v01/dataset/nm_867_1.bulk.csv"
        }
    url = census_urls[table]
        
    df = pd.read_csv(url)
    
    df.columns = (df.columns
                  .str.replace("[:; ]+", "_")
                  .str.replace("\-([0-9])", "neg_\\1")
                  .str.replace("\+([0-9])", "plus_\\1")
                  .str.replace("[\)\(\-]+", "")
                  .str.lower())
    df = df.loc[df.geography_code.isin(oas.oacd.tolist())]   
    
    return df

def merge_census(borough: str = None) -> pd.DataFrame:
    """
    Merge census tables from fetch_census

    Parameters
    ----------
    borough : str, optional
         London borough name. The default is None (all boroughs returned).

    Returns
    -------
    df : pd.DataFrame
        A dataframe of census data from all tables available through fetch_census

    """
    census_tables = ['hhold_comp_bedrooms', 'hhold_comp_occ_rating', 'tenure_occ_rating']
    tables = []
    for table in census_tables:
        df = fetch_census(table, borough = borough)
        tables.append(df)
    start = True
    for table in tables:
        if start:
            df = table.copy()
        else:
            df = df.merge(table)
        start = False
    
    return df


# %% Crime
def env_to_coord_str(env: shapely.geometry.Polygon) -> str:
    """
    Convert a polygon envelope to a string of coordinates to use the Police API.

    Parameters
    ----------
    env : shapely.geometry.Polygon
        A polygon envelope of the area needed.

    Returns
    -------
    coords_str: str
        A string of coordinates in the right form for the Police API.

    """
    ext_coords_xy = list(set(env.exterior.coords))
    ext_coords_yx = [(y, x) for (x, y) in ext_coords_xy]
    max_y = max([y for (y, x) in ext_coords_yx])
    min_y = min([y for (y, x) in ext_coords_yx])
    max_x = max([x for (y, x) in ext_coords_yx])
    min_x = min([x for (y, x) in ext_coords_yx])
    ext_coords_ordered = [(min_y, min_x),
                          (max_y, min_x),
                          (max_y, max_x),
                          (min_y, max_x)]
    coords_str = ":".join([f"{x},{y}" for (x, y) in ext_coords_ordered])
    return coords_str


def crime_month(poly: shapely.geometry.Polygon, date: str = None) -> gpd.GeoDataFrame:
    """
    Fetch monthly crime data from Police API

    Parameters
    ----------
    poly : shapely.geometry.Polygon
        Polygon of area required. Should be EPSG:4326
    date : str, optional
        Month requested in form YYYY-MM.
        The default is None and returns the most recent month of data.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        All crime data in relevant month and polygon at street level.

    """
    API = 'https://data.police.uk/api/crimes-street/all-crime'
    # Get a list of the polygon bounding box coords
    # Get this into the right format for the API
    # lat,long:lat,long...
    pol_num = 1
    envs = [poly.envelope]
    all_df = pd.DataFrame()
    while pol_num <= len(envs):
        env = envs[pol_num - 1]
        pol_num += 1
        coords_str = env_to_coord_str(env)
        params = {'poly': coords_str}
        if date is not None:
            params['date'] = date
        # Send a get request
        r = requests.get(API, params=params)
        if r.status_code == 503:
            new_envs = [shapely.affinity.scale(env, xfact=0.5, yfact=0.5,origin=(env.bounds[0], env.bounds[1])),shapely.affinity.scale(env, xfact=0.5, yfact=0.5,origin=(env.bounds[0], env.bounds[3])),shapely.affinity.scale(env, xfact=0.5, yfact=0.5,origin=(env.bounds[2], env.bounds[1])),shapely.affinity.scale(env, xfact=0.5, yfact=0.5,origin=(env.bounds[2], env.bounds[3]))]
            envs = envs + new_envs
            continue
        elif r.status_code == 500:
            time.sleep(20)
            r = requests.get(API, params=params)
            
        r.raise_for_status()
        response = r.json()
        
        # Convert to pandas df
        df = json_normalize(response)
        df.columns = df.columns.str.replace(".", "_")
        all_df = all_df.append(df)
    all_df.location_latitude = all_df.location_latitude.astype(float)
    all_df.location_longitude = all_df.location_longitude.astype(float)
    # Convert to geopandas
    gdf = gpd.GeoDataFrame(all_df,
                           geometry = gpd.points_from_xy(
                               all_df['location_longitude'],
                               all_df['location_latitude']),
                           crs="EPSG:4326")
    # Only keep points within the original polygon
    gdf = gdf.loc[gdf.geometry.within(poly), :]
    return gdf

def crime_year(borough: str = None, date: str = None) -> gpd.GeoDataFrame:
    """
    
    Fetch yearly crime data from Police API

    Parameters
    ----------
    borough : str, optional
         London borough name. The default is None (all boroughs returned).
        
    date : str, optional
        Final month of year of data requested in form YYYY-MM.
        The default is None and returns the most recent year of data.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        All crime data in relevant year and borough at street level.

    """
    boroughs = reference.london_boroughs(borough=borough, inc_geom=True)
    polygon = boroughs.geometry.unary_union
    num_months = 0
    all_crime = []
    while num_months < 12:
        crime = crime_month(polygon, date=date)
        all_crime.append(crime)
        if date is None:
            date = crime.month.unique()[0]
        date = (datetime.date(int(date[:4]), int(date[-2:]), 1)
                - datetime.timedelta(days=2)).strftime("%Y-%m")
        num_months += 1
    df = pd.concat(all_crime)

    return df


# %% IMD
    
def imd(borough: str = None) -> pd.DataFrame:
    """
    
    Fetch Index of Multiple Deprivation (IMD) data from 
    (here)[https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019].

    Parameters
    ----------
    borough : str, optional
         London borough name. The default is None (all boroughs returned).

    Returns
    -------
    df : pd.DataFrame
        IMD data for the relevant area as a pandas dataframe.

    """
    url = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/833970/File_1_-_IMD2019_Index_of_Multiple_Deprivation.xlsx"
    df = pd.read_excel(url, sheet_name="IMD2019")
    df.columns = (df.columns
                  .str.lower()
                  .str.replace(" ", "_")
                  .str.replace("\(|\)", "")
                  .str.replace("name", "nm")
                  .str.replace("code", "cd")
                  .str.replace("lsoa_", "lsoa")
                  .str.replace("local_authority_district_", "lad")
                  .str.replace("index_of_multiple_deprivation_", "")
                  .str.replace("_[0-9]{4}$", ""))
    if borough is not None:
        df = df.loc[df.ladnm == borough, :]
    else:
        df = df.loc[df.ladcd.str.startswith("E09"), :]
    
    return df

# %% EPC data

def epc(api_key: str, borough: str = None) -> pd.DataFrame:
    """
    
    Fetch Energy Performance Certificates (EPC) data from 
    (here)[https://epc.opendatacommunities.org/].
    An API key is needed.

    Parameters
    ----------
    api_key : str
        An API key for the MHCLG EPC API.
    borough : str, optional
        London borough name. The default is None (all boroughs returned).

    Returns
    -------
    all_df : pd.DataFrame
        EPC data as a pandas dataframe

    """
   
    url = "https://epc.opendatacommunities.org/api/v1/domestic/search"

    codes = reference.london_boroughs(borough).ladcd.tolist()
    dfs = []
    for code in codes:
        num_rows = 5000
    
        current_date = datetime.datetime.now()
        query = {'local-authority': code,
                 'from-month': 1,
                 'from-year': 2008,
                 'to-month': current_date.month,
                 'to-year': current_date.year}
    
        while num_rows == 5000:
            r = requests.get(url=url,
                               params=query,
                               headers={"Authorization": "Basic " + api_key,
                                        "Accept": "text/csv"})
            r.raise_for_status()
            
            if len(r.content.decode('utf-8')) > 0:
                df = pd.read_csv(io.StringIO(r.content.decode('utf-8')),
                                 dtype=str)
                dfs.append(df)
                num_rows = df.shape[0]
                min_date = datetime.datetime.strptime(
                    df.loc[num_rows - 1, 'lodgement-date'], "%Y-%m-%d"
                    ).date()
                next_date = min_date + dateutil.relativedelta.relativedelta(months=1)
                query['to-month'] = next_date.month
                query['to-year'] = next_date.year
            else:
                num_rows = 0
    if len(dfs) > 0:
        all_df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
        all_df.columns = (all_df.columns
                          .str.lower()
                          .str.replace('-', "_")
                          .str.replace("building_reference_number", "brn"))
    else:
        all_df = pd.DataFrame()
    return all_df


# %% Land registry
def land_registry(borough: str = None) -> pd.DataFrame:
    """
    
    Fetch land registry price paid data from 
    (here)[https://landregistry.data.gov.uk/].

    Parameters
    ----------
    borough : str, optional
        London borough name. The default is None (all boroughs returned).

    Returns
    -------
    df : pd.DataFrame
        Land registry data as a pandas dataframe.

    """

    
    data_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-"
    cols = ['trans_id', 'price', 'date', 'postcode', 'prop_type', 'new_build',
            'tenure_duration', 'paon', 'saon', 'street', 'locality',
            'town_city', 'district', 'county', 'ppd_cat', 'status']
    current_date = datetime.datetime.today().date()
    dfs = []
    years = range(current_date.year, 1994, -1)
    boroughs = reference.london_boroughs(borough)
    
    for year in years:
        url = f"{data_url}{year}.csv"
        r = requests.get(url)
        if r.status_code == 200:
            df = pd.read_csv(url, header=None, names=cols)
        else:
            continue
        df = df.loc[df.county == "GREATER LONDON", :]
        df = df.assign(district = df.district.str.replace("CITY OF W", "W"),
                       trans_id = df.trans_id.str.replace("{|}", ""))
        df = df.loc[df.district.isin(boroughs.ladnm.str.upper()),:]
        
        dfs.append(df)
    
    df = pd.concat(dfs)
    
    return df
    

# %% Main
if __name__ == "__main__":
    
    if (len(sys.argv) == 1) | (sys.argv[1] == None):
        borough = None
    else:
        borough = sys.argv[1]    
    
    # %% Airbnb data
    abnb = airbnb(borough)
    file = "data/raw/open/airbnb.csv"
    print("Saving file", file)
    abnb.to_csv(file, index=False)
    
    # %% Census data:
    census = merge_census(borough)
    file = "data/raw/open/census.csv"
    print("Saving file", file)
    census.to_csv(file, index=False)
    
    # %% Crime data
    crime = crime_year(borough)
    file = "data/raw/open/crime.csv"
    print("Saving file", file)
    (crime
     .drop(columns=['geometry'])
     .to_csv(file, index=False))
    
    # %% IMD data
    imd_data = imd(borough)
    file = "data/raw/open/imd.csv"
    print("Saving file", file)
    imd_data.to_csv(file, index=False)
    
    # %% EPC data
    load_dotenv()
    api_key = os.getenv("epc_api_key")
    epc_data = epc(api_key=api_key,
                   borough=borough)
    file = "data/raw/open/epc.csv"
    print("Saving file", file)
    epc_data.to_csv(file, index=False)
    
    # %% Land registry data
    lr_data = land_registry(borough)
    file = "data/raw/open/land_registry.csv"
    print("Saving file", file)
    lr_data.to_csv(file, index=False)
