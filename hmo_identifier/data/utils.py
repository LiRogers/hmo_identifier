# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 11:44:22 2020

@author: lirogers
"""
from hmo_identifier.data import reference
import re

def clean_borough_names(borough:str) -> str:
    """
    
    Cleans London Borough names into a clean format
    

    Parameters
    ----------
    borough : str
        Borough name to be cleaned.

    Returns
    -------
    str
        Clean borough name.

    """
    clean = (borough
             .lower()
             .replace(" and ", " ")
             .replace(" of ", " ")
             .replace(" upon thames", "")
             .replace(" & ", " ")
             .strip())
    clean = re.sub(" +", " ", clean)
    
    return clean

def match_borough_name(borough: str) -> str:
    """
    
    Match a London Borough name to standard format

    Parameters
    ----------
    borough : str
        Borough name to be matched.

    Raises
    ------
    ValueError
        If an invalid borough name is provided.

    Returns
    -------
    str
        A matched borough name.

    """
    boroughs = reference.london_boroughs()
    boroughs['ladnm_clean'] = boroughs.ladnm.apply(clean_borough_names)
    borough_clean = clean_borough_names(borough)
    try:
        borough_match_name = boroughs.loc[boroughs.ladnm_clean == borough_clean,
                                      'ladnm'].values[0]
    except IndexError:
        raise ValueError("Invalid borough name")
    
    return borough_match_name
