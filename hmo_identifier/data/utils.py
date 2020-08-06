# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 11:44:22 2020

@author: lirogers
"""
from hmo_identifier.data import reference
import re

def clean_borough_names(borough):
    clean = (borough
             .lower()
             .replace(" and ", " ")
             .replace(" of ", " ")
             .replace(" upon thames", "")
             .replace(" & ", " ")
             .strip())
    clean = re.sub(" +", " ", clean)
    
    return clean

def match_borough_name(borough):
    boroughs = reference.london_boroughs()
    boroughs['ladnm_clean'] = boroughs.ladnm.apply(clean_borough_names)
    borough_clean = clean_borough_names(borough)
    try:
        borough_match_name = boroughs.loc[boroughs.ladnm_clean == borough_clean,
                                      'ladnm'].values[0]
    except IndexError:
        raise ValueError("Invalid borough name")
    
    return borough_match_name
