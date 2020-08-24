# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 13:30:06 2020

@author: LiRogers
"""

import pandas as pd
from recordlinkage.preprocessing import clean
import recordlinkage
from itertools import product


def clean_estate_name(x: pd.Series) -> pd.Series:
    """
    
    Clean the names of estates to get into a clean format and remove common
    words in preparation for matching to gazatteer.

    Parameters
    ----------
    x : pd.Series
        A pandas series of estate names.

    Returns
    -------
    x : TYPE
        A pandas series of clean estate names.

    """

    common_words = [
        "ESTATE",
        "ROAD",
        "STREET",
        "CRESCENT",
        "CLOSE",
        "PLACE",
        "ST",
        "RD",
        "UNKNOWN",
        "LANE",
        "WAY",
        "EST",
        "END",
        "APARTMENTS",
        "SQUARE",
    ]
    x = (
        x.str.lower()
        .str.replace("\.|,|\(|\)|'", "")
        .str.replace(" ?- ?", "-")
        .replace(pd.np.nan, "")
        .str.replace(r"\b" + "\\b|\\b".join(common_words) + "\\b", "")
        .str.replace(" AND |&", " ")
        .str.replace("[0-9]+-[0-9]+", "")
        .str.replace("[A-Z]-[A-Z]", "")
        .str.replace("\(|\)|\-", "")
        .str.replace(" [A-Z] | [A-Z]$", " ")
        .str.replace("ODD|FLATS|CONS", "")
        .str.replace("[0-9]+", "")
        .str.replace("[ /]+", " ")
        .str.strip()
    )

    return x


def match_prep(df: pd.DataFrame, add_var: str) -> pd.DataFame:
    """
    
    Prepare and clean a dataframe for address matching.
    

    Parameters
    ----------
    df : pd.DataFrame
        A dataframe to prepare.
    add_var : str
        The variable that contains the full address (minus postcode).

    Returns
    -------
    df : pd.DataFrame
        df with 'clean_address' and 'numbers' adding. Postcode column cleaned.

    """
    df = df.copy()
    df["clean_address"] = (
        clean(df[add_var], replace_by_whitespace="[\\_]")
        .str.replace(" +", " ")
        .str.replace("([^0-9])0+([0-9])", "\\1\\2")
        .str.replace("^0+", "")
        .str.strip()
    )

    df["numbers"] = (
        df.clean_address.str.replace("[a-z]{2,}", "")
        .str.replace(" +", " ")
        .str.strip()
        .str.split()
        .apply(sorted)
        .apply(lambda x: " ".join(x))
    )
    df.postcode = clean(df.postcode, replace_by_whitespace="[\\_]")

    return df


def candidate_matches(
    ref: pd.DataFrame,
    ref_id: str,
    ref_addresses: list,
    add: pd.DataFrame,
    add_id: str,
    add_addresses: list,
) -> pd.DataFrame:
    """
    
    Finds address match candidates between ref and add.
    Two records will be a candidate if they have the same postcode.
    Similarity scores between ref_addresses and add_addresses will be added.

    Parameters
    ----------
    ref : pd.DataFrame
        Reference dataset to address match onto.
    ref_id : str
        Record ID column in ref.
    ref_addresses : list
        Address columns in ref.
    add : pd.DataFrame
        Additional dataset to address match from.
    add_id : str
        Record ID column in add.
    add_addresses : list
        Address columns in add.

    Returns
    -------
    features : pd.DataFrame
        Candidate matches between ref and add.

    """

    ref_match = ref.copy()
    add_match = add.copy()
    address_perms = list(product(ref_addresses, add_addresses))
    exact = pd.concat(
        [
            pd.merge(
                ref_match, add_match,
                left_on=[l, "postcode"], right_on=[r, "postcode"]
            )
            for (l, r) in address_perms
        ]
    )

    ref_match = ref_match.loc[~ref_match[ref_id].isin(exact[ref_id]), :].set_index(
        ref_id
    )
    add_match = add_match.loc[~add_match[add_id].isin(exact[add_id]), :].set_index(
        add_id
    )
    indexer = recordlinkage.Index()
    indexer.block("postcode")
    candidate_links = indexer.index(ref_match, add_match)

    compare_cl = recordlinkage.Compare()
    number_perms = list(
        product(
            ref_match.columns[ref_match.columns.str.contains("number")],
            add_match.columns[add_match.columns.str.contains("number")],
        )
    )

    for (l, r) in address_perms:
        compare_cl.string(l, r, method="levenshtein", label=f"{l}_{r}_match")
    for (l, r) in number_perms:
        compare_cl.string(l, r, method="levenshtein", label=f"{l}_{r}_match")

    features = compare_cl.compute(candidate_links, ref_match, add_match)

    features = features.reset_index()

    features = pd.merge(features, ref_match[ref_addresses + ["postcode"]].reset_index())
    features = pd.merge(features, add_match[add_addresses].reset_index())
    features = pd.concat([features, exact], sort=True)
    features.loc[:, features.columns.str.endswith("_match")] = features.loc[
        :, features.columns.str.endswith("_match")
    ].fillna(1)
    features = features[
        [ref_id, add_id]
        + ref_addresses
        + add_addresses
        + list(features.columns[features.columns.str.endswith("_match")])
    ]
    return features


def postcode_regex() -> str:
    """
    Regex to match UK postcodes

    Returns
    -------
    str

    """
    return "[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}"
