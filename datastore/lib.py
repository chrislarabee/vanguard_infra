import re
import datetime as dt

import numpy as np
import pandas as pd
import datagenius as dg


def donation_dt_to_datetime(d_date: str) -> (dt.datetime, str):
    """
    Takes a donation date string and converts it to a datetime object.

    Args:
        d_date (str): A date string with dates separated by /
    Returns: 
        datetime: A datetime object based on d_date, or d_date if no 
            pattern match is found.
    """
    patterns = {
        r'\d{2}/\d{2}/\d{4}': '%m/%d/%Y',
        r'\d{2}/\d{4}': '%m/%Y',
        r'\d{4}': '%Y'
    }
    for re_p, dt_fmt in patterns.items():
        if re.search(re.compile(re_p), d_date):
            return dt.datetime.strptime(d_date, dt_fmt)
    return d_date


def unpack_donation_col(donation_col: str) -> tuple:
    """
    Unpacks a string of donation data in the format of 
    {$1@date1,$2@date2} into a dictionary of metadata on that string.

    Args:
        donation_col (str): A string of donation data.
    Returns:
        tuple: A tuple of metadata about the passed donation data.
    """
    output = (0, 0, -1)
    donation_col = re.sub(r'[{}$]', '', donation_col)
    if donation_col != '':
        donations = [x.split('@') for x in donation_col.split(',')]
        amts = [float(x[0]) for x in donations]
        last_dt = donation_dt_to_datetime(donations[0][1])
        output = (
            sum(amts),
            np.mean(amts),
            (dt.datetime.now() - last_dt).days
        )
    return output


def prep_raw_data(
        df: pd.DataFrame, 
        md: dg.GeniusMetadata = None) -> (pd.DataFrame, dg.GeniusMetadata):
    """
    Unpacks the dem donation data for each row in the raw district_data 
    DataFrame and determines whether or not they are a donor. Performs a
    small amount of cleanup on the party_affiliation column.

    Args:
        df (DataFrame): A pandas DataFrame with a demdonationamounts
            column.
        md (GeniusMetadata, optional): A GeniusMetadata object. Defaults 
            to None.
    Returns:
        (DataFrame, GeniusMetadata): The transformed DataFrame and the 
            originally passed GeniusMetadata object.
    """
    df[['total', 'avg', 'days_since']] = pd.DataFrame(
        df['demdonationamounts'].apply(unpack_donation_col).tolist(),
        index=df.index
    )
    df['party_affiliation'] = df['party_affiliation'].fillna('X')
    df['is_donor'] = df['total'].apply(lambda x: x > 0).astype('int')
    return df, md

