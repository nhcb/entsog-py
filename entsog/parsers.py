import sys
import zipfile
from io import BytesIO
from typing import Union
import re
from .misc import to_snake_case

import bs4
import pandas as pd
import simplejson

from .mappings import REGIONS


GENERATION_ELEMENT = "inBiddingZone_Domain.mRID"
CONSUMPTION_ELEMENT = "outBiddingZone_Domain.mRID"



def _extract_data(json_text):

    json = simplejson.loads(json_text)

    keys = list(json.keys())
    print(keys)
    df = pd.json_normalize(json[keys[1]])

    return df

def parse_general(json_text):
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    return df


def parse_interconnections(json_text):
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['from_region_key'] = df['from_country_key'].map(REGIONS)
    df['to_region_key'] = df['to_country_key'].map(REGIONS)

    return df

def parse_aggregate_data(json_text,
    group_type : str = None,
    entry_exit : bool = False):
    
    # Group on point, operator, balancing zone, country or region.
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['region_key'] = df['country_key'].map(REGIONS)
    # Extract the balancing zone from adjacent system, equal to matching last 11 characters of adjacent_systems_key

    # Only if it starts with transmission
    df['adjacent_bz_key'] = df['adjacent_systems_key'].str.extract(r"^Transmission?.*(.{11}$)").fillna('-----------')

    if entry_exit:
        mask = (df['direction_key']=='exit')
        df['value'][mask] = df['value'][mask] * -1 # Multiply by minus one as it is an exit
        df['direction_key'] = 'aggregated'

    if group_type is None:
        return df

    if group_type == 'point':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'country_key',
        'bz_key',
        'adjacent_bz_key', 
        'adjacent_systems_label',
        'operator_key', 
        'points_name',
        'indicator',
        'direction_key']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'operator':

        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'country_key',
        'bz_key',
        'adjacent_bz_key', 
        'adjacent_systems_label',
        'operator_key',
        'indicator',
        'direction_key']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'balancing_zone':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'country_key',
        'bz_key',
        'adjacent_bz_key', 
        'adjacent_systems_label',
        'indicator',
        'direction_key']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'country':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'country_key',
        'adjacent_bz_key', 
        'adjacent_systems_label',
        'indicator',
        'direction_key']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'region':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'adjacent_bz_key', 
        'adjacent_systems_label',
        'indicator',
        'direction_key']).agg(
            value = ('value', sum) # KWh/d
        )

    return df.reset_index()




def _extract_timeseries(xml_text):
    """
    Parameters
    ----------
    xml_text : str

    Yields
    -------
    bs4.element.tag
    """
    if not xml_text:
        return
    soup = bs4.BeautifulSoup(xml_text, 'html.parser')
    for timeseries in soup.find_all('timeseries'):
        yield timeseries
