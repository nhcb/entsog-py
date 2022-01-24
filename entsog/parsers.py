from cmath import nan
import sys
import zipfile
from io import BytesIO
from typing import Union
import re
from .misc import to_snake_case

import bs4
import pandas as pd
import numpy as np
import simplejson

from .mappings import REGIONS



def _extract_data(json_text):

    json = simplejson.loads(json_text)

    keys = list(json.keys())
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

def parse_aggregate_data(
    json_text,
    interconnections : pd.DataFrame,
    group_type : str = None,
    entry_exit : bool = False):
    
    # Group on point, operator, balancing zone, country or region.
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['region_key'] = df['country_key'].map(REGIONS)
    # Extract the balancing zone from adjacent system, equal to matching last 11 characters of adjacent_systems_key

    # Only if it starts with transmission
    #df['adjacent_bz_key'] = df['adjacent_systems_key'].str.extract(r"^Transmission?.*(.{11}$)").fillna('-----------') # Problem: DK-SE 12 characters
    df['adjacent_bz_key'] = df['adjacent_systems_key'].str.extract(r"^Transmission(.*)$").fillna('-----------').replace(r'^\s*$','-----------', regex = True)
    
    # Join with interconnections (only the ones with Transmission, Transmission)... These are outside Europe Transmissions
    # Entry gets joined with  to_point (points_names) to_operator_key (operator_key)
    # Exit gets joined with from_point (points_names) from_operator_key (operator_key)
    # However, we have adjacent_systems label, thus it doesn't matter ; it always defaults to to_point

    mask = ((df['adjacent_systems_key']=='Transmission') & (df['adjacent_systems_label']=='Transmission'))

    df_unmasked =df[~mask]
    df_unmasked['note'] = '' # Make empty column
    df_unmasked['outside_eu'] = False
    df_masked = df[mask]

    #df_masked.to_csv('data/temp_agg.csv',sep=';')
    # Join with interconnections
    df_masked_joined = pd.merge(
        df_masked,
        interconnections,
        #left_on = ['points_names','operator_key'], # Not possible to do on TSO level, apparently the TSO is often not similar
        #right_on = ['to_point_label','to_operator_key'], 
        left_on = ['points_names'], 
        right_on = ['to_point_label'], 
        suffixes = ('','_ic'), 
        how ='left')
    # Clean up the joined masked

    df_masked_joined['outside_eu'] = True
    #df_masked_joined['adjacent_systems_key'] = df_masked_joined['from_country_key'] Let it be transmission so we know it's outside Europe transmission
    df_masked_joined['adjacent_systems_label'] = df_masked_joined['from_country_label']
    df_masked_joined['note'] = df_masked_joined['from_operator_label'] # e.g. NordStream 2

    # Coalesce
    df_masked_joined['note'] = df_masked_joined['note'].combine_first( df_masked_joined['points_names'])
    # This is to be sure that e.g. Dormund is correctly mapped to Norway
    # Keep in mind tht a lot can not be mapped e.g. Emden (EPT1) (Thyssengas), Dornum / NETRA (GUD)|Emden (EPT1) (GUD)|Greifswald / GUD|Lubmin II
    # Uncomment the line below to check 

    #df_masked_joined[df_unmasked.columns].to_csv('data/temp_agg.csv',sep=';')

    # Only get the columns like in the unmasked version
    df = pd.concat([df_masked_joined[df_unmasked.columns], df_unmasked])
    

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
        'adjacent_systems_key',
        'adjacent_systems_label',
        'operator_key', 
        'points_names',
        'indicator',
        'direction_key',
        'note']).agg(
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
        'adjacent_systems_key',
        'adjacent_systems_label',
        'operator_key',
        'indicator',
        'direction_key',
        'note']).agg(
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
        'adjacent_systems_key',
        'adjacent_systems_label',
        'indicator',
        'direction_key',
        'note']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'country':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'country_key',
        'adjacent_bz_key', 
        'adjacent_systems_key',
        'adjacent_systems_label',
        'indicator',
        'direction_key',
        'note']).agg(
            value = ('value', sum) # KWh/d
        )
    elif group_type == 'region':
        df = df.groupby([
        'period_from',
        'period_to', 
        'region_key',
        'adjacent_bz_key', 
        'adjacent_systems_key',
        'adjacent_systems_label',
        'indicator',
        'direction_key',
        'note']).agg(
            value = ('value', sum) # KWh/d
        )

    return df.reset_index()


def parse_grouped_operational_aggregates(
    data : pd.DataFrame,
    group_type : str,
    entry_exit : str
    ) -> str:

    # Get the regions in Europe

    #data['tso_country'] = data['tso_country'].replace(nan,'Undefined')
    #data['adjacent_country'] = data['adjacent_country'].replace(nan,'Undefined')
    
    #print(group_type)
    #print(set(data['tso_country']))

    data['region_key'] = data['tso_country'].map(REGIONS)
    data['adjacent_region_key'] = data['adjacent_country'].map(REGIONS)


    if entry_exit:
        mask = (data['direction_key']=='exit')
        data['value'][mask] = data['value'][mask] * -1 # Multiply by minus one as it is an exit
        data['direction_key'] = 'aggregated'

    if group_type == 'point':
        df = data.groupby([
        'period_from', # All
        'period_to',  # All
        'point_type', # All
        'cross_border_point_type', # e.g. in-country, within EU, 
        'eu_relationship', # Within EU, outside EU
        'operator_key', # Operator level
        'operator_label', # Operator level
        'tso_country', # Country level
        'adjacent_country', # Country level
        'connected_operators', # E.g. Nordstream - Country level
        'tso_balancing_zone', # Balancing zone level
        'adjacent_zones', # Balancing zone level
        
        'region_key', # Region level
        'adjacent_region_key', # Region level

        'point_key', # Point level
        'point_label', # Point level

        'indicator',
        'direction_key'
        ]).agg(
            value = ('value', sum) # KW/ period_type
        )
    elif group_type == 'operator':

        df = data.groupby([
        'period_from', # All
        'period_to',  # All
        'point_type', # All
        'cross_border_point_type', # e.g. in-country, within EU, 
        'eu_relationship', # Within EU, outside EU
        'operator_key', # Operator level
        'operator_label', # Operator level
        'tso_country', # Country level
        'adjacent_country', # Country level
        'connected_operators', # E.g. Nordstream - Country level
        'tso_balancing_zone', # Balancing zone level
        'adjacent_zones', # Balancing zone level
        
        'region_key', # Region level
        'adjacent_region_key', # Region level

        'indicator',
        'direction_key'
        ]).agg(
            value = ('value', sum) # KW/ period_type
        )
    elif group_type == 'balancing_zone':
        df = data.groupby([
        'period_from', # All
        'period_to',  # All
        'point_type', # All
        'cross_border_point_type', # e.g. in-country, within EU, 
        'eu_relationship', # Within EU, outside EU

        'connected_operators', # E.g. Nordstream - Country level
        'tso_balancing_zone', # Balancing zone level
        'adjacent_zones', # Balancing zone level
        

        'region_key', # Region level
        'adjacent_region_key', # Region level

        'indicator',
        'direction_key'
        ]).agg(
            value = ('value', sum) # KW/ period_type
        )
    elif group_type == 'country':
        df = data.groupby([
        'period_from', # All
        'period_to',  # All
        'point_type', # All
        'cross_border_point_type', # e.g. in-country, within EU, 
        'eu_relationship', # Within EU, outside EU

        'tso_country', # Country level
        'adjacent_country', # Country level

        'connected_operators', # E.g. Nordstream - Country level
        'tso_balancing_zone', # Balancing zone level
        'adjacent_zones', # Balancing zone level
        
        'region_key', # Region level
        'adjacent_region_key', # Region level


        'indicator',
        'direction_key'
        ]).agg(
            value = ('value', sum) # KW/ period_type
        )
    elif group_type == 'region':
        df = data.groupby([
        'period_from', # All
        'period_to',  # All
        'point_type', # All
        'cross_border_point_type', # e.g. in-country, within EU, 
        'eu_relationship', # Within EU, outside EU

        'region_key', # Region level
        'adjacent_region_key', # Region level


        'indicator',
        'direction_key'
        ]).agg(
            value = ('value', sum) # KW/ period_type
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
