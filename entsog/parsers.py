import bs4
import pandas as pd
import json
from .mappings import REGIONS
from .misc import to_snake_case


def _extract_data(json_text):
    json_data = json.loads(json_text)

    keys = list(json_data.keys())
    df = pd.json_normalize(json_data[keys[1]])

    return df


def parse_general(json_text):
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]
    return df


def parse_operational_data(json_text: str, verbose: bool):
    data = parse_general(json_text)
    columns = ['point_key', 'point_label', 'period_from', 'period_to', 'period_type', 'unit', 'indicator',
               'direction_key', 'flow_status', 'value',
               'tso_eic_code', 'tso_item_identifier',
               'interruption_type',
               'restoration_information',
               'capacity_type',
               'last_update_date_time',
               'item_remarks', 'general_remarks']
    if verbose:
        return data
    else:
        return data[columns]


def parse_CMP_unsuccesful_requests(json_text: str, verbose: bool):
    data = parse_general(json_text)
    columns = ['point_key', 'point_label', 'capacity_from', 'capacity_to', 'unit', 'direction_key',
               'requested_volume',
               'allocated_volume',
               'unallocated_volume',
               'last_update_date_time',
               'occurence_count',
               'item_remarks', 'general_remarks']

    if verbose:
        return data
    else:
        return data[columns]


def parse_CMP_unavailable_firm_capacity(json_text: str, verbose: bool):
    data = parse_general(json_text)
    columns = ['point_key', 'point_label', 'period_from', 'period_to', 'unit', 'allocation_process', 'direction_key',
               'requested_volume',
               'allocated_volume',
               'unallocated_volume',
               'last_update_date_time',
               'item_remarks', 'general_remarks']

    if verbose:
        return data
    else:
        return data[columns]


def parse_CMP_auction_premiums(json_text: str, verbose: bool):
    data = parse_general(json_text)
    columns = ['point_key', 'point_label', 'auction_from', 'auction_to', 'capacity_from', 'capacity_to', 'unit',
               'booking_platform_key', 'booking_platform_url', 'direction_key',
               'auction_premium',
               'cleared_price',
               'reserve_price',
               'last_update_date_time',
               'item_remarks', 'general_remarks']

    if verbose:
        return data
    else:
        return data[columns]


def parse_interruptions(json_text: str, verbose: bool):
    data = parse_general(json_text)
    columns = ['point_key', 'point_label', 'period_from', 'period_to', 'direction_key', 'unit', 'interruption_type',
               'capacity_type', 'capacity_commercial_type',
               'value',
               'restoration_information',
               'last_update_date_time',
               'item_remarks', 'general_remarks']

    if verbose:
        return data
    else:
        return data[columns]


# TODO: implement melt...
def parse_tariffs_sim(json_text: str, verbose: bool, melt: bool):
    data = parse_general(json_text)

    renamed_columns = {
        'product_simulation_cost_in_euro': 'product_simulation_cost_in_euro'
    }

    data = data.rename(
        columns=renamed_columns
    )

    columns = ['point_key', 'point_label', 'period_from', 'period_to', 'direction_key', 'connection',
               'tariff_capacity_type', 'tariff_capacity_unit', 'tariff_capacity_remarks',
               'product_type',
               'operator_currency',
               'product_simulation_cost_in_local_currency',
               'product_simulation_cost_in_euro',
               'product_simulation_cost_remarks',
               'exchange_rate_reference_date',
               'last_update_date_time',
               'remarks',
               'item_remarks',
               'general_remarks']

    if verbose:
        return data
    else:
        return data[columns]


def parse_tariffs(json_text: str, verbose: bool, melt: bool):
    # https://transparency.entsog.eu/api/v1/tariffsfulls

    data = parse_general(json_text)

    renamed_columns = {
        'applicable_tariff_per_local_currency_k_wh_d_value': 'applicable_tariff_per_local_currency_kwh_d_value',
        'applicable_tariff_per_local_currency_k_wh_d_unit': 'applicable_tariff_per_local_currency_kwh_d_unit',
        'applicable_tariff_per_local_currency_k_wh_h_value': 'applicable_tariff_per_local_currency_kwh_h_value',
        'applicable_tariff_per_local_currency_k_wh_h_unit': 'applicable_tariff_per_local_currency_kwh_h_unit',
        'applicable_tariff_per_eurk_wh_d_unit': 'applicable_tariff_per_eur_kwh_d_unit',
        'applicable_tariff_per_eurk_wh_d_value': 'applicable_tariff_per_eur_kwh_d_value',
        'applicable_tariff_per_eurk_wh_h_unit': 'applicable_tariff_per_eur_kwh_h_unit',
        'applicable_tariff_per_eurk_wh_h_value': 'applicable_tariff_per_eur_kwh_h_value',
        'applicable_commodity_tariff_local_currency': 'applicable_commodity_tariff_local_currency'
    }

    data = data.rename(
        columns=renamed_columns
    )

    columns = [
        'point_key', 'point_label',
        'period_from', 'period_to', 'direction_key',
        'product_period_from', 'product_period_to',
        'product_type', 'connection',
        'multiplier', 'multiplier_factor_remarks',
        'discount_for_interruptible_capacity_value', 'discount_for_interruptible_capacity_remarks',
        'seasonal_factor', 'seasonal_factor_remarks',
        'operator_currency',
        'applicable_tariff_per_local_currency_kwh_d_value',
        'applicable_tariff_per_local_currency_kwh_d_unit',
        'applicable_tariff_per_local_currency_kwh_h_value',
        'applicable_tariff_per_local_currency_kwh_h_unit',

        'applicable_tariff_per_eur_kwh_d_unit',
        'applicable_tariff_per_eur_kwh_d_value',
        'applicable_tariff_per_eur_kwh_h_unit',
        'applicable_tariff_per_eur_kwh_h_value',

        'applicable_tariff_in_common_unit_value',
        'applicable_tariff_in_common_unit_unit',

        'applicable_commodity_tariff_local_currency',
        'applicable_commodity_tariff_euro',
        'applicable_commodity_tariff_remarks',

        'exchange_rate_reference_date',

        'last_update_date_time',
        'remarks',
        'item_remarks',
        'general_remarks']

    if verbose:
        columns = data.columns  # Pick all
        data = data[columns]
    else:
        data = data[columns]

    if melt:
        melt_columns_value = [
            "applicable_tariff_per_local_currency_kwh_d_value",
            "applicable_tariff_per_local_currency_kwh_h_value",
            "applicable_tariff_per_eur_kwh_h_value",
            "applicable_tariff_per_eur_kwh_d_value",
            "applicable_tariff_in_common_unit_value",
        ]

        melt_columns_unit = [
            "applicable_tariff_per_local_currency_kwh_d_unit",
            "applicable_tariff_per_local_currency_kwh_h_unit",
            "applicable_tariff_per_eur_kwh_h_unit",
            "applicable_tariff_per_eur_kwh_d_unit",
            "applicable_tariff_in_common_unit_unit",
        ]

        id_columns = list(set(columns) - set(melt_columns_unit) - set(melt_columns_value))

        data_value = pd.melt(
            data,
            id_vars=id_columns,
            value_vars=melt_columns_value,
            var_name='variable',
            value_name='value'
        )

        data_value['variable'] = data_value["variable"].str.replace("_value$", "")
        data_unit = pd.melt(
            data,
            id_vars=id_columns,
            value_vars=melt_columns_unit,
            var_name='variable',
            value_name='code'
        )
        data_unit['variable'] = data_unit["variable"].str.replace("_unit$", "")

        merge_columns = id_columns.append('variable')

        data_pivot = data_value.merge(data_unit, on=merge_columns)

        data_pivot['variable'] = data_pivot['variable'].str.extract(r'(local_currency|eur|common_unit)')
        data_pivot['currency'] = data_pivot['code'].str.extract(r'^(.*?)\/')  # ^(.*?)\/ LINE START
        data_pivot['unit'] = data_pivot['code'].str.extract(
            r'\((.*?)\)')  # \((.*?)\) UNIT IN MIDDLE BETWEEN BRACKETS ()
        data_pivot['product_code'] = data_pivot['code'].str.extract(
            r'\)\/(.*?)$')  # \)\/(.*?)$ Product after unit

        # Regex: applicable_tariff_(.*?)(_unit|_value)

        return data_pivot
    else:
        return data


def parse_interconnections(json_text):
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['from_region_key'] = df['from_country_key'].map(REGIONS)
    df['to_region_key'] = df['to_country_key'].map(REGIONS)

    return df


def parse_operator_points_directions(json_text):
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['region'] = df['t_so_country'].map(REGIONS)
    df['adjacent_region'] = df['adjacent_country'].map(REGIONS)

    return df


def parse_aggregate_data(
        json_text,
        verbose: bool
):
    data = parse_general(json_text)

    data['adjacent_bz_key'] = data['adjacent_systems_key'].str.extract(r"^Transmission(.*)$").fillna(
        '-----------').replace(r'^\s*$', '-----------', regex=True)
    columns = [
        'country_key', 'country_label',
        'bz_key', 'bz_short', 'bz_long',
        'operator_key', 'operator_label',
        'adjacent_systems_key', 'adjacent_systems_label', 'adjacent_bz_key',
        'period_from', 'period_to', 'period_type', 'direction_key', 'indicator',
        'unit', 'value']

    if verbose:
        return data
    else:
        return data[columns]


# Legacy stuff
def parse_aggregate_data_complex(
        json_text,
        interconnections: pd.DataFrame,
        group_type: str = None,
        entry_exit: bool = False):
    # Group on point, operator, balancing zone, country or region.
    df = _extract_data(json_text)
    df.columns = [to_snake_case(col) for col in df.columns]

    # Get the regions in Europe
    df['region_key'] = df['country_key'].map(REGIONS)
    # Extract the balancing zone from adjacent system, equal to matching last 11 characters of adjacent_systems_key

    # Only if it starts with transmission
    # df['adjacent_bz_key'] = df['adjacent_systems_key'].str.extract(r"^Transmission?.*(.{11}$)").fillna('-----------') # Problem: DK-SE 12 characters
    df['adjacent_bz_key'] = df['adjacent_systems_key'].str.extract(r"^Transmission(.*)$").fillna('-----------').replace(
        r'^\s*$', '-----------', regex=True)

    # Join with interconnections (only the ones with Transmission, Transmission)... These are outside Europe Transmissions
    # Entry gets joined with  to_point (points_names) to_operator_key (operator_key)
    # Exit gets joined with from_point (points_names) from_operator_key (operator_key)
    # However, we have adjacent_systems label, thus it doesn't matter ; it always defaults to to_point

    mask = ((df['adjacent_systems_key'] == 'Transmission') & (df['adjacent_systems_label'] == 'Transmission'))

    df_unmasked = df[~mask]
    df_unmasked['note'] = ''  # Make empty column
    df_unmasked['outside_eu'] = False
    df_masked = df[mask]

    # df_masked.to_csv('data/temp_agg.csv',sep=';')
    # Join with interconnections
    df_masked_joined = pd.merge(
        df_masked,
        interconnections,
        # left_on = ['points_names','operator_key'], # Not possible to do on TSO level, apparently the TSO is often not similar
        # right_on = ['to_point_label','to_operator_key'],
        left_on=['points_names'],
        right_on=['to_point_label'],
        suffixes=('', '_ic'),
        how='left')
    # Clean up the joined masked

    df_masked_joined['outside_eu'] = True
    # df_masked_joined['adjacent_systems_key'] = df_masked_joined['from_country_key'] Let it be transmission so we know it's outside Europe transmission
    df_masked_joined['adjacent_systems_label'] = df_masked_joined['from_country_label']
    df_masked_joined['note'] = df_masked_joined['from_operator_label']  # e.g. NordStream 2

    # Coalesce
    df_masked_joined['note'] = df_masked_joined['note'].combine_first(df_masked_joined['points_names'])
    # This is to be sure that e.g. Dormund is correctly mapped to Norway
    # Keep in mind tht a lot can not be mapped e.g. Emden (EPT1) (Thyssengas), Dornum / NETRA (GUD)|Emden (EPT1) (GUD)|Greifswald / GUD|Lubmin II
    # Uncomment the line below to check 

    # df_masked_joined[df_unmasked.columns].to_csv('data/temp_agg.csv',sep=';')

    # Only get the columns like in the unmasked version
    df = pd.concat([df_masked_joined[df_unmasked.columns], df_unmasked])

    if entry_exit:
        mask = (df['direction_key'] == 'exit')
        df['value'][mask] = df['value'][mask] * -1  # Multiply by minus one as it is an exit
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
            value=('value', sum)  # KWh/d
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
            value=('value', sum)  # KWh/d
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
            value=('value', sum)  # KWh/d
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
            value=('value', sum)  # KWh/d
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
            value=('value', sum)  # KWh/d
        )

    return df.reset_index()


def parse_grouped_operational_aggregates(
        data: pd.DataFrame,
        group_type: str,
        entry_exit: str
) -> str:
    # Get the regions in Europe

    # data['tso_country'] = data['tso_country'].replace(nan,'Undefined')
    # data['adjacent_country'] = data['adjacent_country'].replace(nan,'Undefined')

    # print(group_type)
    # print(set(data['tso_country']))

    data['region_key'] = data['tso_country'].map(REGIONS)
    data['adjacent_region_key'] = data['adjacent_country'].map(REGIONS)

    if entry_exit:
        mask = (data['direction_key'] == 'exit')
        data['value'][mask] = data['value'][mask] * -1  # Multiply by minus one as it is an exit
        data['direction_key'] = 'aggregated'

    if group_type == 'point':
        df = data.groupby([
            'period_from',  # All
            'period_to',  # All
            'point_type',  # All
            'cross_border_point_type',  # e.g. in-country, within EU,
            'eu_relationship',  # Within EU, outside EU
            'operator_key',  # Operator level
            'operator_label',  # Operator level
            'tso_country',  # Country level
            'adjacent_country',  # Country level
            'connected_operators',  # E.g. Nordstream - Country level
            'tso_balancing_zone',  # Balancing zone level
            'adjacent_zones',  # Balancing zone level

            'region_key',  # Region level
            'adjacent_region_key',  # Region level

            'point_key',  # Point level
            'point_label',  # Point level

            'indicator',
            'direction_key'
        ]).agg(
            value=('value', sum)  # KW/ period_type
        )
    elif group_type == 'operator':

        df = data.groupby([
            'period_from',  # All
            'period_to',  # All
            'point_type',  # All
            'cross_border_point_type',  # e.g. in-country, within EU,
            'eu_relationship',  # Within EU, outside EU
            'operator_key',  # Operator level
            'operator_label',  # Operator level
            'tso_country',  # Country level
            'adjacent_country',  # Country level
            'connected_operators',  # E.g. Nordstream - Country level
            'tso_balancing_zone',  # Balancing zone level
            'adjacent_zones',  # Balancing zone level

            'region_key',  # Region level
            'adjacent_region_key',  # Region level

            'indicator',
            'direction_key'
        ]).agg(
            value=('value', sum)  # KW/ period_type
        )
    elif group_type == 'balancing_zone':
        df = data.groupby([
            'period_from',  # All
            'period_to',  # All
            'point_type',  # All
            'cross_border_point_type',  # e.g. in-country, within EU,
            'eu_relationship',  # Within EU, outside EU

            'connected_operators',  # E.g. Nordstream - Country level
            'tso_balancing_zone',  # Balancing zone level
            'adjacent_zones',  # Balancing zone level

            'region_key',  # Region level
            'adjacent_region_key',  # Region level

            'indicator',
            'direction_key'
        ]).agg(
            value=('value', sum)  # KW/ period_type
        )
    elif group_type == 'country':
        df = data.groupby([
            'period_from',  # All
            'period_to',  # All
            'point_type',  # All
            'cross_border_point_type',  # e.g. in-country, within EU,
            'eu_relationship',  # Within EU, outside EU

            'tso_country',  # Country level
            'adjacent_country',  # Country level

            'connected_operators',  # E.g. Nordstream - Country level
            'tso_balancing_zone',  # Balancing zone level
            'adjacent_zones',  # Balancing zone level

            'region_key',  # Region level
            'adjacent_region_key',  # Region level

            'indicator',
            'direction_key'
        ]).agg(
            value=('value', sum)  # KW/ period_type
        )
    elif group_type == 'region':
        df = data.groupby([
            'period_from',  # All
            'period_to',  # All
            'point_type',  # All
            'cross_border_point_type',  # e.g. in-country, within EU,
            'eu_relationship',  # Within EU, outside EU

            'region_key',  # Region level
            'adjacent_region_key',  # Region level

            'indicator',
            'direction_key'
        ]).agg(
            value=('value', sum)  # KW/ period_type
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
