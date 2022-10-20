# Entsog-py
Python client for the ENTSO-G API (european network of transmission system operators for gas)

Documentation of the API found on https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf

Documentation of the data (user manual) found on https://www.entsog.eu/sites/default/files/2021-07/ENTSOG%20-%20TP%20User%20Manual_v_4.5.pdf

Heavily inspired upon (and forked from) https://github.com/EnergieID/entsoe-py 

## Installation
```
python3 -m pip install entsog-py
```

## Usage
The package comes with 2 clients:
- [`EntsogRawClient`](#EntsogRawClient): Returns data in its raw format, usually JSON
- [`EntsogPandasClient`](#EntsogPandasClient): Returns data parsed as a Pandas DataFrame

It's preferable to use the Pandas Client as this will handle most API limitations itself. However, if you want to obtain the pure raw data; you can use the raw client.

## Example Use Case
On wwww.gasparency.com you can find example use cases of the data. Almost everything there is achieved with the help of this package!

### <a name="EntsogRawClient"></a>EntsogRawClient
```python
from entsog import EntsogRawClient
import pandas as pd

client = EntsogRawClient()

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'NL'  # Netherlands

client.query_connection_points()
client.query_operators()
client.query_balancing_zones()
client.query_operator_point_directions(country_code)
client.query_interconnections()
client.query_aggregate_interconnections()
client.query_urgent_market_messages()
client.query_tariffs(start = start, end = end, country_code = country_code)
client.query_tariffs_sim(start = start, end = end, country_code = country_code)

    operational_options = {   
    interruption_capacity : "Actual interruption of interruptible capacity",
    allocation : "Allocation",
    firm_available : "Firm Available",
    firm_booked : "Firm Booked",
    firm_interruption_planned : "Firm Interruption Planned - Interrupted",
    firm_interruption_unplanned :"Firm Interruption Unplanned - Interrupted",
    firm_technical : "Firm Technical",
    gcv : "GCV",
    interruptible_available : "Interruptible Available",
    interruptible_booked : "Interruptible Booked",
    interruptible_interruption_actual : "Interruptible Interruption Actual – Interrupted",
    interruptible_interruption_planned : "Interruptible Interruption Planned - Interrupted",
    interruptible_total : "Interruptible Total",
    nomination : "Nomination",
    physical_flow : "Physical Flow",
    firm_interruption_capacity_planned : "Planned interruption of firm capacity",
    renomination : "Renomination",
    firm_interruption_capacity_unplanned : "Unplanned interruption of firm capacity",
    wobbe_index : "Wobbe Index",
    oversubscription_available : "Available through Oversubscription",
    surrender_available : "Available through Surrender",
    uioli_available_lt : "Available through UIOLI long-term",
    uioli_available_st : "Available through UIOLI short-term"}

client.query_operational_data(start = start, end = end, country_code = country_code, indicator = ['renomination', 'physical_flow'])





```

### <a name="EntsogPandasClient"></a>EntsogPandasClient
The Pandas Client works similar to the Raw Client, with extras:
- API limitations of big requests are automatically dealt with and put into multiple calls.
- Tariffs (and simulated tariffs) can be melted into nice storable format. Instead of having row with EUR, local currency, shared currency for each seperate product, it will create a row for each.
- Operational data can be either requested as in the raw format (which requires some loading time) or in an aggregate function `query_operational_data_all` which will aggressively request all points in Europe and a lot faster.
- It's easier to navigate points, for instance if you want to check gazprom points. See below.

```python
from entsog import EntsogPandasClient
import pandas as pd

client = EntsogPandasClient()

start = pd.Timestamp('20171228', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'NL'  # Netherlands

client.query_connection_points()
client.query_operators(country_code)
client.query_balancing_zones()
client.query_operator_point_directions()
client.query_interconnections()
client.query_aggregate_interconnections()
client.query_urgent_market_messages()


client.query_tariffs(start = start, end = end, country_code = country_code, melt = True, verbose = True)
client.query_tariffs_sim(start = start, end = end, country_code = country_code, verbose = True)

client.query_aggregated_data(start = start, end = end, country_code = country_code)
# TODO: Add interruptions...
# client.query_interruptions(start = start, end = end)
client.query_CMP_auction_premiums(start = start, end = end)
client.query_CMP_unavailable_firm_capacity(start = start, end = end)

client.query_CMP_unsuccesful_requests(start = start, end = end)

operational_options = {   
    'interruption_capacity' : "Actual interruption of interruptible capacity",
    'allocation' : "Allocation",
    'firm_available' : "Firm Available",
    'firm_booked' : "Firm Booked",
    'firm_interruption_planned' : "Firm Interruption Planned - Interrupted",
    'firm_interruption_unplanned' :"Firm Interruption Unplanned - Interrupted",
    'firm_technical' : "Firm Technical",
    'gcv' : "GCV",
    'interruptible_available' : "Interruptible Available",
    'interruptible_booked' : "Interruptible Booked",
    'interruptible_interruption_actual' : "Interruptible Interruption Actual – Interrupted",
    'interruptible_interruption_planned' : "Interruptible Interruption Planned - Interrupted",
    'interruptible_total' : "Interruptible Total",
    'nomination' : "Nomination",
    'physical_flow' : "Physical Flow",
    'firm_interruption_capacity_planned' : "Planned interruption of firm capacity",
    'renomination' : "Renomination",
    'firm_interruption_capacity_unplanned' : "Unplanned interruption of firm capacity",
    'wobbe_index' : "Wobbe Index",
    'oversubscription_available' : "Available through Oversubscription",
    'surrender_available' : "Available through Surrender",
    'uioli_available_lt' : "Available through UIOLI long-term",
    'uioli_available_st' : "Available through UIOLI short-term"
}

client.query_operational_data(start = start, end = end, country_code = country_code, indicators = ['renomination', 'physical_flow'])
# You should use this when you want to query operational data for the entirety of continental europe.
client.query_operational_data_all(start = start, end = end, indicators = ['renomination', 'physical_flow'])
# Example for if you would like to see Gazprom points.
points = client.query_operator_point_directions()
mask = points['connected_operators'].str.contains('Gazprom')
masked_points = points[mask]
print(masked_points)

keys = []
for idx, item in masked_points.iterrows():
    keys.append(f"{item['operator_key']}{item['point_key']}{item['direction_key']}")

data = client.query_operational_point_data(start = start, end = end, indicators = ['physical_flow'], point_directions = keys, verbose = False)

print(data.head())


```