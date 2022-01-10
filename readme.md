# Entsog-py
Python client for the ENTSO-G API (european network of transmission system operators for gas)

Documentation of the API found on https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf

Documentation of the data (user manual) found on https://www.entsog.eu/sites/default/files/2021-07/ENTSOG%20-%20TP%20User%20Manual_v_4.5.pdf

Heavily inspired upon (and forked from) https://github.com/EnergieID/entsoe-py 

## Installation
DOES NOT WORK (YET)
`python3 -m pip install entsog-py`

## Usage
The package comes with 2 clients:
- [`EntsogRawClient`](#EntsogRawClient): Returns data in its raw format, usually JSON
- [`EntsogPandasClient`](#EntsogPandasClient): Returns data parsed as a Pandas DataFrame
### <a name="EntsogRawClient"></a>EntsogRawClient
```python
from entsog import EntsogRawClient
import pandas as pd

client = EntsogRawClient()

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium

client.query_connection_points()
client.query_operators(country_code)
client.query_balancing_zones()
client.query_operator_point_directions(country_code)
client.query_interconnections(country_code)
client.query_aggregate_interconnections(country_code)
client.query_urgent_market_messages(country_code)
client.query_tariffs(start = start, end = end, country_code = country_code)
client.query_tariffs_sim(start = start, end = end, country_code = country_code)

client.query_aggregated_data(start = start, end = end, country_code = country_code)
client.query_interruptions(start = start, end = end, country_code = country_code)
client.query_CMP_auction_premiums(start = start, end = end, country_code = country_code)
client.query_CMP_unavailable_firm_capacity(start = start, end = end, country_code = country_code)

client.query_CMP_unsuccesful_requests(start = start, end = end, country_code = country_code)

#Nomination, Renominations, Allocations, Physical Flows, GCV, Wobbe Index, Capacities, Interruptions, and CMP CMA
# One could filter on these indicators...
# All possible values:
# Actual interruption of interruptible capacity
# Allocation
# Firm Available
# Firm Booked
# Firm Interruption Planned - Interrupted
# Firm Interruption Unplanned - Interrupted
# Firm Technical
# Firm available
# Firm booked
# Firm technical
# GCV
# Interruptible Available
# Interruptible Booked
# Interruptible Interruption Actual – Interrupted
# Interruptible Interruption Planned - Interrupted
# Interruptible Total
# Nominations
# Physical Flow
# Planned interruption of firm capacity
# Renomination
# Unplanned interruption of firm capacity
# Wobbe Index

# Available through Oversubscription
# Available through Surrender
# Available through UIOLI long-term
# Available through UIOLI short-term

client.query_operational_data(start = start, end = end, country_code = country_code, indicator = ['Nomination', 'Renominations', 'Wobbe Index'])





```

### <a name="EntsogPandasClient"></a>EntsogPandasClient
The Pandas Client works similar to the Raw Client, with extras:
- Time periods that span more than 1 year are automatically dealt with
- Requests of large numbers of files are split over multiple API calls
```python
from Entsog import EntsogPandasClient
import pandas as pd

client = EntsogPandasClient()

start = pd.Timestamp('20171201', tz='Europe/Brussels')
end = pd.Timestamp('20180101', tz='Europe/Brussels')
country_code = 'BE'  # Belgium

# methods that return Pandas DataFrame
client.query_connection_points()
client.query_operators(country_code)
client.query_balancing_zones()
client.query_operator_point_directions(country_code)
client.query_interconnections(country_code)
client.query_aggregate_interconnections(country_code)
client.query_urgent_market_messages(country_code)
client.query_tariffs(start = start, end = end, country_code = country_code)
client.query_tariffs_sim(start = start, end = end, country_code = country_code)

client.query_aggregated_data(start = start, end = end, country_code = country_code)
client.query_interruptions(start = start, end = end, country_code = country_code)
client.query_CMP_auction_premiums(start = start, end = end, country_code = country_code)
client.query_CMP_unavailable_firm_capacity(start = start, end = end, country_code = country_code)

client.query_CMP_unsuccesful_requests(start = start, end = end, country_code = country_code)

client.query_operational_data(start = start, end = end, country_code = country_code, indicator = ['Nomination', 'Renominations', 'Wobbe Index'])



```
#### Dump result to file
See a list of all IO-methods on https://pandas.pydata.org/pandas-docs/stable/io.html
```python
ts = client.query_connection_points(country_code, start=start, end = end=end)
ts.to_csv('outfile.csv')
```

### Mappings
These lists are always evolving, so let us know if something's inaccurate!
#### Domains
```python

```