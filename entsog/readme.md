# Entsog-py
Python client for the ENTSO-G API (european network of transmission system operators for gas)

Documentation of the API found on https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf

Documentation of the data (user manual) found on https://www.entsog.eu/sites/default/files/2021-07/ENTSOG%20-%20TP%20User%20Manual_v_4.5.pdf



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

```
#### Dump result to file
See a list of all IO-methods on https://pandas.pydata.org/pandas-docs/stable/io.html
```python
ts = client.query_connection_points(country_code, start=start, end=end)
ts.to_csv('outfile.csv')
```

### Mappings
These lists are always evolving, so let us know if something's inaccurate!
#### Domains
```python

```