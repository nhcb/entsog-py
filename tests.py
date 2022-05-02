
from entsog import EntsogRawClient, EntsogPandasClient
import pandas as pd
import json
import time

from entsog.mappings import BalancingZone

from entsog.mappings import Country

def get_area():
    client = EntsogRawClient()
    data = json.loads(client.query_operator_point_directions(limit = -1))

    df = pd.json_normalize(data['operatorpointdirections'])

    df_drop = df.drop_duplicates(subset=['tSOCountry'])

    c = {}
    for idx, item in df_drop.iterrows():
        country = item['tSOCountry']

        filtered = df[df['tSOCountry'] == country]

        operatorKey = filtered.loc[:,'operatorKey'].drop_duplicates()
        #print(operatorKey)
        operatorLabel = filtered.loc[:,'operatorLabel'].drop_duplicates()

        if country is None:
            country = 'misc'

        print(f"{country}   =   {list(operatorKey)},  {list(operatorLabel)} ,")


client = EntsogPandasClient()

start = pd.Timestamp(2021, 1, 1)
end = pd.Timestamp(2021,1, 6)
country_code = 'DE'

tik = time.time()
points = client.query_operator_point_directions()
mask = points['connected_operators'].str.contains('Gazprom')
masked_points = points[mask]

print(masked_points)

keys = []
for idx, item in masked_points.iterrows():
    keys.append(f"{item['operator_key']}{item['point_key']}{item['direction_key']}")

data = client.query_operational_point_data(start = start, end = end, indicators = ['physical_flow'], point_directions = keys, verbose = False)

print(data.head())


tok = time.time()
print(data)
print(data.columns)
print(f'All Operational took: {(tok-tik)/60} minutes')


#tik = time.time()
#data = client.query_operational_data(
#    start = start,
#    end = end,
##    verbose = False,
#    country_code=country_code,
#    indicators= ['physical_flow', 'renomination']
#)


#tok = time.time()
#print(data)
#print(data.columns)
#print(f'{country_code} took: {(tok-tik)/60} minutes')