
from entsog import EntsogRawClient, EntsogPandasClient
import pandas as pd
import json

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

start = pd.Timestamp(2018, 1, 1)
end = pd.Timestamp(2018, 2, 1)
data = client.query_interruptions(country_code= 'DE',start = start, end = end, limit = -1)

data = client.query_interruptions(country_code= 'DE',start = start, end = end, limit = -1)
print(data)

op = client.query_operator_point_directions()
print(op)