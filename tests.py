
from entsog import EntsogRawClient, EntsogPandasClient
import pandas as pd
import json

client = EntsogRawClient()

data = json.loads(client.query_operator_point_directions(limit = -1))

df = pd.json_normalize(data['operatorpointdirections'])

#df.to_csv('mapping/operatorpointdirections.csv')

#sprint(df)

client = EntsogPandasClient()

start = pd.Timestamp(2018, 1, 1)
end = pd.Timestamp(2018, 2, 1)
data = (client.query_operational_data(limit = -1, start = start, end = end))

#df.to_csv('mapping/operatorpointdirections.csv')

print(data)