
from entsog import EntsogRawClient, EntsogPandasClient
import pandas as pd
import json

from entsog.mappings import BalancingZone

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
end = pd.Timestamp(2021, 10, 1)
country_code = 'DE'
# data = client.query_interruptions(country_code= 'DE',start = start, end = end, limit = -1)

# data = client.query_interruptions(country_code= 'DE',start = start, end = end, limit = -1)
# print(data)

# op = client.query_operator_point_directions()
# print(op)

# client.query_connection_points().to_csv(f'data/query_connection_points.csv',sep = ';')
# client.query_operators(country_code).to_csv(f'data/query_operators.csv',sep = ';')
# client.query_balancing_zones().to_csv(f'data/query_balancing_zones.csv',sep = ';')
# client.query_operator_point_directions(country_code).to_csv(f'data/query_operator_point_directions_{country_code}.csv',sep = ';')
# client.query_interconnections(country_code).to_csv(f'data/query_interconnections_{country_code}.csv',sep = ';')
# client.query_aggregate_interconnections(country_code).to_csv(f'data/query_aggregate_interconnections_{country_code}.csv',sep = ';')
# client.query_urgent_market_messages(country_code).to_csv(f'data/query_urgent_market_messages_{country_code}.csv',sep = ';')
# client.query_tariffs(start = start, end = end, country_code = country_code).to_csv(f'data/query_tariffs_{country_code}.csv',sep = ';')
# client.query_tariffs_sim(start = start, end = end, country_code = country_code).to_csv(f'data/query_tariffs_sim_{country_code}.csv',sep = ';')
# client.query_aggregated_data(start = start, end = end, country_code = country_code).to_csv(f'data/query_aggregated_data_{country_code}.csv',sep = ';')
# client.query_interruptions(start = start, end = end, country_code = country_code, limit = 1_000).to_csv(f'data/query_interruptions_{country_code}.csv',sep = ';')
# client.query_CMP_auction_premiums(start = start, end = end, country_code = country_code).to_csv(f'data/query_CMP_auction_premiums_{country_code}.csv',sep = ';')
# client.query_CMP_unavailable_firm_capacity(start = start, end = end, country_code = country_code).to_csv(f'data/query_CMP_unavailable_firm_capacity_{country_code}.csv',sep = ';')
# client.query_CMP_unsuccesful_requests(start = start, end = end, country_code = country_code).to_csv(f'data/query_CMP_unsuccesful_requests_{country_code}.csv',sep = ';')
# client.query_operational_data(start = start, end = end, country_code = country_code, indicators = ['nominations','allocation']).to_csv(f'data/query_operational_data_{country_code}.csv',sep = ';')



client.query_aggregated_data(start = start, end = end, country_code = country_code,period_type='day', group_type = 'country',entry_exit = True).to_csv(f'data/query_aggregated_data_{country_code}.csv',sep = ';')

balancing_zone = 'DE_GASPOOL'

#client.query_aggregated_data(start = start, end = end, balancing_zone = balancing_zone, group_type = 'balancing_zone',entry_exit = True).to_csv(f'data/query_aggregated_data_{balancing_zone}.csv',sep = ';')

#get_area()