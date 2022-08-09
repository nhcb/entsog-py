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

# 
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
    'nominations' : "Nominations",
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