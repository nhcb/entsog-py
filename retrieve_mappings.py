from entsog import EntsogRawClient,EntsogPandasClient
import json

from entsog.parsers import parse_general
import pandas as pd



def get_operator_mappings():
    client = EntsogPandasClient()
    operators = client.query_operators(country_code= None)
    points = client.query_operator_point_directions()
    countries = operators.drop_duplicates(subset=['operator_country_key'])

    c = []
    for i, item in countries.iterrows():
        country = item['operator_country_key']

        operators_country = operators[operators['operator_country_key'] == country]

        d = []
        e = []
        for j, jtem in operators_country.iterrows():
            print(jtem)
            operator = jtem['operator_key']
            points_operator = points[points['operator_key'] == country]

            
            for m,mtem in points_operator.iterrows():
                
                e.append(
                    {
                        'point' : mtem['point_key'],
                        'point_label' : mtem['point_label']
                    }
                )      
            
            d.append({**jtem.to_dict(), 'points' : e})

        c.append(
            {
                'country' : country,
                'operators' : d
            }
        )

    with open('mapping/countries.json', 'w') as fp:
        json.dump(c, fp)


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

        print(f"{country}   ={country},   {tuple(operatorKey)},  {tuple(operatorLabel)} ,")

def check_new_operators():
    client = EntsogRawClient()

    json, url = client.query_operators(country_code= None)

    data = parse_general(json)

    result = {}
    for index, item in data.iterrows():
        country = item['operator_country_key']
        # Only TSO's
        if 'TSO' not in item['operator_key']:
            continue

        if country in result.keys():

            result[country]['operators'].append(item['operator_key'])
            result[country]['operator_labels'].append(item['operator_label'])

        else:

            result[country] = {
                'operators' : [item['operator_key']],
                'operator_labels' : [item['operator_label']],
                'label' : item['operator_country_label']
            }

    # Print result

    # Countries
    for key,value in result.items():

        print(f"""{key} = "{key}",   "{value['label']}" """)

    # Enum area
    for key, value in result.items():
        parsed_operators = ','.join([f'"{o}"' for o in value['operators']])
        parsed_operator_labels = ','.join([f'"{o}"' for o in value['operator_labels']])

        print(f"""{key} = ({parsed_operators},),   ({parsed_operator_labels},)""")

    regions = pd.read_csv("https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv")

    # Regions
    for key,value in result.items():

        try:
            region = str(regions[regions['alpha-2'] == key]['sub-region'].iloc[0])
            print(f"""{key} : "{region}" """)
        except Exception as e:
            print(f"""{key} : "REGION" """)
            next
        




#get_operator_mappings()
check_new_operators()