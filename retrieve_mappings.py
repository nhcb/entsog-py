from entsog import EntsogRawClient,EntsogPandasClient
import json

def get_operator_mappings():
    client = EntsogPandasClient()
    operators = client.query_operators()
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

get_operator_mappings()