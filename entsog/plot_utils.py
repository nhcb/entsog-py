
from plistlib import PlistFormat
from typing import Optional
import pandas as pd
from entsog.entsog import EntsogPandasClient
import plotnine as p9

from entsog.mappings import lookup_country

ENTSOG_THEME =  p9.theme(
    axis_text = p9.element_text(),
    axis_ticks = p9.element_line(),

    legend_position="bottom",
    legend_direction="horizontal",
    legend_title_align="center",
    legend_box_spacing=0.4,
    legend_text = p9.element_text(size = 8),
    legend_key=p9.element_blank(),
    axis_line=p9.element_line(size=1, colour="black"),
    panel_grid_major=p9.element_line(colour="#d3d3d3"),
    panel_grid_minor=p9.element_blank(),
    panel_border=p9.element_blank(),
    panel_background=p9.element_blank(),
    plot_title=p9.element_text(size=15, 
                            face="bold"),
    text=p9.element_text(size=11),
    axis_text_x=p9.element_text(colour="black", size=8, angle = 45),
    axis_text_y=p9.element_text(colour="black", size=8),

    strip_background=p9.element_rect(colour="#f0f0f0",fill="#f0f0f0"),
    strip_text = p9.element_text(size = 8, colour = 'black'),
    plot_caption = p9.element_text(size = 8, colour = 'black', margin={'r': -50, 't': 0}),
)

UNIT_TRANSFORMATION = {
    'kWh': 1,
    'MWh': 1_000,
    'GWh': 1_000_000,
    'TWh': 1_000_000_000,
    'mcm' : 10.55 * 1_000_000, # https://learnmetrics.com/m3-gas-to-kwh/
}

def label_func(x):

    if len(x) == 2:
        country = lookup_country(x)
        return country.label
    else:
        return x



def plot_area(
    flow_data : pd.DataFrame,
    point_data : pd.DataFrame,
    facet_row : Optional[str] = 'country',
    facet_col : Optional[str] = 'adjacent_country',
    unit : Optional[str] = 'kWh',
    aggregation : Optional[str]  = '1D'
    ):
    
    assert facet_row in ['country','operator_label','balancing_zone', 'region', 'point_type', 'point_label']

    assert facet_col in ['adjacent_country','connected_operators','adjacent_zone', 'adjacent_region', 'point_type', 'point_label','indicator']
    
    flow_data = flow_data.replace({'': None})


    flow_data['value'] = flow_data['value'].astype(float)

    
    mask = (flow_data['direction_key'] == 'exit')
    flow_data[mask]['value'] =  -1* flow_data[mask]['value']

    # Turn to datetime
    flow_data['period_from'] = pd.to_datetime(flow_data['period_from'], utc = True)


    # Join together
    merged = pd.merge(flow_data, point_data, on = ['tso_item_identifier','tso_eic_code','point_key','direction_key'], suffixes = ('','_y'))


    merged.rename(
        columns = {
            't_so_country' : 'country',
            't_so_balancing_zone' : 'balancing_zone'
        },
        inplace= True
    )

    merged['point_label'] = merged['point_label'] + " - (" + merged['country'] + ")"

    # Group by point and period_from
    merged_grouped = merged.groupby([pd.Grouper(key = 'period_from',freq = aggregation, label = 'right'), 'point_label','flow_status', facet_row, facet_col]).agg(
        {'value': 'sum'}
    ).reset_index()


    #merged_grouped['label'] = f"{merged_grouped['point_label']} - {merged_grouped[facet_row]}"

    # If flow status == 'Confirmed', delete the other rows for the keys ['period_from','tso_item_identifier','tso_eic_code','point_key','direction_key']
    # Pandas drop duplicates, but keep the first row where flow_status == 'Confirmed'
    merged_grouped = merged_grouped.sort_values(by = ['flow_status'])
    merged_grouped = merged_grouped.drop_duplicates(subset=['period_from','point_label'], keep='first')
    

    plot = (
    p9.ggplot(merged_grouped)
    + p9.aes(x='period_from', y='value', fill = 'point_label')
    #+ p9.geom_line(size = 1) 
    #+ p9.geom_area(alpha = 0.6)
    + p9.labs(x='', y='', title = '', caption = f'Source: Entsog\nLibrary: entsog-py', color = 'Point Label', fill = 'Point Label')
    + p9.scale_x_date()
    + p9.scale_y_continuous(labels= lambda l: [f"{(round(v / UNIT_TRANSFORMATION[unit]))} {unit}" for v in l],
                            expand = (0, 1))
    + p9.facet_grid(f'{facet_row}~{facet_col}', scales='free_y', labeller = p9.labeller(rows = label_func, cols = label_func))
    + ENTSOG_THEME
    #+ p9.scale_color_brewer(type = 'qual', palette = 'Pastel2')
    + p9.scale_fill_brewer(type = 'qual', palette = 'Pastel1') 
    + p9.geom_area(alpha = 0.8,position = 'stack', color = 'black', size = 0.35)

    )

    return plot