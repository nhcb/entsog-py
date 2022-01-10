import enum
from typing import Union
#import EntsogPandasClient

def lookup_area(s: Union['Area', str]) -> 'Area':
    if isinstance(s, Area):
        # If it already is an Area object, we're happy
        area = s
    else:  # It is a string
        try:
            # If it is a 'country code' string, we do a lookup
            area = Area[s]
        except KeyError:
            # It is not, it may be a direct code
            try:
                area = [area for area in Area if area.value == s][0]
            except IndexError:
            # None argument
                area = None

    return area

def lookup_indicator(s: Union['Indicator', str]) -> 'Indicator':
    if isinstance(s, Indicator):
        # If it already is an Area object, we're happy
        area = s
    else:  # It is a string
        try:
            # If it is a 'country code' string, we do a lookup
            area = Indicator[s]
        except KeyError:
            # It is not, it may be a direct code
            try:
                area = [area for area in Indicator if area.value == s][0]
            except IndexError:
            # None argument
                area = None

    return area

#POINTS = EntsogPandasClient().query_operator_point_directions(limit = -1)

class Points():
    def __init__(self):
        self.points = POINTS # Initialize with the original dataset

    #def filter_operators(operators : Union[list[Operator], list[str]]):
    #    return

    #def filter_country_codes(country_codes: Union[list[Area], list[str]]):
    #    return


class Area(enum.Enum):
    '''
    ENUM containing 2 things about an Area: OperatorKeys, OperatorLabels
    '''
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, operator_labels: tuple):
        self._operator_labels = operator_labels

    def __str__(self):
        return self.value

    @property
    def code(self):
        return (self.value)

    @property
    def operator_labels(self):
        return (self._operator_labels)

    # One element tuple consists of (element, )
    BE   =   ('BE-TSO-0001',),  ('Fluxys Belgium',) ,
    BG   =   ('BG-TSO-0001',),  ('Bulgartransgaz',) ,
    CZ   =   ('CZ-TSO-0001',),  ('NET4GAS',) ,
    DE   =   ('DE-TSO-0001', 'DE-TSO-0002', 'DE-TSO-0003', 'DE-TSO-0005', 'DE-TSO-0009', 'DE-TSO-0010', 'DE-TSO-0013', 'DE-TSO-0014', 'DE-TSO-0006', 'DE-TSO-0004', 'DE-TSO-0007', 'DE-TSO-0008', 'DE-TSO-0015', 'DE-TSO-0016', 'DE-TSO-0017', 'DE-TSO-0018', 'DE-TSO-0020'),  ('GASCADE Gastransport', 'Thyssengas', 'ONTRAS', 'Gasunie Deutschland ', 'Open Grid Europe', 'bayernets', 'jordgas Transport', 'terranets bw', 'Gastransport Nord', 'GRTgaz Deutschland', 'Fluxys TENP', 'Nowega', 'GOAL', 'OPAL Gastransport', 'NEL Gastransport', 'Fluxys Deutschland', 'LBTG') ,
    EE   =   ('EE-TSO-0001',),  ('Elering',) ,
    ES   =   ('ES-TSO-0006',),  ('Enagas',) ,
    FR   =   ('FR-TSO-0002', 'FR-TSO-0003'),  ('TERÉGA', 'GRTgaz') ,
    GR   =   ('GR-TSO-0001',),  ('DESFA',) ,
    HR   =   ('HR-TSO-0001',),  ('Plinacro Ltd',) ,
    IE   =   ('IE-TSO-0002',),  ('GNI',) ,
    IT   =   ('IT-TSO-0001', 'IT-TSO-0003', 'IT-TSO-0004'),  ('Snam Rete Gas', 'SGI', 'ITG') ,
    LV   =   ('LV-TSO-0001',),  ('Conexus',) ,
    NL   =   ('NL-TSO-0001', 'UK-TSO-0004'),  ('GTS', 'BBL company') ,
    PL   =   ('PL-TSO-0002', 'PL-TSO-0001'),  ('GAZ-SYSTEM', 'GAZ-SYSTEM (ISO)') ,
    PT   =   ('PT-TSO-0001',),  ('REN - Gasodutos',) ,
    RO   =   ('RO-TSO-0001',),  ('Transgaz',) ,
    SI   =   ('SI-TSO-0001',),  ('Plinovodi',) ,
    UA   =   ('UA-TSO-0001',),  ('Gas TSO UA',) ,
    UK   =   ('UK-TSO-0001', 'UK-TSO-0002', 'IE-TSO-0001', 'UK-TSO-0003'),  ('National Grid Gas', 'Premier Transmission', 'GNI (UK)', 'Interconnector') ,
    LU   =   ('LU-TSO-0001',),  ('Creos Luxembourg',) ,
    HU   =   ('HU-TSO-0001',),  ('FGSZ',) ,
    AT   =   ('AT-TSO-0001', 'AT-TSO-0003'),  ('Gas Connect Austria', 'TAG') ,
    LT   =   ('LT-TSO-0001',),  ('Amber Grid',) ,
    SK   =   ('SK-TSO-0001',),  ('eustream',) ,
    FI   =   ('FI-TSO-0003',),  ('Gasgrid Finland',) ,
    CH   =   ('AL-TSO-0001',),  ('TAP',) ,
    DK   =   ('DK-TSO-0001'),  ('Energinet',) 

# TODO: Add label containing description
class Indicator(enum.Enum):
    '''
    ENUM containing full label of indicator
    '''
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str):
        self = self

    def __str__(self):
        return self.value

    @property
    def code(self):
        return (self.value)

    interruption_capacity = "Actual interruption of interruptible capacity",
    allocation = "Allocation",
    firm_available = "Firm Available",
    firm_booked = "Firm Booked",
    firm_interruption_planned = "Firm Interruption Planned - Interrupted",
    firm_interruption_unplanned ="Firm Interruption Unplanned - Interrupted",
    firm_technical = "Firm Technical",
    gcv = "GCV",
    interruptible_available = "Interruptible Available",
    interruptible_booked = "Interruptible Booked",
    interruptible_interruption_actual = "Interruptible Interruption Actual – Interrupted",
    interruptible_interruption_planned = "Interruptible Interruption Planned - Interrupted",
    interruptible_total = "Interruptible Total",
    nominations = "Nominations",
    physical_flow = "Physical Flow",
    firm_interruption_capacity_planned = "Planned interruption of firm capacity",
    renomination = "Renomination",
    firm_interruption_capacity_unplanned = "Unplanned interruption of firm capacity",
    wobbe_index = "Wobbe Index",
    oversubscription_available = "Available through Oversubscription",
    surrender_available = "Available through Surrender",
    uioli_available_lt = "Available through UIOLI long-term",
    uioli_available_st = "Available through UIOLI short-term"







DATASET_MAPPINGS = {
    '1': 'Operators and Operational data',
    '2': 'Points and CMP Unsuccessful Request',
    '3': 'Balancing Zones CMP Auction Premium, balancing zones',
    '4': 'Interconnections and CMP Unavailable Firm',
    '5': 'Operator Point Directions',
    '6': 'Aggregate Interconnections'
}


PSRTYPE_MAPPINGS = {
    'A03': 'Mixed',
    'A04': 'Generation',
    'A05': 'Load',
    'B01': 'Biomass',
    'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas',
    'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal',
    'B06': 'Fossil Oil',
    'B07': 'Fossil Oil shale',
    'B08': 'Fossil Peat',
    'B09': 'Geothermal',
    'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-river and poundage',
    'B12': 'Hydro Water Reservoir',
    'B13': 'Marine',
    'B14': 'Nuclear',
    'B15': 'Other renewable',
    'B16': 'Solar',
    'B17': 'Waste',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    'B20': 'Other',
    'B21': 'AC Link',
    'B22': 'DC Link',
    'B23': 'Substation',
    'B24': 'Transformer'}

DOCSTATUS = {'A01': 'Intermediate',
             'A02': 'Final',
             'A05': 'Active',
             'A09': 'Cancelled',
             'X01': 'Estimated'}

BSNTYPE = {'A29': 'Already allocated capacity (AAC)',
           'A43': 'Requested capacity (without price)',
           'A46': 'System Operator redispatching',
           'A53': 'Planned maintenance',
           'A54': 'Unplanned outage',
           'A85': 'Internal redispatch',
           'A95': 'Frequency containment reserve',
           'A96': 'Automatic frequency restoration reserve',
           'A97': 'Manual frequency restoration reserve',
           'A98': 'Replacement reserve',
           'B01': 'Interconnector network evolution',
           'B02': 'Interconnector network dismantling',
           'B03': 'Counter trade',
           'B04': 'Congestion costs',
           'B05': 'Capacity allocated (including price)',
           'B07': 'Auction revenue',
           'B08': 'Total nominated capacity',
           'B09': 'Net position',
           'B10': 'Congestion income',
           'B11': 'Production unit'}

MARKETAGREEMENTTYPE = {'A01': 'Daily',
                       'A02': 'Weekly',
                       'A03': 'Monthly',
                       'A04': 'Yearly',
                       'A05': 'Total',
                       'A06': 'Long term',
                       'A07': 'Intraday',
                       'A13': 'Hourly'}

DOCUMENTTYPE = {'A09': 'Finalised schedule',
                'A11': 'Aggregated energy data report',
                'A15': 'Acquiring system operator reserve schedule',
                'A24': 'Bid document',
                'A25': 'Allocation result document',
                'A26': 'Capacity document',
                'A31': 'Agreed capacity',
                'A38': 'Reserve allocation result document',
                'A44': 'Price Document',
                'A61': 'Estimated Net Transfer Capacity',
                'A63': 'Redispatch notice',
                'A65': 'System total load',
                'A68': 'Installed generation per type',
                'A69': 'Wind and solar forecast',
                'A70': 'Load forecast margin',
                'A71': 'Generation forecast',
                'A72': 'Reservoir filling information',
                'A73': 'Actual generation',
                'A74': 'Wind and solar generation',
                'A75': 'Actual generation per type',
                'A76': 'Load unavailability',
                'A77': 'Production unavailability',
                'A78': 'Transmission unavailability',
                'A79': 'Offshore grid infrastructure unavailability',
                'A80': 'Generation unavailability',
                'A81': 'Contracted reserves',
                'A82': 'Accepted offers',
                'A83': 'Activated balancing quantities',
                'A84': 'Activated balancing prices',
                'A85': 'Imbalance prices',
                'A86': 'Imbalance volume',
                'A87': 'Financial situation',
                'A88': 'Cross border balancing',
                'A89': 'Contracted reserve prices',
                'A90': 'Interconnection network expansion',
                'A91': 'Counter trade notice',
                'A92': 'Congestion costs',
                'A93': 'DC link capacity',
                'A94': 'Non EU allocations',
                'A95': 'Configuration document',
                'B11': 'Flow-based allocations'}

PROCESSTYPE = {
    'A01': 'Day ahead',
    'A02': 'Intra day incremental',
    'A16': 'Realised',
    'A18': 'Intraday total',
    'A31': 'Week ahead',
    'A32': 'Month ahead',
    'A33': 'Year ahead',
    'A39': 'Synchronisation process',
    'A40': 'Intraday process',
    'A46': 'Replacement reserve',
    'A47': 'Manual frequency restoration reserve',
    'A51': 'Automatic frequency restoration reserve',
    'A52': 'Frequency containment reserve',
    'A56': 'Frequency restoration reserve'
}

# neighbouring bidding zones that have cross_border flows
NEIGHBOURS = {
    'BE': ['NL', 'DE_AT_LU', 'FR', 'GB', 'DE_LU'],
    'NL': ['BE', 'DE_AT_LU', 'DE_LU', 'GB', 'NO_2', 'DK_1'],
    'DE_AT_LU': ['BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'IT_NORD', 'IT_NORD_AT', 'NL', 'PL', 'SE_4', 'SI'],
    'FR': ['BE', 'CH', 'DE_AT_LU', 'DE_LU', 'ES', 'GB', 'IT_NORD', 'IT_NORD_FR'],
    'CH': ['AT', 'DE_AT_LU', 'DE_LU', 'FR', 'IT_NORD', 'IT_NORD_CH'],
    'AT': ['CH', 'CZ', 'DE_LU', 'HU', 'IT_NORD', 'SI'],
    'CZ': ['AT', 'DE_AT_LU', 'DE_LU', 'PL', 'SK'],
    'GB': ['BE', 'FR', 'IE_SEM', 'NL'],
    'NO_2': ['DK_1', 'NL', 'NO_5'],
    'HU': ['AT', 'HR', 'RO', 'RS', 'SK', 'UA'],
    'IT_NORD': ['CH', 'DE_AT_LU', 'FR', 'SI', 'AT', 'IT_CNOR'],
    'ES': ['FR', 'PT'],
    'SI': ['AT', 'DE_AT_LU', 'HR', 'IT_NORD'],
    'RS': ['AL', 'BA', 'BG', 'HR', 'HU', 'ME', 'MK', 'RO'],
    'PL': ['CZ', 'DE_AT_LU', 'DE_LU', 'LT', 'SE_4', 'SK', 'UA'],
    'ME': ['AL', 'BA', 'RS'],
    'DK_1': ['DE_AT_LU', 'DE_LU', 'DK_2', 'NO_2', 'SE_3', 'NL'],
    'RO': ['BG', 'HU', 'RS', 'UA'],
    'LT': ['BY', 'LV', 'PL', 'RU_KGD', 'SE_4'],
    'BG': ['GR', 'MK', 'RO', 'RS', 'TR'],
    'SE_3': ['DK_1', 'FI', 'NO_1', 'SE_4'],
    'LV': ['EE', 'LT', 'RU'],
    'IE_SEM': ['GB'],
    'BA': ['HR', 'ME', 'RS'],
    'NO_1': ['NO_2', 'NO_3', 'NO_5', 'SE_3'],
    'SE_4': ['DE_AT_LU', 'DE_LU', 'DK_2', 'LT', 'PL'],
    'NO_5': ['NO_1', 'NO_2', 'NO_3'],
    'SK': ['CZ', 'HU', 'PL', 'UA'],
    'EE': ['FI', 'LV', 'RU'],
    'DK_2': ['DE_AT_LU', 'DE_LU', 'SE_4'],
    'FI': ['EE', 'NO_4', 'RU', 'SE_1', 'SE_3'],
    'NO_4': ['SE_2', 'FI', 'SE_1'],
    'SE_1': ['FI', 'NO_4', 'SE_2'],
    'SE_2': ['NO_3', 'NO_4', 'SE_3'],
    'DE_LU': ['AT', 'BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'NL', 'PL', 'SE_4'],
    'MK': ['BG', 'GR', 'RS'],
    'PT': ['ES'],
    'GR': ['AL', 'BG', 'IT_BRNN', 'IT_GR', 'MK', 'TR'],
    'NO_3': ['NO_4', 'NO_5', 'SE_2'],
    'IT': ['AT', 'FR', 'GR', 'MT', 'ME', 'SI', 'CH'],
    'IT_BRNN': ['GR', 'IT_SUD'],
    'IT_SUD': ['IT_BRNN', 'IT_CSUD', 'IT_FOGN', 'IT_ROSN', 'IT_CALA'],
    'IT_FOGN': ['IT_SUD'],
    'IT_ROSN': ['IT_SICI', 'IT_SUD'],
    'IT_CSUD': ['IT_CNOR', 'IT_SARD', 'IT_SUD'],
    'IT_CNOR': ['IT_NORD', 'IT_CSUD', 'IT_SARD'],
    'IT_SARD': ['IT_CNOR', 'IT_CSUD'],
    'IT_SICI': ['IT_CALA', 'IT_ROSN', 'MT'],
    'IT_CALA': ['IT_SICI', 'IT_SUD'],
    'MT': ['IT_SICI']
}
