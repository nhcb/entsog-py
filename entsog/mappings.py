import enum
from logging import error
from typing import Union

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

    return _lookup(s, Indicator)

def lookup_balancing_zone(s: Union['BalancingZone', str]) -> 'BalancingZone':

    return _lookup(s, BalancingZone)

def lookup_country(s: Union['Country', str]) -> 'Country':

    return _lookup(s, Country)


def _lookup(s, object):
    if isinstance(s, object):
        # If it already is the required object, we're happy
        _object = s
    else:  # It is a string
        try:
            # If it is a code string, we do a lookup
            _object = object[s]
        except KeyError:
            # It is not, it may be a direct code
            try:
                _object = [_object for _object in object if _object.value == s][0]
            except IndexError as e:
                print(f"{s} is not contained in {object}. This information is hardcoded, please raise an issue. Message: {e}")
                raise

    return _object



class BalancingZone(enum.Enum):
    '''
    ENUM containing 3 things about a BalancingZone: Key, Label, Manager
    '''
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, label: str, manager_label:str):
        self._label = label
        self._manager_label = manager_label

    def __str__(self):
        return self.value

    @property
    def code(self):
        return (self.value)

    @property
    def label(self):
        return (self._operator_labels)

    @property
    def manager_label(self):
        return (self._operator_labels)


    AT="AT---------","Austria","Central European Gas Hub AG",
    BE_H="BE-H-ZONE--","H-Zone","Fluxys Belgium",
    BE_L="BE-L-ZONE--","L-Zone","Fluxys Belgium",
    BE_LUX="BE-LUX------","BeLux","Fluxys Belgium",
    BG_GNTT="BG-GTNTT---","GTNTT-BG","Bulgartransgaz EAD",
    BG_NGTS="BG-NGTS----","Bulgaria","Bulgartransgaz EAD",
    CH="CH---------","Switzerland","Swissgas AS",
    CZ="CZ---------","Czech","NET4GAS, s.r.o.",
    DE_GASPOOL="DE-GASPOOL-","GASPOOL","GASPOOL Balancing Services GmbH",
    DE_NCG="DE-NCG-----","NCG","Net Connect Germany",
    DK="DK---------","Denmark","Energinet",
    EE="EE---------","Estonia","Elering AS",
    ES="ES---------","Spain","Enagas Transporte S.A.U.",
    FI="FI---------","Finland","Gasgrid Finland Oy",
    FR_NORTH="FR-NORTH---","PEG North","GRTgaz",
    FR_SOUTH="FR-SOUTH---","PEG South","GRTgaz",
    FR_TIGF="FR-TIGF----","PEG TIGF","TERÃ‰GA",
    FR_TRS="FR-TRS------","TRS","GRTgaz",
    GR="GR---------","Greece","DESFA S.A.",
    HR="HR---------","Croatia","Plinacro Ltd",
    HU="HU---------","Hungary","FGSZ Ltd.",
    IE="IE---------","Ireland","Gas Networks Ireland",
    IT="IT---------","Italy","Snam Rete Gas S.p.A.",
    LT="LT---------","Lithuania","AB Amber Grid",
    LU="LU---------","Luxemburg","Creos Luxembourg S.A.",
    LV="LV---------","Latvia","Conexus Baltic Grid",
    NL="NL---------","Netherlands","Gasunie Transport Services B.V.",
    PL="PL---------","Poland H-gas","GAZ-SYSTEM S.A.",
    PL_YAMAL="PL-YAMAL---","TGPS (YAMAL)","GAZ-SYSTEM S.A.",
    PT="PT---------","Portugal","REN - Gasodutos, S.A.",
    RO="RO---------","RO_NTS","SNTGN Transgaz S.A.",
    RO_TBP="RO-TBP-----","RO_DTS","SNTGN Transgaz S.A.",
    SE="SE---------","Sweden","Swedegas AB",
    SI="SI---------","Slovenia","Plinovodi d.o.o.",
    SK="SK---------","Slovakia","eustream, a.s.",
    UK="UK---------","UK","National Grid Gas plc",
    UK_IUK="UK-IUK-----","IUK","Interconnector",
    UK_NI="UK-NI------","NI","Premier Transmission Ltd",
    PL_L="PL-L-gas---","Poland L-gas","GAZ-SYSTEM S.A. (ISO)",
    FR="FR----------","TRF","GRTgaz",
    DK_SE="DK-SE-------","Joint Bal Zone DK/SE","Energinet",
    UA="UA---------","Ukraine","LLC Gas TSO of Ukraine",
    MD="MD---------","Moldova","Moldovatransgaz LLC",
    TR="TR---------","Turkey","",
    MK="MK---------","North Macedonia","GA-MA - Skopje",
    RS="RS---------","Serbia","Srbijagas",
    EE_LV="EE-LV------","Joint Bal Zone EE/LV","Elering AS",
    DE_THE="DE-THE-----","DE THE BZ",""

class Country(enum.Enum):
    '''
    ENUM containing 2 things about a country: code, label
    '''
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, label: str):
        self._label = label

    def __str__(self):
        return self.value

    @property
    def code(self):
        return (self.value)

    @property
    def label(self):
        return (self._operator_labels)

    AT="AT","Austria",
    BE="BE","Belgium",
    BG="BG","Bulgaria",
    CH="CH","Switzerland",
    CZ="CZ","Czech",
    DE="DE","Germany",
    DK="DK","Denmark",
    EE="EE","Estonia",
    ES="ES","Spain",
    FI="FI","Finland",
    FR="FR","France",
    GR="GR","Greece",
    HR="HR","Croatia",
    HU="HU","Hungary",
    IE="IE","Ireland",
    IT="IT","Italy",
    LT="LT","Lithuania",
    LU="LU","Luxemburg",
    LV="LV","Latvia",
    NL="NL","Netherlands",
    PL="PL","Poland",
    PT="PT","Portugal",
    RO="RO","Romania",
    SE="SE","Sweden",
    SI="SI","Slovenia",
    SK="SK","Slovakia",
    UK="UK","UK",
    UA="UA","Ukraine",
    MD="MD","Moldova",
    TR="TR","Turkey",
    MK="MK","North Macedonia",
    RS="RS","Serbia"




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

REGIONS = {
    'BE': 'WE',
    'NL': 'WE',
    'DE': 'WE',
    'FR': 'WE',
    'CH': 'WE',
    'AT': 'WE',
    'CZ': 'EE',
    'UK': 'NE',
    'NO': 'NE',
    'HU': 'EE',
    'IT': 'SE',
    'ES': 'SE',
    'SI': 'EE',
    'RS': 'EE',
    'PL': 'EE',
    'ME': 'EE', # Montenegro
    'DK': 'NE',
    'RO': 'EE',
    'LT': 'EE',
    'BG': 'EE',
    'SE': 'NE',
    'LV': 'EE',
    'IE': 'NE',
    'BA': 'EE',
    'FI': 'NE',
    'MK': 'EE',
    'PT': 'SE',
    'GR': 'SE',
    'MT': 'SE'
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
