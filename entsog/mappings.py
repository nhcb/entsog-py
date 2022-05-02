import enum
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
                print(
                    f"{s} is not contained in {object}. This information is hardcoded, please raise an issue. Message: {e}")
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
    def __init__(self, _: str, label: str, manager_label: str):
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

    AT = "AT---------", "Austria", "Central European Gas Hub AG",
    BE_H = "BE-H-ZONE--", "H-Zone", "Fluxys Belgium",
    BE_L = "BE-L-ZONE--", "L-Zone", "Fluxys Belgium",
    BE_LUX = "BE-LUX------", "BeLux", "Fluxys Belgium",
    BG_GNTT = "BG-GTNTT---", "GTNTT-BG", "Bulgartransgaz EAD",
    BG_NGTS = "BG-NGTS----", "Bulgaria", "Bulgartransgaz EAD",
    CH = "CH---------", "Switzerland", "Swissgas AS",
    CZ = "CZ---------", "Czech", "NET4GAS, s.r.o.",
    DE_GASPOOL = "DE-GASPOOL-", "GASPOOL", "GASPOOL Balancing Services GmbH",
    DE_NCG = "DE-NCG-----", "NCG", "Net Connect Germany",
    DK = "DK---------", "Denmark", "Energinet",
    EE = "EE---------", "Estonia", "Elering AS",
    ES = "ES---------", "Spain", "Enagas Transporte S.A.U.",
    FI = "FI---------", "Finland", "Gasgrid Finland Oy",
    FR_NORTH = "FR-NORTH---", "PEG North", "GRTgaz",
    FR_SOUTH = "FR-SOUTH---", "PEG South", "GRTgaz",
    FR_TIGF = "FR-TIGF----", "PEG TIGF", "TERÃ‰GA",
    FR_TRS = "FR-TRS------", "TRS", "GRTgaz",
    GR = "GR---------", "Greece", "DESFA S.A.",
    HR = "HR---------", "Croatia", "Plinacro Ltd",
    HU = "HU---------", "Hungary", "FGSZ Ltd.",
    IE = "IE---------", "Ireland", "Gas Networks Ireland",
    IT = "IT---------", "Italy", "Snam Rete Gas S.p.A.",
    LT = "LT---------", "Lithuania", "AB Amber Grid",
    LU = "LU---------", "Luxemburg", "Creos Luxembourg S.A.",
    LV = "LV---------", "Latvia", "Conexus Baltic Grid",
    NL = "NL---------", "Netherlands", "Gasunie Transport Services B.V.",
    PL = "PL---------", "Poland H-gas", "GAZ-SYSTEM S.A.",
    PL_YAMAL = "PL-YAMAL---", "TGPS (YAMAL)", "GAZ-SYSTEM S.A.",
    PT = "PT---------", "Portugal", "REN - Gasodutos, S.A.",
    RO = "RO---------", "RO_NTS", "SNTGN Transgaz S.A.",
    RO_TBP = "RO-TBP-----", "RO_DTS", "SNTGN Transgaz S.A.",
    SE = "SE---------", "Sweden", "Swedegas AB",
    SI = "SI---------", "Slovenia", "Plinovodi d.o.o.",
    SK = "SK---------", "Slovakia", "eustream, a.s.",
    UK = "UK---------", "UK", "National Grid Gas plc",
    UK_IUK = "UK-IUK-----", "IUK", "Interconnector",
    UK_NI = "UK-NI------", "NI", "Premier Transmission Ltd",
    PL_L = "PL-L-gas---", "Poland L-gas", "GAZ-SYSTEM S.A. (ISO)",
    FR = "FR----------", "TRF", "GRTgaz",
    DK_SE = "DK-SE-------", "Joint Bal Zone DK/SE", "Energinet",
    UA = "UA---------", "Ukraine", "LLC Gas TSO of Ukraine",
    MD = "MD---------", "Moldova", "Moldovatransgaz LLC",
    TR = "TR---------", "Turkey", "",
    MK = "MK---------", "North Macedonia", "GA-MA - Skopje",
    RS = "RS---------", "Serbia", "Srbijagas",
    EE_LV = "EE-LV------", "Joint Bal Zone EE/LV", "Elering AS",
    DE_THE = "DE-THE-----", "DE THE BZ", ""


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
        return (self._label)

    AL = "AL", "Albania"
    CH = "CH", "Switzerland"
    AT = "AT", "Austria"
    AZ = "AZ", "Azerbaijan"
    BA = "BA", "Bosnia Herzegovina"
    BE = "BE", "Belgium"
    BG = "BG", "Bulgaria"
    BY = "BY", "Belarus"
    CY = "CY", "Cyprus"
    CZ = "CZ", "Czechia"
    DE = "DE", "Germany"
    DK = "DK", "Denmark"
    EE = "EE", "Estonia"
    ES = "ES", "Spain"
    FI = "FI", "Finland"
    FR = "FR", "France"
    GR = "GR", "Greece"
    HR = "HR", "Croatia"
    HU = "HU", "Hungary"
    IE = "IE", "Ireland"
    UK = "UK", "United Kingdom"
    IT = "IT", "Italy"
    LT = "LT", "Lithuania"
    LU = "LU", "Luxemburg"
    LV = "LV", "Latvia"
    LY = "LY", "Libya"
    MD = "MD", "Moldavia"
    MK = "MK", "North Macedonia"
    MT = "MT", "Malta"
    NL = "NL", "Netherlands"
    NO = "NO", "Norway"
    PL = "PL", "Poland"
    PT = "PT", "Portugal"
    RO = "RO", "Romania"
    RS = "RS", "Serbia"
    RU = "RU", "Russia"
    SE = "SE", "Sweden"
    SI = "SI", "Slovenia"
    SK = "SK", "Slovakia"
    TN = "TN", "Tunisia"
    TR = "TR", "Turkey"
    SM = 'SM', 'San Marino'
    UA = "UA", "Ukraine"


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
    CH = ("AL-TSO-0001", "CH-TSO-0001", "CH-TSO-0002", "CH-TSO-0003", "CH-TSO-0004",), (
    "TAP AG", "Swissgas", "FluxSwiss", "South Stream", "NABUCCO",)
    AL = ("AL-TSO-0002", "AL-TSO-0003", "AL-TSO-0004", "AL-TSO-0005",), (
    "TEE", "MIE Albania & Albgaz", "Albgaz", "ALKOGAP",)
    AT = ("AT-TSO-0001", "AT-TSO-0002", "AT-TSO-0003", "AT-TSO-0004", "AT-TSO-0005", "AT-TSO-0006", "AT-TSO-0007",
          "AT-TSO-0008", "AT-TSO-0009",), (
         "GCA", "BOG", "TAG GmbH", "TIGAS", "OÖ. Ferngas", "Salzburg", "KELAG Netz", "EVA", "South Stream AT",)
    AZ = ("AZ-TSO-0001", "AZ-TSO-0002", "AZ-TSO-0003",), ("BP Exp (Shah Deniz)", "W-Stream Caspian", "SMO",)
    BA = ("BA-TSO-0001",), ("BH Gas",)
    BE = ("BE-TSO-0001", "BE-TSO-0002",), ("Fluxys Belgium", "Unknown",)
    BG = ("BG-TSO-0001", "BG-TSO-0002", "BG-TSO-0003", "BG-TSO-0004",), (
    "Bulgartransgaz", "IBS Future Operator", "ICGB", "South Stream BG",)
    BY = ("BY-TSO-0001",), ("Gazprom Belarus",)
    CY = ("CY-TSO-0001",), ("Cy",)
    CZ = ("CZ-TSO-0001",), ("N4G",)
    DE = ("DE-TSO-0001", "DE-TSO-0002", "DE-TSO-0003", "DE-TSO-0004", "DE-TSO-0005", "DE-TSO-0006", "DE-TSO-0007",
          "DE-TSO-0008", "DE-TSO-0009", "DE-TSO-0010", "DE-TSO-0011", "DE-TSO-0012", "DE-TSO-0013", "DE-TSO-0014",
          "DE-TSO-0015", "DE-TSO-0016", "DE-TSO-0017", "DE-TSO-0018", "DE-TSO-0019", "DE-TSO-0020", "DE-TSO-0021",
          "DE-TSO-0022",), (
         "GASCADE", "Thyssengas", "ONTRAS Gastransport GmbH", "GRTD", "GUD", "GTG", "Fluxys TENP GmbH", "Nowega", "OGE",
         "Bayernets", "Tauerngasleitung", "OPAL NEL Transport", "JGT (TSO)", "TNBW", "GOAL", "OPAL", "NEL",
         "Fluxys Deutschland", "Fluxys TENP & OGE", "LBTG", "GRTD und OGE", "Germany DCS",)
    DK = ("DK-TSO-0001", "DK-TSO-0002",), ("Energinet", "DONG",)
    EE = ("EE-TSO-0001", "EE-TSO-0002",), ("Elering", "Balti Gaas",)
    ES = ("ES-TSO-0001", "ES-TSO-0002", "ES-TSO-0003", "ES-TSO-0004", "ES-TSO-0005", "ES-TSO-0006", "ES-TSO-0007",
          "ES-TSO-0008", "ES-TSO-0009", "ES-TSO-0010",), (
         "Medgaz", "Reganosa (LSO)", "Saggas", "ETN", "BBG", "Enagas", "GNA", "EMPL", "Reganosa", "Enagas (LSO)",)
    FI = ("FI-TSO-0001", "FI-TSO-0002", "FI-TSO-0003",), ("Gasum", "Baltic Connector Oy", "Gasgrid Finland",)
    FR = ("FR-TSO-0001", "FR-TSO-0002", "FR-TSO-0003",), ("Gaz de Normandie", "TERÉGA", "GRTgaz",)
    GR = ("GR-TSO-0001", "GR-TSO-0002", "GR-TSO-0003", "GR-TSO-0004", "GR-TSO-0005",), (
    "DESFA S.A.", "IGI Poseidon", "Future Greek Trans-M", "HRADF", "East Med Operator",)
    HR = ("HR-TSO-0001",), ("Plinacro",)
    HU = ("HU-TSO-0001", "HU-TSO-0002",), ("FGSZ", "MGT",)
    UK = ("IE-TSO-0001", "UK-TSO-0001", "UK-TSO-0002", "UK-TSO-0003", "UK-TSO-0005", "UK-TSO-0006", "UK-TSO-0007",), (
    "GNI (UK)", "National Grid", "PTL", "IUK", "Belfast Gas", "BGE (NI)", "White Stream",)
    IE = ("IE-TSO-0002",), ("GNI",)
    IT = ("IT-TSO-0001", "IT-TSO-0002", "IT-TSO-0003", "IT-TSO-0004", "IT-TSO-0005",), (
    "SNAM RETE GAS", "Galsi", "S.G.I. S.p.A.", "Infrastrutture Trasporto Gas", "ENURA",)
    LT = ("LT-TSO-0001",), ("Amber Grid",)
    LU = ("LU-TSO-0001",), ("CREOS",)
    LV = ("LV-TSO-0001",), ("Conexus Baltic Grid JSC",)
    LY = ("LY-TSO-0001",), ("Green Stream",)
    MD = ("MD-TSO-0001", "RO-TSO-0004",), ("Moldovatransgaz LLC", "Vestmoldtransgaz",)
    MK = ("MK-TSO-0001", "MK-TSO-0002", "MK-TSO-0003",), ("Makpetrol", "GA-MA - Skopje", "MER",)
    MT = ("MT-TSO-0001", "MT-TSO-0002", "MT-TSO-0003",), ("MEW Malta", "Melita TransGas", "ICM",)
    NL = ("NL-TSO-0001", "UK-TSO-0004",), ("GTS", "BBL",)
    NO = ("NO-TSO-0001",), ("Gassco",)
    PL = ("PL-TSO-0001", "PL-TSO-0002",), ("GAZ-SYSTEM (ISO)", "GAZ-SYSTEM",)
    PT = ("PT-TSO-0001", "PT-TSO-0002",), ("REN", "REN Atlantico",)
    RO = ("RO-TSO-0001", "RO-TSO-0002", "RO-TSO-0003",), ("Transgaz", "GdF Energy Romania", "AGRI",)
    RS = ("RS-TSO-0001", "RS-TSO-0002", "RS-TSO-0003",), ("Srbijagas", "Kosovo TSO", "GASTRANS",)
    RU = ("RU-TSO-0001", "RU-TSO-0002", "RU-TSO-0003", "RU-TSO-0004",), (
    "Gazprom", "Nord Stream", "Nord Stream 2", "Chornomornaftogaz",)
    SE = ("SE-TSO-0001", "SE-TSO-0002",), ("Swedegas", "Svenska Kraftnat",)
    SI = ("SI-TSO-0001",), ("Plinovodi",)
    SK = ("SK-TSO-0001", "SK-TSO-0002",), ("Eustream", "Eastring",)
    TN = ("TN-TSO-0001",), ("TMPC",)
    TR = ("TR-TSO-0001", "TR-TSO-0002", "TR-TSO-0003", "TR-TSO-0004",), ("Botas", "TANAP", "Leviathan TSO", "TAGTAS",)
    UA = ("UA-TSO-0001",), ("Gas TSO UA",)


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
    firm_interruption_unplanned = "Firm Interruption Unplanned - Interrupted",
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

# TODO: All countries must be represented in operational_aggregates, or this does NOT work
REGIONS = {
    "AL": "Northern Africa",
    'SM': "Southern Europe",
    "CH": "Western Europe",
    "AT": "Western Europe",
    "AZ": "Western Asia",
    "BA": "Southern Europe",
    "BE": "Western Europe",
    "BG": "Eastern Europe",
    "BY": "Eastern Europe",
    "CY": "Western Asia",
    "CZ": "Eastern Europe",
    "DE": "Western Europe",
    "DK": "Northern Europe",
    "EE": "Northern Europe",
    "ES": "Southern Europe",
    "FI": "Northern Europe",
    "FR": "Western Europe",
    "GR": "Southern Europe",
    "HR": "Southern Europe",
    "HU": "Eastern Europe",
    "IE": "Northern Europe",
    "UK": "Western Europe",
    "IT": "Southern Europe",
    "LT": "Northern Europe",
    "LU": "Western Europe",
    "LV": "Northern Europe",
    "LY": "Northern Africa",
    "MD": "Eastern Europe",
    "MK": "Southern Europe",
    "MT": "Southern Europe",
    "NL": "Western Europe",
    "NO": "Northern Europe",
    "PL": "Eastern Europe",
    "PT": "Southern Europe",
    "RO": "Eastern Europe",
    "RS": "Southern Europe",
    "RU": "Eastern Europe",
    "SE": "Northern Europe",
    "SI": "Southern Europe",
    "SK": "Eastern Europe",
    "TN": "Northern Africa",
    "TR": "Western Asia",
    "UA": "Eastern Europe",
    'Undefined': 'Undefined'
}
