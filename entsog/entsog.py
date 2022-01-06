import logging
from typing import Union, Optional, Dict

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
import pytz
import requests
import simplejson

from bs4 import BeautifulSoup

from entsog.exceptions import InvalidPSRTypeError, InvalidBusinessParameterError
from .exceptions import NoMatchingDataError, PaginationError
from .mappings import Area, NEIGHBOURS, lookup_area
from .parsers import parse_general, parse_prices, parse_loads, parse_generation, \
    parse_installed_capacity_per_plant, parse_crossborder_flows, \
    parse_unavailabilities, parse_contracted_reserve, parse_imbalance_prices_zip, \
    parse_netpositions, parse_procured_balancing_capacity
from .decorators import retry, paginated, year_limited, day_limited


__title__ = "Entsog-py"
__version__ = "0.0.1"
__author__ = "nhcb"
__license__ = "MIT"

URL = 'https://transparency.entsog.eu/api/v1'


class EntsogRawClient:

    """
        Client to perform API calls and return the raw responses API-documentation:
        https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf
        User Manual:
        https://www.entsog.eu/sites/default/files/2021-07/ENTSOG%20-%20TP%20User%20Manual_v_4.5.pdf

        Attributions: Entire framework is based upon the existing scraper for Entsoe authored from EnergieID.be
        """

    def __init__(
            self, session: Optional[requests.Session] = None,
            retry_count: int = 1, retry_delay: int = 0,
            proxies: Optional[Dict] = None, timeout: Optional[int] = None):
        """
        Parameters
        ----------
        session : requests.Session
        retry_count : int
            number of times to retry the call if the connection fails
        retry_delay: int
            amount of seconds to wait between retries
        proxies : dict
            requests proxies
        timeout : int
        """

        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.timeout = timeout



    @retry
    def _base_request(self, endpoint: str, params: Dict) -> requests.Response:

        """
        Parameters
        ----------
        endpoint: str
            endpoint to url to gather data, in format /<endpoint>
        params : dict

        Returns
        -------
        requests.Response
        """

        url = URL + endpoint
        base_params = {
            'timeZone' : 'UCT'
        }

        params.update(base_params)

        logging.debug(f'Performing request to {url} with params {params}')
        response = self.session.get(url=url, params=params,
                                    proxies=self.proxies, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('text')
            if len(text):
                error_text = soup.find('text').text
                if 'No matching data found' in error_text:
                    raise NoMatchingDataError
                elif "check you request against dependency tables" in error_text:
                    raise InvalidBusinessParameterError
                elif "is not valid for this area" in error_text:
                    raise InvalidPSRTypeError
                elif 'amount of requested data exceeds allowed limit' in error_text:
                    requested = error_text.split(' ')[-2]
                    allowed = error_text.split(' ')[-5]
                    raise PaginationError(
                        f"The API is limited to {allowed} elements per "
                        f"request. This query requested for {requested} "
                        f"documents and cannot be fulfilled as is.")
            raise e
        else:
            if response.headers.get('content-type', '') == 'application/xml':
                if 'No matching data found' in response.text:
                    raise NoMatchingDataError
            return response

    @staticmethod
    def _datetime_to_str(dtm: pd.Timestamp) -> str:
        """
        Convert a datetime object to a string in UTC
        of the form YYYYMMDDhh00

        Parameters
        ----------
        dtm : pd.Timestamp
            Recommended to use a timezone-aware object!
            If timezone-naive, UTC is assumed

        Returns
        -------
        str
        """
        if dtm.tzinfo is not None and dtm.tzinfo != pytz.UTC:
            dtm = dtm.tz_convert("UTC")

        ret_str = dtm.date()
        #fmt = '%Y%m%d%H00'
        #ret_str = dtm.strftime(fmt).date()
        
        return ret_str

    """
    REFERENTIAL DATA
    "pointKey",
    "pointLabel",
    "isSingleOperator",
    "pointTooltip",
    "pointEicCode",
    "controlPointType",
    "tpMapX",
    "tpMapY",
    "pointType",
    "commercialType",
    "importFromCountryKey",
    "importFromCountryLabel",
    "hasVirtualPoint",
    "virtualPointKey",
    "virtualPointLabel",
    "hasData",
    "isPlanned",
    "isInterconnection",
    "isImport",
    "infrastructureKey",
    "infrastructureLabel",
    "isCrossBorder",
    "euCrossing",
    "isInvalid",
    "isMacroPoint",
    "isCAMRelevant",
    "isPipeInPipe",
    "isCMPRelevant",
    "id",
    "dataSet"
    """

    def query_connection_points(self, limit : int = -1) -> str:
        """
        Type: JSON
        Interconnection points as visible on the Map. Please note that
        this only included the Main points and not the sub points. To
        download all points, the API for Operator Point Directions
        should be used.
        
        Parameters
        ----------
        limit: int

        Returns
        -------
        str
        """

        params = {
            'limit': limit
        }
        

        response = self._base_request(endpoint = '/connectionpoints',params=params)

        return response.text

    """
    "operatorLogoUrl",
    "operatorKey",
    "operatorLabel",
    "operatorLabelLong",
    "operatorTooltip",
    "operatorCountryKey",
    "operatorCountryLabel",
    "operatorCountryFlag",
    "operatorTypeLabel",
    "operatorTypeLabelLong",
    "participates",
    "membershipLabel",
    "tsoEicCode",
    "tsoDisplayName",
    "tsoShortName",
    "tsoLongName",
    "tsoStreet",
    "tsoBuildingNumber",
    "tsoPostOfficeBox",
    "tsoZipCode",
    "tsoCity",
    "tsoContactName",
    "tsoContactPhone",
    "tsoContactEmail",
    "tsoContactUrl",
    "tsoContactRemarks",
    "tsoGeneralWebsiteUrl",
    "tsoGeneralWebsiteUrlRemarks",
    "tsoTariffInformationUrl",
    "tsoTariffInformationUrlRemarks",
    "tsoTariffCalculatorUrl",
    "tsoTariffCalculatorUrlRemarks",
    "tsoCapacityInformationUrl",
    "tsoCapacityInformationUrlRemarks",
    "tsoGasQualityURL",
    "tsoGasQualityURLRemarks",
    "tsoAccessConditionsUrl",
    "tsoAccessConditionsUrlRemarks",
    "tsoContractDocumentsUrl",
    "tsoContractDocumentsUrlRemarks",
    "tsoMaintainanceUrl",
    "tsoMaintainanceUrlRemarks",
    "gasDayStartHour",
    "gasDayStartHourRemarks",
    "multiAnnualContractsIsAvailable",
    "multiAnnualContractsRemarks",
    "annualContractsIsAvailable",
    "annualContractsRemarks",
    "halfAnnualContractsIsAvailable",
    "halfAnnualContractsRemarks",
    "quarterlyContractsIsAvailable",
    "quarterlyContractsRemarks",
    "monthlyContractsIsAvailable",
    "monthlyContractsRemarks",
    "dailyContractsIsAvailable",
    "dailyContractsRemarks",
    "withinDayContractsIsAvailable",
    "withinDayContractsRemarks",
    "availableContractsRemarks",
    "firmCapacityTariffIsApplied",
    "firmCapacityTariffUnit",
    "firmCapacityTariffRemarks",
    "interruptibleCapacityTariffIsApplied",
    "interruptibleCapacityTariffUnit",
    "interruptibleCapacityTariffRemarks",
    "auctionIsApplied",
    "auctionTariffIsApplied",
    "auctionCapacityTariffUnit",
    "auctionRemarks",
    "commodityTariffIsApplied",
    "commodityTariffUnit",
    "commodityTariffPrice",
    "commodityTariffRemarks",
    "othersTariffIsApplied",
    "othersTariffRemarks",
    "generalTariffInformationRemarks",
    "generalCapacityRemark",
    "firstComeFirstServedIsApplied",
    "firstComeFirstServedRemarks",
    "openSubscriptionWindowIsApplied",
    "openSubscriptionWindowRemarks",
    "firmTechnicalRemark",
    "firmBookedRemark",
    "firmAvailableRemark",
    "interruptibleTotalRemark",
    "interruptibleBookedRemark",
    "interruptibleAvailableRemark",
    "tsoGeneralRemarks",
    "balancingModel",
    "bMHourlyImbalanceToleranceIsApplied",
    "bMHourlyImbalanceToleranceIsInformation",
    "bMHourlyImbalanceToleranceIsRemarks",
    "bMDailyImbalanceToleranceIsApplied",
    "bMDailyImbalanceToleranceIsInformation",
    "bMDailyImbalanceToleranceIsRemarks",
    "bMAdditionalDailyImbalanceToleranceIsApplied",
    "bMAdditionalDailyImbalanceToleranceIsInformation",
    "bMAdditionalDailyImbalanceToleranceIsRemarks",
    "bMCumulatedImbalanceToleranceIsApplied",
    "bMCumulatedImbalanceToleranceIsInformation",
    "bMCumulatedImbalanceToleranceIsRemarks",
    "bMAdditionalCumulatedImbalanceToleranceIsApplied",
    "bMAdditionalCumulatedImbalanceToleranceIsInformation",
    "bMAdditionalCumulatedImbalanceToleranceIsRemarks",
    "bMStatusInformation",
    "bMStatusInformationFrequency",
    "bMPenalties",
    "bMCashOutRegime",
    "bMRemarks",
    "gridTransportModelType",
    "gridTransportModelTypeRemarks",
    "gridConversionFactorCapacityDefault",
    "gridConversionFactorCapacityDefaultRemaks",
    "gridGrossCalorificValueDefaultValue",
    "gridGrossCalorificValueDefaultValueTo",
    "gridGrossCalorificValueDefaultUnit",
    "gridGrossCalorificValueDefaultRemarks",
    "gridGasSourceDefault",
    "lastUpdateDateTime",
    "transparencyInformationURL",
    "transparencyInformationUrlRemarks",
    "transparencyGuidelinesInformationURL",
    "transparencyGuidelinesInformationUrlRemarks",
    "tsoUmmRssFeedUrlGas",
    "tsoUmmRssFeedUrlOther",
    "includeUmmInAcerRssFeed",
    "id",
    "dataSet"
    """

    def query_operators(self,
    country_code: Optional[Union[Area, str]] = None,
    operator_key : Optional[str] = None,
    has_data : int = 1,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All operators connected to the transmission system

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        if country_code is not None:
            area = lookup_area(country_code)
            params = {
                'operatorKey' : list(area.code),
                'hasData' : has_data,
                'limit': limit
            }
        elif operator_key is not None:
            params = {
                'limit': limit,
                'operatorKey' : operator_key,
                'hasData' : has_data
            }
        else:
            params = {
                'limit': limit,
                'hasData' : has_data
            }
        
        response = self._base_request(endpoint = '/operators',params=params)

        return response.text

    """
    "tpMapX",
    "tpMapY",
    "controlPointType",
    "bzKey",
    "bzLabel",
    "bzLabelLong",
    "bzTooltip",
    "bzEicCode",
    "bzManagerKey",
    "bzManagerLabel",
    "replacedBy",
    "isDeactivated",
    "id",
    "dataSet"
    """

    def query_balancing_zones(self, limit : int = -1) -> str:
        
        """
        Type: JSON
        European balancing zones

        Parameters
        ----------
        limit: int

        Returns
        -------
        str
        """

        params = {
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/balancingzones',params=params)

        return response.text

    """
    "pointKey",
    "pointLabel",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "directionKey",
    "validFrom",
    "validTo",
    "hasData",
    "isVirtualizedCommercially",
    "virtualizedCommerciallySince",
    "isVirtualizedOperationally",
    "virtualizedOperationallySince",
    "isPipeInPipe",
    "relatedOperators",
    "relatedPoints",
    "pipeInPipeWithTsoKey",
    "pipeInPipeWithTsoLabel",
    "isDoubleReporting",
    "doubleReportingWithTsoKey",
    "doubleReportingWithTsoLabel",
    "tsoItemIdentifier",
    "tpTsoItemLabel",
    "tpTsoValidFrom",
    "tpTsoValidTo",
    "tpTsoRemarks",
    "tpTsoConversionFactor",
    "tpRmkGridConversionFactorCapacityDefault",
    "tpTsoGCVMin",
    "tpTsoGCVMax",
    "tpTsoGCVRemarks",
    "tpTsoGCVUnit",
    "tpTsoEntryExitType",
    "multiAnnualContractsIsAvailable",
    "multiAnnualContractsRemarks",
    "annualContractsIsAvailable",
    "annualContractsRemarks",
    "halfAnnualContractsIsAvailable",
    "halfAnnualContractsRemarks",
    "quarterlyContractsIsAvailable",
    "quarterlyContractsRemarks",
    "monthlyContractsIsAvailable",
    "monthlyContractsRemarks",
    "dailyContractsIsAvailable",
    "dailyContractsRemarks",
    "dayAheadContractsIsAvailable",
    "dayAheadContractsRemarks",
    "availableContractsRemarks",
    "sentenceCMPUnsuccessful",
    "sentenceCMPUnavailable",
    "sentenceCMPAuction",
    "sentenceCMPMadeAvailable",
    "lastUpdateDateTime",
    "isInvalid",
    "isCAMRelevant",
    "isCMPRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "virtualReverseFlow",
    "virtualReverseFlowRemark",
    "tSOCountry",
    "tSOBalancingZone",
    "crossBorderPointType",
    "eURelationship",
    "connectedOperators", 
    "adjacentTsoEic",
    "adjacentOperatorKey",
    "adjacentCountry",
    "pointType",
    "idPointType",
    "adjacentZones",
    "id",
    "dataSet"
    """

    def query_operator_point_directions(self,
    country_code: Optional[Union[Area, str]] = None,
    has_data : int = 1,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the possible flow directions, being combination of an
        operator, a point, and a flow direction

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        if country_code is not None:
            area = lookup_area(country_code)
            params = {
                'operatorKey' : list(area.code),
                'hasData' : has_data,
                'limit': limit
            }
        else:
            params = {
                'limit': limit,
                'hasData' : has_data
            }
        
        response = self._base_request(endpoint = '/operatorpointdirections',params=params)

        return response.text

    """
    "pointKey",
    "pointLabel",
    "isSingleOperator",
    "pointTpMapX",
    "pointTpMapY",
    "fromSystemLabel",
    "fromInfrastructureTypeLabel",
    "fromCountryKey",
    "fromCountryLabel",
    "fromBzKey",
    "fromBzLabel",
    "fromBzLabelLong",
    "fromOperatorKey",
    "fromOperatorLabel",
    "fromOperatorLongLabel",
    "fromPointKey",
    "fromPointLabel",
    "fromIsCAM",
    "fromIsCMP",
    "fromBookingPlatformKey",
    "fromBookingPlatformLabel",
    "fromBookingPlatformURL",
    "toIsCAM",
    "toIsCMP",
    "toBookingPlatformKey",
    "toBookingPlatformLabel",
    "toBookingPlatformURL",
    "fromTsoItemIdentifier",
    "fromTsoPointLabel",
    "fromDirectionKey",
    "fromHasData",
    "toSystemLabel",
    "toInfrastructureTypeLabel",
    "toCountryKey",
    "toCountryLabel",
    "toBzKey",
    "toBzLabel",
    "toBzLabelLong",
    "toOperatorKey",
    "toOperatorLabel",
    "toOperatorLongLabel",
    "toPointKey",
    "toPointLabel",
    "toDirectionKey",
    "toHasData",
    "toTsoItemIdentifier",
    "toTsoPointLabel",
    "validFrom",
    "validto",
    "lastUpdateDateTime",
    "isInvalid",
    "entryTpNeMoUsage",
    "exitTpNeMoUsage",
    "id",
    "dataSet"
    """

    def query_interconnections(self,
    country_code: Optional[Union[Area, str]] = None,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the interconnections between an exit system and an entry
        system

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        if country_code is not None:
            area = lookup_area(country_code)
            params = {
                'operatorKey' : list(area.code),
                'limit': limit
            }
        else:
            params = {
                'limit': limit
            }

        response = self._base_request(endpoint = '/interconnections',params=params)

        return response.text    

    """
    "countryKey",
    "countryLabel",
    "bzKey",
    "bzLabel",
    "bzLabelLong",
    "operatorKey",
    "operatorLabel",
    "directionKey",
    "adjacentSystemsKey",
    "adjacentSystemsCount",
    "adjacentSystemsAreBalancingZones",
    "adjacentSystemsLabel",
    "id",
    "dataSet"
    """
    def query_aggregate_interconnections(self,
    country_code: Optional[Union[Area, str]] = None,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the connections between transmission system operators
        and their respective balancing zones

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        if country_code is not None:
            area = lookup_area(country_code)
            params = {
                'operatorKey' : list(area.code),
                'limit': limit
            }
        else:
            params = {
                'limit': limit
            }
        
        response = self._base_request(endpoint = '/aggregateInterconnections',params=params)

        return response.text    
    """
    "id",
    "messageId",
    "marketParticipantKey",
    "marketParticipantEic",
    "marketParticipantName",
    "messageType",
    "publicationDateTime",
    "threadId",
    "versionNumber",
    "eventStatus",
    "eventType",
    "eventStart",
    "eventStop",
    "unavailabilityType",
    "unavailabilityReason",
    "unitMeasure",
    "balancingZoneKey",
    "balancingZoneEic",
    "balancingZoneName",
    "affectedAssetIdentifier",
    "affectedAssetName",
    "affectedAssetEic",
    "direction",
    "unavailableCapacity",
    "availableCapacity",
    "technicalCapacity",
    "remarks",
    "lastUpdateDateTime",
    "sharePointPointId",
    "isLatestVersion",
    "sharePointPublicationId",
    "uMMType",
    "isArchived"
    """
    def query_urgent_market_messages(self,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Urgent Market Messages

        Parameters
        ----------

        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/urgentmarketmessages',params=params)

        return response.text    
    """
    <property>Tariff Period</property>
    <property>Tariff Period Remarks</property>
    <property>Point Name</property>
    <property>Point Identifier (EIC)</property>
    <property>Direction</property>
    <property>Operator</property>
    <property>Country code</property>
    <property>Connection</property>
    <property>Remarks fo connection</property>
    <property>From BZ</property>
    <property>To BZ</property>
    <property>Start time of validity</property>
    <property>End time of validity</property>
    <property>Capacity Type</property>
    <property>Unit</property>
    <property>Product type according to its duration</property>
    <property>Multiplier</property>
    <property>Remarks for multiplier</property>
    <property>Discount for interruptible capacity </property>
    <property>Remarks for discount</property>
    <property>Seasonal factor</property>
    <property>Remarks for seasonal factor</property>
    <property>Operator Currency</property>
    <property>Applicable tariff per kWh/d (local)</property>
    <property>Local Currency/ kWh/d</property>
    <property>Applicable tariff per kWh/h (local)</property>
    <property>Local Currency/ kWh/h</property>
    <property>Applicable tariff per kWh/d (Euro)</property>
    <property>EUR / kWh/d</property>
    <property>Applicable tariff per kWh/h (Euro)</property>
    <property>EUR / kWh/h</property>
    <property>Remarks for applicable tariff</property>
    <property>Applicable tariff in common unit</property>
    <property>EUR/kWh/h/d for all products EUR/kWh/h/h for within-day</property>
    <property>Remarks for applicable tariff in common unit</property>
    <property>Applicable commodity tariff per kWh, if any, in the Local Currency</property>
    <property>Applicable commodity tariff per kWh, if any, in the EURO</property>
    <property>Remarks for applicable commodity</property>
    <property>Last Update Date</property>
    <property>Exchange Rate Reference Date</property>
    <property>Remarks</property>
    <property>Operator key</property>
    <property>Tso Eic code</property>
    <property>Point key</property>
    """
    def query_tariffs(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Information about the various tariff types and components
        related to the tariffs

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/tariffsfulls',params=params)

        return response.text    

    """
    <property>Tariff Period</property>
    <property>Tariff Period Remarks</property>
    <property>Point Name</property>
    <property>Point Identifier (EIC)</property>
    <property>Direction</property>
    <property>Operator</property>
    <property>Country code</property>
    <property>Connection</property>
    <property>Remarks for connection </property>
    <property>From BZ</property>
    <property>To BZ</property>
    <property>Capacity Type</property>
    <property>Unit</property>
    <property>Product type according to its duration</property>
    <property>Operator Currency</property>
    <property>Simulation of all the costs foe flowing 1 GWh/day/year in Local currency</property>
    <property>Simulation of all the costs for flowing 1 GWh/day/year in EUR</property>
    <property>Remars for Simulation costs</property>
    <property>Last Update Date</property>
    <property>Exchange Rate Reference Date</property>
    <property>Remarks</property>
    <property>Operator key</property>
    <property>Tso Eic code</property>
    <property>Point key</property>
    """

    def query_tariffs_sim(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Simulation of all the costs for flowing 1 GWh/day/year for
        each IP per product type and tariff period

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/tariffsSimulations',params=params)

        return response.text    


    """

    "id",
    "dataSet",
    "dataSetLabel",
    "indicator",
    "periodType",
    "periodFrom",
    "periodTo",
    "countryKey",
    "countryLabel",
    "bzKey",
    "bzShort",
    "bzLong",
    "operatorKey",
    "operatorLabel",
    "tsoEicCode",
    "directionKey",
    "adjacentSystemsKey",
    "adjacentSystemsLabel",
    "year",
    "month",
    "day",
    "unit",
    "value",
    "countPointPresents",
    "flowStatus",
    "pointsNames",
    "lastUpdateDateTime"

    """

    def query_aggregated_data(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
        """
        Type: JSON
        Latest nominations, allocations, physical flow

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/aggregatedData',params=params)

        return response.text

    """"
    periodFrom",
    "periodTo",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "pointKey",
    "pointLabel",
    "tsoItemIdentifier",
    "directionKey",
    "interruptionType",
    "capacityType",
    "capacityCommercialType",
    "unit",
    "value",
    "restorationInformation",
    "lastUpdateDateTime",
    "isOverlapping",
    "id",
    "dataSet",
    "indicator",
    "periodType",
    "itemRemarks",
    "generalRemarks",
    "isUnlimited",
    "flowStatus",
    "capacityBookingStatus",
    "isCamRelevant",
    "isNA",
    "originalPeriodFrom",
    "isCmpRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "interruptionCalculationRemark",
    "pointType",
    "idPointType",
    "isArchived"
    """

    def query_interruptions(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Interruptions

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/interruptions',params=params)

        return response.text

    """
    "auctionFrom",
    "auctionTo",
    "capacityFrom",
    "capacityTo",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "pointKey",
    "pointLabel",
    "tsoItemIdentifier",
    "directionKey",
    "unit",
    "itemRemarks",
    "generalRemarks",
    "auctionPremium",
    "clearedPrice",
    "reservePrice",
    "lastUpdateDateTime",
    "isCAMRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "pointType",
    "idPointType",
    "id",
    "dataSet",
    "indicator",
    "periodType",
    "periodFrom",
    "periodTo",
    "value",
    "isUnlimited",
    "flowStatus",
    "interruptionType",
    "restorationInformation",
    "capacityType",
    "capacityBookingStatus",
    "isNA",
    "originalPeriodFrom",
    "isCmpRelevant",
    "interruptionCalculationRemark",
    "isArchived"
    """

    def query_CMP_auction_premiums(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Auction Premiums

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/cmpauctions',params=params)

        return response.text

    """
    "periodFrom",
    "periodTo",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "pointKey",
    "pointLabel",
    "tsoItemIdentifier",
    "directionKey",
    "allocationProcess",
    "itemRemarks",
    "generalRemarks",
    "lastUpdateDateTime",
    "pointType",
    "idPointType",
    "id",
    "dataSet",
    "indicator",
    "periodType",
    "unit",
    "value",
    "isUnlimited",
    "flowStatus",
    "interruptionType",
    "restorationInformation",
    "capacityType",
    "capacityBookingStatus",
    "isCamRelevant",
    "isNA",
    "originalPeriodFrom",
    "isCmpRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "interruptionCalculationRemark",
    "isArchived"
    """
    def query_CMP_unavailable_firm_capacity(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/cmpunavailables',params=params)

        return response.text

    """
    "auctionFrom",
    "auctionTo",
    "capacityFrom",
    "capacityTo",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "pointKey",
    "pointLabel",
    "tsoItemIdentifier",
    "directionKey",
    "unit",
    "itemRemarks",
    "generalRemarks",
    "requestedVolume",
    "allocatedVolume",
    "unallocatedVolume",
    "lastUpdateDateTime",
    "occurenceCount",
    "indicator",
    "periodType",
    "isUnlimited",
    "flowStatus",
    "interruptionType",
    "restorationInformation",
    "capacityType",
    "capacityBookingStatus",
    "value",
    "pointType",
    "idPointType",
    "id",
    "dataSet",
    "periodFrom",
    "periodTo",
    "isCamRelevant",
    "isNA",
    "originalPeriodFrom",
    "isCmpRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "interruptionCalculationRemark",
    "isArchived"
    """

    def query_CMP_unsuccesful_requests(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:

        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }
        
        response = self._base_request(endpoint = '/cmpUnsuccessfulRequests',params=params)

        return response.text

    """
    "id",
    "dataSet",
    "indicator",
    "periodType",
    "periodFrom",
    "periodTo",
    "operatorKey",
    "tsoEicCode",
    "operatorLabel",
    "pointKey",
    "pointLabel",
    "c",
    "directionKey",
    "unit",
    "itemRemarks",
    "generalRemarks",
    "value",
    "lastUpdateDateTime",
    "isUnlimited",
    "flowStatus",
    "interruptionType",
    "restorationInformation",
    "capacityType",
    "capacityBookingStatus",
    "isCamRelevant",
    "isNA",
    "originalPeriodFrom",
    "isCmpRelevant",
    "bookingPlatformKey",
    "bookingPlatformLabel",
    "bookingPlatformURL",
    "interruptionCalculationRemark",
    "pointType",
    "idPointType",
    "isArchived"
    """

    def query_operational_data(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
        
        """
        Type: JSON
        Nomination, Renominations, Allocations, Physical Flows, GCV,
        Wobbe Index, Capacities, Interruptions, and CMP CMA

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'operatorKey' : list(area.code),
            'limit': limit
        }

        response = self._base_request(endpoint = '/operationaldatas',params=params)

        return response.text

class EntsogPandasClient(EntsogRawClient):

    def query_connection_points(self, limit : int = -1) -> str:
        """
        Type: JSON
        Interconnection points as visible on the Map. Please note that
        this only included the Main points and not the sub points. To
        download all points, the API for Operator Point Directions
        should be used.
        
        Parameters
        ----------
        limit: int

        Returns
        -------
        str
        """

        json = super(EntsogPandasClient, self).query_connection_points(
            limit =limit
        )
        data = parse_general(json)

        return data    


    def query_operators(self,
    country_code: Optional[Union[Area, str]] = None,
    operator_key : Optional[str] = None,
    has_data : int = 1,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All operators connected to the transmission system

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_operators(
            country_code = area , operator_key = operator_key, has_data = has_data, limit =limit
        )
        data = parse_general(json)

        return data    

    def query_balancing_zones(self, limit : int = -1) -> str:
        
        """
        Type: JSON
        European balancing zones

        Parameters
        ----------
        limit: int

        Returns
        -------
        str
        """

        json = super(EntsogPandasClient, self).query_balancing_zones(
            limit =limit
        )
        data = parse_general(json)

        return data    



    def query_operator_point_directions(self,
    country_code: Optional[Union[Area, str]] = None,
    has_data : int = 1,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the possible flow directions, being combination of an
        operator, a point, and a flow direction

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_operator_point_directions(
            country_code = area ,has_data = has_data, limit =limit
        )
        data = parse_general(json)

        return data    

    def query_interconnections(self,
    country_code: Optional[Union[Area, str]] = None,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the interconnections between an exit system and an entry
        system

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """

        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_interconnections(
            country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data    


    def query_aggregate_interconnections(self,
    country_code: Optional[Union[Area, str]] = None,
    limit : int = -1) -> str:
    
        """
        Type: JSON
        All the connections between transmission system operators
        and their respective balancing zones

        Parameters
        ----------
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_aggregate_interconnections(
            country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data   

    def query_urgent_market_messages(self,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Urgent Market Messages

        Parameters
        ----------

        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_urgent_market_messages(
            country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data   

    @year_limited
    def query_tariffs(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Information about the various tariff types and components
        related to the tariffs

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_tariffs(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data     

    @year_limited
    def query_tariffs_sim(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Simulation of all the costs for flowing 1 GWh/day/year for
        each IP per product type and tariff period

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_tariffs_sim(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data   

    @year_limited
    def query_aggregated_data(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
        """
        Type: JSON
        Latest nominations, allocations, physical flow

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_aggregated_data(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data

    @year_limited
    def query_interruptions(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Interruptions

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_interruptions(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data


    def query_CMP_auction_premiums(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Auction Premiums

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_CMP_auction_premiums(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data

    def query_CMP_unavailable_firm_capacity(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_CMP_unavailable_firm_capacity(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data

    @year_limited
    def query_CMP_unsuccesful_requests(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:

        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_CMP_unsuccesful_requests(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data

    @year_limited
    def query_operational_data(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Area, str],
    limit : int = -1) -> str:
        
        """
        Type: JSON
        Nomination, Renominations, Allocations, Physical Flows, GCV,
        Wobbe Index, Capacities, Interruptions, and CMP CMA

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country Union[Area, str]
        limit: int

        Returns
        -------
        str
        """
        area = lookup_area(country_code)
        json = super(EntsogPandasClient, self).query_operational_data(
            start = start, end = end, country_code = area ,limit =limit
        )
        data = parse_general(json)

        return data


    @year_limited
    def query_net_position_dayahead(self, country_code: Union[Area, str],
                            start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        """

        Parameters
        ----------
        country_code
        start
        end

        Returns
        -------

        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_net_position_dayahead(
            country_code=area, start=start, end=end)
        series = parse_netpositions(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_day_ahead_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.Series:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_day_ahead_prices(
            country_code=area, start=start, end=end)
        series = parse_prices(text)
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_load(self, country_code: Union[Area, str], start: pd.Timestamp,
                   end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_load(
            country_code=area, start=start, end=end)
        series = parse_loads(text, process_type='A16')
        series = series.tz_convert(area.tz)
        series = series.truncate(before=start, after=end)
        return series

    @year_limited
    def query_load_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01') -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_load_forecast(
            country_code=area, start=start, end=end, process_type=process_type)

        df = parse_loads(text, process_type=process_type)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    def query_load_and_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """
        utility function to combina query realised load and forecasted day ahead load.
        this mimics the html view on the page Total Load - Day Ahead / Actual

        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df_load_forecast_da = self.query_load_forecast(country_code, start=start, end=end)
        df_load = self.query_load(country_code, start=start, end=end)
        return df_load_forecast_da.join(df_load, sort=True, how='inner')


    @year_limited
    def query_generation_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str = 'A01',
            nett: bool = False) -> Union[pd.DataFrame, pd.Series]:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
        nett : bool
            condense generation and consumption into a nett number

        Returns
        -------
        pd.DataFrame | pd.Series
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_generation_forecast(
            country_code=area, start=start, end=end, process_type=process_type)
        df = parse_generation(text, nett=nett)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_wind_and_solar_forecast(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            process_type: str = 'A01', **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        process_type : str

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_wind_and_solar_forecast(
            country_code=area, start=start, end=end, psr_type=psr_type,
            process_type=process_type)
        df = parse_generation(text, nett=True)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_generation(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            nett: bool = False, **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        nett : bool
            condense generation and consumption into a nett number

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_generation(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text, nett=nett)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_installed_generation_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(
            EntsogPandasClient, self).query_installed_generation_capacity(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text)
        df = df.tz_convert(area.tz)
        # Truncate to YearBegin and YearEnd, because answer is always year-based
        df = df.truncate(before=start - YearBegin(), after=end + YearEnd())
        return df

    @year_limited
    def query_installed_generation_capacity_per_unit(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(
            EntsogPandasClient,
            self).query_installed_generation_capacity_per_unit(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_installed_capacity_per_plant(text)
        return df

    @year_limited
    def query_crossborder_flows(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_crossborder_flows(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_scheduled_exchanges(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str],
            start: pd.Timestamp,
            end: pd.Timestamp,
            dayahead: bool = False,
            **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        dayahead : bool
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_scheduled_exchanges(
            country_code_from=area_from,
            country_code_to=area_to,
            dayahead=dayahead,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_dayahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_net_transfer_capacity_dayahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_weekahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_net_transfer_capacity_weekahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_net_transfer_capacity_monthahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_net_transfer_capacity_monthahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts
    
    @year_limited
    def query_net_transfer_capacity_yearahead(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_net_transfer_capacity_yearahead(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts

    @year_limited
    def query_intraday_offered_capacity(
        self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, implicit:bool = True, **kwargs) -> pd.Series:
        """
        Note: Result will be in the timezone of the origin country  --> to check

        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        implicit: bool (True = implicit - default for most borders. False = explicit - for instance BE-GB)
        Returns
        -------
        pd.Series
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        text = super(EntsogPandasClient, self).query_intraday_offered_capacity(
            country_code_from=area_from,
            country_code_to=area_to,
            start=start,
            end=end,
            implicit=implicit)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(area_from.tz)
        ts = ts.truncate(before=start, after=end)
        return ts
    
    @year_limited
    def query_imbalance_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        archive = super(EntsogPandasClient, self).query_imbalance_prices(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_prices_zip(zip_contents=archive)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def query_procured_balancing_capacity(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, process_type: str,
            type_marketagreement_type: Optional[str] = None) -> bytes:
        """
        Activated Balancing Energy [17.1.E]
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        process_type : str
            A51 ... aFRR; A47 ... mFRR
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_procured_balancing_capacity(
            country_code=area, start=start, end=end,
            process_type=process_type, type_marketagreement_type=type_marketagreement_type)
        df = parse_procured_balancing_capacity(text, area.tz)
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    def query_activated_balancing_energy(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, business_type: str, 
            psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Activated Balancing Energy [17.1.E]
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        business_type: str
            type of contract (see mappings.BSNTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_activated_balancing_energy(
            country_code=area, start=start, end=end, 
            business_type=business_type, psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "quantity")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df
    
    @year_limited
    @paginated
    def query_contracted_reserve_prices(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area, str
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_contracted_reserve_prices(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "procurement_price.amount")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def query_contracted_reserve_amount(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, type_marketagreement_type: str,
            psr_type: Optional[str] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        type_marketagreement_type : str
            type of contract (see mappings.MARKETAGREEMENTTYPE)
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_contracted_reserve_amount(
            country_code=area, start=start, end=end,
            type_marketagreement_type=type_marketagreement_type,
            psr_type=psr_type)
        df = parse_contracted_reserve(text, area.tz, "quantity")
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    @year_limited
    @paginated
    def _query_unavailability(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, doctype: str, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        doctype : str
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        content = super(EntsogPandasClient, self)._query_unavailability(
            country_code=area, start=start, end=end, doctype=doctype,
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate)
        df = parse_unavailabilities(content, doctype)
        df = df.tz_convert(area.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area.tz))
        df = df.truncate(before=start, after=end)
        return df

    def query_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        df = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A80",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate)
        return df

    def query_unavailability_of_production_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        df = self._query_unavailability(
            country_code=country_code, start=start, end=end, doctype="A77",
            docstatus=docstatus, periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate)
        return df

    @paginated
    def query_unavailability_transmission(
            self, country_code_from: Union[Area, str],
            country_code_to: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, docstatus: Optional[str] = None,
            periodstartupdate: Optional[pd.Timestamp] = None,
            periodendupdate: Optional[pd.Timestamp] = None,
            **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code_from : Area|str
        country_code_to : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional
        periodstartupdate : pd.Timestamp, optional
        periodendupdate : pd.Timestamp, optional

        Returns
        -------
        pd.DataFrame
        """
        area_to = lookup_area(country_code_to)
        area_from = lookup_area(country_code_from)
        content = super(EntsogPandasClient,
                        self).query_unavailability_transmission(
            area_from, area_to, start, end, docstatus, periodstartupdate,
            periodendupdate)
        df = parse_unavailabilities(content, "A78")
        df = df.tz_convert(area_from.tz)
        df['start'] = df['start'].apply(lambda x: x.tz_convert(area_from.tz))
        df['end'] = df['end'].apply(lambda x: x.tz_convert(area_from.tz))
        df = df.truncate(before=start, after=end)
        return df

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df = self.query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end, docstatus='A13')
        df = df.truncate(before=start, after=end)
        return df

    @day_limited
    def query_generation_per_plant(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp, psr_type: Optional[str] = None,
            include_eic: bool = False,
            nett: bool = False, **kwargs) -> pd.DataFrame:
        """
        Parameters
        ----------
        country_code : Area|str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        nett : bool
            condense generation and consumption into a nett number
        include_eic: bool
            if True also include the eic code in the output

        Returns
        -------
        pd.DataFrame
        """
        area = lookup_area(country_code)
        text = super(EntsogPandasClient, self).query_generation_per_plant(
            country_code=area, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text, per_plant=True, include_eic=include_eic)
        df.columns = df.columns.set_levels(df.columns.levels[0].str.encode('latin-1').str.decode('utf-8'), level=0)
        df = df.tz_convert(area.tz)
        # Truncation will fail if data is not sorted along the index in rare
        # cases. Ensure the dataframe is sorted:
        df = df.sort_index(0)
        df = df.truncate(before=start, after=end)
        return df

    def query_import(self, country_code: Union[Area, str], start: pd.Timestamp,
                     end: pd.Timestamp) -> pd.DataFrame:
        """
        Adds together all incoming cross-border flows to a country
        The neighbours of a country are given by the NEIGHBOURS mapping
        """
        area = lookup_area(country_code)
        imports = []
        for neighbour in NEIGHBOURS[area.name]:
            try:
                im = self.query_crossborder_flows(country_code_from=neighbour,
                                                  country_code_to=country_code,
                                                  end=end,
                                                  start=start,
                                                  lookup_bzones=True)
            except NoMatchingDataError:
                continue
            im.name = neighbour
            imports.append(im)
        df = pd.concat(imports, axis=1)
        # drop columns that contain only zero's
        df = df.loc[:, (df != 0).any(axis=0)]
        df = df.tz_convert(area.tz)
        df = df.truncate(before=start, after=end)
        return df

    def query_generation_import(
            self, country_code: Union[Area, str], start: pd.Timestamp,
            end: pd.Timestamp) -> pd.DataFrame:
        """Query the combination of both domestic generation and imports"""
        generation = self.query_generation(country_code=country_code, end=end,
                                           start=start, lookup_bzones=True)
        generation = generation.loc[:, (generation != 0).any(
            axis=0)]  # drop columns that contain only zero's
        generation = generation.resample('H').sum()
        imports = self.query_import(country_code=country_code, start=start,
                                    end=end)

        data = {f'Generation': generation, f'Import': imports}
        df = pd.concat(data.values(), axis=1, keys=data.keys())
        df = df.truncate(before=start, after=end)
        return df

