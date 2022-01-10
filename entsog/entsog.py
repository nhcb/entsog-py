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
from .mappings import Area, NEIGHBOURS, lookup_area, Indicator, lookup_indicator
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
        print("-------------------------------------------")
        print(url)
        params.update(base_params)

        logging.debug(f'Performing request to {url} with params {params}')
        response = self.session.get(url=url, params=params,
                                    proxies=self.proxies, timeout=self.timeout, 
                                    verify=False) # TODO: Important to remove as it raises security concerns. However due to weird SSL issues within NP, only dirty solution.
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
    period_type : str = 'day',
    limit : int = -1) -> str:
        """
        Type: JSON
        Latest nominations, allocations, physical flow

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
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
    period_type : str = 'day',
    limit : int = -1) -> str:
    
        """
        Type: JSON
        Interruptions

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
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
    period_type : str = 'day',
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Auction Premiums

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
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
    period_type : str = 'day',
    limit : int = -1) -> str:
    
        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
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
    period_type: str = 'day',
    limit : int = -1) -> str:

        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
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
    period_type: str = 'day',
    # TODO: type hinting
    indicators = None,
    #indicators : Union[list[Indicator],list[str]] = None,
    #indicator, #: Optional[list[str]] = None,
    limit : int = -1) -> str:
        
        """
        Type: JSON
        Nomination, Renominations, Allocations, Physical Flows, GCV,
        Wobbe Index, Capacities, Interruptions, and CMP CMA

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Area, str]
        period_type: str
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
            'periodType' : period_type,
            'limit': limit
        }

        if indicators:
            decoded_indicators = []
            for indicator in indicators:
                decoded_indicators.append(lookup_indicator(indicator).code)

            params['indicator'] = decoded_indicators

        response = self._base_request(endpoint = '/operationaldatas',params=params)

        return response.text

class EntsogPandasClient(EntsogRawClient):

    BALANCING_ZONES = self.query_balancing_zones()
    INTERCONNECTIONS = self.query_interconnections()

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
    #indicators : Union[list[Indicator],list[str]] = None,
    # TODO: type hint
    indicators = None,
    #indicator, #: Optional[list[str]] = None,
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

        print(indicators)
        json = super(EntsogPandasClient, self).query_operational_data(
            start = start, end = end, country_code = country_code ,
            indicators = indicators,
            limit =limit
        )
        data = parse_general(json)

        return data

