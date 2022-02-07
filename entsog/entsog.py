from itertools import count
import logging
from multiprocessing.sharedctypes import Value
from typing import Union, Optional, Dict

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd
import pytz
import requests
import simplejson
import urllib.request
import urllib.parse

from typing import List
from bs4 import BeautifulSoup

from entsog.exceptions import InvalidPSRTypeError, InvalidBusinessParameterError
from .exceptions import NoMatchingDataError, PaginationError
from .mappings import Area, NEIGHBOURS, lookup_area, lookup_country, Indicator, lookup_balancing_zone, lookup_country, lookup_indicator, Country, BalancingZone
from .parsers import parse_aggregate_data, parse_general, parse_interconnections, parse_grouped_operational_aggregates
from .decorators import month_limited, operator_limited, retry, paginated, year_limited, day_limited, week_limited


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
            'limit' : -1,
            'timeZone' : 'UCT'
        }
        params.update(base_params)

        logging.debug(f'Performing request to {url} with params {params}')

        params = urllib.parse.urlencode(params, safe = ',') # ENTSOG uses comma-seperated values
        
        response = self.session.get(url=url, params=params,
                                    proxies=self.proxies, timeout=self.timeout)#,verify=False) # TODO: Important to remove as it raises security concerns. However due to weird SSL issues within NP, only dirty solution.
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, response.url, 'html.parser')
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
                if 'No matching data found' in response.text, response.url:
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

    def query_connection_points(self) -> str:
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



        response = self._base_request(endpoint = '/connectionpoints')

        return response.text, response.url

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
        country_code : Union[Country, str],
        has_data : int = 1) -> str:
    
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
        params = {
                'hasData' : has_data
        }

        if country_code is not None:
            params['operatorCountryKey'] = lookup_country(country_code).code

        response = self._base_request(endpoint = '/operators',params=params)

        return response.text, response.url

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

    def query_balancing_zones(self) -> str:
        
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
            
        }
        
        response = self._base_request(endpoint = '/balancingzones',params=params)

        return response.text, response.url

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
        country_code : Union[Country, str] = None,
        has_data : int = 1) -> str:
    
        """
        Type: JSON
        All the possible flow directions, being combination of an
        operator, a point, and a flow direction

        Parameters
        ----------
        country_code : Union[Country, str]
        limit: int

        Returns
        -------
        str
        """
        params = {
            'hasData' : has_data
        }
        if country_code is not None:
            params['tSOCountry'] = lookup_country(country_code).code
        
        response = self._base_request(endpoint = '/operatorpointdirections',params=params)

        return response.text, response.url

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
        from_country_code : Union[Country, str],
        to_country_code : Union[Country, str]  = None,
        from_balancing_zone : Union[BalancingZone, str] = None,
        to_balancing_zone : Union[BalancingZone, str] = None,
        from_operator: str = None,
        to_operator: str = None) -> str:
    
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
        params = {

        }



        if from_country_code is not None:
            params['fromCountryKey'] = lookup_country(from_country_code).code
        if to_country_code is not None:
            params['toCountryKey'] = lookup_country(to_country_code).code

        if from_balancing_zone is not None:
            params['fromBzKey'] = lookup_balancing_zone(from_balancing_zone).code
        if to_balancing_zone is not None:
            params['toBzKeys'] = lookup_balancing_zone(to_balancing_zone).code

        if from_operator is not None:
            params['fromOperatorKey'] = from_operator
        if to_operator is not None:
            params['toOperatorKey'] = to_operator
        

        response = self._base_request(endpoint = '/interconnections',params=params)

        return response.text, response.url    




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
        country_code : Union[Country, str] = None,
        balancing_zone : Union[BalancingZone, str] = None,
        operator_key: str = None) -> str:
    
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
        params = {

        }
        if country_code is not None:
            country_code = lookup_country(country_code)
            params['countryKey'] = country_code.code
        if balancing_zone is not None:
            balancing_zone = lookup_balancing_zone(balancing_zone)
            params['bzKey'] = balancing_zone.code
        if operator_key is not None:
            params['operatorKey'] = operator_key

        
        response = self._base_request(endpoint = '/aggregateInterconnections',params=params)

        return response.text, response.url    
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
        balancing_zone : Union[BalancingZone, str]) -> str:
    
        """
        Type: JSON
        Urgent Market Messages

        Parameters
        ----------

        balancing_zone : Union[BalancingZone, str]
        limit: int

        Returns
        -------
        str
        """
        params = {

        }
        if balancing_zone is not None:
            balancing_zone = lookup_balancing_zone(balancing_zone)
            params['balancingZoneKey'] = balancing_zone.code


        response = self._base_request(endpoint = '/urgentmarketmessages',params=params)

        return response.text, response.url    
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
        country_code: Union[Country, str]) -> str:
    
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

        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end)
        }
        if country_code is not None:
            country_code = lookup_country(country_code)
            params['countryKey'] = country_code.code
        
        response = self._base_request(endpoint = '/tariffsfulls',params=params)

        return response.text, response.url    

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
    country_code : Union[Country, str]) -> str:
    
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
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
        }
        if country_code is not None:
            country_code = lookup_country(country_code)
            params['countryKey'] = country_code.code
        
        response = self._base_request(endpoint = '/tariffsSimulations',params=params)

        return response.text, response.url    


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
        country_code : Union[Country, str] = None,
        balancing_zone: Union[BalancingZone, str] = None,
        period_type : str = 'day') -> str:
        """
        Type: JSON
        Latest nominations, allocations, physical flow

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """

        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end)
        }
        if country_code is not None:
            country_code = lookup_country(country_code)
            params['countryKey'] = country_code.code

        if balancing_zone is not None:
            balancing_zone = lookup_balancing_zone(balancing_zone)
            params['bzKey'] = balancing_zone.code

        if period_type is not None:
            params['periodType'] = period_type

        response = self._base_request(endpoint = '/aggregatedData',params=params)

        return response.text, response.url

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
        country_code: Union[Country, str],
        period_type : str = 'day') -> str:
    
        """
        Type: JSON
        Interruptions

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
        }
        if country_code is not None:
            params['countryKey'] = lookup_country(country_code).code

        if period_type is not None:
            params['periodType'] = period_type
        
        response = self._base_request(endpoint = '/interruptions',params=params)

        return response.text, response.url

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
        #country_code: Union[Country, str],
        period_type : str = 'day') -> str:
    
        """
        Type: JSON
        CMP Auction Premiums

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """


        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'periodType' : period_type,
        }
        
        response = self._base_request(endpoint = '/cmpauctions',params=params)

        return response.text, response.url

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
        #country_code: Union[Country, str],
        period_type : str = 'day') -> str:
    
        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """
        #area = lookup_country(country_code)
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'periodType' : period_type
        }

        response = self._base_request(endpoint = '/cmpunavailables',params=params)

        return response.text, response.url

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
        #country_code: Union[Country, str],
        period_type: str = 'day') -> str:

        """
        Type: JSON
        CMP Unsuccessful requests

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """

        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'periodType' : period_type
        }
        
        response = self._base_request(endpoint = '/cmpUnsuccessfulRequests',params=params)

        return response.text, response.url

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

    def query_operational_data(self, 
        start: pd.Timestamp, 
        end: pd.Timestamp,
        operator: Optional[str] = None,
        period_type: str = 'day',
        indicators : Union[List[Indicator],List[str]] = None) -> str:
        
        """
        Type: JSON
        Nomination, Renominations, Allocations, Physical Flows, GCV,
        Wobbe Index, Capacities, Interruptions, and CMP CMA

        Parameters
        ----------
        start: pd.Timestamp
        end: pd.Timestamp
        country_code: Union[Country, str]
        period_type: str
        limit: int

        Returns
        -------
        str
        """
        params = {
            'from' : self._datetime_to_str(start),
            'to' : self._datetime_to_str(end),
            'periodType' : period_type
        }

        if operator is not None:
            params['operatorKey'] = operator

        if indicators is not None:
            decoded_indicators = []
            for indicator in indicators:
                decoded_indicators.append(lookup_indicator(indicator).code)

            params['indicator'] = ','.join(decoded_indicators)

        response = self._base_request(endpoint = '/operationaldatas',params=params)

        return response.text, response.url



# TODO: This client needs to save constant data


class EntsogPandasClient(EntsogRawClient):

    def __init__(self):
        super(EntsogPandasClient, self).__init__()
        self._interconnections = None
        self._operator_point_directions = None

    def query_connection_points(self) -> str:
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

        json, url = super(EntsogPandasClient, self).query_connection_points(

        )
        data = parse_general(json)
        data['url'] = url

        return data    


    def query_operators(self,
        country_code : Union[Country, str],
        has_data : int = 1) -> str:
    
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
        country_code = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_operators(
            country_code = country_code, has_data = has_data
        )
        data = parse_general(json)
        data['url'] = url

        return data    

    def query_balancing_zones(self) -> str:
        
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

        json, url = super(EntsogPandasClient, self).query_balancing_zones(

        )
        data = parse_general(json)
        data['url'] = url

        return data    



    def query_operator_point_directions(self,
        country_code: Optional[Union[Country, str]] = None,
        has_data : int = 1) -> str:
    
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
            country_code = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_operator_point_directions(
            country_code = country_code ,has_data = has_data
        )
        data = parse_general(json)
        data['url'] = url

        return data    

    def query_interconnections(self,
        from_country_code : Union[Country, str] = None,
        to_country_code : Union[Country, str]  = None,
        from_balancing_zone : Union[BalancingZone, str] = None,
        to_balancing_zone : Union[BalancingZone, str] = None,
        from_operator: str = None,
        to_operator: str = None) -> str:
    
        """
        Type: JSON
        All the interconnections between an exit system and an entry
        system

        Parameters
        ----------



        Returns
        -------
        str
        """

        if from_country_code is not None:
            from_country_code = lookup_country(from_country_code).code
        if to_country_code is not None:
            to_country_code = lookup_country(to_country_code).code

        if from_balancing_zone is not None:
            from_balancing_zone = lookup_balancing_zone(from_balancing_zone).code
        if to_balancing_zone is not None:
            to_balancing_zone = lookup_balancing_zone(to_balancing_zone).code

        if from_operator is not None:
            from_operator = from_operator
        if to_operator is not None:
            to_operator = to_operator


        json, url = super(EntsogPandasClient, self).query_interconnections(
            from_country_code,
            to_country_code,
            from_balancing_zone,
            to_balancing_zone,
            from_operator,
            to_operator
        )
        data = parse_interconnections(json)

        return data    


    def query_aggregate_interconnections(self,
        country_code: Optional[Union[Country, str]] = None) -> str:
    
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
            country_code = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_aggregate_interconnections(
            country_code = country_code
        )
        data = parse_general(json)
        data['url'] = url

        return data   

    @operator_limited
    def query_urgent_market_messages(self,
        balancing_zone : Union[BalancingZone, str],
        operator: List[str] = None) -> str:
    
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
        balancing_zone = lookup_balancing_zone(balancing_zone)

        json, url = super(EntsogPandasClient, self).query_urgent_market_messages(
            balancing_zone = balancing_zone , operator = operator
        )
        data = parse_general(json)
        data['url'] = url

        return data   

    @week_limited
    def query_tariffs(self, start: pd.Timestamp, end: pd.Timestamp,
        country_code: Union[Country, str]) -> str:
    
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
        country_code = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_tariffs(
            start = start, end = end, country_code = country_code
        )
        data = parse_general(json)
        data['url'] = url

        return data     

    @week_limited
    def query_tariffs_sim(self, start: pd.Timestamp, end: pd.Timestamp,
        country_code: Union[Country, str]) -> str:
    
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
        country_code = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_tariffs_sim(
            start = start, end = end, country_code = country_code
        )
        data = parse_general(json)
        data['url'] = url

        return data   

    @week_limited
    def query_aggregated_data(self, start: pd.Timestamp, end: pd.Timestamp,
        country_code : Union[Country, str] = None,
        balancing_zone : Union[BalancingZone, str] = None,
        period_type : str = 'day',
        group_type : str = None,
        entry_exit : bool = False) -> str:
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
        if self._interconnections is None:
            self._interconnections = self.query_interconnections()


        if country_code is not None:
            country_code = lookup_country(country_code)
        if balancing_zone is not None:
            balancing_zone = lookup_balancing_zone(balancing_zone)


        json, url = super(EntsogPandasClient, self).query_aggregated_data(
            start = start, end = end, country_code = country_code, balancing_zone = balancing_zone , period_type = period_type
        )

        data = parse_aggregate_data(json_text = json, interconnections= self._interconnections, group_type= group_type, entry_exit = entry_exit)

        return data

    @week_limited
    def query_interruptions(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Country, str]) -> str:
    
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
        area = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_interruptions(
            start = start, end = end, country_code = area
        )
        data = parse_general(json)
        data['url'] = url

        return data


    def query_CMP_auction_premiums(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Country, str]) -> str:
    
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
        area = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_CMP_auction_premiums(
            start = start, end = end, country_code = area
        )
        data = parse_general(json)
        data['url'] = url

        return data

    def query_CMP_unavailable_firm_capacity(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Country, str]) -> str:
    
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
        area = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_CMP_unavailable_firm_capacity(
            start = start, end = end, country_code = area
        )
        data = parse_general(json)
        data['url'] = url

        return data

    @week_limited
    def query_CMP_unsuccesful_requests(self, start: pd.Timestamp, end: pd.Timestamp,
    country_code: Union[Country, str]) -> str:

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
        area = lookup_country(country_code)
        json, url = super(EntsogPandasClient, self).query_CMP_unsuccesful_requests(
            start = start, end = end, country_code = area
        )
        data = parse_general(json)
        data['url'] = url

        return data


    def query_operational_data(self, 
        start: pd.Timestamp, 
        end: pd.Timestamp,
        country_code : Union[Area, str],
        period_type: str = 'day',
        indicators : Union[List[Indicator],List[str]] = None) -> str:
        
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
        operators = list(area.value)

        data = []
        for operator in operators:
            try:
                small = self._query_operational_data(
                    start = start, 
                    end = end, 
                    operator = operator, 
                    period_type = period_type, 
                    indicators= indicators)

                data.append(small)
            except Exception as e:
                print(f"{operator}: {e}")
                next

        result = pd.concat(data)

        return result


    @week_limited
    def _query_operational_data(self, 
        start: pd.Timestamp, 
        end: pd.Timestamp,
        operator: str,
        period_type: str = 'day',
        indicators : Union[List[Indicator],List[str]] = None) -> str:

        try:
            json, url = super(EntsogPandasClient, self).query_operational_data(
                start = start, 
                end = end, 
                operator = operator,
                period_type = period_type,
                indicators = indicators
            )

            data = parse_general(json)
            data['url'] = url
            return data

        except Exception as e:
            print(
                f"Wrong or no data available for OPERATOR: {operator} and INDICATORS:{indicators}. " +
                f"Due to the poor structure and documentation of the data, this occurs frequently: {e}")
            return None

    
    def get_operational_aggregates(
        self,
        start : pd.Timestamp,
        end: pd.Timestamp,
        country_code: Union[Area, str],
        period_type: str = 'day',
        indicators : Union[List[Indicator],List[str]] = None) -> str:

        if self._operator_point_directions is None:
            self._operator_point_directions = self.query_operator_point_directions()


        # Get the data
        data = self.query_operational_data(
            start = start,
            end = end,
            country_code= country_code,
            period_type= period_type,
            indicators = indicators
        )
        # Merge the data
        merged = pd.merge(
            data,
            self._operator_point_directions,
            left_on = ['operator_key', 'point_key'],
            right_on = ['operator_key', 'point_key'],
            suffixes = ['','_static'],
            how = 'left'
        )

        columns = [
            "indicator",
            "period_type",
            "period_from",
            "period_to",
            "operator_key",
            "tso_eic_code",
            "operator_label",
            "point_key",
            "point_label",
            "tso_item_identifier",
            "last_update_date_time",
            "direction_key",
            "value",
            "flow_status",
            "is_cmp_relevant",
            "booking_platform_key",
            "point_type",
            "is_pipe_in_pipe",
            "tp_tso_item_label",
            "last_update_date_time_static",
            "virtual_reverse_flow",
            "t_so_country", # Rename
            "t_so_balancing_zone", # Rename
            "cross_border_point_type",
            "e_u_relationship", # Rename
            "connected_operators",
            "adjacent_operator_key",
            "adjacent_country",
            "adjacent_zones" # e.g. Switzerland
        ]

        subset = merged[columns].rename(
            columns = {
                't_so_country' : 'tso_country',
                't_so_balancing_zone' : 'tso_balancing_zone',
                'e_u_relationship' : 'eu_relationship'
            }
        )

        return subset


    def get_grouped_operational_aggregates(
        self,
        start : pd.Timestamp,
        end: pd.Timestamp,
        country_code: Union[Area, str],
        period_type: str = 'day',
        indicators : Union[List[Indicator],List[str]] = None,
        groups : List[str] = ['point', 'operator', 'country', 'balancing_zone', 'region'],
        entry_exit: bool = False) -> dict:

        if list(set(groups).difference(['point', 'operator', 'country', 'balancing_zone', 'region'])):
            raise ValueError(f'{groups} contain not a valid group type, please specify any of the following: {groups}.')

        data = self.get_operational_aggregates(
            start = start,
            end = end,
            country_code = country_code,
            period_type = period_type,
            indicators = indicators
        )
        data.to_csv(f"data/get_operational_aggregates_{country_code}.csv",sep=';')
        
        result = {}
        for group in groups:
            small = parse_grouped_operational_aggregates(
                data = data,
                group_type = group,
                entry_exit= entry_exit
            )
            result[group] = small

        return result