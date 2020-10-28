import logging
from datetime import datetime
from typing import List, Union, Generator, Dict, Optional

import dateparser
import pandas as pd
from pandas import Timestamp

from wetterdienst.core.observations import ObservationDataCore
from wetterdienst.core.sites import SitesCore
from wetterdienst.core.source import Source
from wetterdienst.dwd.index import _create_file_index_for_dwd_server
from wetterdienst.dwd.metadata.column_map import create_humanized_column_names_mapping
from wetterdienst.dwd.observations.access import collect_climate_observations_data
from wetterdienst.dwd.observations.metadata.parameter import (
    DWDObservationParameterSetStructure,
)
from wetterdienst.dwd.observations.metadata.parameter_set import (
    RESOLUTION_PARAMETER_MAPPING,
)
from wetterdienst.dwd.observations.metadata import (
    DWDObservationPeriod,
    DWDObservationParameter,
    DWDObservationParameterSet,
    DWDObservationResolution,
)
from wetterdienst.dwd.observations.stations import metadata_for_climate_observations
from wetterdienst.store import StorageAdapter
from wetterdienst.dwd.observations.util.parameter import (
    create_parameter_to_parameter_set_combination,
    check_dwd_observations_parameter_set,
)
from wetterdienst.util.parameter import build_parameter_identifier
from wetterdienst.util.enumeration import (
    parse_enumeration_from_template,
    parse_enumeration,
)
from wetterdienst.exceptions import (
    InvalidParameterCombination,
    StartDateEndDateError,
)
from wetterdienst.dwd.metadata.constants import DWDCDCBase
from wetterdienst.metadata.column_names import MetaColumns

log = logging.getLogger(__name__)


class DWDObservationData(ObservationDataCore):
    """
    The DWDObservationData class represents a request for
    observation data as provided by the DWD service.
    """
    _source = Source.DWD
    _observation_parameter_template = DWDObservationParameter

    def __init__(
        self,
        station_ids: List[Union[int, str]],
        parameters: List[
            Union[str, DWDObservationParameter, DWDObservationParameterSet]
        ],
        resolution: Union[str, DWDObservationResolution],
        periods: Optional[List[Union[str, DWDObservationPeriod]]] = None,
        start_date: Optional[Union[str, Timestamp, datetime]] = None,
        end_date: Optional[Union[str, Timestamp, datetime]] = None,
        storage: Optional[StorageAdapter] = None,
        tidy_data: bool = True,
        humanize_column_names: bool = False,
    ) -> None:
        """
        Class with mostly flexible arguments to define a request regarding DWD data.
        Special handling for period type. If start_date/end_date are given all period
        types are considered and merged together and the data is filtered for the given
        dates afterwards.

        :param station_ids: definition of stations by str, int or list of str/int,
                            will be parsed to list of int
        :param parameters:           Observation measure
        :param resolution:     Frequency/granularity of measurement interval
        :param periods:         Recent or historical files (optional), if None
                                    and start_date and end_date None, all period
                                    types are used
        :param start_date:          Replacement for period type to define exact time
                                    of requested data, if used, period type will be set
                                    to all period types (hist, recent, now)
        :param end_date:            Replacement for period type to define exact time
                                    of requested data, if used, period type will be set
                                    to all period types (hist, recent, now)
        :param storage:             Storage adapter.
        :param tidy_data:           Reshape DataFrame to a more tidy
                                    and row-based version of data
        :param humanize_column_names: Replace column names by more meaningful ones
        """
        # required for parameter parsing in Core
        self.resolution = parse_enumeration_from_template(
            resolution, DWDObservationResolution
        )

        super().__init__(
            station_ids,
            parameters,
            start_date,
            end_date,
            storage,
            humanize_column_names,
        )

        # If any date is given, use all period types and filter, else if not period type
        # is given use all period types
        if start_date or end_date or not periods:
            self.periods = [*DWDObservationPeriod]
        # Otherwise period types will be parsed
        else:
            # For the case that a period_type is given, parse the period type(s)
            self.periods = sorted(parse_enumeration(DWDObservationPeriod, periods))

        # If more then one parameter requested, automatically tidy data
        self.tidy_data = (
            len(self.parameters) > 1
            or any(
                [
                    not isinstance(parameter, DWDObservationParameterSet)
                    for parameter, parameter_set in self.parameters
                ]
            )
            or tidy_data
        )
        self.humanize_column_names = humanize_column_names

    def _parse_parameter(self, parameter):
        """ Overrides core parsing function as for DWD we need a corresponding
        parameter set for each parameter """
        return create_parameter_to_parameter_set_combination(
            parameter, self.resolution
        )

    def __eq__(self, other):
        return (
            self.station_ids == other.station_ids
            and self.parameters == other.parameters
            and self.resolution == other.resolution
            and self.periods == other.periods
            and self.start_date == other.start_date
            and self.end_date == other.end_date
        )

    def __str__(self):
        station_ids_joined = ", ".join(
            [str(station_id) for station_id in self.station_ids]
        )
        return ", ".join(
            [
                f"station_ids [{station_ids_joined}]",
                "& ".join(
                    [parameter.value for parameter, parameter_set in self.parameters]
                ),
                self.resolution.value,
                "& ".join([period_type.value for period_type in self.periods]),
                self.start_date.value if self.start_date else str(None),
                self.end_date.value if self.end_date else str(None),
            ]
        )

    def _collect_data(
        self, station_id: int, parameter: DWDObservationParameterSet, **kwargs
    ) -> pd.DataFrame:
        """
        Method to collect data for one specified parameter. Manages restoring,
        collection and storing of data, transformation and combination of different
        periods.

        Args:
            station_id: station id for which parameter is collected
            parameter: chosen parameter that is collected

        Returns:
            pandas.DataFrame for given parameter of station
        """
        parameter, parameter_set = parameter

        parameter_df = pd.DataFrame()

        for period in self.periods:
            # todo: replace fuzzy args with
            period_df = super()._collect_data(
                station_id=station_id,
                parameter=parameter_set,
                period=period,
                resolution=self.resolution  # required for storage identification
            )

            # Filter out values which already are in the DataFrame
            try:
                period_df = period_df[
                    ~period_df[MetaColumns.DATE.value].isin(
                        parameter_df[MetaColumns.DATE.value]
                    )
                ]
            except KeyError:
                pass

            parameter_df = parameter_df.append(period_df)

        if self.tidy_data:
            parameter_df = parameter_df.dwd.tidy_up_data()

            parameter_df.insert(2, MetaColumns.PARAMETER_SET.value, parameter_set.name)

        if parameter not in DWDObservationParameterSet:
            parameter_df = parameter_df[
                parameter_df[MetaColumns.PARAMETER.value]
                == parameter.value
            ]

        return parameter_df

    def _get_data(self, station_id: int, parameter, **kwargs) -> pd.DataFrame:
        parameter_set = parameter
        resolution = kwargs.get("resolution")
        period = kwargs.get("period")

        try:
            period_df = collect_climate_observations_data(
                station_id=station_id,
                parameter_set=parameter_set,
                resolution=resolution,
                period=period
            )
        except InvalidParameterCombination:
            log.info(
                f"Invalid combination {parameter_set.value}/"
                f"{self.resolution.value}/{period} is skipped."
            )

            period_df = pd.DataFrame()

        return period_df

    def _invalidate_storage(self, parameter) -> None:
        """
        Wrapper for storage invalidation for a certain parameter. Has to be implemented
        for DWD to accommodate the different available periods. p

        Returns:
            None
        """
        parameter, parameter_set = parameter

        for period in self.periods:
            storage = self.storage.hdf5(
                parameter_set.value, self.resolution.value, period.value,
            )

            storage.invalidate()

    def _rename_to_humanized_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Reimplementation as data from DWD comes originally not tidied and thus
        renaming depends on if the data is tidied and parameters are in one column. """
        hcnm = self._create_humanized_column_names_mapping()

        if self.tidy_data:
            df[MetaColumns.PARAMETER.value] = df[
                MetaColumns.PARAMETER.value
            ].apply(lambda x: hcnm[x])
        else:
            df = df.rename(columns=hcnm)

        return df

    def _create_humanized_column_names_mapping(self) -> Dict[str, str]:
        """ Required """
        return create_humanized_column_names_mapping(
            self.resolution, DWDObservationParameterSetStructure
        )


class DWDObservationSites(SitesCore):
    """
    The DWDObservationSites class represents a request for
    a station list as provided by the DWD service.
    """

    def __init__(
        self,
        parameter_set: Union[str, DWDObservationParameterSet],
        resolution: Union[str, DWDObservationResolution],
        period: Union[str, DWDObservationPeriod] = None,
        start_date: Union[None, str, Timestamp] = None,
        end_date: Union[None, str, Timestamp] = None,
    ):
        super().__init__(start_date=start_date, end_date=end_date)

        parameter_set = parse_enumeration_from_template(
            parameter_set, DWDObservationParameterSet
        )
        resolution = parse_enumeration_from_template(
            resolution, DWDObservationResolution
        )
        period = parse_enumeration_from_template(period, DWDObservationPeriod)

        if not check_dwd_observations_parameter_set(parameter_set, resolution, period):
            raise InvalidParameterCombination(
                f"The combination of {parameter_set.value}, {resolution.value}, "
                f"{period.value} is invalid."
            )

        self.parameter = parameter_set
        self.resolution = resolution
        self.period = period

    def _metadata(self) -> pd.DataFrame:
        metadata = metadata_for_climate_observations(
            parameter_set=self.parameter,
            resolution=self.resolution,
            period=self.period,
        )

        # Filter only for stations that have a file
        metadata = metadata[metadata[MetaColumns.HAS_FILE.value].values]

        metadata = metadata.drop(columns=[MetaColumns.HAS_FILE.value])

        return metadata


class DWDObservationMetadata:
    """
    Inquire metadata about weather observations on the
    public DWD data repository.
    """

    def __init__(
        self,
        parameter_set: Optional[List[Union[str, DWDObservationParameterSet]]] = None,
        resolution: Optional[List[Union[str, DWDObservationResolution]]] = None,
        period: Optional[List[Union[str, DWDObservationPeriod]]] = None,
    ):

        if not parameter_set:
            parameter_set = [*DWDObservationParameterSet]
        else:
            parameter_set = parse_enumeration(DWDObservationParameterSet, parameter_set)
        if not resolution:
            resolution = [*DWDObservationResolution]
        else:
            resolution = parse_enumeration(DWDObservationResolution, resolution)
        if not period:
            period = [*DWDObservationPeriod]
        else:
            period = parse_enumeration(DWDObservationPeriod, period)

        self.parameter = parameter_set
        self.resolution = resolution
        self.period = period

    def discover_parameter_sets(self) -> dict:
        """
        Function to print/discover available time_resolution/parameter/period_type
        combinations.

        :return:                        Available parameter combinations.
        """
        trp_mapping_filtered = {
            ts: {
                par: [p for p in pt if p in self.period]
                for par, pt in parameters_and_period_types.items()
                if par in self.parameter
            }
            for ts, parameters_and_period_types in RESOLUTION_PARAMETER_MAPPING.items()  # noqa:E501,B950
            if ts in self.resolution
        }

        time_resolution_parameter_mapping = {
            str(time_resolution): {
                str(parameter): [str(period) for period in periods]
                for parameter, periods in parameters_and_periods.items()
                if periods
            }
            for time_resolution, parameters_and_periods in trp_mapping_filtered.items()
            if parameters_and_periods
        }

        return time_resolution_parameter_mapping

    def discover_parameters(self) -> Dict[str, List[str]]:
        """Return available parameters for the given time resolution, independent of
        source parameter set"""
        available_parameters = {
            resolution.name: [
                parameter.name for parameter in DWDObservationParameter[resolution.name]
            ]
            for resolution in self.resolution
        }

        return available_parameters

    def describe_fields(self) -> dict:
        if len(self.parameter) > 1 or len(self.resolution) > 1 or len(self.period) > 1:
            raise NotImplementedError(
                "'describe_fields is only available for a single"
                "parameter, resolution and period"
            )

        file_index = _create_file_index_for_dwd_server(
            parameter_set=self.parameter[0],
            resolution=self.resolution[0],
            period=self.period[0],
            cdc_base=DWDCDCBase.CLIMATE_OBSERVATIONS,
        )

        file_index = file_index[
            file_index[MetaColumns.FILENAME.value].str.contains("DESCRIPTION_")
        ]

        description_file_url = str(
            file_index[MetaColumns.FILENAME.value].tolist()[0]
        )

        from wetterdienst.dwd.observations.fields import read_description

        document = read_description(description_file_url)

        return document
