from abc import abstractmethod
from typing import Dict, List, Union, Type, Optional, Generator
from enum import Enum
import logging

import pandas as pd

from wetterdienst.core.core import Core
from wetterdienst.metadata.column_names import MetaColumns
from wetterdienst.store import StorageAdapter
from wetterdienst.exceptions import NoParametersFound, InvalidParameter, InvalidEnumeration
from wetterdienst.util.enumeration import parse_enumeration_from_template
from wetterdienst.util.parameter import build_parameter_identifier

log = logging.getLogger(__name__)


class ObservationDataCore(Core):
    @property
    @abstractmethod
    def _observation_parameter_template(self) -> Type[Enum]:
        pass

    @property
    @abstractmethod
    def _date_fields(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def _integer_fields(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def _string_fields(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def _quality_fields(self) -> List[str]:
        pass

    @staticmethod
    def _parse_station_ids(station_ids: List[Union[int, str]]) -> List[int]:
        return pd.Series(station_ids).astype(int).tolist()

    def _parse_parameters(self, parameters):
        parameters_ = []

        for parameter in parameters:
            try:
                parameters_.append(self._parse_parameter(parameter))
            except InvalidParameter as e:
                log.info(str(e))

        return parameters_

    def _parse_parameter(self, parameter):
        try:
            return parse_enumeration_from_template(
                parameter, self._observation_parameter_template)
        except InvalidEnumeration as e:
            raise InvalidParameter from e

    def __init__(
        self,
        station_ids: List[Union[int, str]],
        parameters: List[Union[str, Type[Enum]]],
        start_date,
        end_date,
        storage: Optional[StorageAdapter] = None,
        humanize_column_names: bool = False,
    ) -> None:
        super(ObservationDataCore, self).__init__(start_date, end_date)

        self.station_ids = self._parse_station_ids(station_ids)
        self.parameters = self._parse_parameters(parameters)

        if not self.parameters:
            raise NoParametersFound(f"No parameters could be parsed from {parameters}")

        if storage:
            storage.source = self._source

        self.storage = storage
        self.humanize_column_names = humanize_column_names

    def __eq__(self, other):
        return (
            self.station_ids == other.station_ids
            and self.parameters == other.parameters
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
                self.start_date.value if self.start_date else str(None),
                self.end_date.value if self.end_date else str(None),
            ]
        )

    def collect_data(self) -> Generator[pd.DataFrame, None, None]:
        """
        Collect data for defined station ids and parameters. Yields a pandas.DataFrame
        for each station id with all combined parameters for the station.
        Yields:
            pandas.DataFrame with data for given parameters of a station id
        """
        for station_id in self.station_ids:
            station_df = []

            for parameter in self.parameters:
                parameter_df = self._collect_data(station_id, parameter)

                station_df.append(parameter_df)

            station_df = pd.concat(station_df)

            if self.humanize_column_names:
                station_df = self._rename_to_humanized_parameters(station_df)

            if self.start_date:
                try:
                    station_df = station_df[
                        (station_df[MetaColumns.DATE.value] >= self.start_date)
                        & (station_df[MetaColumns.DATE.value] <= self.end_date)
                        ]
                except KeyError:
                    pass

            if station_df.empty:
                continue

            yield station_df

    def _collect_data(self, station_id: int, parameter: Type[Enum]) -> pd.DataFrame:
        """
        Manages the data collection for one explicit station and parameter combination.
        If wanted the data will be taken from local storage or rather freshly collected
        with method self._get_data which has to be implemented by the source.

        Args:
            station_id: id of station for which data is collected
            parameter: parameter enumeration for which station is collected

        Returns:

        """
        storage = None
        if self.storage:
            storage = self.storage.hdf5(
                parameter.value
            )

            if self.storage.invalidate:
                self._invalidate_storage(parameter)

            parameter_df = storage.restore(station_id)

            if not parameter_df.empty:
                return parameter_df

        parameter_identifier = build_parameter_identifier(
            station_id, parameter.value)

        log.info(f"Acquiring observations data for {parameter_identifier}.")

        parameter_df = self._get_data(station_id, parameter)

        self.coerce_field_types(parameter_df)

        if self.storage and self.storage.persist:
            storage.store(station_id=station_id, df=parameter_df)

        return parameter_df

    @abstractmethod
    def _get_data(self, station_id: int, parameter) -> pd.DataFrame:
        """
         Method for new acquisition of data from the internet. Has to be implemented
         for the source.

        Args:
            station_id: station id for which data is gathered
            parameter: parameter enumeration for which data is gathered

        Returns:
            pandas.DataFrame with a specific parameter
        """
        pass

    def coerce_field_types(self, df: pd.DataFrame) -> None:
        for column in df.columns:
            # Station ids are handled separately as they are expected to not have any nans
            if column == MetaColumns.STATION_ID.value:
                df[column] = df[column].astype(int)
            elif column in self._date_fields:
                df[column] = self._coerce_date_field_types(df[column])
            elif column in self._integer_fields or column in self._quality_fields:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                    pd.Int64Dtype()
                )
            elif column in self._string_fields:
                df[column] = df[column].astype(pd.StringDtype())
            else:
                df[column] = df[column].astype(float)

    @staticmethod
    def _coerce_date_field_types(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, infer_datetime_format=True)

    def collect_safe(self) -> pd.DataFrame:
        """
        Collect all data from ``DWDObservationData``.
        """

        data = list(self.collect_data())

        if not data:
            raise ValueError("No data available for given constraints")

        return pd.concat(data)

    def _invalidate_storage(self, parameter) -> None:
        """
        Wrapper for storage invalidation for all kinds of defined parameters and
        periods. Used before gathering of data as it has no relation to any specific
        station id.

        Returns:
            None
        """
        storage = self.storage.hdf5(
            parameter.value
        )

        storage.invalidate()

    def _rename_to_humanized_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Method to rename parameters to humanized ones """
        df = df.rename(columns=self._create_humanized_column_names_mapping())

        return df

    def _create_humanized_column_names_mapping(self) -> Dict[str, str]:
        """ Method to create original parameter to humanized parameter mapping """
        hcnm = {
            parameter.value: parameter.name
            for parameter in self._observation_parameter_template
        }

        return hcnm
