from abc import abstractmethod
from datetime import datetime
from typing import Union
import logging

import numpy as np
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp

from wetterdienst.core.core import Core
from wetterdienst.metadata.column_names import MetaColumns
from wetterdienst.dwd.util import parse_datetime
from wetterdienst.exceptions import StartDateEndDateError
from wetterdienst.util.geo import Coordinates, derive_nearest_neighbours


logger = logging.getLogger(__name__)


class SitesCore(Core):
    def __init__(
        self,
        start_date: Union[None, str, Timestamp] = None,
        end_date: Union[None, str, Timestamp] = None,
    ) -> None:

        super().__init__(start_date, end_date)

    @abstractmethod
    def _metadata(self) -> pd.DataFrame:
        """
        Abstract method for gathering of sites information for a given implementation.
        Information consist of a DataFrame with station ids, location, name, etc

        Returns:
            pandas.DataFrame with the information of different available sites
        """
        pass

    def all(self) -> pd.DataFrame:
        """
        Wraps the _all method and applies date filters.

        Returns:
            pandas.DataFrame with the information of different available sites
        """
        metadata = self._metadata()

        if self.start_date:
            metadata = metadata[
                metadata[MetaColumns.FROM_DATE.value] <= self.start_date
                ]

        if self.end_date:
            metadata = metadata[metadata[MetaColumns.TO_DATE.value] >= self.end_date]

        return metadata

    def nearby_number(
        self,
        latitude: float,
        longitude: float,
        num_stations_nearby: int,
    ) -> pd.DataFrame:
        """
        Wrapper for get_nearby_stations_by_number using the given parameter set. Returns
        nearest stations defined by number.

        Args:
            latitude: latitude in degrees
            longitude: longitude in degrees
            num_stations_nearby: number of stations to be returned, greater 0

        Returns:
            pandas.DataFrame with station information for the selected stations
        """
        if num_stations_nearby <= 0:
            raise ValueError("'num_stations_nearby' has to be at least 1.")

        coords = Coordinates(np.array(latitude), np.array(longitude))

        metadata = self.all()

        metadata = metadata.reset_index(drop=True)

        distances, indices_nearest_neighbours = derive_nearest_neighbours(
            metadata.LAT.values, metadata.LON.values, coords, num_stations_nearby
        )

        distances = pd.Series(distances)
        indices_nearest_neighbours = pd.Series(indices_nearest_neighbours)

        # If num_stations_nearby is higher then the actual amount of stations
        # further indices and distances are added which have to be filtered out
        distances = distances[: min(metadata.shape[0], num_stations_nearby)]
        indices_nearest_neighbours = indices_nearest_neighbours[
            : min(metadata.shape[0], num_stations_nearby)
        ]

        distances_km = np.array(distances * KM_EARTH_RADIUS)

        metadata_location = metadata.iloc[indices_nearest_neighbours, :].reset_index(
            drop=True
        )

        metadata_location[MetaColumns.DISTANCE_TO_LOCATION.value] = distances_km

        if metadata_location.empty:
            logger.warning(
                f"No weather stations were found for coordinate "
                f"{latitude}°N and {longitude}°E "
            )

        return metadata_location

    def nearby_radius(
        self,
        latitude: float,
        longitude: float,
        max_distance_in_km: int,
    ) -> pd.DataFrame:
        """
        Wrapper for get_nearby_stations_by_distance using the given parameter set.
        Returns nearest stations defined by distance (km).

        Args:
            latitude: latitude in degrees
            longitude: longitude in degrees
            max_distance_in_km: distance (km) for which stations will be selected

        Returns:
            pandas.DataFrame with station information for the selected stations
        """

        # Theoretically a distance of 0 km is possible
        if max_distance_in_km < 0:
            raise ValueError("'max_distance_in_km' has to be at least 0.0.")

        metadata = self.all()

        all_nearby_stations = self.nearby_number(latitude, longitude, metadata.shape[0])

        nearby_stations_in_distance = all_nearby_stations[
            all_nearby_stations[MetaColumns.DISTANCE_TO_LOCATION.value]
            <= max_distance_in_km
        ]

        return nearby_stations_in_distance.reset_index(drop=True)


KM_EARTH_RADIUS = 6371
