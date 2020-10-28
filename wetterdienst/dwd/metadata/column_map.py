""" mapping from german column names to english column names"""
from typing import Type, Dict

from functools import lru_cache
from numpy import datetime64

from wetterdienst.dwd.observations.metadata import (
    DWDObservationResolution,
    DWDObservationParameterSet,
)
from wetterdienst.dwd.metadata.column_names import (
    DWDOrigMetaColumns,
)
from wetterdienst.metadata.column_names import MetaColumns
from wetterdienst.util.parameter import WDParameterStructureBase

GERMAN_TO_ENGLISH_COLUMNS_MAPPING = {
    DWDOrigMetaColumns.STATION_ID.value: MetaColumns.STATION_ID.value,
    DWDOrigMetaColumns.DATE.value: MetaColumns.DATE.value,
    DWDOrigMetaColumns.FROM_DATE.value: MetaColumns.FROM_DATE.value,
    DWDOrigMetaColumns.TO_DATE.value: MetaColumns.TO_DATE.value,
    DWDOrigMetaColumns.FROM_DATE_ALTERNATIVE.value: MetaColumns.FROM_DATE.value,
    DWDOrigMetaColumns.TO_DATE_ALTERNATIVE.value: MetaColumns.TO_DATE.value,
    DWDOrigMetaColumns.STATION_HEIGHT.value: MetaColumns.STATION_HEIGHT.value,
    DWDOrigMetaColumns.LATITUDE.value: MetaColumns.LATITUDE.value,
    DWDOrigMetaColumns.LATITUDE_ALTERNATIVE.value: MetaColumns.LATITUDE.value,
    DWDOrigMetaColumns.LONGITUDE.value: MetaColumns.LONGITUDE.value,
    DWDOrigMetaColumns.LONGITUDE_ALTERNATIVE.value: MetaColumns.LONGITUDE.value,
    DWDOrigMetaColumns.STATION_NAME.value: MetaColumns.STATION_NAME.value,
    DWDOrigMetaColumns.STATE.value: MetaColumns.STATE.value,
}

METADATA_DTYPE_MAPPING = {
    MetaColumns.STATION_ID.value: int,
    MetaColumns.FROM_DATE.value: datetime64,
    MetaColumns.TO_DATE.value: datetime64,
    MetaColumns.STATION_HEIGHT.value: float,
    MetaColumns.LATITUDE.value: float,
    MetaColumns.LONGITUDE.value: float,
    MetaColumns.STATION_NAME.value: str,
    MetaColumns.STATE.value: str,
}


@lru_cache
def create_humanized_column_names_mapping(
    resolution: DWDObservationResolution,
    parameter_structure: Type[WDParameterStructureBase],
) -> Dict[str, str]:
    """
    Function to create a humanized column names mapping. The function
    takes care of the special cases of quality columns. Therefore it requires the
    time resolution and parameter.

    Args:
        resolution: time resolution enumeration
        parameter_structure: original column names in enumeration style

    Returns:
        dictionary with mappings extended by quality columns mappings
    """
    parameter_set_enums = [
        value
        for key, value in parameter_structure[resolution.name].__dict__.items()
        if not key.startswith("_")
    ]

    hcnm = {
        parameter.value: parameter.name
        for parameter_set in parameter_set_enums
        for parameter in parameter_structure[resolution.name][parameter_set.__name__]
    }

    return hcnm
