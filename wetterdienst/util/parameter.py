from typing import Dict, Tuple


class _GetAttrMeta(type):
    # https://stackoverflow.com/questions/33727217/subscriptable-objects-in-class
    def __getitem__(cls, x):
        return getattr(cls, x)


class WDParameterStructureBase(metaclass=_GetAttrMeta):
    pass


def build_parameter_identifier(
    station_id: int,
    *args: Tuple[str]
) -> str:
    """ Create parameter set identifier that is used for storage interactions. Args
    are required for flexible parameter input especially for DWD source """
    return f"{'/'.join(args)}/station_id_{str(station_id)}"
