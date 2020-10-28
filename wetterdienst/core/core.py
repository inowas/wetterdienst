from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, Type

from datetime import datetime

import dateparser
from pandas._libs.tslibs.timestamps import Timestamp

from wetterdienst.core.source import Source
from wetterdienst.exceptions import StartDateEndDateError


class Core(ABC):
    @property
    @abstractmethod
    def _source(self) -> Source:
        pass

    def __init__(
        self,
        start_date: Union[None, str, datetime, Timestamp] = None,
        end_date: Union[None, str, datetime, Timestamp] = None,
    ) -> None:
        if start_date:
            start_date = Timestamp(dateparser.parse(str(start_date)))
        if end_date:
            end_date = Timestamp(dateparser.parse(str(end_date)))

        if start_date or end_date:
            # If only one date given, make the other one equal
            if not start_date:
                start_date = end_date

            if not end_date:
                end_date = start_date

            if start_date > end_date:
                raise StartDateEndDateError("'start_date' has to be before 'end_date'")

        self.start_date = start_date
        self.end_date = end_date
