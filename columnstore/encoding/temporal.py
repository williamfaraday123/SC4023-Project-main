import re

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import InvalidDateError, DataOverflowError


class MonthEncoder(FieldEncoder):
    """Encodes MMM-YY month strings into a 2-byte representation as (year * 12 + (month - 1))."""

    MONTH_MAP = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
        "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
        "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def __init__(self):
        self._pattern = re.compile(r"^[A-Za-z]{3,4}-[0-9]{2}$")

    def byte_width(self) -> int:
        return 2

    def encode(self, value: str) -> int:
        if not re.match(self._pattern, value):
            raise InvalidDateError(f"{value} is not in a supported format (MMM-YY).")

        month_str, year_str = value.split("-")
        month_str = month_str.title()
        if month_str not in self.MONTH_MAP:
            raise InvalidDateError(f"{month_str} is not a valid month abbreviation.")

        month = self.MONTH_MAP[month_str]
        year = 2000 + int(year_str)

        # Compact month encoding: unique integer per month across years
        encoded = year * 12 + (month - 1)
        if encoded > 2**16 - 1:
            raise DataOverflowError(f"{value} is too big to fit into 2 bytes.")
        return encoded

    def decode(self, value: int) -> str:
        month = value % 12 + 1
        year = value // 12
        month_abbr = self.MONTH_NAMES[month - 1]
        year_short = str(year % 100).zfill(2)
        return f"{month_abbr}-{year_short}"
