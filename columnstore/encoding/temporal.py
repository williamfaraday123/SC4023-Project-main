import re

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import InvalidDateError, DataOverflowError, InvalidBlockError


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


class BlockIdEncoder(FieldEncoder):
    """Encodes block identifiers like '123' or '123A' into a 3-byte compact format."""

    def __init__(self):
        self._pattern = re.compile(r"[0-9]+[A-Z]?")

    def byte_width(self) -> int:
        return 3

    def _parse(self, value: str) -> int:
        if not re.match(self._pattern, value):
            raise InvalidBlockError(
                f"{value} should be a number followed by an optional uppercase letter."
            )

        # Pack into 3 bytes: [letter byte][block number as 2 bytes]
        result = 0
        if ord("A") <= ord(value[-1]) <= ord("Z"):
            result = ord(value[-1])
            value = value[:-1]

        result <<= 16
        block = int(value)

        if block > 2**16 - 1:
            raise DataOverflowError(f"{block} too big to fit into 2 bytes.")

        result += block
        return result

    def decode(self, value: int) -> str:
        block = str(value & 0xFFFF)
        letter = value >> 16
        if letter != 0:
            block += chr(letter)
        return block
