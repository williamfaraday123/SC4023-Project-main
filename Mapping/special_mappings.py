import re

from Mapping.mapper import Mapper
from exceptions import InvalidDateException, DataOverflowException, InvalidBlockException

class MonthMapper(Mapper):
    """Maps MMM-YY month strings into a 2-byte representation as (year * 12 + (month - 1))."""

    def __init__(self):
        """Compiles regex patterns for validating month strings."""
        self.pattern_textual = re.compile(r"^[A-Za-z]{3,4}-[0-9]{2}$")
        self.month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
        }
        self.month_names = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]

    def mapped_size(self) -> int:
        return 2

    def map_value(self, value: str) -> int:
        """Parses and validates MMM-YY month strings."""
        if re.match(self.pattern_textual, value):
            month_str, year_str = value.split("-")
            month_str = month_str.title()
            if month_str not in self.month_map:
                raise InvalidDateException(f"{month_str} is not a valid month abbreviation.")
            month = self.month_map[month_str]
            year = 2000 + int(year_str)
        else:
            raise InvalidDateException(f"{value} is not in a supported format (MMM-YY).")

        if not (1 <= month <= 12):
            raise InvalidDateException(f"{month} is not a valid month.")

        mapped = year * 12 + (month - 1)
        if mapped > 2 ** 16 - 1:
            raise DataOverflowException(f"{value} is too big to fit into 2 bytes.")

        return mapped

    def unmap_value(self, value: int) -> str:
        """Converts the internal numeric representation back to MMM-YY  format."""
        month = value % 12 + 1
        year = value // 12

        # Map numeric month back to abbreviation
        month_abbr = self.month_names[month - 1]

        # Convert year to 2-digit format (assuming 2000+)
        year_short = str(year % 100).zfill(2)

        return f"{month_abbr}-{year_short}"


class BlockMapper(Mapper):
    """Maps block identifiers like "123", "123A" into a 3-byte format."""
    def __init__(self):
        """Compiles a regex pattern for validating block identifiers."""
        self.pattern = re.compile(r"[0-9]+[A-Z]?")

    def mapped_size(self) -> int:
        """Returns 3 bytes: 2 for the number and 1 for the optional letter."""
        return 3

    def internal_map(self, value: str) -> int:
        """Parses and encodes a block number and optional letter into a compact integer."""
        if not re.match(self.pattern, value):
            raise InvalidBlockException(f"{value} should be a number followed by an optional uppercase letter.")

        result = 0
        if ord('A') <= ord(value[-1]) <= ord('Z'):
            result = ord(value[-1])
            value = value[:-1]

        result <<= 16
        block = int(value)

        if block > 2 ** 16 - 1:
            raise DataOverflowException(f"{block} too big to fit into 2 bytes.")

        result += int(value)
        return result

    def unmap_value(self, value: int) -> str:
        """Decodes the numeric-encoded block back into its original string format."""
        block = str(value & 0xFFFF)
        letter = value >> 16
        if letter != 0:
            block += chr(letter)

        return block
