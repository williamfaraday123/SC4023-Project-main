import re

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import DataOverflowError, InvalidBlockError


class BlockIdEncoder(FieldEncoder):
    """Encodes block identifiers like '123' or '123A' into a 3-byte compact format."""

    def __init__(self):
        self._pattern = re.compile(r"^[0-9]+[A-Z]?$")

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
