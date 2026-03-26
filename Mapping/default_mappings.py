import struct

from Mapping.mapper import Mapper
from exceptions import InvalidConversionException, InvalidMapException, DataOverflowException

class FloatMapper(Mapper):
    """Maps float values into 4-byte float representation."""
    def mapped_size(self) -> int:
        """Returns 4 bytes for a standard float representation."""
        return 4

    def internal_map(self, value: str) -> float:
        """Parses a string into a float, raises exception if invalid."""
        try:
            return float(value)
        except Exception:
            raise InvalidConversionException(f"{value} cannot be converted to a float.")

    def to_bytes(self, value: float) -> bytes:
        """Serializes a float to 4 bytes."""
        return struct.pack(">f", value)

    def from_bytes(self, value: bytes) -> float:
        """Deserializes 4 bytes into a float."""
        return struct.unpack(">f", value)[0]

    def unmap_value(self, value: float) -> str:
        """Returns the float value as a string."""
        return str(value)


class CharMapper(Mapper):
    """Maps strings to a fixed-size ASCII representation padded with null bytes."""
    def __init__(self, size: int):
        """Initializes the mapper with a fixed size."""
        self.size = size

    def mapped_size(self) -> int:
        """Returns the fixed byte size reserved for the string."""
        return self.size

    def internal_map(self, value: str) -> str:
        """Validates the string fits within the fixed size."""
        if len(value) > self.size:
            raise InvalidMapException(f"{value} is longer than fixed size {self.size}")
        return value

    def to_bytes(self, value: str) -> bytes:
        """Encodes the string in ASCII and pads it to the required size."""
        return value.encode("ascii").ljust(self.mapped_size(), b"\x00")

    def from_bytes(self, value: bytes) -> str:
        """Decodes a byte string, stripping trailing null bytes."""
        return value.rstrip(b"\x00").decode("ascii")

    def unmap_value(self, value: str) -> str:
        """Returns the original string."""
        return value


class ShortMapper(Mapper):
    """Maps integers into 2-byte unsigned short representation."""
    def mapped_size(self) -> int:
        """Returns 2 bytes for a short."""
        return 2

    def internal_map(self, value: str) -> int:
        """Converts a string to an integer and validates short size."""
        try:
            mapped = int(value)
        except Exception:
            raise InvalidConversionException(f"{value} cannot be converted to a short.")

        if mapped > 2 ** 16 - 1:
            raise DataOverflowException(f"{value} is too large for a short.")

        return mapped

    def unmap_value(self, value: int) -> str:
        """Returns the integer value as a string."""
        return str(value)
