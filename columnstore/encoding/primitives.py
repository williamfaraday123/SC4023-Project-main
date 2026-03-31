import struct

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import InvalidConversionError, InvalidValueError, DataOverflowError


class Float32Encoder(FieldEncoder):
    """Encodes float values into 4-byte IEEE 754 representation."""

    def byte_width(self) -> int:
        return 4

    def _parse(self, value: str) -> float:
        try:
            return float(value)
        except Exception:
            raise InvalidConversionError(f"{value} cannot be converted to a float.")

    def serialize(self, value: float) -> bytes:
        return struct.pack(">f", value)

    def deserialize(self, data: bytes) -> float:
        return struct.unpack(">f", data)[0]

    def decode(self, value: float) -> str:
        return str(value)


class FixedStringEncoder(FieldEncoder):
    """Encodes strings into a fixed-size ASCII representation padded with null bytes."""

    def __init__(self, size: int):
        self.size = size

    def byte_width(self) -> int:
        return self.size

    def _parse(self, value: str) -> str:
        if len(value) > self.size:
            raise InvalidValueError(f"{value} is longer than fixed size {self.size}")
        return value

    def serialize(self, value: str) -> bytes:
        return value.encode("ascii").ljust(self.byte_width(), b"\x00")

    def deserialize(self, data: bytes) -> str:
        return data.rstrip(b"\x00").decode("ascii")

    def decode(self, value: str) -> str:
        return value


class UInt16Encoder(FieldEncoder):
    """Encodes integers into 2-byte unsigned short representation."""

    def byte_width(self) -> int:
        return 2

    def _parse(self, value: str) -> int:
        try:
            mapped = int(value)
        except Exception:
            raise InvalidConversionError(f"{value} cannot be converted to a short.")
        if mapped > 2**16 - 1:
            raise DataOverflowError(f"{value} is too large for a short.")
        return mapped

    def decode(self, value: int) -> str:
        return str(value)
