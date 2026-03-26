from exceptions import MappingException


class Mapper:
    """Base class for all mappers. Handles byte-level serialization and deserialization."""
    def mapped_size(self) -> int:
        """Returns the number of bytes needed to store the mapped value."""
        pass

    def internal_map(self, value: str) -> int:
        """Defines the logic for mapping a string to its internal representation."""
        pass

    def from_bytes(self, value: bytes) -> int:
        """Deserializes a byte sequence into an integer."""
        return int.from_bytes(value, byteorder="big")

    def to_bytes(self, value: int) -> bytes:
        """Serializes an integer into a byte sequence of fixed size."""
        return value.to_bytes(self.mapped_size(), byteorder="big")

    def map_value(self, value: str) -> int:
        """Maps a string to an internal integer value."""
        try:
            mapped = self.internal_map(value)
        except Exception as e:
            raise MappingException(e)
        return mapped

    def unmap_value(self, value: int) -> str:
        """Converts an internal representation back to a string value."""
        pass
