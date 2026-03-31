from columnstore.errors import EncodingError


class FieldEncoder:
    """Base class for all field encoders. Handles byte-level serialization and deserialization."""

    def byte_width(self) -> int:
        """Returns the number of bytes needed to store the encoded value."""
        raise NotImplementedError

    def _parse(self, value: str):
        """Defines the logic for parsing a string into its internal representation."""
        raise NotImplementedError

    def deserialize(self, data: bytes) -> int:
        """Deserializes a byte sequence into an integer."""
        return int.from_bytes(data, byteorder="big")

    def serialize(self, value: int) -> bytes:
        """Serializes an integer into a byte sequence of fixed size."""
        return value.to_bytes(self.byte_width(), byteorder="big")

    def encode(self, value: str):
        """Encodes a string into its internal representation."""
        try:
            return self._parse(value)
        except Exception as e:
            raise EncodingError(e)

    def decode(self, value) -> str:
        """Converts an internal representation back to a string value."""
        raise NotImplementedError
