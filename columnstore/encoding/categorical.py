import math

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import InvalidValueError


class CategoricalEncoder(FieldEncoder):
    """Encodes string values to unique integer indices based on a fixed list of allowed values."""

    def __init__(self, values: list[str]):
        self.lookup = {v: i for i, v in enumerate(values)}
        self.values = list(values)

    def byte_width(self) -> int:
        # Minimum bytes needed to represent all category indices
        return int(math.ceil(len(self.lookup).bit_length() / 8.0))

    def _parse(self, value: str) -> int:
        if value not in self.lookup:
            raise InvalidValueError(
                f"{value} is not present in the mappings for {self.__class__.__name__}"
            )
        return self.lookup[value]

    def decode(self, value: int) -> str:
        return self.values[value]


class TownEncoder(CategoricalEncoder):
    pass


class FlatTypeEncoder(CategoricalEncoder):
    pass


class FlatModelEncoder(CategoricalEncoder):
    pass


class StoreyRangeEncoder(CategoricalEncoder):
    pass


class StreetNameEncoder(CategoricalEncoder):
    """Encodes street names to integer indices built dynamically from the dataset."""
    pass
