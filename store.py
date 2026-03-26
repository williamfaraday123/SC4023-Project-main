from pathlib import Path
import os
import math
from Mapping.mapper import Mapper
from exceptions import StorageException, MappingException

BLOCK_SIZE = 4096

class ColumnStore:
    """
    A disk-based column-oriented storage system for structured data.
    Each column is stored in its own file, and records are inserted column-wise.
    """
    def __init__(
        self,
        columns: list[str],
        mappings: list[Mapper],
        critical: list[int],
        basic: bool = False,
    ):
        """Initializes the column store."""
        self.basic = basic
        if len(mappings) != len(columns):
            raise StorageException(
                f"Number of columns {len(columns)} must match number of mappings {len(self.mappings)}."
            )

        self.critical = critical
        self.mappings = mappings
        self.size = 0
        self.reads = 0

        source = Path(__file__).resolve().parent
        self.columns = [os.path.join(source, f"_{c}") for c in columns]

        self.write_pointers = [open(c, "wb") for c in self.columns]
        self.write_buffers = [b""] * len(columns)

        self.read_pointers = [None] * len(columns)
        self.read_buffers = [b""] * len(columns)

        self.town_zone_map = []
        self.town_zmap_val = 0

        self.area_zmap_min = math.inf
        self.area_zmap_max = -math.inf
        self.area_zone_map = []

        self.month_index = [set() for _ in range(121)]

    def clear_disk(self) -> None:
        """Flush buffers and delete all column files from disk."""
        self.flush_write_buffers()
        self.clear_read_state()
        for column in self.columns:
            if os.path.isfile(column):
                os.remove(column)

    def clear_read_state(self) -> None:
        """Closes all read pointers and clears read buffers."""
        self.reads = 0
        for fp in self.read_pointers:
            if fp is not None:
                fp.close()

        self.read_pointers = [None] * len(self.columns)
        self.read_buffers = [b""] * len(self.columns)

    def flush_write_buffer(self, i: int) -> None:
        """Flushes a single write buffer to disk for column i. Adds zone map info for town and area."""
        if self.write_buffers[i] == b"":
            return

        self.write_pointers[i].write(self.write_buffers[i].ljust(4096, b"\x00"))
        self.write_buffers[i] = b""

        if i == 1:
            self.town_zone_map.append(self.town_zmap_val)
            self.town_zmap_val = 0
        elif i == 6:
            self.area_zone_map.append((self.area_zmap_min, self.area_zmap_max))
            self.area_zmap_min = math.inf
            self.area_zmap_max = -math.inf

    def flush_write_buffers(self) -> None:
        """Flush all column write buffers and close their files."""
        for i in range(len(self.write_buffers)):
            self.flush_write_buffer(i)
            self.write_pointers[i].close()

    def print_storage_stats(self) -> None:
        """Prints number of disk blocks used per column and total."""
        row_format = "{:>20} {:>15}"
        print(row_format.format("Column", "Blocks"))
        total = 0
        for column in self.columns:
            size = os.path.getsize(column) // BLOCK_SIZE
            print(row_format.format(os.path.basename(column)[1:], size))
            total += size
        print(row_format.format("Total", total))

    def add_entry(self, tokens: list[str]) -> None:
        """Adds a new row (record) to the store, mapping and writing each column's value."""
        if len(tokens) != len(self.mappings):
            raise StorageException(
                f"Expected {self.mappings} tokens, got {len(tokens)}."
            )

        for pos in self.critical:
            if not tokens[pos]:
                raise StorageException(f"Expected attribute {pos} to be non-empty.")

        try:
            for i in range(len(tokens)):
                tokens[i] = self.mappings[i].map_value(tokens[i])
        except MappingException as e:
            raise StorageException(f"Unable to store row due to error: {str(e)}")

        if not self.basic:
            self.area_zmap_min = min(self.area_zmap_min, tokens[6])
            self.area_zmap_max = max(self.area_zmap_max, tokens[6])
            self.town_zmap_val |= 1 << tokens[1]

        self.size += 1
        for i in range(len(tokens)):
            packed = self.mappings[i].to_bytes(tokens[i])

            if len(self.write_buffers[i]) + self.mappings[i].mapped_size() > BLOCK_SIZE:
                self.flush_write_buffer(i)

            self.write_buffers[i] += packed

        if not self.basic:
            self.month_index[tokens[0] - 24168].add(
                self.write_pointers[0].tell() // BLOCK_SIZE
            )

    def get_size(self) -> int:
        """Returns the number of records in the store."""
        return self.size

    def pos_to_block(self, pos: int, i: int) -> int:
        """Returns the block number for column `i` where position `pos` is stored."""
        items_per_page = BLOCK_SIZE // self.mappings[i].mapped_size()
        return pos // items_per_page

    def get_zonemap_item(self, pos: int, i: int, zmap: list):
        """Retrieves the zonemap entry for column i at position pos."""
        return zmap[self.pos_to_block(pos, i)]

    def get_item(self, pos: int, i: int) -> int | float | str:
        """Retrieves the decoded value from column `i` at position `pos`, loading the block into memory if needed."""
        block_number = self.pos_to_block(pos, i)
        if self.read_pointers[i] is None:
            self.read_pointers[i] = open(self.columns[i], "rb")

        if self.read_pointers[i].tell() != (block_number + 1) * BLOCK_SIZE:
            self.reads += 1
            self.read_pointers[i].seek(block_number * BLOCK_SIZE)
            self.read_buffers[i] = self.read_pointers[i].read(BLOCK_SIZE)

        mapped_size = self.mappings[i].mapped_size()
        items_per_page = BLOCK_SIZE // mapped_size
        start = (pos % items_per_page) * mapped_size
        return self.mappings[i].from_bytes(
            self.read_buffers[i][start : start + mapped_size]
        )

    def get_pos_in_block(self, block: int, i: int) -> list[int]:
        """Returns the position range covered by the given block for column `i`."""
        mapped_size = self.mappings[i].mapped_size()
        items_per_page = BLOCK_SIZE // mapped_size

        start = block * items_per_page
        end = start + items_per_page
        return range(start, end)

    # GET record for specific columns:
    def get_month(self, pos: int) -> int:
        return self.get_item(pos, 0)

    def get_town(self, pos: int) -> int:
        return self.get_item(pos, 1)

    def get_floor_area_sqm(self, pos: int) -> float:
        return self.get_item(pos, 6)

    def get_resale_price(self, pos: int) -> float:
        return self.get_item(pos, 9)

    # GET record for zonemap and index:
    def get_town_zmap_entry(self, pos: int) -> int:
        return self.get_zonemap_item(pos, 1, self.town_zone_map)

    def get_area_zmap_entry(self, pos: int) -> list[float]:
        return self.get_zonemap_item(pos, 6, self.area_zone_map)

    def get_pos_has_month(self, pos: int, month1: int, month2: int) -> bool:
        block_number = self.pos_to_block(pos, 0)
        month1 -= 24168
        month2 -= 24168

        for month in range(month1, min(month2 + 1, len(self.month_index))):
            if block_number in self.month_index[month]:
                return True

        return False

    def unmap_town(self, index: int) -> str:
        """Converts mapped town index back to its string representation."""
        return self.mappings[1].unmap_value(index)

    def unmap_month(self, month: int) -> str:
        """Converts mapped month back to MMM-YY format."""
        return self.mappings[0].unmap_value(month)
