from pathlib import Path
import os
import math

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import StorageError

BLOCK_SIZE = 4096


class DiskColumnStore:
    """
    Disk-based column-oriented storage system for structured data.
    Each column is stored in its own file, with records inserted column-wise.
    Supports zone maps for town (bitmask) and area (min/max) columns,
    and a block-level index for the month column.
    """

    def __init__(
        self,
        columns: list[str],
        encoders: list[FieldEncoder],
        critical: list[int],
        basic: bool = False,
    ):
        if len(encoders) != len(columns):
            raise StorageError(
                f"Number of columns {len(columns)} must match number of encoders {len(encoders)}."
            )

        self.basic = basic
        self.critical = critical
        self.encoders = encoders
        self.size = 0
        self.reads = 0

        source = Path(__file__).resolve().parent.parent
        self.columns = [os.path.join(source, f"_{c}") for c in columns]

        self.write_pointers = [open(c, "wb") for c in self.columns]
        self.write_buffers = [b""] * len(columns)

        self.read_pointers = [None] * len(columns)
        self.read_buffers = [b""] * len(columns)

        # Zone map for town column (bitmask per block)
        self.town_zone_map = []
        self._town_zmap_val = 0

        # Zone map for floor_area_sqm column (min/max per block)
        self._area_zmap_min = math.inf
        self._area_zmap_max = -math.inf
        self.area_zone_map = []

        # Block-level index for the month column.
        # Each entry is a set of block numbers containing that month.
        # Covers Jan 2015 (encoded 24180) to Dec 2026 (encoded 24323).
        # Offset by 24168 so index 0 = encoded value 24168.
        self.month_index = [set() for _ in range(144)]

    def clear_disk(self) -> None:
        self.flush_write_buffers()
        self.clear_read_state()
        for column in self.columns:
            if os.path.isfile(column):
                os.remove(column)

    def clear_read_state(self) -> None:
        self.reads = 0
        for fp in self.read_pointers:
            if fp is not None:
                fp.close()
        self.read_pointers = [None] * len(self.columns)
        self.read_buffers = [b""] * len(self.columns)

    def _flush_write_buffer(self, i: int) -> None:
        if self.write_buffers[i] == b"":
            return

        self.write_pointers[i].write(self.write_buffers[i].ljust(BLOCK_SIZE, b"\x00"))
        self.write_buffers[i] = b""

        if i == 1:
            self.town_zone_map.append(self._town_zmap_val)
            self._town_zmap_val = 0
        elif i == 6:
            self.area_zone_map.append((self._area_zmap_min, self._area_zmap_max))
            self._area_zmap_min = math.inf
            self._area_zmap_max = -math.inf

    def flush_write_buffers(self) -> None:
        for i in range(len(self.write_buffers)):
            self._flush_write_buffer(i)
            self.write_pointers[i].close()

    def print_storage_stats(self) -> None:
        row_format = "{:>20} {:>15}"
        print(row_format.format("Column", "Blocks"))
        total = 0
        for column in self.columns:
            size = os.path.getsize(column) // BLOCK_SIZE
            print(row_format.format(os.path.basename(column)[1:], size))
            total += size
        print(row_format.format("Total", total))

    def add_entry(self, tokens: list[str]) -> None:
        if len(tokens) != len(self.encoders):
            raise StorageError(
                f"Expected {len(self.encoders)} tokens, got {len(tokens)}."
            )

        for pos in self.critical:
            if not tokens[pos]:
                raise StorageError(f"Expected attribute {pos} to be non-empty.")

        try:
            for i in range(len(tokens)):
                tokens[i] = self.encoders[i].encode(tokens[i])
        except Exception as e:
            raise StorageError(f"Unable to store row due to error: {str(e)}")

        if not self.basic:
            self._area_zmap_min = min(self._area_zmap_min, tokens[6])
            self._area_zmap_max = max(self._area_zmap_max, tokens[6])
            self._town_zmap_val |= 1 << tokens[1]

        self.size += 1
        for i in range(len(tokens)):
            packed = self.encoders[i].serialize(tokens[i])

            if len(self.write_buffers[i]) + self.encoders[i].byte_width() > BLOCK_SIZE:
                self._flush_write_buffer(i)

            self.write_buffers[i] += packed

        if not self.basic:
            self.month_index[tokens[0] - 24168].add(
                self.write_pointers[0].tell() // BLOCK_SIZE
            )

    def get_size(self) -> int:
        return self.size

    def _pos_to_block(self, pos: int, i: int) -> int:
        items_per_page = BLOCK_SIZE // self.encoders[i].byte_width()
        return pos // items_per_page

    def _get_zonemap_item(self, pos: int, i: int, zmap: list):
        return zmap[self._pos_to_block(pos, i)]

    def get_item(self, pos: int, i: int):
        """Retrieves the decoded value from column i at position pos, loading the block if needed."""
        block_number = self._pos_to_block(pos, i)
        if self.read_pointers[i] is None:
            self.read_pointers[i] = open(self.columns[i], "rb")

        # Only read from disk if the requested block is not already buffered.
        # tell() == (block+1)*BLOCK_SIZE means we just finished reading that block.
        if self.read_pointers[i].tell() != (block_number + 1) * BLOCK_SIZE:
            self.reads += 1
            self.read_pointers[i].seek(block_number * BLOCK_SIZE)
            self.read_buffers[i] = self.read_pointers[i].read(BLOCK_SIZE)

        bw = self.encoders[i].byte_width()
        items_per_page = BLOCK_SIZE // bw
        start = (pos % items_per_page) * bw
        return self.encoders[i].deserialize(self.read_buffers[i][start : start + bw])

    def get_pos_in_block(self, block: int, i: int) -> range:
        bw = self.encoders[i].byte_width()
        items_per_page = BLOCK_SIZE // bw
        start = block * items_per_page
        return range(start, start + items_per_page)

    # Column accessors
    def get_month(self, pos: int):
        return self.get_item(pos, 0)

    def get_town(self, pos: int):
        return self.get_item(pos, 1)

    def get_block_id(self, pos: int):
        return self.get_item(pos, 3)

    def get_floor_area_sqm(self, pos: int) -> float:
        return self.get_item(pos, 6)

    def get_flat_model(self, pos: int):
        return self.get_item(pos, 7)

    def get_lease_date(self, pos: int):
        return self.get_item(pos, 8)

    def get_resale_price(self, pos: int) -> float:
        return self.get_item(pos, 9)

    # Zone map / index accessors
    def get_town_zmap_entry(self, pos: int) -> int:
        return self._get_zonemap_item(pos, 1, self.town_zone_map)

    def get_area_zmap_entry(self, pos: int) -> tuple[float, float]:
        return self._get_zonemap_item(pos, 6, self.area_zone_map)

    def pos_has_month_in_range(self, pos: int, month_start: int, month_end: int) -> bool:
        block_number = self._pos_to_block(pos, 0)
        idx_start = month_start - 24168
        idx_end = month_end - 24168
        for month in range(idx_start, min(idx_end + 1, len(self.month_index))):
            if block_number in self.month_index[month]:
                return True
        return False

    # Decode helpers
    def decode_town(self, index: int) -> str:
        return self.encoders[1].decode(index)

    def decode_month(self, month: int) -> str:
        return self.encoders[0].decode(month)

    def decode_block_id(self, value: int) -> str:
        return self.encoders[3].decode(value)

    def decode_flat_model(self, value: int) -> str:
        return self.encoders[7].decode(value)
