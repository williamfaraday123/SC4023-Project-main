from pathlib import Path
import os
import math

from columnstore.encoding.base import FieldEncoder
from columnstore.errors import StorageError

PAGE_SIZE = 4096

MONTH_COL = 0
TOWN_COL = 1
AREA_COL = 6

MONTH_INDEX_OFFSET = 24168
MONTH_INDEX_SIZE = 144


class DiskColumnStore:
    """
    Disk-based column store for structured data.
    Each column is stored in its own file, with records inserted column-wise.
    Supports zone maps for town (bitmask) and area (min/max) columns,
    and a page-level index for the month column.
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

        # Zone map for town column (bitmask per page)
        self.town_zone_map = []
        self._town_zmap_val = 0

        # Zone map for floor_area_sqm column (min/max per page)
        self._area_zmap_min = math.inf
        self._area_zmap_max = -math.inf
        self.area_zone_map = []

        # Page-level index for the month column.
        # Each entry is a set of page numbers containing that month.
        # Covers Jan 2015 (encoded 24180) to Dec 2026 (encoded 24323).
        self.month_index = [set() for _ in range(MONTH_INDEX_SIZE)]

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

        self.write_pointers[i].write(self.write_buffers[i].ljust(PAGE_SIZE, b"\x00"))
        self.write_buffers[i] = b""

        if i == TOWN_COL:
            self.town_zone_map.append(self._town_zmap_val)
            self._town_zmap_val = 0
        elif i == AREA_COL:
            self.area_zone_map.append((self._area_zmap_min, self._area_zmap_max))
            self._area_zmap_min = math.inf
            self._area_zmap_max = -math.inf

    def flush_write_buffers(self) -> None:
        for i in range(len(self.write_buffers)):
            self._flush_write_buffer(i)
            self.write_pointers[i].close()

    def print_storage_stats(self) -> None:
        row_format = "{:>20} {:>15}"
        print(row_format.format("Column", "Pages"))
        total = 0
        for column in self.columns:
            size = os.path.getsize(column) // PAGE_SIZE
            print(row_format.format(os.path.basename(column)[1:], size))
            total += size
        print(row_format.format("Total", total))

    def add_entry(self, values: list[str]) -> None:
        if len(values) != len(self.encoders):
            raise StorageError(
                f"Expected {len(self.encoders)} values, got {len(values)}."
            )

        for pos in self.critical:
            if not values[pos]:
                raise StorageError(f"Expected attribute {pos} to be non-empty.")

        try:
            for i in range(len(values)):
                values[i] = self.encoders[i].encode(values[i])
        except Exception as e:
            raise StorageError(f"Unable to store row due to error: {str(e)}")

        if not self.basic:
            self._area_zmap_min = min(self._area_zmap_min, values[AREA_COL])
            self._area_zmap_max = max(self._area_zmap_max, values[AREA_COL])
            self._town_zmap_val |= 1 << values[TOWN_COL]

        self.size += 1
        for i in range(len(values)):
            packed = self.encoders[i].serialize(values[i])

            if len(self.write_buffers[i]) + self.encoders[i].byte_width() > PAGE_SIZE:
                self._flush_write_buffer(i)

            self.write_buffers[i] += packed

        if not self.basic:
            self.month_index[values[MONTH_COL] - MONTH_INDEX_OFFSET].add(
                self.write_pointers[0].tell() // PAGE_SIZE
            )

    def get_size(self) -> int:
        return self.size

    def _pos_to_page(self, pos: int, i: int) -> int:
        items_per_page = PAGE_SIZE // self.encoders[i].byte_width()
        return pos // items_per_page

    def _get_zone_map_entry(self, pos: int, i: int, zmap: list):
        return zmap[self._pos_to_page(pos, i)]

    def get_item(self, pos: int, i: int):
        """Retrieves the decoded value from column i at position pos, loading the page if needed."""
        page_number = self._pos_to_page(pos, i)
        if self.read_pointers[i] is None:
            self.read_pointers[i] = open(self.columns[i], "rb")

        # Only read from disk if the requested page is not already buffered.
        # tell() == (page+1)*PAGE_SIZE means we just finished reading that page.
        if self.read_pointers[i].tell() != (page_number + 1) * PAGE_SIZE:
            self.reads += 1
            self.read_pointers[i].seek(page_number * PAGE_SIZE)
            self.read_buffers[i] = self.read_pointers[i].read(PAGE_SIZE)

        byte_width = self.encoders[i].byte_width()
        items_per_page = PAGE_SIZE // byte_width
        start = (pos % items_per_page) * byte_width
        return self.encoders[i].deserialize(self.read_buffers[i][start : start + byte_width])

    def get_pos_in_page(self, page: int, i: int) -> range:
        byte_width = self.encoders[i].byte_width()
        items_per_page = PAGE_SIZE // byte_width
        start = page * items_per_page
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
    def get_town_zone_map_entry(self, pos: int) -> int:
        return self._get_zone_map_entry(pos, TOWN_COL, self.town_zone_map)

    def get_area_zone_map_entry(self, pos: int) -> tuple[float, float]:
        return self._get_zone_map_entry(pos, AREA_COL, self.area_zone_map)

    def pos_has_month_in_range(self, pos: int, month_start: int, month_end: int) -> bool:
        page_number = self._pos_to_page(pos, MONTH_COL)
        idx_start = month_start - MONTH_INDEX_OFFSET
        idx_end = month_end - MONTH_INDEX_OFFSET
        for month in range(idx_start, min(idx_end + 1, len(self.month_index))):
            if page_number in self.month_index[month]:
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
