import math
import itertools

from columnstore.metrics import Metrics
from columnstore.storage import DiskColumnStore


MONTH_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


class ScanEngine:
    """Executes filtered queries on a DiskColumnStore and calculates statistics."""

    def __init__(self, store: DiskColumnStore):
        self.results: dict = {}
        self.store = store
        self.clear_results()

    def get_results(self) -> str:
        return "Year,Month,town,Category,Value\n" + "\n".join(
            [self.results[cat] for cat in self.results]
        )

    def clear_results(self) -> None:
        self.results = {}

    def _add_result(self, month_start: int, town: int, category: Metrics, value) -> None:
        town_str = self.store.decode_town(town)
        month_str = self.store.decode_month(month_start)
        month_name, year_suffix = month_str.split("-")
        year = f"20{year_suffix}"
        month_num = str(MONTH_NUM[month_name]).zfill(2)
        if isinstance(value, float):
            value = str(round(value, 2))
        self.results[category] = ",".join([year, month_num, town_str, category.value, str(value)])

    # ── Filtering ──────────────────────────────────────────────────

    def filter_by_month(self, positions, start_month: int, month_span: int,
                        use_index: bool = False) -> list[int]:
        """Filters records within [start_month, start_month + month_span - 1]."""
        end_month = start_month + month_span - 1
        result = []
        for pos in positions:
            if use_index:
                if not self.store.pos_has_month_in_range(pos, start_month, end_month):
                    continue
            month = self.store.get_month(pos)
            if start_month <= month <= end_month:
                result.append(pos)
        return result

    def filter_by_town(self, positions: list[int], town: int,
                       use_zone_map: bool = False) -> list[int]:
        result = []
        for pos in positions:
            if use_zone_map:
                zval = self.store.get_town_zmap_entry(pos)
                if not (zval & (1 << town)):
                    continue
            t = self.store.get_town(pos)
            if t == town:
                result.append(pos)
        return result

    def filter_by_area(self, positions: list[int], min_area: int,
                       use_zone_map: bool = False) -> list[int]:
        result = []
        for pos in positions:
            if use_zone_map:
                _, zmax = self.store.get_area_zmap_entry(pos)
                if zmax < min_area:
                    continue
            sqm = self.store.get_floor_area_sqm(pos)
            if sqm >= min_area:
                result.append(pos)
        return result

    def apply_filters(self, start_month: int, town: int,
                      start_idx: int, stop_idx: int,
                      month_span: int = 2, min_area: int = 80) -> list[int]:
        """Composite filter: month range -> town -> area."""
        pos = self.filter_by_month(range(start_idx, stop_idx), start_month, month_span, True)
        pos = self.filter_by_town(pos, town, True)
        pos = self.filter_by_area(pos, min_area)
        return pos

    # ── Analysis helpers (demo / permutation testing) ──────────────

    def test_filter_permutations(self, start_month: int, town: int,
                                 use_zone_map: bool, use_index: bool,
                                 month_span: int = 2, min_area: int = 80) -> None:
        row_format = "{:>20} {:>20}"
        print(row_format.format("Permutation", "Blocks"))

        data_range = range(self.store.get_size())
        permutations = list(itertools.permutations(["month", "town", "area"]))

        for perm in permutations:
            self.store.clear_read_state()
            filtered = data_range

            filters = {
                "month": lambda data: self.filter_by_month(
                    data, start_month, month_span, use_index=use_index
                ),
                "town": lambda data: self.filter_by_town(
                    data, town, use_zone_map=use_zone_map
                ),
                "area": lambda data: self.filter_by_area(
                    data, min_area, use_zone_map=use_zone_map
                ),
            }

            reads = []
            for step in perm:
                filtered = filters[step](filtered)
                reads.append(str(self.store.reads))

            print(row_format.format(str(perm), "|".join(reads)))

    # ── Statistics ────────────────────────────────────────────────

    def minimum_price(self, start_month: int, town: int,
                      start_idx: int = 0, stop_idx: int = None,
                      month_span: int = 2, min_area: int = 80) -> float:
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        positions = self.apply_filters(start_month, town, start_idx, stop_idx, month_span, min_area)
        if not positions:
            self._add_result(start_month, town, Metrics.MIN_PRICE, "No result")
            return math.inf

        min_price = math.inf
        for pos in positions:
            price = self.store.get_resale_price(pos)
            if price < min_price:
                min_price = price

        self._add_result(start_month, town, Metrics.MIN_PRICE, min_price)
        return min_price

    def average_price(self, start_month: int, town: int,
                      start_idx: int = 0, stop_idx: int = None,
                      month_span: int = 2, min_area: int = 80) -> tuple:
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        positions = self.apply_filters(start_month, town, start_idx, stop_idx, month_span, min_area)
        if not positions:
            self._add_result(start_month, town, Metrics.AVG_PRICE, "No result")
            return 0, 0

        total_price = sum(self.store.get_resale_price(p) for p in positions)
        self._add_result(start_month, town, Metrics.AVG_PRICE, total_price / len(positions))
        return total_price, len(positions)

    def stddev_price(self, start_month: int, town: int,
                     start_idx: int = 0, stop_idx: int = None,
                     month_span: int = 2, min_area: int = 80) -> tuple:
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        positions = self.apply_filters(start_month, town, start_idx, stop_idx, month_span, min_area)
        if not positions:
            self._add_result(start_month, town, Metrics.STDDEV, "No result")
            return 0, 0, 0

        # Welford's algorithm to compute standard deviation without storing all values
        count = 0
        average = 0
        ssd = 0
        for pos in positions:
            count += 1
            price = self.store.get_resale_price(pos)
            diff = price - average
            average += diff / count
            updated_diff = price - average
            ssd += diff * updated_diff

        stddev = math.sqrt(ssd / count)
        self._add_result(start_month, town, Metrics.STDDEV, stddev)
        return count, average, ssd

    def minimum_price_per_sqm(self, start_month: int, town: int,
                              start_idx: int = 0, stop_idx: int = None,
                              month_span: int = 2, min_area: int = 80) -> float:
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        positions = self.apply_filters(start_month, town, start_idx, stop_idx, month_span, min_area)
        if not positions:
            self._add_result(start_month, town, Metrics.MIN_PRICE_PER_SQM, "No result")
            return math.inf

        min_ppsqm = math.inf
        for pos in positions:
            price = self.store.get_resale_price(pos)
            sqm = self.store.get_floor_area_sqm(pos)
            ppsqm = price / sqm
            if ppsqm < min_ppsqm:
                min_ppsqm = ppsqm

        self._add_result(start_month, town, Metrics.MIN_PRICE_PER_SQM, min_ppsqm)
        return min_ppsqm

    def shared_scan(self, start_month: int, town: int,
                    month_span: int = 2, min_area: int = 80) -> None:
        """Single pass computing all statistics to minimize block reads."""
        self.store.clear_read_state()
        positions = self.apply_filters(start_month, town, 0, self.store.get_size(), month_span, min_area)

        if not positions:
            for m in (Metrics.MIN_PRICE, Metrics.STDDEV, Metrics.AVG_PRICE, Metrics.MIN_PRICE_PER_SQM):
                self._add_result(start_month, town, m, "No result")
            return

        min_price = math.inf
        min_ppsqm = math.inf
        total_price = 0
        count = 0
        average = 0
        ssd = 0

        for pos in positions:
            count += 1
            price = self.store.get_resale_price(pos)
            total_price += price
            if price < min_price:
                min_price = price
            sqm = self.store.get_floor_area_sqm(pos)
            ppsqm = price / sqm
            if ppsqm < min_ppsqm:
                min_ppsqm = ppsqm

            # Welford's running standard deviation
            diff = price - average
            average += diff / count
            updated_diff = price - average
            ssd += diff * updated_diff

        stddev = math.sqrt(ssd / count)
        avg_price = total_price / count

        self._add_result(start_month, town, Metrics.MIN_PRICE, min_price)
        self._add_result(start_month, town, Metrics.AVG_PRICE, avg_price)
        self._add_result(start_month, town, Metrics.STDDEV, stddev)
        self._add_result(start_month, town, Metrics.MIN_PRICE_PER_SQM, min_ppsqm)

    def vector_a_time(self, start_month: int, town: int,
                      month_span: int = 2, min_area: int = 80,
                      vector_size: int = 32) -> None:
        """Processes data in chunks for memory-efficient aggregation."""
        self.store.clear_read_state()
        store_size = self.store.get_size()

        min_price = math.inf
        price_sum, price_count = 0, 0
        min_ppsqm = math.inf
        sd_count, sd_avg, sd_ssd = 0, 0, 0

        for vec_start in range(0, store_size, vector_size):
            vec_stop = min(vec_start + vector_size, store_size)

            batch_min = self.minimum_price(start_month, town, vec_start, vec_stop, month_span, min_area)
            batch_sum, batch_cnt = self.average_price(start_month, town, vec_start, vec_stop, month_span, min_area)
            batch_sd_cnt, batch_sd_avg, batch_sd_ssd = self.stddev_price(
                start_month, town, vec_start, vec_stop, month_span, min_area
            )
            batch_min_ppsqm = self.minimum_price_per_sqm(
                start_month, town, vec_start, vec_stop, month_span, min_area
            )

            min_price = min(min_price, batch_min)
            price_sum += batch_sum
            price_count += batch_cnt

            if batch_sd_cnt > 0:
                delta = batch_sd_avg - sd_avg
                new_count = sd_count + batch_sd_cnt
                sd_avg += delta * batch_sd_cnt / new_count
                sd_ssd += batch_sd_ssd + delta * delta * sd_count * batch_sd_cnt / new_count
                sd_count = new_count

            min_ppsqm = min(min_ppsqm, batch_min_ppsqm)

        for metric, val, fallback in [
            (Metrics.MIN_PRICE, min_price, math.inf),
            (Metrics.AVG_PRICE, price_sum / price_count if price_count else None, None),
            (Metrics.STDDEV, math.sqrt(sd_ssd / sd_count) if sd_count else None, None),
            (Metrics.MIN_PRICE_PER_SQM, min_ppsqm, math.inf),
        ]:
            if val is None or val == fallback:
                self._add_result(start_month, town, metric, "No result")
            else:
                self._add_result(start_month, town, metric, val)

    # ── ScanResult generation ────────────────────────────────────

    def find_min_price_per_sqm_record(self, positions: list[int]) -> tuple:
        """Returns (position, price_per_sqm) of the record with minimum price/sqm, or (None, inf)."""
        best_pos = None
        best_ppsqm = math.inf
        for pos in positions:
            price = self.store.get_resale_price(pos)
            sqm = self.store.get_floor_area_sqm(pos)
            ppsqm = price / sqm
            if ppsqm < best_ppsqm:
                best_ppsqm = ppsqm
                best_pos = pos
        return best_pos, best_ppsqm

    def get_record_details(self, pos: int) -> dict:
        """Retrieves full record details for ScanResult CSV output."""
        month_val = self.store.get_month(pos)
        month_str = self.store.decode_month(month_val)
        month_name, year_suffix = month_str.split("-")
        year = f"20{year_suffix}"
        month_num = str(MONTH_NUM[month_name]).zfill(2)

        return {
            "year": year,
            "month": month_num,
            "town": self.store.decode_town(self.store.get_town(pos)),
            "block": self.store.decode_block_id(self.store.get_block_id(pos)),
            "floor_area": int(self.store.get_floor_area_sqm(pos)),
            "flat_model": self.store.decode_flat_model(self.store.get_flat_model(pos)),
            "lease_commence_date": int(self.store.get_lease_date(pos)),
        }
