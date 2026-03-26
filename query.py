import math
import itertools

from Enum.Metrics import Metrics
from store import ColumnStore

class QueryHelper:
    """Class for executing filtered queries on a ColumnStore and calculating statistics."""
    def __init__(self, store: ColumnStore):
        self.results: dict = {}
        self.store: ColumnStore = store
        self.clear_results()

    def get_results(self) -> str:
        """Returns a CSV-formatted string of statistic results."""
        return "Year,Month,town,Category,Value\n" + "\n".join(
            [self.results[cat] for cat in self.results]
        )

    def clear_results(self) -> None:
        """Clears all previously stored results."""
        self.results = {}

    def add_result(self, month1: int, town: int, category: Metrics, value: float|str) -> None:
        """Adds a result entry to stored results dictionary in CSV format."""
        town = self.store.unmap_town(town)
        month = self.store.unmap_month(month1)
        month_name, year_suffix = month.split("-")
        year = f"20{year_suffix}"
        month = str(
            {
                "Jan": 1,
                "Feb": 2,
                "Mar": 3,
                "Apr": 4,
                "May": 5,
                "Jun": 6,
                "Jul": 7,
                "Aug": 8,
                "Sep": 9,
                "Oct": 10,
                "Nov": 11,
                "Dec": 12,
            }[month_name]
        ).zfill(2)
        if type(value) == float:
            value = str(round(value, 2))
        self.results[category] = ",".join([year, month, town, category.value, value])

    def filter_month(self, pos_list: list, month1: int, use_index: bool = False) -> list[int]:
        """Filters records that fall within the month range [month1, month1 + 1]."""
        pos2 = []

        for pos in pos_list:
            if use_index:
                if not self.store.get_pos_has_month(pos, month1, month1 + 1):
                    continue

            month = self.store.get_month(pos)
            if month1 <= month <= month1 + 1:
                pos2.append(pos)

        return pos2

    def filter_town(self, pos_list: list[int], town: int, use_zone_map: bool = False) -> list[int]:
        """Filters records by town."""
        pos2 = []

        for pos in pos_list:
            if use_zone_map:
                zval = self.store.get_town_zmap_entry(pos)
                if not (zval & (1 << town)):
                    continue

            t = self.store.get_town(pos)
            if t == town:
                pos2.append(pos)

        return pos2

    def filter_area(self, pos_list: list[int], use_zone_map: bool = False) -> list[int]:
        """Filters records where floor area >= 80 sqm."""
        pos2 = []
        for pos in pos_list:
            if use_zone_map:
                zmin, zmax = self.store.get_area_zmap_entry(pos)
                if zmax < 80:
                    continue

            sqm = self.store.get_floor_area_sqm(pos)
            if sqm >= 80:
                pos2.append(pos)

        return pos2

    def filter(self, month1: int, town: int, start_idx: int, stop_idx: int) -> list[int]:
        """Composite filter applying month, town, and area filters."""
        pos2 = self.filter_month(range(start_idx, stop_idx), month1, True)
        pos3 = self.filter_town(pos2, town, True)
        pos4 = self.filter_area(pos3)
        return pos4

    def test_filter_permutations(self, month1: int, town: int, use_zone_map: bool, use_index: bool) -> None:
        """Evaluates the number of block reads for all permutations of month, town, and area filtering."""
        row_format = "{:>20} {:>20}"
        print(row_format.format("Permutation", "Blocks"))

        data_range = range(self.store.get_size())
        permutations = list(itertools.permutations(["month", "town", "area"]))

        for perm in permutations:
            self.store.clear_read_state()
            filtered = data_range

            filters = {
                "month": lambda data: self.filter_month(
                    data, month1, use_index=use_index
                ),
                "town": lambda data: self.filter_town(
                    data, town, use_zone_map=use_zone_map
                ),
                "area": lambda data: self.filter_area(data, use_zone_map=use_zone_map),
            }

            reads = []
            for step in perm:
                filtered = filters[step](filtered)
                reads.append(str(self.store.reads))

            print(row_format.format(str(perm), "|".join(reads)))

    def minimum_price(self, month1: int, town: int, start_idx: int = 0, stop_idx: int = None) -> float:
        """Computes and stores the minimum resale price for the given filters."""
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        pos4 = self.filter(month1, town, start_idx, stop_idx)

        if len(pos4) == 0:
            self.add_result(month1, town, Metrics.MIN_PRICE, "No result")
            return math.inf

        min_price = math.inf
        for pos in pos4:
            price = self.store.get_resale_price(pos)
            if price < min_price:
                min_price = price

        self.add_result(month1, town, Metrics.MIN_PRICE, min_price)
        return min_price

    def average_price(self, month1: int, town: int, start_idx: int = 0, stop_idx: int = None) -> (float, int):
        """Computes and stores the average resale price for the given filters."""
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        pos4 = self.filter(month1, town, start_idx, stop_idx)

        if len(pos4) == 0:
            self.add_result(month1, town, Metrics.AVG_PRICE, "No result")
            return 0, 0

        total_price = 0
        for pos in pos4:
            price = self.store.get_resale_price(pos)
            total_price += price

        self.add_result(month1, town, Metrics.AVG_PRICE, total_price / len(pos4))
        return total_price, len(pos4)

    def stddev_price(self, month1: int, town: int, start_idx: int = 0, stop_idx: int = None) -> (int, float, float):
        """Computes and stores the standard deviation of resale price using Welford’s algorithm for the given filters."""
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()
        """
        Calculation of standard deviation using single reference to pos4 instead of 2 passes of avg then stddev.
        Credits: https://jonisalonen.com/2013/deriving-welfords-method-for-computing-variance/
        """

        pos4 = self.filter(month1, town, start_idx, stop_idx)

        if len(pos4) == 0:
            self.add_result(month1, town, Metrics.STDDEV, "No result")
            return 0, 0, 0

        # Welford's Algorithm
        count = 0
        average = 0
        ssd = 0  
        for pos in pos4:
            count += 1
            price = self.store.get_resale_price(pos)
            diff = price - average
            average += diff/count
            updated_diff = price - average
            ssd += diff * updated_diff

        stddev = math.sqrt(ssd / count)
        self.add_result(month1, town, Metrics.STDDEV, stddev)
        return count, average, ssd

    def minimum_price_per_sqm(self, month1: int, town: int, start_idx: int = 0, stop_idx: int = None) -> float:
        """Computes and stores the minimum price per square meter for the given filters."""
        if stop_idx is None:
            self.store.clear_read_state()
            stop_idx = self.store.get_size()

        pos4 = self.filter(month1, town, start_idx, stop_idx)

        if len(pos4) == 0:
            self.add_result(month1, town, Metrics.MIN_PRICE_PER_SQM, "No result")
            return math.inf

        min_price_per_sqm = math.inf
        for pos in pos4:
            price = self.store.get_resale_price(pos)
            sqm = self.store.get_floor_area_sqm(pos)
            price_per_sqm = price / sqm
            if price_per_sqm < min_price_per_sqm:
                min_price_per_sqm = price_per_sqm

        self.add_result(
            month1, town, Metrics.MIN_PRICE_PER_SQM, min_price_per_sqm
        )
        return min_price_per_sqm

    def shared_scan(self, month1: int, town: int) -> None:
        """Performs a single pass to calculate all statistics together to reduce reads."""
        self.store.clear_read_state()

        pos4 = self.filter(month1, town, 0, self.store.get_size())

        if len(pos4) == 0:
            self.add_result(month1, town, Metrics.MIN_PRICE, "No result")
            self.add_result(month1, town, Metrics.STDDEV, "No result")
            self.add_result(month1, town, Metrics.AVG_PRICE, "No result")
            self.add_result(month1, town, Metrics.MIN_PRICE_PER_SQM, "No result")
            return

        min_price = math.inf
        min_price_per_sqm = math.inf
        total_price = 0

        count = 0
        average = 0
        ssd = 0  
        for pos in pos4:
            count += 1
            price = self.store.get_resale_price(pos)
            total_price += price
            if price < min_price:
                min_price = price
            sqm = self.store.get_floor_area_sqm(pos)
            price_per_sqm = price / sqm
            if price_per_sqm < min_price_per_sqm:
                min_price_per_sqm = price_per_sqm

            # Welford's Algorithm
            diff = price - average
            average += diff/count
            updated_diff = price - average
            ssd += diff * updated_diff

        stddev = math.sqrt(ssd / count)
        average_price = total_price / count

        self.add_result(month1, town, Metrics.MIN_PRICE, min_price)
        self.add_result(month1, town, Metrics.AVG_PRICE, average_price)
        self.add_result(month1, town, Metrics.STDDEV, stddev)
        self.add_result(
            month1, town, Metrics.MIN_PRICE_PER_SQM, min_price_per_sqm
        )

    def vector_a_time(self, month1: int, town: int, vector_size: int = 32) -> None:
        """
        Optimized vector-based scan, processing data in chunks of `vector_size`
        to compute aggregated statistics in a memory-efficient manner.
        """
        self.store.clear_read_state()
        store_size = self.store.get_size()

        min_price = math.inf
        price_sum, price_count = 0, 0
        min_price_per_sqm = math.inf

        stddev_count, stddev_average, stddev_ssd = 0, 0, 0

        for vector_start in range(0, store_size, vector_size):
            vector_stop = store_size
            if vector_start + vector_size <= store_size:
                vector_stop = vector_start + vector_size

            batch_min_price = self.minimum_price(
                month1, town, vector_start, vector_stop
            )
            batch_price_sum, batch_price_count = self.average_price(
                month1, town, vector_start, vector_stop
            )
            batch_stddev_count, batch_stddev_average, batch_stddev_ssd = (
                self.stddev_price(month1, town, vector_start, vector_stop)
            )
            batch_min_price_per_sqm = self.minimum_price_per_sqm(
                month1, town, vector_start, vector_stop
            )

            min_price = min(min_price, batch_min_price)

            price_sum += batch_price_sum
            price_count += batch_price_count

            if batch_stddev_count > 0:
                delta = batch_stddev_average - stddev_average
                new_count = stddev_count + batch_stddev_count
                stddev_average += delta * batch_stddev_count / new_count
                stddev_ssd += (
                    batch_stddev_ssd
                    + delta * delta * stddev_count * batch_stddev_count / new_count
                )
                stddev_count = new_count

            min_price_per_sqm = min(min_price_per_sqm, batch_min_price_per_sqm)

        # min
        if min_price == math.inf:
            self.add_result(month1, town, Metrics.MIN_PRICE, "No result")
        else:
            self.add_result(month1, town, Metrics.MIN_PRICE, min_price)

        # avg
        if price_count == 0:
            self.add_result(month1, town, Metrics.AVG_PRICE, "No result")
        else:
            self.add_result(month1, town, Metrics.AVG_PRICE, price_sum / price_count)

        # stddev
        if stddev_count == 0:
            self.add_result(month1, town, Metrics.STDDEV, "No result")
        else:
            variance = stddev_ssd / stddev_count
            stddev = math.sqrt(variance)
            self.add_result(month1, town, Metrics.STDDEV, stddev)

        # min per sqm
        if min_price_per_sqm == math.inf:
            self.add_result(month1, town, Metrics.MIN_PRICE_PER_SQM, "No result")
        else:
            self.add_result(
                month1, town, Metrics.MIN_PRICE_PER_SQM, min_price_per_sqm
            )
