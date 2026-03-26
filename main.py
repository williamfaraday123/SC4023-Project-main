import sys
import os
import csv

from constants import assignment_towns, town, flat_type, storey_range, flat_model
from store import ColumnStore
from query import QueryHelper
from Mapping.default_mappings import *
from Mapping.enum_mappings import *
from Mapping.special_mappings import *

def perform_analysis(analysis_store: ColumnStore, sorted_analysis: bool = True):
    """Perform full analysis on a given store"""
    analysis_store.flush_write_buffers()
    print("\n---------COMPRESSED STORE---------")
    analysis_store.print_storage_stats()

    print(
        f"\n\nRunning queries for {TOWN_NAME} from months {int(MATRIC[-3])} to {int(MATRIC[-3])+1} in {YEAR}"
    )

    # Run different filter permutations to analyze block read impact
    if sorted_analysis:
        print("\n---------FILTER PERMUTATIONS (ZM OFF; IDX OFF)---------")
        analysis_query = QueryHelper(store=analysis_store)
        analysis_query.test_filter_permutations(MONTH, TOWN, False, False)

        print("\n---------FILTER PERMUTATIONS (ZM ON; IDX OFF)---------")
        analysis_query = QueryHelper(store=analysis_store)
        analysis_query.test_filter_permutations(MONTH, TOWN, True, False)

    print("\n---------FILTER PERMUTATIONS (ZM OFF; IDX ON)---------")
    analysis_query = QueryHelper(store=analysis_store)
    analysis_query.test_filter_permutations(MONTH, TOWN, False, True)

    print("\n---------FILTER PERMUTATIONS (ZM ON; IDX ON)---------")
    analysis_query = QueryHelper(store=analysis_store)
    analysis_query.test_filter_permutations(MONTH, TOWN, True, True)

    # Run and analyze block read for individual scans
    if sorted_analysis:
        reads = 0
        print("\n---------INDIVIDUAL SCANS---------")
        analysis_query.minimum_price(MONTH, TOWN)
        reads += analysis_store.reads
        print(f"{analysis_store.reads} block reads for min price")

        analysis_query.average_price(MONTH, TOWN)
        reads += analysis_store.reads
        print(f"{analysis_store.reads} block reads for avg price")

        analysis_query.stddev_price(MONTH, TOWN)
        reads += analysis_store.reads
        print(f"{analysis_store.reads} block reads for stddev price")

        analysis_query.minimum_price_per_sqm(MONTH, TOWN)
        reads += analysis_store.reads
        print(f"{analysis_store.reads} block reads for min price/sqm")
        print(f"{reads} total block reads")

        analysis_results = analysis_query.get_results()
        with open(f"ScanResult_{MATRIC}.csv", "w") as g:
            g.write(analysis_results)
        print(analysis_results)

        analysis_query.clear_results()

    # Run and analyze block read for shared scans
    print("\n---------SHARED SCANS---------")
    analysis_query.shared_scan(MONTH, TOWN)
    print(f"{analysis_store.reads} block reads")
    print(analysis_query.get_results())

    analysis_query.clear_results()

    # Run and analyze block read for vector-at-a-time
    if sorted_analysis:
        print("\n---------VECTOR AT A TIME---------")
        analysis_query.vector_a_time(MONTH, TOWN)
        print(f"{analysis_store.reads} block reads")
        print(analysis_query.get_results())

    analysis_store.clear_disk()

# Check command-line arguments
if len(sys.argv) != 3:
    print("Usage: python3 main.py <CSV file> <Matric>")
    sys.exit(1)

DATAFILE = sys.argv[1]
if not os.path.isfile(DATAFILE):
    print(f"{DATAFILE} not found!")
    sys.exit(1)

MATRIC = sys.argv[2]
pattern = re.compile(r'[A-Z][0-9]{7}[A-Z]')
if not re.match(pattern=pattern, string=MATRIC):
    print("Invalid Matric format. Should be A1234567B")
    sys.exit(1)

# Map values
townMapper = TownMapper(town)

TOWN_NAME = assignment_towns[int(MATRIC[-4])]
TOWN = townMapper.map_value(TOWN_NAME)

monthMapper = MonthMapper()
floatMapper = FloatMapper()
shortMapper = ShortMapper()

YEAR = int(MATRIC[-2]) + 2010
if YEAR < 2014:
    YEAR += 10

month_number = 10 if f"0{MATRIC[-3]}" == "00" else int(f"0{MATRIC[-3]}")
month_str = monthMapper.month_names[month_number - 1]
MONTH = monthMapper.map_value(f"{month_str}-{str(YEAR % 100).zfill(2)}")

print("Loading data")

# Define mappings for basic and compressed stores
basic_mappings = [
    CharMapper(7),
    CharMapper(15),
    CharMapper(16),
    CharMapper(5),
    CharMapper(20),
    CharMapper(12),
    floatMapper,
    CharMapper(22),
    shortMapper,
    floatMapper,
]

compressed_mappings = [
    monthMapper,
    townMapper,
    FlatTypeMapper(flat_type),
    BlockMapper(),
    CharMapper(20),
    StoreyRangeMapper(storey_range),
    floatMapper,
    FlatModelMapper(flat_model),
    shortMapper,
    floatMapper,
]

# Define columns that must not be empty
critical = [0, 1, 6, 9]

# Experiments on column store
with open(DATAFILE, "r", newline="") as f:
    print("\n---------BASIC STORE---------")
    reader = csv.reader(f)
    columns = next(reader)
    basic_store = ColumnStore(
        columns=columns, mappings=basic_mappings, critical=critical, basic=True
    )
    for row in reader:
        try:
            basic_store.add_entry(row)
        except Exception as s:
            print(
                f"Row {row} (columns={len(row)}):",
                type(s).__name__,
                " -> ",
                str(s),
                "Skipping...",
            )
    basic_store.flush_write_buffers()
    basic_store.print_storage_stats()
    basic_store.clear_disk()

with open(DATAFILE, "r", newline="") as f:
    reader = csv.reader(f)
    columns = next(reader)
    store = ColumnStore(
        columns=columns, mappings=compressed_mappings, critical=critical
    )
    sorted_rows = []
    for row in reader:
        raw_row = row.copy()
        try:
            store.add_entry(row)
            sorted_rows.append(raw_row)
        except Exception as s:
            print(
                f"Row {row} (columns={len(row)}):",
                type(s).__name__,
                " -> ",
                str(s),
                "Skipping...",
            )

    print("\n=========WITHOUT SORTING=========")
    perform_analysis(store, False)

    store_sorted = ColumnStore(
        columns=columns, mappings=compressed_mappings, critical=critical
    )

    sorted_rows.sort(key=lambda row: (row[0], row[1], row[6]))
    for sorted_row in sorted_rows:
        try:
            store_sorted.add_entry(sorted_row)
        except Exception as s:
            print(f"Line {sorted_row}:", type(s).__name__, " -> ", str(s), "Skipping...")

    print("\n=========WITH SORTING=========")
    perform_analysis(store_sorted)

if __name__ == '__main__':
    pass
