import argparse
import csv
import math
import re

from columnstore.storage import DiskColumnStore
from columnstore.engine import ScanEngine
from columnstore.catalog import (
    MATRIC_DIGIT_TO_TOWN, ALL_TOWNS, ALL_FLAT_TYPES,
    ALL_STOREY_RANGES, ALL_FLAT_MODELS,
)
from columnstore.encoding import (
    MonthEncoder, TownEncoder, FlatTypeEncoder, BlockIdEncoder,
    FixedStringEncoder, StoreyRangeEncoder, Float32Encoder,
    FlatModelEncoder, UInt16Encoder, StreetNameEncoder,
)


def parse_args():
    parser = argparse.ArgumentParser(description="SC4023 Column Store Project")
    parser.add_argument("csv_path", help="Path to ResalePricesSingapore.csv")
    parser.add_argument("matric", help="Matriculation number (e.g., U2220031B)")
    parser.add_argument("--analysis", action="store_true",
                        help="Run demo analysis with block read counts")
    return parser.parse_args()


def parse_matric(matric: str):
    """
    Extracts query parameters from a matriculation number.
    Returns (year, start_month_encoded, list_of_town_names).
    """
    pattern = re.compile(r"[A-Z][0-9]{7}[A-Z]")
    if not re.match(pattern, matric):
        raise ValueError("Invalid Matric format. Should be A1234567B")

    # Target year: last digit + 2010, wrap to 2020s if before 2014
    year = int(matric[-2]) + 2010
    if year < 2014:
        year += 10

    # Starting month: second-last digit maps 1-9 directly, 0 means October
    month_digit = int(matric[-3])
    month_number = 10 if month_digit == 0 else month_digit

    # Encode as (year * 12 + month - 1) for the column store month format
    month_enc = MonthEncoder()
    month_str = month_enc.MONTH_NAMES[month_number - 1]
    start_month = month_enc.encode(f"{month_str}-{str(year % 100).zfill(2)}")

    # Towns from ALL digits in the matric number (deduplicated, order preserved)
    digits = [int(ch) for ch in matric if ch.isdigit()]
    seen = set()
    town_names = []
    for d in digits:
        name = MATRIC_DIGIT_TO_TOWN[d]
        if name not in seen:
            seen.add(name)
            town_names.append(name)

    return year, start_month, town_names


def load_csv(csv_path: str) -> tuple[list[str], list[list[str]]]:
    """Reads the CSV and returns (column_names, rows)."""
    with open(csv_path, "r", newline="") as f:
        reader = csv.reader(f)
        columns = next(reader)
        rows = list(reader)
    return columns, rows


def build_store(columns: list[str], rows: list[list[str]], encoders: list, critical: list,
                sort: bool = False, basic: bool = False):
    """Loads rows into a DiskColumnStore. Optionally sorts by (month, town, area)."""
    # Sort by (month, town, floor_area) to improve zone map effectiveness
    if sort:
        rows = sorted(rows, key=lambda row: (row[0], row[1], row[6]))

    store = DiskColumnStore(columns=columns, encoders=encoders, critical=critical, basic=basic)
    for row in rows:
        try:
            store.add_entry(row)
        except Exception as e:
            print(f"Row {row}: {type(e).__name__} -> {e}. Skipping...")

    store.flush_write_buffers()
    return store


def run_analysis(store: DiskColumnStore, start_month: int, town_encoded: int, matric: str):
    """Runs the demo analysis: basic store stats, filter permutations, shared scans, etc."""
    print("\n---------COMPRESSED STORE---------")
    store.print_storage_stats()

    month_enc = store.encoders[0]
    month_str = month_enc.decode(start_month)
    month_name, year_suffix = month_str.split("-")
    town_name = store.decode_town(town_encoded)
    print(f"\n\nRunning queries for {town_name} from month {month_name} in 20{year_suffix}")

    print("\n---------FILTER PERMUTATIONS (ZM OFF; IDX ON)---------")
    engine = ScanEngine(store=store)
    engine.test_filter_permutations(start_month, town_encoded, False, True)

    print("\n---------FILTER PERMUTATIONS (ZM ON; IDX ON)---------")
    engine = ScanEngine(store=store)
    engine.test_filter_permutations(start_month, town_encoded, True, True)

    print("\n---------SHARED SCANS---------")
    engine.shared_scan(start_month, town_encoded)
    print(f"{store.reads} block reads")
    print(engine.get_results())

    engine.clear_results()

    print("\n---------VECTOR AT A TIME---------")
    engine.vector_a_time(start_month, town_encoded)
    print(f"{store.reads} block reads")
    print(engine.get_results())


def generate_scan_result(store: DiskColumnStore, matric: str,
                         start_month: int, town_names: list[str]):
    """
    Generates ScanResult_<Matric>.csv by iterating all (x, y) pairs.
    x = month span [1..8], y = min area [80..150].
    For each valid (x, y), finds the record with minimum price/sqm <= 4725.
    """
    town_enc = store.encoders[1]
    towns_encoded = [town_enc.encode(t) for t in town_names]
    engine = ScanEngine(store=store)

    # x = month span (how many months to look back)
    # y = minimum floor area in sqm
    rows = []
    for x in range(1, 9):
        for y in range(80, 151):
            best_pos = None
            best_ppsqm = math.inf

            for town_idx in towns_encoded:
                store.clear_read_state()
                positions = engine.apply_filters(
                    start_month, town_idx, 0, store.get_size(),
                    month_span=x, min_area=y,
                )
                if not positions:
                    continue

                pos, ppsqm = engine.find_min_price_per_sqm_record(positions)
                if ppsqm < best_ppsqm:
                    best_ppsqm = ppsqm
                    best_pos = pos

            if best_pos is not None and best_ppsqm <= 4725:
                rec = engine.get_record_details(best_pos)
                rows.append([
                    f"({x}, {y})",
                    rec["year"],
                    rec["month"],
                    rec["town"],
                    rec["block"],
                    rec["floor_area"],
                    rec["flat_model"],
                    rec["lease_commence_date"],
                    round(best_ppsqm),
                ])

    output_path = f"ScanResult_{matric}.csv"
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "(x, y)", "Year", "Month", "Town", "Block",
            "Floor_Area", "Flat_Model", "Lease_Commence_Date",
            "Price_Per_Square_Meter",
        ])
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow(["No result", "", "", "", "", "", "", "", ""])

    print(f"\nScanResult written to {output_path} ({len(rows)} records)")


if __name__ == "__main__":
    args = parse_args()

    # Validate matric
    year, start_month, town_names = parse_matric(args.matric)
    print(f"Matric: {args.matric}")
    print(f"Year: {year}, Towns: {town_names}")
    print("Loading data...")

    columns, rows = load_csv(args.csv_path)

    # Build street name dictionary from the data (column index 4)
    street_names = sorted(set(row[4] for row in rows))

    # Encoders for the compressed column store
    month_enc = MonthEncoder()
    float_enc = Float32Encoder()
    short_enc = UInt16Encoder()

    compressed_encoders = [
        month_enc,                              # 0: month
        TownEncoder(ALL_TOWNS),                 # 1: town
        FlatTypeEncoder(ALL_FLAT_TYPES),         # 2: flat_type
        BlockIdEncoder(),                       # 3: block
        StreetNameEncoder(street_names),        # 4: street_name (dictionary encoded)
        StoreyRangeEncoder(ALL_STOREY_RANGES),  # 5: storey_range
        float_enc,                              # 6: floor_area_sqm
        FlatModelEncoder(ALL_FLAT_MODELS),       # 7: flat_model
        short_enc,                              # 8: lease_commence_date
        float_enc,                              # 9: resale_price
    ]

    # Columns where empty values are not allowed
    critical = [0, 1, 6, 9]

    # Build basic store for stats comparison
    print("\n---------BASIC STORE---------")
    basic_encoders = [
        FixedStringEncoder(7),   # month
        FixedStringEncoder(15),  # town
        FixedStringEncoder(16),  # flat_type
        FixedStringEncoder(5),   # block
        FixedStringEncoder(20),  # street_name
        FixedStringEncoder(12),  # storey_range
        float_enc,               # floor_area_sqm
        FixedStringEncoder(22),  # flat_model
        short_enc,               # lease_commence_date
        float_enc,               # resale_price
    ]
    basic_store = build_store(columns, rows, basic_encoders, critical, basic=True)
    basic_store.print_storage_stats()
    basic_store.clear_disk()

    # Build compressed sorted store
    store = build_store(columns, rows, compressed_encoders, critical, sort=True)

    # Generate the required ScanResult CSV
    generate_scan_result(store, args.matric, start_month, town_names)

    # Optional analysis demo
    if args.analysis:
        town_enc = store.encoders[1]
        first_town_encoded = town_enc.encode(town_names[0])
        run_analysis(store, start_month, first_town_encoded, args.matric)

    store.clear_disk()
