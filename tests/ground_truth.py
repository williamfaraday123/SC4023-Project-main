import csv
import math
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import parse_matric
from columnstore.encoding.temporal import MonthEncoder


def compute_ground_truth(csv_path: str, matric: str) -> list[dict]:
    """
    Reference implementation for verifying ScanResult output.
    Runs the same query directly on the raw CSV using plain Python, with no column store.
    Returns a list of dicts matching the ScanResult CSV format.
    """
    _, start_month_enc, town_names = parse_matric(matric)
    town_set = set(town_names)
    month_enc = MonthEncoder()

    # Load raw CSV and pre-encode month values for fast comparison
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        raw_rows = []
        for row in reader:
            try:
                enc = month_enc.encode(row["month"])
                raw_rows.append((enc, row))
            except Exception:
                continue

    results = []
    for x in range(1, 9):
        end_month_enc = start_month_enc + x - 1
        for y in range(80, 151):
            best_row = None
            best_ppsqm = math.inf

            for row_enc, row in raw_rows:
                if not (start_month_enc <= row_enc <= end_month_enc):
                    continue
                if row["town"] not in town_set:
                    continue
                try:
                    floor_area = float(row["floor_area_sqm"])
                    resale_price = float(row["resale_price"])
                except (ValueError, KeyError):
                    continue
                if floor_area < y:
                    continue
                ppsqm = resale_price / floor_area
                if ppsqm < best_ppsqm:
                    best_ppsqm = ppsqm
                    best_row = row

            if best_row is not None and best_ppsqm <= 4725:
                decoded = month_enc.decode(month_enc.encode(best_row["month"]))
                month_name, year_suffix = decoded.split("-")
                year = f"20{year_suffix}"
                month_num = str(MonthEncoder.MONTH_MAP[month_name]).zfill(2)
                results.append({
                    "(x, y)": f"({x}, {y})",
                    "Year": year,
                    "Month": month_num,
                    "Town": best_row["town"],
                    "Block": best_row["block"],
                    "Floor_Area": str(int(float(best_row["floor_area_sqm"]))),
                    "Flat_Model": best_row["flat_model"],
                    "Lease_Commence_Date": best_row["lease_commence_date"],
                    "Price_Per_Square_Meter": str(round(best_ppsqm)),
                })

    return results
