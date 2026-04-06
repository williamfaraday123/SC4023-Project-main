import csv
import math
import os
import re

from columnstore.catalog import MATRIC_DIGIT_TO_TOWN
from columnstore.encoding.temporal import MonthEncoder


def parse_matric_reference(matric: str) -> tuple[int, int, list[str]]:
    """Independent implementation of the project brief's matric parsing rules."""
    if not re.fullmatch(r"[A-Z][0-9]{7}[A-Z]", matric):
        raise ValueError("Invalid Matric format. Should be A1234567B")

    year_digit = int(matric[-2])
    year = 2020 + year_digit if year_digit <= 4 else 2010 + year_digit

    month_digit = int(matric[-3])
    month_number = 10 if month_digit == 0 else month_digit

    month_enc = MonthEncoder()
    month_str = month_enc.MONTH_NAMES[month_number - 1]
    start_month = month_enc.encode(f"{month_str}-{str(year % 100).zfill(2)}")

    seen = set()
    town_names = []
    for ch in matric:
        if ch.isdigit():
            name = MATRIC_DIGIT_TO_TOWN[int(ch)]
            if name not in seen:
                seen.add(name)
                town_names.append(name)

    return year, start_month, town_names


def compute_ground_truth(csv_path: str, matric: str) -> list[dict]:
    """
    Reference implementation for verifying ScanResult output.
    Runs the same query directly on the raw CSV using plain Python, with no column store.
    Returns a list of dicts matching the ScanResult CSV format.
    """
    _, start_month_enc, town_names = parse_matric_reference(matric)
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
