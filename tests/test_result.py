import csv
import glob
import os
import subprocess

import pytest


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(PROJECT_ROOT, "ResalePricesSingapore.csv")
MAIN_PY = os.path.join(PROJECT_ROOT, "main.py")


@pytest.fixture(autouse=True)
def cleanup_scan_results():
    """Remove any ScanResult files after each test."""
    yield
    for f in glob.glob(os.path.join(PROJECT_ROOT, "ScanResult_*.csv")):
        try:
            os.remove(f)
        except FileNotFoundError:
            pass


def run_main(matric: str, analysis: bool = False):
    cmd = ["python", MAIN_PY, CSV_PATH, matric]
    if analysis:
        cmd.append("--analysis")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
    assert result.returncode == 0, f"main.py failed:\n{result.stderr}"
    return result


def read_scan_result(matric: str) -> list[dict]:
    path = os.path.join(PROJECT_ROOT, f"ScanResult_{matric}.csv")
    assert os.path.exists(path), f"{path} not found"
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


class TestMatricParsing:
    def test_invalid_matric_rejected(self):
        result = subprocess.run(
            ["python", MAIN_PY, CSV_PATH, "INVALID"],
            capture_output=True, text=True, cwd=PROJECT_ROOT,
        )
        assert result.returncode != 0

    def test_valid_matric_runs(self):
        run_main("U2220031B")


class TestScanResultFormat:
    @pytest.fixture(autouse=True)
    def run_scan(self):
        run_main("U2220031B")
        self.rows = read_scan_result("U2220031B")

    def test_csv_has_expected_columns(self):
        expected = [
            "(x, y)", "Year", "Month", "Town", "Block",
            "Floor_Area", "Flat_Model", "Lease_Commence_Date",
            "Price_Per_Square_Meter",
        ]
        assert list(self.rows[0].keys()) == expected

    def test_has_results(self):
        assert len(self.rows) > 0

    def test_xy_format(self):
        for row in self.rows:
            xy = row["(x, y)"]
            assert xy.startswith("(") and xy.endswith(")")

    def test_price_per_sqm_within_threshold(self):
        for row in self.rows:
            ppsqm = int(row["Price_Per_Square_Meter"])
            assert ppsqm <= 4725

    def test_x_range(self):
        for row in self.rows:
            xy = row["(x, y)"].strip("()")
            x = int(xy.split(",")[0].strip())
            assert 1 <= x <= 8

    def test_y_range(self):
        for row in self.rows:
            xy = row["(x, y)"].strip("()")
            y = int(xy.split(",")[1].strip())
            assert 80 <= y <= 150


class TestAnalysisMode:
    def test_analysis_flag_produces_output(self):
        result = run_main("U2220031B", analysis=True)
        assert "COMPRESSED STORE" in result.stdout
        assert "SHARED SCANS" in result.stdout
