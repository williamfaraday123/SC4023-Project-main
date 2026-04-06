# SC4023 Project

## Setup

```bash
conda create -n sc4023 python=3.12 -y
conda activate sc4023
pip install -r tests/requirements.txt
```

## Usage

```bash
# Generate ScanResult CSV
python main.py ResalePricesSingapore.csv $MATRIC_NUMBER

# With analysis demo
python main.py ResalePricesSingapore.csv $MATRIC_NUMBER --analysis
```

The `--analysis` flag runs additional demos: filter permutation comparison, shared scans, and vector-at-a-time processing.

## Query execution

Each column is stored in 4KB pages on disk. Filters run in order to skip as many pages as possible:

1. **Month** — page-level index maps each page to its month range. Pages outside the target range are skipped.
2. **Town** — page-level bitmask (one bit per town). Pages with no matching towns are skipped.
3. **Area** — reads floor area values from disk. Applied last after the cheaper filters reduce the scan set.

Data is sorted by (month, town, area) at load time to cluster related records into the same pages.

## Output

- `ScanResult_<MatricNum>.csv` — All valid (x, y) pairs with minimum price per square meter <= 4725.

## Tests

```bash
# Run all tests
pytest -v tests/

# Run tests for a specific matric number
TEST_MATRIC=U2220031B pytest -v tests/

# Run ground truth comparison with printed output
pytest -v -s tests/test_result.py::TestGroundTruth
```

`TestGroundTruth` compares column store output row-by-row against an independent plain Python reference. Defaults to `U2220031B`, override with `TEST_MATRIC`.

## Project Structure

```
main.py                         Entry point
columnstore/
  storage.py                    Disk storage layer
  engine.py                     ScanEngine (filtering, statistics, scan result generation)
  catalog.py                    Town/flat type/model constants
  errors.py                     Custom exceptions
  metrics.py                    Metrics enum
  encoding/
    base.py                     FieldEncoder base class
    identifiers.py              HDB block ID encoder
    primitives.py               Float32, FixedString, UInt16 encoders
    categorical.py              Categorical encoders (town, flat type, street name, etc.)
    temporal.py                 Month encoder
tests/
  test_result.py                Integration tests
  ground_truth.py               Plain Python reference implementation for result verification
ResalePricesSingapore.csv       Input data
docs/                           Project description
```
