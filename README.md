# SC4023 Project

Column-oriented storage and query engine for Singapore HDB resale flat data.

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

The `--analysis` flag runs additional demos after generating the ScanResult CSV. It compares block read counts across filter permutations (with and without zone maps), shared scans, and vector-at-a-time processing.

## Filter order

Queries filter in a fixed order: month, then town, then area. Month is filtered first because it has a block-level index that can skip non-matching blocks without reading from disk. Town is filtered next using a per-block bitmask zone map. Area is filtered last as it requires reading actual values. This puts the cheapest filters first to reduce the number of records before hitting disk. Data is also sorted by (month, town, area) at load time to maximize the effectiveness of these acceleration structures.

## Output

- `ScanResult_<MatricNum>.csv` — All valid (x, y) pairs with minimum price per square meter <= 4725.

## Tests

```bash
pytest -v tests/
```

## Project Structure

```
main.py                         Entry point
columnstore/
  storage.py                    DiskColumnStore (disk-based column store)
  engine.py                     ScanEngine (filtering, statistics, scan result generation)
  catalog.py                    Town/flat type/model constants
  errors.py                     Custom exceptions
  metrics.py                    Metrics enum
  encoding/
    base.py                     FieldEncoder base class
    primitives.py               Float32, FixedString, UInt16 encoders
    categorical.py              Categorical encoders (town, flat type, etc.)
    temporal.py                 Month and block ID encoders
tests/
  test_result.py                Integration tests
ResalePricesSingapore.csv       Input data
docs/                           Project description
```
