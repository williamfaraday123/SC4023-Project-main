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

The `--analysis` flag runs additional demos after generating the ScanResult CSV. It compares block read counts across filter permutations (with and without zone maps), shared scans, and vector-at-a-time processing.

## Query execution

Each column is stored in 4KB blocks on disk. Filters run in a fixed order chosen to skip as many blocks as possible before touching disk:

1. **Month** — a per-block min/max index records which month range each block covers. Blocks outside the target range are skipped entirely without a disk read.
2. **Town** — a per-block bitmask (one bit per town) records which towns appear in each block. Blocks with no overlap with the target towns are skipped by checking the bitmask only.
3. **Area** — requires reading actual floor area values from disk. Applied last, after the two cheaper filters have reduced the number of blocks to scan.

Data is sorted by (month, town, area) at load time to cluster related records into the same blocks, maximising the effectiveness of the above skips.

The ground truth implementation in `tests/ground_truth.py` runs the same query as a plain Python loop over the raw CSV — no index, no zone maps, no block structure, every row examined. Both approaches must produce identical results.

## Output

- `ScanResult_<MatricNum>.csv` — All valid (x, y) pairs with minimum price per square meter <= 4725.

## Tests

```bash
# Run all tests
pytest -v tests/

# Run ground truth comparison with printed output
pytest -v -s tests/test_result.py::TestGroundTruth
```

`TestGroundTruth` compares the column store output row-by-row against the plain Python reference for the same matric number and prints a sample of matched rows.

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
    categorical.py              Categorical encoders (town, flat type, street name, etc.)
    temporal.py                 Month and block ID encoders
tests/
  test_result.py                Integration tests
  ground_truth.py               Plain Python reference implementation for result verification
ResalePricesSingapore.csv       Input data
docs/                           Project description
```
