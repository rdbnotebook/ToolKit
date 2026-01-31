# ToolKit

A small collection of Python utilities used for data sampling, IBKR contract lookups, Parquet validation/conversion, and repo housekeeping.

## Contents

### codexbot
- `codexbot.py` – CLI wrapper around the `codex` command with a standardized prompt.
  - Usage:
    ```bash
    python codexbot.py "text to append to the prompt"
    python codexbot.py --json '{"key":"value"}'
    python codexbot.py --json /path/to/file.json
    python codexbot.py --path /path/to/repo "text"
    ```
  - Requirements: `codex` CLI available in `PATH`.

### IBKR utilities (`ibkr_data/`)
These scripts expect IBKR TWS or IB Gateway to be running locally with API access enabled.

- `ibkr_data/get_ticker_jsons.py` – Search IBKR for contracts matching a ticker, pull details, and write a CSV into `./jsons`.
  - Notes:
    - Uses `ib_insync`.
    - Default `--securities-csv` points to an external repo path; override if needed.
  - Usage:
    ```bash
    python ibkr_data/get_ticker_jsons.py --ticker ES
    python ibkr_data/get_ticker_jsons.py --ticker ZT --port 4002 --client-id 12345
    python ibkr_data/get_ticker_jsons.py --ticker ZT --securities-csv /path/to/securities.csv
    ```

- `ibkr_data/ibkr_contract_details.py` – Simple IB API example to request contract details via `ibapi`.
  - Usage:
    ```bash
    python ibkr_data/ibkr_contract_details.py --symbol KRW --sec-type FUT --exchange CME --currency USD
    ```

- `ibkr_data/get_single_future_contract.py` – Fetch historical 1-minute bars for a single futures contract.
  - Features:
    - Supports FRD (`H25`) and IBKR (`ZTH5`) contract formats.
    - Retry logic and optional MEDIAN fallback.
    - Outputs CSV to `historic_future_data_contracts/`.
  - Usage:
    ```bash
    python ibkr_data/get_single_future_contract.py --ticker ZT --contract H25
    python ibkr_data/get_single_future_contract.py --ticker ES --contract M24 --duration "6 M" --fallback
    ```

### Parquet tools (`parquet/`)
- `parquet/parquet2csv.py` – Convert Parquet files to CSV (resets index to preserve timestamps).
  - Usage:
    ```bash
    python parquet/parquet2csv.py --input /path/to/file.parquet
    python parquet/parquet2csv.py --directory /path/to/dir --all
    ```

- `parquet/check_parquet_corruption.py` – Validate Parquet files under `sec=*/year=*/` and write a report.
  - Outputs:
    - `parquet_corruption_report.txt`
    - `parquet_corruption_check.log`
  - Usage:
    ```bash
    python parquet/check_parquet_corruption.py
    ```

### Sampling helpers (`sampler/`)
- `sampler/sampler.py` – Sample the first N rows of CSV/TXT/Parquet files.
- `sampler/sample-one-line.py` – Sample only the first row.
- `sampler/sample-headers.py` – Show only headers without any data rows.

Examples:
```bash
python sampler/sampler.py --all --directory . --sample-size 5
python sampler/sample-one-line.py --all --directory .
python sampler/sample-headers.py --all --directory .
```

### Python crawler (`PYCrawler/`)
- `PYCrawler/PYcrawler.py` – Dump the directory structure and contents of Python files to `crawl_dump.txt`.
  - Usage:
    ```bash
    python PYCrawler/PYcrawler.py
    ```

## Requirements

Python 3.9+ recommended.

Common dependencies:
- `pandas`
- `pyarrow` (for Parquet tools)
- `ib_insync` (for IBKR data fetchers)
- `ibapi` (for contract details example)
- `python-dotenv` (optional for `get_ticker_jsons.py`)

Example install:
```bash
pip install -r requirements.txt
```

Optional (pretty markdown tables):
```bash
pip install tabulate
```

## Notes
- Generated artifacts and local outputs are ignored via `.gitignore` (logs, CSVs, Parquet outputs, `jsons/`, etc.).
- No secrets or credentials are stored in this repo. If you add new scripts, avoid embedding API keys or personal data.
