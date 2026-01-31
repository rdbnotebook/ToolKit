# ToolKit

A collection of Python utilities used for data sampling, IBKR contract lookups, Parquet validation/conversion, and repo housekeeping.

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

- `ibkr_data/get_ibkr_hist_futures_contracts_1min.py` – Back-fill or update 1-minute data for individual futures contracts (front month and expired).
  - Notes:
    - Reads a `securities_daily_update.csv` input (path via `--input-file`).
    - Writes to `data/bronze/ibkr/futures_contracts/` (or `_bidask/` when `--bid-ask` is used).
  - Usage:
    ```bash
    python ibkr_data/get_ibkr_hist_futures_contracts_1min.py --back-fill --input-file /path/to/securities_daily_update.csv
    python ibkr_data/get_ibkr_hist_futures_contracts_1min.py --update --ticker ES
    ```

- `ibkr_data/get_ibkr_historic_1min.py` – Back-fill or update 1-minute data for non-futures securities (stocks/forex/index/crypto).
  - Notes:
    - Reads a `securities_daily_update.csv` input (path via `--input-file`).
    - Writes to `data/bronze/ibkr/historic_data/` (or `_bidask/` when `--bid-ask` is used).
  - Usage:
    ```bash
    python ibkr_data/get_ibkr_historic_1min.py --back-fill --input-file /path/to/securities_daily_update.csv
    python ibkr_data/get_ibkr_historic_1min.py --update --conid 123
    ```

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

- `ibkr_data/get_ibkr_options.py` – Fetch historical options data at multiple bar sizes; can aggregate to EOD.
  - Notes:
    - Requires a YAML config file (default `config/options_intraday.yaml`) and optional `option_styles.yaml`.
    - Sample configs:
      - `config/options_intraday.sample.yaml` (copy to `config/options_intraday.yaml`)
      - `ibkr_data/option_styles.sample.yaml` (copy to `ibkr_data/option_styles.yaml`)
    - By default writes under `data/bronze/ibkr/options/`.
    - Can skip consolidation with `--skip-consolidation` if `ibkr_continuous_builder.py` is not present.
  - Usage:
    ```bash
    python ibkr_data/get_ibkr_options.py --back-fill --config /path/to/options_intraday.yaml
    python ibkr_data/get_ibkr_options.py --update --symbol SPY --skip-consolidation
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

- `parquet/inspect_parquet.py` – Inspect specific columns or the timestamp index in a Parquet file.
  - Usage:
    ```bash
    python parquet/inspect_parquet.py /path/to/file.parquet "timestamp,open,close"
    ```

- `parquet/sample_parquet.py` – Sample the first N rows from Parquet files and save to CSV.
  - Usage:
    ```bash
    python parquet/sample_parquet.py --input /path/to/file.parquet --sample-size 200
    python parquet/sample_parquet.py --directory /path/to/dir --all --sample-size 200
    ```

- `parquet/sample_5k_parquet.py` – Sample the first 5,000 rows from Parquet files and save to CSV.
  - Usage:
    ```bash
    python parquet/sample_5k_parquet.py --input /path/to/file.parquet
    python parquet/sample_5k_parquet.py --directory /path/to/dir --all
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
- `numpy` (used by `inspect_parquet.py`)
- `pandas`
- `pyarrow` (for Parquet tools)
- `ib_insync` (for IBKR data fetchers)
- `ibapi` (for contract details example)
- `python-dotenv` (optional for `get_ticker_jsons.py`)
- `PyYAML` (for `get_ibkr_options.py`)

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
