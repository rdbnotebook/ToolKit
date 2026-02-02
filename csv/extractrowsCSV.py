#!/usr/bin/env python3

import argparse
import csv
import glob
import os
from pathlib import Path

DEFAULT_ROWS = 40_000
DEFAULT_PREFIX = "Small"


def _build_output_path(input_file: str, prefix: str) -> tuple[str, str]:
    directory, filename = os.path.split(input_file)
    new_filename = f"{prefix}{filename}"
    output_file = os.path.join(directory, new_filename)
    return output_file, new_filename


def _extract_rows_stdlib(input_file: str, num_rows: int, prefix: str) -> bool:
    output_file, new_filename = _build_output_path(input_file, prefix)
    filename = os.path.basename(input_file)

    try:
        with open(input_file, "r", newline="") as infile, open(output_file, "w", newline="") as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            header = next(reader)
            writer.writerow(header)

            extracted_rows = 0
            for row in reader:
                if extracted_rows >= num_rows:
                    break
                writer.writerow(row)
                extracted_rows += 1

        print(f"Successfully extracted {extracted_rows} row(s) from '{filename}'")
        print(f"Output saved to '{new_filename}'")
        return True
    except Exception as exc:
        print(f"Error processing '{filename}': {exc}")
        return False


def _extract_rows_pandas(input_file: str, num_rows: int, prefix: str) -> bool:
    output_file, new_filename = _build_output_path(input_file, prefix)
    filename = os.path.basename(input_file)

    try:
        import pandas as pd
    except Exception as exc:
        print(f"Pandas is not available ({exc}); falling back to stdlib CSV reader.")
        return _extract_rows_stdlib(input_file, num_rows=num_rows, prefix=prefix)

    try:
        print(f"Reading the first {num_rows} row(s) from '{filename}'...")
        df = pd.read_csv(input_file, nrows=num_rows, dtype=str, keep_default_na=False)
        print(f"Writing {len(df)} row(s) to '{new_filename}'...")
        df.to_csv(output_file, index=False)
        print(f"Successfully extracted {len(df)} row(s) from '{filename}'")
        print(f"Output saved to '{new_filename}'")
        return True
    except Exception as exc:
        print(f"Error processing '{filename}': {exc}")
        return False


def extract_rows(input_file: str, num_rows: int = DEFAULT_ROWS, prefix: str = DEFAULT_PREFIX, method: str = "auto") -> bool:
    """
    Extract the first N data rows from a CSV file and write them to a new CSV with a prefix (default: "Small").

    Notes:
    - The header row is preserved (copied once).
    - `num_rows` refers to data rows (excluding the header).
    - `method="auto"` uses pandas if available, otherwise falls back to the Python stdlib `csv` module.
    """
    if method == "pandas":
        return _extract_rows_pandas(input_file, num_rows=num_rows, prefix=prefix)
    if method == "stdlib":
        return _extract_rows_stdlib(input_file, num_rows=num_rows, prefix=prefix)
    if method == "auto":
        return _extract_rows_pandas(input_file, num_rows=num_rows, prefix=prefix)
    raise ValueError(f"Unknown method: {method}")


def process_all_csv_files(directory: str = ".", num_rows: int = DEFAULT_ROWS, prefix: str = DEFAULT_PREFIX, method: str = "auto") -> None:
    csv_files = glob.glob(os.path.join(directory, "*.csv"))

    # Avoid re-processing output files (e.g. Small*.csv -> SmallSmall*.csv).
    csv_files = [f for f in csv_files if not os.path.basename(f).startswith(prefix)]

    if not csv_files:
        print(f"No CSV files found in '{directory}'.")
        return

    print(f"Found {len(csv_files)} CSV file(s) to process.")

    successful = 0
    failed = 0
    for csv_file in csv_files:
        print(f"\nProcessing: {os.path.basename(csv_file)}")
        if extract_rows(csv_file, num_rows=num_rows, prefix=prefix, method=method):
            successful += 1
        else:
            failed += 1

    print(f"\nSummary: Processed {len(csv_files)} file(s)")
    print(f"  - Successfully processed: {successful}")
    print(f"  - Failed to process: {failed}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the first N data rows from one CSV (or all CSVs in a directory) into Small*.csv files."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Path to a CSV file or a directory (default: current directory).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of data rows to extract (default: {DEFAULT_ROWS}).",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=DEFAULT_PREFIX,
        help=f"Prefix for the output filename (default: {DEFAULT_PREFIX!r}).",
    )
    parser.add_argument(
        "--method",
        choices=["auto", "pandas", "stdlib"],
        default="auto",
        help="Extraction method (default: auto).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    target = Path(args.path)
    if target.is_file() and target.suffix.lower() == ".csv":
        print(f"Processing single file: {target}")
        extract_rows(str(target), num_rows=args.rows, prefix=args.prefix, method=args.method)
        return

    if target.is_dir():
        process_all_csv_files(str(target), num_rows=args.rows, prefix=args.prefix, method=args.method)
        return

    print(f"Error: '{args.path}' is not a valid directory or CSV file.")


if __name__ == "__main__":
    main()
