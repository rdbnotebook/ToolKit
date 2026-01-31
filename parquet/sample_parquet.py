#!/usr/bin/env python3
"""
Script to sample the first 200 rows of CPER parquet files and save them as CSV.
"""

import pandas as pd
import os
import time
import argparse
import glob
from pathlib import Path

def sample_parquet_to_csv(parquet_file, output_csv=None, sample_size=200):
    """
    Sample the first 200 rows of a parquet file and save as CSV.
    
    Args:
        parquet_file (str): Path to the parquet file
        output_csv (str, optional): Path to the output CSV file. If None, will use the same name as parquet file with _sample.csv extension.
        sample_size (int, optional): Number of rows to sample. Default is 200.
    
    Returns:
        str: Path to the created CSV file
    """
    start_time = time.time()
    
    # Create output filename if not provided
    if output_csv is None:
        output_csv = str(Path(parquet_file).with_suffix('')) + "_sample.csv"
    
    print(f"Reading parquet file: {parquet_file}")
    # Read the parquet file and take only the first sample_size rows
    df = pd.read_parquet(parquet_file).head(sample_size)
    
    print(f"Parquet file sampled with shape: {df.shape}")
    print(f"Columns: {', '.join(df.columns)}")
    
    # Display a sample of the data
    print("\nSample data (first 5 rows):")
    print(df.head().to_string())
    
    # Write to CSV
    print(f"\nSaving sample to CSV: {output_csv}")
    df.to_csv(output_csv, index=False)
    
    # Get file sizes for reporting
    parquet_size = os.path.getsize(parquet_file) / (1024 * 1024)  # MB
    csv_size = os.path.getsize(output_csv) / (1024 * 1024)  # MB
    
    elapsed_time = time.time() - start_time
    
    print(f"\nSampling completed successfully!")
    print(f"Original parquet file size: {parquet_size:.2f} MB")
    print(f"Sample CSV file size: {csv_size:.2f} MB")
    print(f"Time taken: {elapsed_time:.2f} seconds")
    
    return output_csv

def find_parquet_files(directory="."):
    """
    Find all parquet files in the specified directory.
    
    Args:
        directory (str): Directory to search for parquet files
        
    Returns:
        list: List of paths to parquet files
    """
    # Find all .parquet files in the directory
    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))
    
    # Also find directories with .parquet extension
    parquet_dirs = [d for d in glob.glob(os.path.join(directory, "*")) 
                   if os.path.isdir(d) and d.endswith(".parquet")]
    
    return parquet_files + parquet_dirs

def sample_all_parquet_files(directory=".", sample_size=200):
    """
    Sample all parquet files in the specified directory and save as CSV.
    
    Args:
        directory (str): Directory to search for parquet files
        sample_size (int): Number of rows to sample
        
    Returns:
        list: List of paths to created CSV files
    """
    parquet_files = find_parquet_files(directory)
    
    if not parquet_files:
        print(f"No parquet files found in directory: {directory}")
        return []
    
    print(f"Found {len(parquet_files)} parquet file(s) to sample:")
    for i, file in enumerate(parquet_files, 1):
        print(f"  {i}. {file}")
    print()
    
    csv_files = []
    for i, parquet_file in enumerate(parquet_files, 1):
        print(f"\n[{i}/{len(parquet_files)}] Processing: {parquet_file}")
        print("=" * 80)
        try:
            csv_file = sample_parquet_to_csv(parquet_file, sample_size=sample_size)
            csv_files.append(csv_file)
            print("=" * 80)
        except Exception as e:
            print(f"Error sampling {parquet_file}: {str(e)}")
            print("=" * 80)
    
    return csv_files

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Sample the first 200 rows of parquet files and save as CSV.')
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Path to a specific input parquet file (if not specified, all parquet files in the current directory will be sampled)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Path to the output CSV file (default: same name as input with _sample.csv suffix)')
    parser.add_argument('--directory', '-d', type=str, default=".",
                        help='Directory to search for parquet files (default: current directory)')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Sample all parquet files in the specified directory')
    parser.add_argument('--sample-size', '-s', type=int, default=200,
                        help='Number of rows to sample (default: 200)')
    
    args = parser.parse_args()
    
    # If a specific input file is provided, sample only that file
    if args.input:
        csv_file = sample_parquet_to_csv(args.input, args.output, sample_size=args.sample_size)
        print(f"\nSample CSV file created at: {csv_file}")
    # Otherwise, sample all parquet files in the directory
    else:
        csv_files = sample_all_parquet_files(args.directory, sample_size=args.sample_size)
        if csv_files:
            print(f"\nSampled {len(csv_files)} parquet file(s) to CSV:")
            for i, file in enumerate(csv_files, 1):
                print(f"  {i}. {file}")
        else:
            print("\nNo files were sampled.") 