#!/usr/bin/env python3
"""
Script to convert CPER parquet files to CSV format.
"""

import pandas as pd
import os
import time
import argparse
import glob
from pathlib import Path

def convert_parquet_to_csv(parquet_file, output_csv=None):
    """
    Convert a parquet file to CSV format.
    
    Args:
        parquet_file (str): Path to the parquet file
        output_csv (str, optional): Path to the output CSV file. If None, will use the same name as parquet file with .csv extension.
    
    Returns:
        str: Path to the created CSV file
    """
    # Use the more robust convert_parquet_to_csv_with_index function
    return convert_parquet_to_csv_with_index(parquet_file, output_csv)

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

def convert_all_parquet_files(directory="."):
    """
    Convert all parquet files in the specified directory to CSV.
    
    Args:
        directory (str): Directory to search for parquet files
        
    Returns:
        list: List of paths to created CSV files
    """
    parquet_files = find_parquet_files(directory)
    
    if not parquet_files:
        print(f"No parquet files found in directory: {directory}")
        return []
    
    print(f"Found {len(parquet_files)} parquet file(s) to convert:")
    for i, file in enumerate(parquet_files, 1):
        print(f"  {i}. {file}")
    print()
    
    csv_files = []
    for i, parquet_file in enumerate(parquet_files, 1):
        print(f"\n[{i}/{len(parquet_files)}] Processing: {parquet_file}")
        print("=" * 80)
        try:
            csv_file = convert_parquet_to_csv(parquet_file)
            csv_files.append(csv_file)
            print("=" * 80)
        except Exception as e:
            print(f"Error converting {parquet_file}: {str(e)}")
            print("=" * 80)
    
    return csv_files

def convert_parquet_to_csv_with_index(parquet_file, output_csv=None):
    """
    Convert a parquet file to CSV format, preserving the index as a column.
    
    Args:
        parquet_file (str): Path to the parquet file
        output_csv (str, optional): Path to the output CSV file. If None, will use the same name as parquet file with .csv extension.
    
    Returns:
        str: Path to the created CSV file
    """
    start_time = time.time()
    
    # Create output filename if not provided
    if output_csv is None:
        output_csv = Path(parquet_file).with_suffix('.csv')
    
    print(f"Reading parquet file: {parquet_file}")
    # Read the parquet file
    df = pd.read_parquet(parquet_file)
    
    print(f"Parquet file loaded with shape: {df.shape}")
    print(f"Columns: {', '.join(df.columns)}")
    print(f"Index name: {df.index.name}")
    print(f"Index type: {type(df.index)}")
    
    # Display a sample of the data
    print("\nSample data (first 5 rows):")
    print(df.head().to_string())
    
    # Always reset the index to ensure we capture timestamp or any other index
    # The original condition might not be sufficient for all cases
    print(f"\nResetting index to preserve timestamp column")
    df = df.reset_index()
    print(f"After reset_index, columns: {', '.join(df.columns)}")
    
    # Write to CSV, ensuring all data is preserved
    print(f"\nConverting to CSV: {output_csv}")
    df.to_csv(output_csv, index=False)
    
    # Get file sizes for reporting
    parquet_size = os.path.getsize(parquet_file) / (1024 * 1024)  # MB
    csv_size = os.path.getsize(output_csv) / (1024 * 1024)  # MB
    
    elapsed_time = time.time() - start_time
    
    print(f"\nConversion completed successfully!")
    print(f"Parquet file size: {parquet_size:.2f} MB")
    print(f"CSV file size: {csv_size:.2f} MB")
    print(f"Time taken: {elapsed_time:.2f} seconds")
    
    return output_csv

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Convert parquet files to CSV format.')
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Path to a specific input parquet file (if not specified, all parquet files in the current directory will be converted)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Path to the output CSV file (default: same name as input with .csv extension)')
    parser.add_argument('--directory', '-d', type=str, default=".",
                        help='Directory to search for parquet files (default: current directory)')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Convert all parquet files in the specified directory')
    
    args = parser.parse_args()
    
    # If a specific input file is provided, convert only that file
    if args.input:
        csv_file = convert_parquet_to_csv(args.input, args.output)
        print(f"\nCSV file created at: {csv_file}")
    # Otherwise, convert all parquet files in the directory
    else:
        csv_files = convert_all_parquet_files(args.directory)
        if csv_files:
            print(f"\nConverted {len(csv_files)} parquet file(s) to CSV:")
            for i, file in enumerate(csv_files, 1):
                print(f"  {i}. {file}")
        else:
            print("\nNo files were converted.") 