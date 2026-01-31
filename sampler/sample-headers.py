#!/usr/bin/env python3
"""
Script to display headers of CSV files (.csv, .txt) and Parquet files (.parquet) without showing any data rows.
"""

import os
import argparse
import pandas as pd
from pathlib import Path

def sample_csv_headers(file_path, markdown=False):
    """
    Display headers of a CSV file.
    
    Args:
        file_path (str): Path to the CSV file
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    try:
        # Read just one row to get the headers
        df = pd.read_csv(file_path, nrows=1)
        
        # Display only the headers
        display_headers(df, file_path, markdown)
        
    except Exception as e:
        print(f"Error reading CSV file: {file_path} - {str(e)}")

def sample_parquet_headers(file_path, markdown=False):
    """
    Display headers of a Parquet file.
    
    Args:
        file_path (str): Path to the Parquet file
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    try:
        # Read a small sample to get column names without loading all data
        df = pd.read_parquet(file_path).head(0)
        
        # Display only the headers
        display_headers(df, file_path, markdown)
        
    except Exception as e:
        print(f"Error reading Parquet file: {file_path} - {str(e)}")

def display_headers(df, file_path, markdown=False):
    """
    Display only the headers of the dataframe.
    
    Args:
        df (pandas.DataFrame): Dataframe to get headers from
        file_path (str): Path to the source file
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    filename = Path(file_path).name
    
    if markdown:
        print(f"## {filename}")
        print("| " + " | ".join(df.columns) + " |")
        print("| " + " | ".join(["---"] * len(df.columns)) + " |")
    else:
        print(f"\n{filename}:")
        print(" ".join(df.columns))
    
def find_files(directory=".", formats=['.csv', '.txt', '.parquet']):
    """
    Find all specified file formats in the specified directory.
    
    Args:
        directory (str): Directory to search for files
        formats (list): List of file extensions to search for
        
    Returns:
        list: List of paths to found files
    """
    # Find all files with the specified extensions in the directory
    found_files = []
    for path in Path(directory).iterdir():
        if path.is_file() and path.suffix.lower() in formats:
            found_files.append(str(path))
    
    return found_files

def sample_all_headers(directory=".", markdown=False, formats=['.csv', '.txt', '.parquet']):
    """
    Display headers for all files with specified formats in the specified directory.
    
    Args:
        directory (str): Directory to search for files
        markdown (bool): Whether to display in markdown format
        formats (list): List of file extensions to search for
        
    Returns:
        None
    """
    files = find_files(directory, formats)
    
    if not files:
        format_str = ", ".join(formats)
        print(f"No files ({format_str}) found in directory: {directory}")
        return
    
    if markdown:
        print(f"# File headers ({len(files)} files)\n")
    else:
        print(f"File headers ({len(files)} files):\n")
    
    for file_path in files:
        # Select appropriate sampling function based on file extension
        file_ext = Path(file_path).suffix.lower()
        if file_ext == '.parquet':
            sample_parquet_headers(file_path, markdown=markdown)
        else:  # .csv or .txt
            sample_csv_headers(file_path, markdown=markdown)

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Display headers of CSV (.csv, .txt) and Parquet (.parquet) files without showing any data rows.')
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Path to a specific input file (if not specified, all supported files in the current directory will be processed)')
    parser.add_argument('--directory', '-d', type=str, default=".",
                        help='Directory to search for files (default: current directory)')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Process all supported files in the specified directory')
    parser.add_argument('--markdown', '-m', action='store_true',
                        help='Display output in markdown format')
    parser.add_argument('--formats', '-f', type=str, default='csv,txt,parquet',
                        help='Comma-separated list of file formats to process (default: csv,txt,parquet)')
    
    args = parser.parse_args()
    
    # Parse formats
    formats = [f'.{fmt.strip().lower()}' for fmt in args.formats.split(',')]
    
    # If a specific input file is provided, sample only that file
    if args.input:
        file_ext = Path(args.input).suffix.lower()
        if file_ext in formats:
            if file_ext == '.parquet':
                sample_parquet_headers(args.input, markdown=args.markdown)
            else:  # .csv or .txt
                sample_csv_headers(args.input, markdown=args.markdown)
        else:
            supported_formats = ', '.join(f.lstrip('.') for f in formats)
            print(f"Error: {args.input} is not a supported file format. Supported formats: {supported_formats}")
    # Otherwise, sample all files in the directory
    elif args.all or not args.input:
        sample_all_headers(args.directory, markdown=args.markdown, formats=formats) 