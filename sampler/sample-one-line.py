#!/usr/bin/env python3
"""
Script to sample the first row of CSV files (.csv, .txt) and Parquet files (.parquet) and display them on screen.
"""

import os
import argparse
import pandas as pd
from pathlib import Path

def sample_csv_file(file_path, sample_size=1, markdown=False):
    """
    Sample the first row of a CSV file and display on screen.
    
    Args:
        file_path (str): Path to the CSV file
        sample_size (int, optional): Number of rows to sample. Default is 1.
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    try:
        # Read the CSV file and take only the first sample_size rows
        df = pd.read_csv(file_path).head(sample_size)
        
        # Display the data
        display_sample_data(df, file_path, markdown)
        
    except Exception as e:
        print(f"Error reading CSV file: {file_path} - {str(e)}")

def sample_parquet_file(file_path, sample_size=1, markdown=False):
    """
    Sample the first row of a Parquet file and display on screen.
    
    Args:
        file_path (str): Path to the Parquet file
        sample_size (int, optional): Number of rows to sample. Default is 1.
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    try:
        # Read the Parquet file and take only the first sample_size rows
        df = pd.read_parquet(file_path).head(sample_size)
        
        # Display the data
        display_sample_data(df, file_path, markdown)
        
    except Exception as e:
        print(f"Error reading Parquet file: {file_path} - {str(e)}")

def display_sample_data(df, file_path, markdown=False):
    """
    Display the sampled dataframe.
    
    Args:
        df (pandas.DataFrame): Dataframe to display
        file_path (str): Path to the source file
        markdown (bool, optional): Whether to display in markdown format. Default is False.
    
    Returns:
        None
    """
    filename = Path(file_path).name
    
    if markdown:
        print(f"## {filename}")
        try:
            print(df.head(1).to_markdown(index=False))
        except AttributeError:
            print("```")
            headers = "| " + " | ".join(df.columns) + " |"
            separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
            print(headers)
            print(separator)
            
            for _, row in df.head(1).iterrows():
                values = "| " + " | ".join([str(val) for val in row.values]) + " |"
                print(values)
            print("```")
    else:
        print(f"\n{filename}:")
        print(df.head(1).to_string(index=False))
    
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

def sample_all_files(directory=".", sample_size=1, markdown=False, formats=['.csv', '.txt', '.parquet']):
    """
    Sample all files with specified formats in the specified directory.
    
    Args:
        directory (str): Directory to search for files
        sample_size (int): Number of rows to sample
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
        print(f"# File samples ({len(files)} files)\n")
    else:
        print(f"File samples ({len(files)} files):\n")
    
    for file_path in files:
        # Select appropriate sampling function based on file extension
        file_ext = Path(file_path).suffix.lower()
        if file_ext == '.parquet':
            sample_parquet_file(file_path, sample_size=sample_size, markdown=markdown)
        else:  # .csv or .txt
            sample_csv_file(file_path, sample_size=sample_size, markdown=markdown)

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Sample the first row of CSV (.csv, .txt) and Parquet (.parquet) files and display them on screen.')
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Path to a specific input file (if not specified, all supported files in the current directory will be sampled)')
    parser.add_argument('--directory', '-d', type=str, default=".",
                        help='Directory to search for files (default: current directory)')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Sample all supported files in the specified directory')
    parser.add_argument('--sample-size', '-s', type=int, default=1,
                        help='Number of rows to sample (default: 1)')
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
                sample_parquet_file(args.input, sample_size=args.sample_size, markdown=args.markdown)
            else:  # .csv or .txt
                sample_csv_file(args.input, sample_size=args.sample_size, markdown=args.markdown)
        else:
            supported_formats = ', '.join(f.lstrip('.') for f in formats)
            print(f"Error: {args.input} is not a supported file format. Supported formats: {supported_formats}")
    # Otherwise, sample all files in the directory
    elif args.all or not args.input:
        sample_all_files(args.directory, sample_size=args.sample_size, markdown=args.markdown, formats=formats) 