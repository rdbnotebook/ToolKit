#!/usr/bin/env python3
"""
Script to check parquet files for corruption in the financial dataset.
This script will validate all parquet files and report any corruption issues.
"""

import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from typing import List, Tuple, Dict
import traceback

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parquet_corruption_check.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ParquetCorruptionChecker:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.corrupted_files = []
        self.valid_files = []
        self.error_details = {}
        
    def check_single_file(self, file_path: Path) -> Tuple[str, bool, str]:
        """
        Check a single parquet file for corruption.
        Returns: (file_path, is_valid, error_message)
        """
        try:
            # Method 1: Try to read with pyarrow
            table = pq.read_table(file_path)
            
            # Method 2: Try to convert to pandas (more thorough check)
            df = table.to_pandas()
            
            # Method 3: Check basic properties
            if len(df) == 0:
                return str(file_path), False, "Empty dataframe"
            
            # Method 4: Check for any null columns or suspicious data
            null_cols = df.isnull().all()
            if null_cols.any():
                null_col_names = null_cols[null_cols].index.tolist()
                logger.warning(f"File {file_path} has completely null columns: {null_col_names}")
            
            return str(file_path), True, "Valid"
            
        except pa.ArrowInvalid as e:
            return str(file_path), False, f"Arrow Invalid: {str(e)}"
        except pa.ArrowIOError as e:
            return str(file_path), False, f"Arrow IO Error: {str(e)}"
        except Exception as e:
            return str(file_path), False, f"General Error: {str(e)}"
    
    def find_all_parquet_files(self) -> List[Path]:
        """Find all parquet files in the dataset."""
        parquet_files = []
        
        # Look for parquet files in all sec= directories
        for sec_dir in self.base_path.glob("sec=*"):
            if sec_dir.is_dir():
                # Look in year subdirectories
                for year_dir in sec_dir.glob("year=*"):
                    if year_dir.is_dir():
                        # Find all parquet files
                        for parquet_file in year_dir.glob("*.parquet"):
                            parquet_files.append(parquet_file)
        
        logger.info(f"Found {len(parquet_files)} parquet files to check")
        return parquet_files
    
    def check_specific_files(self, file_patterns: List[str]) -> Dict[str, Tuple[bool, str]]:
        """Check specific files mentioned in the AWS logs."""
        results = {}
        
        for pattern in file_patterns:
            matching_files = list(self.base_path.glob(pattern))
            if not matching_files:
                logger.warning(f"No files found matching pattern: {pattern}")
                continue
                
            for file_path in matching_files:
                if file_path.suffix == '.parquet':
                    file_str, is_valid, error_msg = self.check_single_file(file_path)
                    results[file_str] = (is_valid, error_msg)
        
        return results
    
    def check_all_files(self, max_workers: int = 4) -> Dict[str, Dict]:
        """Check all parquet files using parallel processing."""
        parquet_files = self.find_all_parquet_files()
        
        if not parquet_files:
            logger.error("No parquet files found!")
            return {}
        
        results = {
            'valid': [],
            'corrupted': [],
            'errors': {}
        }
        
        logger.info(f"Starting corruption check on {len(parquet_files)} files using {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.check_single_file, file_path): file_path 
                for file_path in parquet_files
            }
            
            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_file)):
                file_path = future_to_file[future]
                
                try:
                    file_str, is_valid, error_msg = future.result()
                    
                    if is_valid:
                        results['valid'].append(file_str)
                        if i % 100 == 0:  # Log progress every 100 files
                            logger.info(f"Processed {i+1}/{len(parquet_files)} files...")
                    else:
                        results['corrupted'].append(file_str)
                        results['errors'][file_str] = error_msg
                        logger.error(f"CORRUPTED: {file_str} - {error_msg}")
                        
                except Exception as e:
                    error_msg = f"Exception during processing: {str(e)}"
                    results['corrupted'].append(str(file_path))
                    results['errors'][str(file_path)] = error_msg
                    logger.error(f"EXCEPTION: {file_path} - {error_msg}")
        
        return results
    
    def generate_report(self, results: Dict) -> str:
        """Generate a detailed report of the corruption check."""
        report = []
        report.append("=" * 80)
        report.append("PARQUET CORRUPTION CHECK REPORT")
        report.append("=" * 80)
        report.append(f"Total files checked: {len(results['valid']) + len(results['corrupted'])}")
        report.append(f"Valid files: {len(results['valid'])}")
        report.append(f"Corrupted files: {len(results['corrupted'])}")
        report.append(f"Corruption rate: {len(results['corrupted']) / (len(results['valid']) + len(results['corrupted'])) * 100:.2f}%")
        report.append("")
        
        if results['corrupted']:
            report.append("CORRUPTED FILES:")
            report.append("-" * 40)
            for file_path in sorted(results['corrupted']):
                error_msg = results['errors'].get(file_path, "Unknown error")
                report.append(f"• {file_path}")
                report.append(f"  Error: {error_msg}")
                report.append("")
        
        # Group corrupted files by security
        if results['corrupted']:
            sec_groups = {}
            for file_path in results['corrupted']:
                # Extract security name from path
                parts = file_path.split('/')
                sec_part = next((part for part in parts if part.startswith('sec=')), 'unknown')
                sec_name = sec_part.replace('sec=', '') if sec_part != 'unknown' else 'unknown'
                
                if sec_name not in sec_groups:
                    sec_groups[sec_name] = []
                sec_groups[sec_name].append(file_path)
            
            report.append("CORRUPTED FILES BY SECURITY:")
            report.append("-" * 40)
            for sec_name, files in sorted(sec_groups.items()):
                report.append(f"{sec_name}: {len(files)} corrupted files")
        
        return "\n".join(report)

def main():
    # Initialize checker
    base_path = "."  # Current directory
    checker = ParquetCorruptionChecker(base_path)
    
    # First, check the specific files mentioned in the AWS logs
    logger.info("Checking specific files mentioned in AWS logs...")
    
    # Files specifically mentioned in the logs
    specific_patterns = [
        "sec=RTY/year=2025/part-000.parquet",
        "sec=RVX/year=2024/part-000.parquet", 
        "sec=RVX/year=2025/part-000.parquet",
        "sec=SB/year=2024/part-000.parquet",
        "sec=SB/year=2025/part-000.parquet"
    ]
    
    specific_results = checker.check_specific_files(specific_patterns)
    
    if specific_results:
        logger.info("Results for files mentioned in AWS logs:")
        for file_path, (is_valid, error_msg) in specific_results.items():
            status = "VALID" if is_valid else "CORRUPTED"
            logger.info(f"{status}: {file_path} - {error_msg}")
    
    # Now check all files
    logger.info("Starting comprehensive check of all parquet files...")
    results = checker.check_all_files(max_workers=8)
    
    # Generate and save report
    report = checker.generate_report(results)
    
    # Save report to file
    with open('parquet_corruption_report.txt', 'w') as f:
        f.write(report)
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total files: {len(results['valid']) + len(results['corrupted'])}")
    print(f"Valid files: {len(results['valid'])}")
    print(f"Corrupted files: {len(results['corrupted'])}")
    
    if results['corrupted']:
        print(f"\nCorrupted files found:")
        for file_path in sorted(results['corrupted'])[:10]:  # Show first 10
            print(f"  • {file_path}")
        if len(results['corrupted']) > 10:
            print(f"  ... and {len(results['corrupted']) - 10} more")
    
    print(f"\nDetailed report saved to: parquet_corruption_report.txt")
    print(f"Full log saved to: parquet_corruption_check.log")
    
    return len(results['corrupted']) == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 