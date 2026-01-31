import pandas as pd
import numpy as np
import sys

def inspect_parquet_features(file_path, feature_names_str):
    try:
        print(f"--- Inspecting: {file_path} ---")
        df = pd.read_parquet(file_path)
        feature_names = [name.strip() for name in feature_names_str.split(',')]

        if not isinstance(df.index, pd.DatetimeIndex):
            if "timestamp" in df.columns:
                df = df.set_index("timestamp")
            elif hasattr(df.index, "name") and df.index.name == "timestamp":
                pass # already indexed by timestamp
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df.sort_index()

        for feature_name in feature_names:
            print(f"\nFeature: {feature_name}")
            if feature_name not in df.columns and feature_name != df.index.name:
                 # Check against index name if it's 'timestamp' and requested
                if feature_name == "timestamp" and df.index.name == "timestamp":
                    print(f"  Exists as Index: True")
                    print(f"  Is all NaN: {df.index.isna().all()}") # pd.Index doesn't have .isna().all() directly, isna() returns array
                    print(f"  NaN count: {pd.Series(df.index).isna().sum()}")
                    print(f"  Non-NaN count: {pd.Series(df.index).notna().sum()}")
                    print(f"  Head(5):\n{pd.Series(df.index).head(5).to_string()}")
                    print(f"  Tail(5):\n{pd.Series(df.index).tail(5).to_string()}")
                    continue
                print(f"  Column NOT FOUND")
                continue
            
            col_data = df[feature_name] if feature_name in df.columns else pd.Series(df.index, name="timestamp")

            print(f"  Exists: True")
            print(f"  Is all NaN: {col_data.isna().all()}")
            print(f"  NaN count: {col_data.isna().sum()}")
            print(f"  Non-NaN count: {col_data.notna().sum()}")
            if col_data.notna().any(): # Reverted based on user's last diff
                # Check if data type is numeric before attempting numeric operations
                if pd.api.types.is_numeric_dtype(col_data):
                    print(f"  Mean: {col_data.mean()}")
                    print(f"  Std Dev: {col_data.std()}")
                    print(f"  Min: {col_data.min()}")
                    print(f"  Max: {col_data.max()}")
                else:
                    print(f"  Feature is non-numeric. Skipping numeric stats.")
            print(f"  Head(5):\n{col_data.head(5).to_string()}")
            print(f"  Tail(5):\n{col_data.tail(5).to_string()}")
        print("--- Inspection complete ---\n")

    except Exception as e:
        print(f"Error inspecting {file_path} for features {feature_names_str}: {e}")
        print("--- Inspection failed ---\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python inspect_parquet.py <file_path> <comma_separated_feature_names>")
        sys.exit(1)
    
    file_path_arg = sys.argv[1]
    features_arg = sys.argv[2]
    inspect_parquet_features(file_path_arg, features_arg) 