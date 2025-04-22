#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "pandas",
#   "wbgapi",
#   "ibis-framework[duckdb]",
#   "openpyxl",
#   "pyarrow"
# ]
# ///
"""
Main script for data processing pipeline.
Extracts, transforms, and loads data from various sources.
"""
import os
import pandas as pd
import logging
from pathlib import Path
import sys

# Ensure the script can find the other modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import processing functions
from process import (
    process_isora, 
    process_wb, 
    process_gfi, 
    process_usaid, 
    process_fsi, 
    process_unodc, 
    clean_nexus_data
)

# Path configuration
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw"
PROCESSED_DATA_PATH = DATA_DIR / "processed"

# Ensure output directory exists
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

def print_shapes(dfs: dict):
    """Print the shape of each DataFrame."""
    for name, df in dfs.items():
        rows, columns = df.shape
        print(f"{name}: rows = {rows}, cols = {columns}")

def main():
    """
    Main function to run the data processing pipeline.
    Extracts and processes data from all sources and saves the results.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("Starting data processing pipeline")
    
    # Process each data source
    logger.info("Processing ISORA data")
    isora = process_isora(RAW_DATA_PATH)
    
    logger.info("Processing World Bank data")
    pefa, taxwb, wb_nexus = process_wb(RAW_DATA_PATH)
    
    logger.info("Processing GFI data")
    gfi = process_gfi(RAW_DATA_PATH)
    
    logger.info("Processing USAID data")
    usaid = process_usaid(RAW_DATA_PATH)
    
    logger.info("Processing FSI data")
    fsi = process_fsi(RAW_DATA_PATH)
    
    logger.info("Processing UNODC data")
    unodc = process_unodc(RAW_DATA_PATH)
    
    # Combine all data sources
    logger.info("Combining all NEXUS data sources")
    nexus = pd.concat(
        [isora, wb_nexus, gfi, usaid, fsi, unodc],
        ignore_index=True
    )
    
    # Clean and process the combined data
    logger.info("Cleaning combined data")
    nexus_cleaned = clean_nexus_data(nexus)
    
    # Print the shape of each DataFrame
    datasets = {
        "nexus": nexus_cleaned,
        "isora": isora,
        "worldbank": wb_nexus,
        "gfi": gfi,
        "usaid": usaid,
        "fsi": fsi,
        "unodc": unodc
    }
    print_shapes(datasets)
    
    # Prepare output datasets
    processed_output = {
        "pefa": pefa,
        "taxwb": taxwb,
        "nexus": nexus_cleaned
    }
    
    # Export each DataFrame to Parquet
    logger.info("Exporting data to Parquet files")
    for name, df in processed_output.items():
        output_path = PROCESSED_DATA_PATH / f"{name}.parquet"
        logger.info(f"Writing {name} to {output_path}")
        df.to_parquet(output_path, index=False)
    
    logger.info("Data processing complete")

if __name__ == "__main__":
    main()