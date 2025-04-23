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
import time
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
    clean_nexus_data,
    country_classifiers
)

# Path configuration
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw"
PROCESSED_DATA_PATH = DATA_DIR / "processed"

# Ensure output directory exists
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

def main():
    """
    Main function to run the data processing pipeline.
    Extracts and processes data from all sources and saves the results.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("Starting data processing pipeline")

    # Process sources and store results
    source_funcs = {
        "isora": process_isora,
        "wb_nexus": process_wb,
        "gfi": process_gfi,
        "usaid": process_usaid,
        "fsi": process_fsi,
        "unodc": process_unodc,
    }

    results = {}

    for source, func in source_funcs.items():
        start_time = time.time()
        try:
            results[source] = func(RAW_DATA_PATH)
            processing_time = time.time() - start_time
            rows, columns = results[source].shape
            logger.info(f"SUCCESS - {source}: rows = {rows}, cols = {columns} ({processing_time:.2f} seconds)")
        except Exception as e:
            logger.error(f"Error processing {source} data: {e}")
    
    # Combine all data sources
    logger.info("Combining all NEXUS data sources")
    nexus = pd.concat(
        [results[key] for key in ["isora", "wb_nexus", "gfi", "usaid", "fsi", "unodc"]],
        ignore_index=True
    )
    
    # Clean and process the combined data


    logger.info("Cleaning missing data & merging country classifiers")
    nexus_cleaned = clean_nexus_data(nexus)
    nexus_extended = country_classifiers(nexus_cleaned, DATA_DIR /'country_classifiers.csv')
    rows, columns = nexus_extended.shape
    
    output_path = PROCESSED_DATA_PATH / "nexus.parquet"
    logger.info(f"Writing Nexus to {output_path}")
    nexus_extended.to_parquet(output_path, index=False)
    
    logger.info("Data processing complete")
    logger.info("NEXUS INFO")
    print(f"rows = {rows}, cols = {columns}")
    print(f'{nexus_extended.info()}')

if __name__ == "__main__":
    main()