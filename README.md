# 📊 Data Nexus

A comprehensive data processing pipeline for fiscal and economic datasets from multiple sources including IMF ISORA, World Bank, GFI, USAID, and more.

## Overview

This project extracts, transforms, and standardizes datasets from multiple sources into a unified format, enabling cross-dataset analysis and exploration. The pipeline processes diverse fiscal and economic indicators across countries and years, creating a standardized structure with consistent metadata.

## 🚀 Quick Start

### Setup with UV (Recommended)

```bash
# Clone the repository
git clone https://github.com/yourusername/data-nexus.git
cd data-nexus

# Run the pipeline directly (creates virtual env automatically)
uvx main.py

# Or synchronize dependencies to your active environment
uv sync --active
```

### Setup with Pip

```bash
# Clone the repository
git clone https://github.com/yourusername/data-nexus.git
cd data-nexus

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
```

## 📂 Project Structure

```
data-nexus/
├── config.py                # Configuration dictionaries for data sources
├── process.py               # Data processing functions for each source
├── main.py                  # Main pipeline script
├── pyproject.toml           # Project dependencies
├── country_classifiers.csv  # Country classification data
├── data/
│   ├── raw/                 # Raw input data files
│   └── processed/           # Output data files
└── notebooks/               # Jupyter notebooks for exploration
```

## Data Sources

- **IMF ISORA**: Revenue administration survey data (staff, operations, ICT)
- **World Bank**: PEFA assessments, tax capacity, WDI indicators, governance metrics
- **GFI**: Trade mispricing and illicit financial flows
- **USAID**: Tax effort and buoyancy data
- **TJN FSI**: Financial secrecy index
- **UNODC**: Drug prices and seizures data

## Workflow

1. **Run the pipeline**: `uvx main.py` processes all data sources and creates standardized parquet files.
2. **Explore the data**: Use the included notebooks or create your own to analyze the processed data.

## Data Model: One Big Table (OBT) Approach

The Nexus dataset follows a "One Big Table" (OBT) approach with a **pivoted-long** format, where:

- Each row represents a unique country-year-indicator combination
- Indicator codes/names are stored in columns rather than being used as column headers themselves
- Filtering by indicator is done through queries rather than column selection
- Metadata columns provide context for each data point

This approach makes it easy to:
- Filter across multiple dimensions (countries, years, indicators)
- Join with other datasets using common keys (country, year)
- Apply consistent transformations across all indicators

The `explore.ipynb` notebook includes a `src_tbl()` function that can pivot this data back to a wide format for traditional analysis after filtering.

## Metadata Structure

The Nexus dataset uses a hierarchical metadata structure to organize indicators:

- **source**: Organization that produced the data (e.g., "World Bank", "ISORA")
- **database**: Original database or file name (e.g., "WDI", "IMF ISORA.xlsx")
- **collection**: Specific collection or sheet within the source (e.g., "PEFA", "Tax administration expenditures")
- **indicator_code**: Unique identifier for the indicator
- **indicator_label**: Human-readable description of the indicator

This structure makes it possible to navigate the large number of indicators available. The `dt_map()` function in the exploration notebook provides a quick summary of available indicators grouped by these metadata fields.

## Country Classification Data

The dataset includes extensive country classification metadata (23 additional columns) that enables filtering and analysis by:

- Geographic classifications (region, sub-region)
- Development categories (least developed, landlocked, small island states)
- Income groups (high, upper-middle, lower-middle, low income)
- Political and economic groupings (OECD, Arab states, oil exporters)
- Fragility and debt indicators (fragile states, HIPC)

These classifications can be used to analyze patterns across different country groups or to focus analysis on specific categories of countries.

## 📈 Nexus Dataset Overview

The final `nexus.parquet` file contains over 616,000 observations with 34 columns:

```
# Data columns (total 34 columns):
 0   year                               616409 non-null  Int64  
 1   value                              582793 non-null  float64
 2   source                             616409 non-null  object 
 3   indicator_code                     616409 non-null  object 
 4   indicator_label                    616409 non-null  object 
 5   database                           616409 non-null  object 
 6   collection                         616409 non-null  object 
 7   value_meta                         33928 non-null   object 
 8-33 [Country classification columns]  ~600000 non-null  mixed  
```

The dataset combines over 400 unique indicators from different sources, providing a comprehensive resource for fiscal and economic analysis.

## Data Exploration

For interactive data exploration, you can use the provided notebook:

```bash
# Create notebook directory if it doesn't exist
mkdir -p notebooks
# Copy the exploration notebook
cp explore.ipynb notebooks/
# Start Jupyter Notebook
jupyter notebook notebooks/explore.ipynb
```

The notebook provides useful functions for data exploration:

- `dt_map(df, code=True)`: Get a count of records by source/database/collection/indicator, providing a quick overview of available data
- `src_tbl(df, ind='code')`: Create pivot tables from indicator data, converting from long to wide format for analysis

Example:
```python
# Get a summary of all available data sources
sources_summary = dt_map(nexus, code=False)

# Create a wide-format table for specific indicators
tax_data = (nexus
            .query("indicator_code.str.contains('tax')")
            .query("country in ['USA', 'GBR', 'DEU']")
            .pipe(src_tbl))
```

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests with improvements.

## 📄 License

MIT © Mirian Lima  
UN Office of the Special Adviser on Africa
