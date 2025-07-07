# Build Database

Creates a DuckDB database from CSV containing author-affiliation data.

## Installation
```bash
pip install -r requirements.txt
```


## Usage

```bash
python build_db.py --reference-file /path/to/data.csv --db-file publications.duckdb
```

## Arguments

- `--reference-file`: Path to CSV with author affiliations (required)
- `--db-file`: Output DuckDB database path (required)  
- `--memory-limit`: Memory limit for processing (default: *GB)

## Input CSV Requirements

Must contain columns:
- `doi`
- `full_name`
- `normalized_full_name`
- `normalized_affiliation`

## Output

Creates indexed DuckDB database with:
- Table: `author_references`
- Indexes on: DOI, normalized name, normalized affiliation