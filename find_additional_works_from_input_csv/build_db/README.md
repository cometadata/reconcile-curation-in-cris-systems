# Build Database

Creates and manages DuckDB databases from OpenAlex author-affiliation CSV data.

## Installation
```bash
pip install duckdb unidecode
```

## Main Script: build_db.py

Creates optimized DuckDB database from large CSV files with robust error handling.

### Usage
```bash
python build_db.py --reference-file /path/to/data.csv --db-file publications.duckdb
```

### Arguments
- `--reference-file` (`-r`): Path to input CSV file (required)
- `--db-file` (`-d`): Output DuckDB database path (required)
- `--memory-limit` (`-m`): Memory limit for processing (default: 8GB)
- `--temp-dir`: Directory for disk spilling when memory limit reached
- `--chunk-size`: Process CSV in chunks of N rows (for very large files)
- `--skip-indexes`: Skip index creation during build

### Input CSV Schema
Required columns:
- `work_id`: OpenAlex work identifier
- `doi`: Digital Object Identifier (optional)
- `author_sequence`: Author position in work
- `author_name`: Author full name
- `normalized_author_name`: Normalized author name
- `affiliation_sequence`: Affiliation position
- `affiliation_name`: Institution name
- `normalized_affiliation_name`: Normalized institution name
- `affiliation_ror`: ROR identifier for institution

### Output Database
Creates table `author_references` with:
- All input columns preserved
- Additional `normalized_affiliation_key` for fast lookups
- Optional indexes on: work_id, doi, normalized_author_name, normalized_affiliation_key, affiliation_ror
- Error tracking table `import_errors` for problematic rows

## Utility Scripts

### utils/create_indexes.py
Creates indexes on existing database (useful when built with `--skip-indexes`).

```bash
python utils/create_indexes.py -d publications.duckdb --indexes all
```

Options:
- `--memory-limit`: Memory for index creation (default: 16GB)
- `--temp-dir`: Temporary directory for disk operations
- `--indexes`: Choose specific indexes or "all"

### utils/verify_db.py
Verifies database integrity and provides statistics.

```bash
python utils/verify_db.py -d publications.duckdb --sample 10
```

Displays:
- Table structure and row counts
- Index information
- Data quality statistics
- Sample data rows

## Performance Tips
- Use `--chunk-size` for files >50GB
- Increase the `--memory-limit` for faster processing if you have the RAM to spare
- Use `--skip-indexes` when building the database, then create later with the `create_indexes.py` utility
- Specify a `--temp-dir` on fast SSD for large datasets (which this one for sure will be!)