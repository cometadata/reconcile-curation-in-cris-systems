# Find Additional Works from Input CSV

Link authors to affiliations and find works with overlapping affiliations using DuckDB.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Process works and find author-affiliation linkages
```bash
python query_db.py --process-file \
  --input-file input.csv \
  --output-file results.csv \
  --db-file publications.duckdb \
  --config config.yaml
```

Generates:
- `results_linkage.csv`: Author-affiliation mappings
- `results_discovered_works.csv`: Related works by shared affiliations

### Search works by affiliation
```bash
python query_db.py --search-affiliation \
  --input-file affiliations.csv \
  --output-file works.csv \
  --db-file publications.duckdb \
  --config config.yaml
```


## Configuration

Create a `config.yaml` file to specify your input file format and its processing rules.

### Configuration File Format

```yaml
# ----------------------------------------------------
# 1. Input File Column Mapping (Required)
#    Maps your input CSV column names to the script's internal names.
# ----------------------------------------------------
input_columns:
  doi: "DOI"                        # Column containing DOI identifiers
  authors: "Authors"    # Column containing author names
  author_separator: ";"             # Delimiter to use if multiple authors are in one field

# ----------------------------------------------------
# 2. Author Name Normalization(Required)
#    Specify the format of names in both reference and input files.
#    The tool will normalize names to match between datasets.
# ----------------------------------------------------
reference_name_style: "first last"  # Format in reference database
input_name_style: "last f"          # Format in your input CSV

# Supported name styles:
# - "first last":  "John Smith" → normalized to "smith j"
# - "last, first": "Smith, John" → normalized to "smith j"  
# - "last f":      "Smith J" → normalized to "smith j"
# - "last first":  "Smith John" → normalized to "smith j"
# - "last, f":     "Smith, J" → normalized to "smith j"
# - "last":        "Smith" → normalized to "smith"

# ----------------------------------------------------
# 3. Affiliation Disambiguation(Optional)
#    When an author has multiple affiliations, prioritize
#    affiliations containing these organization names.
# ----------------------------------------------------
organization_names:
  - "Organization Short Name"
  - "Organization Legal Name"
  - "Organization Acronym"
  # Add any variations of your organization name

# ----------------------------------------------------
# 4. Affiliation Search Columns (Required for --search-affiliation mode)
#    Specifies which column contains affiliations when searching.
# ----------------------------------------------------
affiliation_search_columns:
  affiliation_name: "Institution"   # Column name containing affiliations to search
```

### Configuration File Details

#### Input Columns Section
- doi: The column in your input CSV that contains DOIs
- authors: The column containing author names (can have multiple authors)
- author_separator: Character(s) used to separate multiple authors, when multiple are in a single cell

#### Name Normalization
The script normalizes author names to enable matching between different formats:
- Names are converted to lowercase
- First names are reduced to initials
- Name format is standardized to "lastname initial"

Choose the appropriate style based on how names appear in your data:
- Use `"first last"` for names like "John Smith"
- Use `"last, first"` for names like "Smith, John"
- Use `"last f"` for names like "Smith J"
- Use `"last"` when only last names are available

#### Organization Names (Optional)
When authors have multiple affiliations, the script can prioritize specific name variants that occur in the affiliation strings:
- List all variations of your organization name
- The tool will select affiliations containing these strings first
- If no match is found, the first available affiliation is used

#### Affiliation Search (Required for search mode)
Only necessary when using `--search-affiliation` mode:
- Specifies which column in the input file contains affiliation names to search for
