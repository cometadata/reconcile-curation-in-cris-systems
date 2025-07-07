# Author Affiliation Parser

Rust utility for processing and normalizing author affiliation data.


## Usage

```bash
cargo run -- -i input.csv -o output.csv
```

## Input Format

Expects a CSV file sorted by DOI with columns:
- `doi`: Document identifier
- `field_name`: Field type (e.g., "author.given", "author.family", "author.affiliation.name")
- `subfield_path`: Path with indices (e.g., "author[0]", "author[0].affiliation[1]")
- `value`: Field value

## Output Format

Produces a normalized CSV with:
- DOI and author sequence
- Full name (original and normalized)
- Given/family names (original and normalized)
- Affiliation data (sequence, original, and normalized)