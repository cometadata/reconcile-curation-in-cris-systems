# Author Affiliation Parser

Rust utility for processing and normalizing author affiliation data from OpenAlex.

## Usage

```bash
cargo run -- -i input.csv -o output.csv
```

## Input Format

Expects a CSV file with columns:
- `doi`: Document identifier
- `field_name`: Field type (e.g., "authorships.author.display_name", "authorships.affiliations.raw_affiliation_string")
- `subfield_path`: Path with indices (e.g., "authorships[0].author.display_name", "authorships[0].affiliations[1].raw_affiliation_string")
- `value`: Field value
- `source_id`: Source identifier
- `doi_prefix`: DOI prefix

The input file will be sorted by DOI during processing using an external sort algorithm optimized for large files.

## Output Format

Produces a normalized CSV with:
- `doi`: Document identifier
- `author_sequence`: Author position in the document
- `author_name`: Original author display name
- `normalized_author_name`: Normalized author name (lowercased, unicode-decoded, punctuation removed)
- `affiliation_sequence`: Affiliation position for the author
- `affiliation_name`: Original affiliation string
- `normalized_affiliation_name`: Normalized affiliation name
- `affiliation_ror`: ROR identifier for the affiliation (if available)