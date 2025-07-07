### Reconcile Curation in CRIS Systems Pipeline

The pipeline begins by processing a Crossref public data file snapshot using the [crossref-fast-field-parse utility](https://github.com/cometadata/reconcile-curation-in-cris-systems/tree/main/parsing-utils/crossref-fast-field-parse). Here, we extract the metadata fields necessary for analysis and reconciliation (e.g. `author.family`, `author.given`, `author.affiliation.name`, and `DOI`) from the gzipped JSONL files and output in CSV format.

This raw, field-level CSV is then fed into the [parse_join_normalize_author_affiliation_metadata utility](https://github.com/cometadata/reconcile-curation-in-cris-systems/tree/main/parsing-utils/parse_join_normalize_author_affiliation_metadata). This processes the input, aggregating and normalizing the various fields into coherent entries where authors and affiliations are linked, outputting the results as a new CSV file.

With this clean dataset of author-affiliation metadata, the [build_db.py script](https://github.com/cometadata/reconcile-curation-in-cris-systems/tree/main/find_additional_works_from_input_csv/build_db) is used to create a DuckDB database therefrom. In this database, indexes are created for `doi`, `normalized_full_name`, and `normalized_affiliation_key` columns.

The core analysis is then conducted by the [query_db.py script](https://github.com/cometadata/reconcile-curation-in-cris-systems/tree/main/find_additional_works_from_input_csv/query_db), which operates in one of two modes:

1.  File Processing Mode: This mode is designed to enrich an external list of publications, such as those produced from curation in a CRIS system. It takes as input a CSV containing DOIs and author names and queries the DuckDB database to link each author to their corresponding affiliation data. This linking process is customizable via a `config.yaml` file, which maps the CSV input columns to the database, specifying how to normalize author names to ensure they match between the input and DB. If an author is associated with multiple affiliations, the configuration allows for prioritizing those that contain variant names for the organization. As output, it generates two files:
    * A `_linkage.csv` file, which details the discovered links between authors and their affiliations.
    * A `_discovered_works.csv` file, which uses the linked affiliations to find other works in the database by wher an author shares an affiliations that overlaps with those in the linkage file, excluding the original query DOIs.

2. Affiliation Search Mode: Alternatively, we can do a direct search of the database using a list of affiliation names provided in an input CSV. The script queries the database for all works where the `normalized_affiliation_key` matches the provided names, saving the results to an output file.
