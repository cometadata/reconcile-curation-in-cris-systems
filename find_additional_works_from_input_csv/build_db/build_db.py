import os
import re
import sys
import argparse
import duckdb
from unidecode import unidecode


def is_latin_char_text(text):
    if not isinstance(text, str):
        return False
    for char in text:
        if '\u0000' <= char <= '\u024F':
            return True
    return False


def normalize_text(text):
    if not isinstance(text, str):
        return text
    if is_latin_char_text(text):
        text = unidecode(text)
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = text.strip()
    return text


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="""
        Tool to setup a DuckDB database for linking authors to affiliations.

        Usage:
        python %(prog)s --reference-file /path/to/100gb.csv --db-file publications.duckdb
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-r", "--reference-file",
        required=True,
        help="Path to the reference CSV file with author affiliations."
    )
    parser.add_argument(
        "-d", "--db-file",
        required=True,
        help="Path to the DuckDB database file to be created. Default: works.db"
    )
    parser.add_argument(
        "-m", "--memory-limit",
        default="8GB",
        help="Memory limit for DB processing (e.g., '16GB', '2GB'). Default: 8GB."
    )

    return parser.parse_args()


def setup_database(db_file, reference_file, memory_limit):
    print("--- Running Database Setup ---")
    if not os.path.exists(reference_file):
        print(f"Error: Reference file not found at '{reference_file}'")
        sys.exit(1)

    print(f"Creating and optimizing new DuckDB database at '{db_file}'...")
    con = duckdb.connect(database=db_file, read_only=False)

    print(f"Setting memory limit to {memory_limit}.")
    con.execute(f"SET memory_limit='{memory_limit}';")

    print(f"Loading and transforming reference data from '{reference_file}'. This may take some time...")

    try:
        create_table_sql = f"""
            CREATE OR REPLACE TABLE author_references AS
            SELECT
                *,
                lower(trim(normalized_affiliation_name)) as normalized_affiliation_key
            FROM read_csv_auto(
                '{reference_file}',
                HEADER=TRUE,
                DELIM=',',
                ALL_VARCHAR=TRUE,
                SAMPLE_SIZE=-1
            )
            WHERE author_name IS NOT NULL AND normalized_affiliation_name IS NOT NULL;
        """

        con.execute(create_table_sql)
        print("Reference data loaded and transformed.")

        print("Creating indexes for fast lookups...")
        con.execute("CREATE INDEX idx_doi ON author_references (doi);")
        con.execute(
            "CREATE INDEX idx_norm_name ON author_references (normalized_author_name);")
        con.execute(
            "CREATE INDEX idx_norm_affil ON author_references (normalized_affiliation_key);")
        con.execute(
            "CREATE INDEX idx_ror ON author_references (affiliation_ror);")
        print("Indexes created successfully.")

        print("Database build is complete!")

    except Exception as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)
    finally:
        con.close()


if __name__ == '__main__':
    args = parse_arguments()
    setup_database(args.db_file, args.reference_file, args.memory_limit)