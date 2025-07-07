import os
import sys
import yaml
import argparse
import duckdb
import re
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
        Query the author-affiliation database to find author-affiliation linkages and related works.
        
        Two modes:
        
        1. Process work and author file to find linked affiliations:
           python %(prog)s --process-file --input-file /path/to/input.csv --output-file results.csv --db-file publications.duckdb --config config.yaml
           
        2. Search for works by affiliation:
           python %(prog)s --search-affiliation --input-file affiliations.csv --output-file works.csv --db-file publications.duckdb --config config.yaml
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--db-file",
        required=True,
        help="Path to the DuckDB database file to use. Default: works.db"
    )
    parser.add_argument(
        "--memory-limit",
        default="8GB",
        help="Memory limit for DB processing (e.g., '16GB', '2GB'). Default: 8GB."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the YAML configuration file."
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--process-file",
        action="store_true",
        help="Run in file processing mode. Requires --input-file and --output-file."
    )
    mode.add_argument(
        "--search-affiliation",
        action="store_true",
        help="Search for works using a list of affiliations from an input file. Requires --input-file and --output-file."
    )

    parser.add_argument(
        "-i", "--input-file",
        help="Path to the input CSV file."
    )
    parser.add_argument(
        "-o", "--output-file",
        help="Path to the output CSV file where results will be saved."
    )

    args = parser.parse_args()

    if args.process_file:
        if not args.input_file or not args.output_file:
            parser.error(
                "--process-file mode requires --input-file and --output-file.")
    elif args.search_affiliation:
        if not args.input_file or not args.output_file:
            parser.error(
                "--search-affiliation mode requires --input-file and --output-file.")

    return args


def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        required_keys = ['input_columns',
                         'reference_name_style', 'input_name_style']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")

        input_cols_required = ['doi', 'authors', 'author_separator']
        for key in input_cols_required:
            if key not in config['input_columns']:
                raise ValueError(f"Missing required input_columns key: {key}")

        return config
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)


def generate_name_normalization_sql(name_style, column_name, is_for_matching=False):
    if name_style == 'first last':
        # 'John Doe' -> 'doe j'
        return f"""
            lower(
                list_extract(string_split({column_name}, ' '), -1) || ' ' || 
                substr(list_extract(string_split({column_name}, ' '), 1), 1, 1)
            )
        """
    elif name_style == 'last first':
        # 'Doe John' -> 'doe j'
        return f"""
            lower(
                list_extract(string_split({column_name}, ' '), 1) || ' ' || 
                substr(list_extract(string_split({column_name}, ' '), 2), 1, 1)
            )
        """
    elif name_style == 'last, first':
        # 'Doe, John' -> 'doe j'
        return f"""
            lower(
                list_extract(string_split({column_name}, ', '), 1) || ' ' || 
                substr(list_extract(string_split({column_name}, ', '), 2), 1, 1)
            )
        """
    elif name_style == 'last, f':
        # 'Doe, J' -> 'doe j'
        return f"""
            lower(
                list_extract(string_split({column_name}, ', '), 1) || ' ' || 
                lower(list_extract(string_split({column_name}, ', '), 2))
            )
        """
    elif name_style == 'last f':
        # 'Doe J' -> 'doe j'
        return f"lower(trim({column_name}))"
    elif name_style == 'last':
        # 'Doe' -> 'doe'
        if is_for_matching:
            return f"lower(trim(list_extract(string_split({column_name}, ' '), 1)))"
        else:
            return f"lower(trim({column_name}))"
    else:
        raise ValueError(f"Unsupported name style: {name_style}")


def process_publications(db_file, input_file, output_file, memory_limit, config):
    print("--- Running in Processing Mode ---")
    if not os.path.exists(db_file):
        print(f"Error: Database file not found. Run build_db.py first.")
        sys.exit(1)
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        sys.exit(1)

    base_path, _ = os.path.splitext(output_file)
    linkage_output_file = f"{base_path}_linkage.csv"
    discovery_output_file = f"{base_path}_discovered_works.csv"

    con = duckdb.connect(database=db_file, read_only=True)

    print(f"Setting memory limit to {memory_limit}.")
    con.execute(f"SET memory_limit='{memory_limit}';")

    print(f"Processing publications from '{input_file}'...")

    try:
        input_doi_col = config['input_columns']['doi']
        authors_col = config['input_columns']['authors']
        author_sep = config['input_columns']['author_separator']
        input_name_style = config.get('input_name_style', 'last f')
        input_norm_sql = generate_name_normalization_sql(
            input_name_style, 'input_author_raw')
        
        ref_norm_sql = generate_name_normalization_sql('first last', 'ref.full_name')

        org_names = config.get('organization_names', [])
        if org_names:
            org_conditions = [f"lower(ref.normalized_affiliation) ILIKE '%{name.lower()}%'" for name in org_names]
            org_filter_sql = " OR ".join(org_conditions)
            ranking_sql = f"""
                ROW_NUMBER() OVER(
                    PARTITION BY inp.input_doi, inp.input_author_raw 
                    ORDER BY
                        CASE 
                            WHEN {org_filter_sql} THEN 1
                            ELSE 2
                        END
                ) as affiliation_rank
            """
            match_priority_sql = f"""
                CASE 
                    WHEN {org_filter_sql} THEN 'organization_name_match'
                    ELSE 'first_available'
                END
            """
        else:
            ranking_sql = """
                ROW_NUMBER() OVER(
                    PARTITION BY inp.input_doi, inp.input_author_raw 
                    ORDER BY ref.normalized_affiliation
                ) as affiliation_rank
            """
            match_priority_sql = "'first_available'"

        sql_query_linkage = f"""
        COPY (
            WITH
            -- Read input and split author field
            input_authors AS (
                SELECT
                    "{input_doi_col}" AS input_doi,
                    UNNEST(string_split(trim("{authors_col}"), $${author_sep}$$)) AS input_author_raw
                FROM read_csv_auto('{input_file}', HEADER=TRUE, DELIM=',', QUOTE='"')
            ),
            
            -- Normalize input authors
            normalized_input AS (
                SELECT
                    input_doi,
                    input_author_raw,
                    {input_norm_sql} AS normalized_input_author
                FROM input_authors
            ),

            -- Find and rank affiliations
            found_affiliations_ranked AS (
                SELECT
                    inp.input_doi,
                    inp.input_author_raw,
                    ref.full_name AS found_full_name,
                    ref.normalized_affiliation AS found_affiliation,
                    ref.normalized_affiliation_key,
                    {ranking_sql},
                    {match_priority_sql} AS match_priority
                FROM normalized_input AS inp
                JOIN author_references AS ref
                    ON inp.input_doi = ref.doi
                    AND (
                        -- Exact match using on-the-fly normalization
                        inp.normalized_input_author = ({ref_norm_sql})
                        OR
                        -- Last name only match: check against the on-the-fly normalized name
                        (
                            NOT CONTAINS(inp.input_author_raw, ' ') 
                            AND inp.normalized_input_author = list_extract(string_split(({ref_norm_sql}), ' '), 1)
                        )
                    )
            ),

            -- Filter for only the top-ranked affiliation
            best_affiliation AS (
                SELECT * FROM found_affiliations_ranked WHERE affiliation_rank = 1
            )
            
            -- Select and rename columns for the linkage file
            SELECT 
                input_doi,
                input_author_raw AS input_author_name,
                found_full_name AS ref_author_name,
                found_affiliation AS ref_affiliation,
                match_priority
            FROM best_affiliation
            ORDER BY input_doi, input_author_name

        ) TO '{linkage_output_file}' (HEADER, DELIMITER ',');
        """

        print(f"Running linkage analysis... saving to '{linkage_output_file}'")
        con.execute(sql_query_linkage)

        sql_query_discovery = f"""
        COPY (
            WITH linkage_data AS (
                SELECT
                    input_doi,
                    input_author_name,
                    ref_affiliation,
                    -- We need the normalized key for an efficient join
                    lower(trim(ref_affiliation)) as normalized_affiliation_key
                FROM read_csv_auto('{linkage_output_file}', HEADER=TRUE)
                WHERE ref_affiliation IS NOT NULL  -- Filter out NULL affiliations
            )
            SELECT
                ld.input_doi AS query_doi,
                ld.input_author_name AS query_author_name,
                ld.ref_affiliation AS linking_affiliation,
                collab.doi AS discovered_work_doi,
                collab.full_name AS discovered_work_author,
                collab.normalized_affiliation AS discovered_work_author_affiliation
            FROM linkage_data AS ld
            JOIN author_references AS collab 
                ON ld.normalized_affiliation_key = collab.normalized_affiliation_key
                AND ld.normalized_affiliation_key IS NOT NULL
                AND collab.normalized_affiliation_key IS NOT NULL
                AND ld.input_doi != collab.doi
            ORDER BY query_doi, query_author_name, discovered_work_doi

        ) TO '{discovery_output_file}' (HEADER, DELIMITER ',');
        """
        
        print(f"Discovering related works... saving to '{discovery_output_file}'")
        con.execute(sql_query_discovery)

    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")
        print(f"\nDebug - Generated SQL:\n{sql_query_linkage[:500]}...")
        sys.exit(1)
    finally:
        con.close()

    print(f"\nProcessing complete.")
    print(f"-> Linkage results saved to '{linkage_output_file}'")
    print(f"-> Discovered works saved to '{discovery_output_file}'")


def search_by_affiliation(db_file, input_file, output_file, memory_limit, config):
    print(f"--- Running in Batch Affiliation Search Mode ---")

    if not os.path.exists(db_file):
        print(f"Error: Database file not found. Run build_db.py first.")
        sys.exit(1)
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        sys.exit(1)

    try:
        search_col = config['affiliation_search_columns']['affiliation_name']
    except KeyError:
        print("Error: Config file must contain affiliation_search_columns.affiliation_name")
        sys.exit(1)

    con = duckdb.connect(database=db_file, read_only=True)
    con.execute(f"SET memory_limit='{memory_limit}';")

    try:
        con.create_function('normalize_affiliation_udf', normalize_text, [str], str)

        sql_query = f"""
        COPY (
            WITH input_data AS (
                -- Read the input file and apply the normalization UDF
                SELECT
                    *,
                    normalize_affiliation_udf("{search_col}") AS normalized_search_key
                FROM read_csv_auto('{input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
            )
            -- Join with the main reference table on the normalized keys
            SELECT
                inp."{search_col}" AS input_search_term,
                ref.doi AS ref_doi,
                ref.full_name AS ref_author_name,
                ref.normalized_affiliation AS ref_affiliation,
                ref.normalized_affiliation_key AS ref_affiliation_normalized_key
            FROM author_references AS ref
            JOIN input_data AS inp
              ON ref.normalized_affiliation_key = inp.normalized_search_key
            ORDER BY input_search_term, ref.doi, ref.full_name
        ) TO '{output_file}' (HEADER, DELIMITER ',');
        """

        print(f"Searching for affiliations from '{input_file}'...")
        con.execute(sql_query)
        print(f"Search complete. Enriched results saved to '{output_file}'.")

    except Exception as e:
        print(f"An error occurred during search: {e}")
        sys.exit(1)
    finally:
        con.close()


if __name__ == '__main__':
    args = parse_arguments()
    config = load_config(args.config)

    if args.process_file:
        process_publications(args.db_file, args.input_file,
                             args.output_file, args.memory_limit, config)
    elif args.search_affiliation:
        search_by_affiliation(
            args.db_file, args.input_file, args.output_file, args.memory_limit, config)