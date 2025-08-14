import os
import re
import csv
import sys
import yaml
import argparse
import unicodedata
import duckdb
import jellyfish
from unidecode import unidecode
from nameparser import HumanName


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="""
        Query the author-affiliation database to find author-affiliation linkages and related works.

        Three modes:

        1. Process work and author file to find linked affiliations:
           python %(prog)s --process-file --input-file /path/to/input.csv --output-file results.csv --db-file publications.duckdb --config config.yaml

        2. Search for works by affiliation:
           python %(prog)s --search-affiliation --input-file affiliations.csv --output-file works.csv --db-file publications.duckdb --config config.yaml
           
        3. Discover works related to input IDs (DOIs/Work IDs) via shared affiliations:
           python %(prog)s --id-search --input-file ids.csv --output-file results.csv --db-file publications.duckdb --config config.yaml
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-i", "--input-file",
                        help="Path to the input CSV file.")
    parser.add_argument("-o", "--output-file",
                        help="Path to the output CSV file where results will be saved.")
    parser.add_argument("-d", "--db-file", required=True,
                        help="Path to the DuckDB database file to use.")
    parser.add_argument("-m", "--memory-limit", default="8GB",
                        help="Memory limit for DB processing (e.g., '16GB', '2GB'). Default: 8GB.")
    parser.add_argument("--config", required=True,
                        help="Path to the YAML configuration file.")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-p", "--process-file", action="store_true",
                      help="Run in file processing mode. Requires --input-file and --output-file.")
    mode.add_argument("-s", "--search-affiliation", action="store_true",
                      help="Search for works using a list of affiliations from an input file.")
    mode.add_argument("-ids", "--id-search", action="store_true",
                      help="Run in ID discovery mode. Finds new works via affiliations from input DOIs or Work IDs.")
    mode.add_argument("-ds", "--doi-search", action="store_true",
                      help="(Legacy) Same as --id-search. Kept for backward compatibility.")

    args = parser.parse_args()

    if args.process_file or args.search_affiliation or args.doi_search or args.id_search:
        if not args.input_file or not args.output_file:
            parser.error("Both --input-file and --output-file are required.")
    
    # Map legacy doi-search to id-search
    if args.doi_search:
        args.id_search = True

    return args


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


def parse_name_by_style(name: str, style: str) -> dict:
    name = name.strip()

    if style == 'last_initial':
        parts = name.split()
        if len(parts) >= 2:
            last_name = ' '.join(parts[:-1])
            initials = parts[-1]
            first_initial = initials[0].lower() if initials else ''

            return {
                'first': first_initial,
                'last': last_name.lower(),
                'middle': '',
                'normalized': f"{last_name.lower()} {first_initial}",
                'original': name,
                'style': style
            }
        else:
            return {
                'first': '',
                'last': name.lower(),
                'middle': '',
                'normalized': name.lower(),
                'original': name,
                'style': style
            }

    elif style == 'last_comma_first':
        if ',' in name:
            parts = name.split(',', 1)
            last = parts[0].strip()
            rest = parts[1].strip() if len(parts) > 1 else ''

            rest_parts = rest.split()
            first = rest_parts[0].lower() if rest_parts else ''
            middle = ' '.join(rest_parts[1:]).lower() if len(
                rest_parts) > 1 else ''

            return {
                'first': first,
                'last': last.lower(),
                'middle': middle,
                'normalized': f"{first} {middle} {last.lower()}".strip(),
                'original': name,
                'style': style
            }

    elif style == 'last_first':
        parts = name.split()
        if len(parts) >= 2:
            last = parts[0]
            first = parts[1] if len(parts) > 1 else ''
            middle = ' '.join(parts[2:]) if len(parts) > 2 else ''

            return {
                'first': first.lower(),
                'last': last.lower(),
                'middle': middle.lower(),
                'normalized': f"{first.lower()} {middle.lower()} {last.lower()}".strip(),
                'original': name,
                'style': style
            }

    elif style == 'first_initial_last':
        parts = name.split()
        initials = []
        last_idx = -1

        for i, part in enumerate(parts):
            if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
                initials.append(part.replace('.', '').lower())
            else:
                last_idx = i
                break

        if last_idx >= 0:
            last = ' '.join(parts[last_idx:])
            first = initials[0] if initials else ''
            middle = ' '.join(initials[1:]) if len(initials) > 1 else ''

            return {
                'first': first,
                'last': last.lower(),
                'middle': middle,
                'normalized': f"{first} {middle} {last.lower()}".strip(),
                'original': name,
                'style': style
            }

    parsed = HumanName(name)
    first = (parsed.first or '').strip()
    last = (parsed.last or '').strip()
    middle = (parsed.middle or '').strip()

    clean = f"{first} {middle} {last}".strip()
    clean = unicodedata.normalize('NFKD', clean).encode(
        'ascii', 'ignore').decode()
    normalized = re.sub(r'[-.,]', ' ', clean.lower()).strip()

    return {
        'first': first.lower(),
        'last': last.lower(),
        'middle': middle.lower(),
        'normalized': normalized,
        'original': name,
        'style': 'first_last'
    }


def are_names_similar(name1_str, name2_str, name1_style='auto', name2_style='auto', threshold=0.85):
    name1 = parse_name_by_style(name1_str, name1_style)
    name2 = parse_name_by_style(name2_str, name2_style)

    if not name1['last'] or not name2['last']:
        return name1['normalized'] == name2['normalized']

    last_similarity = jellyfish.jaro_winkler_similarity(
        name1['last'],
        name2['last']
    )
    if last_similarity < threshold:
        return False

    if name1['first'] and name2['first']:
        if len(name1['first']) == 1 or len(name2['first']) == 1:
            if name1['first'][0] == name2['first'][0]:
                return True
        else:
            first_similarity = jellyfish.jaro_winkler_similarity(
                name1['first'],
                name2['first']
            )
            if first_similarity >= threshold:
                return True

    if last_similarity >= 0.95:
        return True

    return False


def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)


def process_publications_enhanced(db_file, input_file, output_file, memory_limit, config):
    print("--- Running in Processing Mode ---")
    if not os.path.exists(db_file):
        print(f"Error: Database file not found. Run build_db.py first.")
        sys.exit(1)
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        sys.exit(1)

    try:
        # Get ID columns - at least one should be present
        input_doi_col = config['input_columns'].get('doi')
        input_work_id_col = config['input_columns'].get('work_id')
        
        if not input_doi_col and not input_work_id_col:
            print("Error: Config must specify at least one of 'doi' or 'work_id' in input_columns")
            sys.exit(1)
            
        authors_col = config['input_columns']['authors']
        author_sep = config['input_columns']['author_separator']
    except KeyError as e:
        print(f"Error: Config file missing required key in 'input_columns': {e}")
        sys.exit(1)

    base_path, _ = os.path.splitext(output_file)
    linkage_output_file = f"{base_path}_linkage.csv"
    full_log_output_file = f"{base_path}_full_discovery_log.csv"
    discovery_output_file = f"{base_path}_discovered_works.csv"

    con = duckdb.connect(database=db_file, read_only=True)

    print(f"Setting memory limit to {memory_limit}.")
    con.execute(f"SET memory_limit='{memory_limit}';")

    try:
        print("Registering all input IDs for exclusion...")
        
        # Build query based on available columns
        id_columns = []
        if input_doi_col:
            id_columns.append(f'"{input_doi_col}" AS doi')
        else:
            id_columns.append('NULL AS doi')
            
        if input_work_id_col:
            id_columns.append(f'"{input_work_id_col}" AS work_id')
        else:
            id_columns.append('NULL AS work_id')
            
        all_input_ids_query = f"""
            CREATE OR REPLACE TEMP VIEW all_input_ids AS
            SELECT DISTINCT {', '.join(id_columns)}
            FROM read_csv_auto('{input_file}', HEADER=TRUE);
        """
        con.execute(all_input_ids_query)

        print(f"Processing publications from '{input_file}'...")

        input_name_style = config.get('input_name_style', 'auto')
        reference_name_style = config.get('reference_name_style', 'first_last')
        matching_threshold = config.get('name_matching_threshold', 0.85)

        organization_names = config.get('organization_names', [])
        normalized_org_names = [normalize_text(
            name) for name in organization_names] if organization_names else []

        print(f"Using input name style: {input_name_style}")
        print(f"Using reference name style: {reference_name_style}")
        print(f"Using matching threshold: {matching_threshold}")
        if normalized_org_names:
            print(f"Prioritizing {len(normalized_org_names)} organization names.")

        print("Loading input authors...")
        
        # Build ID selection based on available columns
        id_selection = []
        if input_doi_col:
            id_selection.append(f'"{input_doi_col}" AS input_doi')
        else:
            id_selection.append('NULL AS input_doi')
            
        if input_work_id_col:
            id_selection.append(f'"{input_work_id_col}" AS input_work_id')
        else:
            id_selection.append('NULL AS input_work_id')
            
        input_authors_query = f"""
            SELECT DISTINCT
                {', '.join(id_selection)},
                UNNEST(string_split(trim("{authors_col}"), $${author_sep}$$)) AS input_author_raw
            FROM read_csv_auto('{input_file}', HEADER=TRUE, DELIM=',', QUOTE='"')
            WHERE "{authors_col}" IS NOT NULL
        """
        input_authors = con.execute(input_authors_query).fetchall()
        print(f"Found {len(input_authors)} author entries to process")

        print("Finding matches using enhanced name and affiliation validation...")
        matches = []
        processed_ids = set()

        for row in input_authors:
            # Row always has 3 columns due to our query structure
            # (input_doi or NULL, input_work_id or NULL, author_name)
            doi, work_id, author_name = row
            
            # Convert database NULLs to Python None
            if doi is None or doi == 'NULL':
                doi = None
            if work_id is None or work_id == 'NULL':
                work_id = None
            
            # Create unique ID for tracking
            id_key = (doi, work_id)
            if id_key in processed_ids:
                continue

            # Build query to match on either doi or work_id
            where_conditions = []
            if doi:
                # Escape single quotes in DOI for SQL
                escaped_doi = doi.replace("'", "''")
                where_conditions.append(f"doi = '{escaped_doi}'")
            if work_id:
                # Escape single quotes in work_id for SQL
                escaped_work_id = work_id.replace("'", "''")
                where_conditions.append(f"work_id = '{escaped_work_id}'")
            
            if not where_conditions:
                continue
                
            db_authors_query = f"""
                SELECT DISTINCT author_name, normalized_affiliation_name
                FROM author_references 
                WHERE {' OR '.join(where_conditions)}
            """
            db_authors = con.execute(db_authors_query).fetchall()
            processed_ids.add(id_key)

            # Get all input authors for this work
            work_input_authors = []
            for r in input_authors:
                # All rows have 3 columns
                r_doi, r_work_id, r_author = r
                
                # Convert database NULLs to Python None for comparison
                if r_doi is None or r_doi == 'NULL':
                    r_doi = None
                if r_work_id is None or r_work_id == 'NULL':
                    r_work_id = None
                
                # Check if this row belongs to the same work
                same_doi = (r_doi == doi) if (r_doi is not None or doi is not None) else True
                same_work_id = (r_work_id == work_id) if (r_work_id is not None or work_id is not None) else True
                
                if same_doi and same_work_id:
                    work_input_authors.append((r_doi, r_work_id, r_author))

            for _, _, input_author in work_input_authors:
                potential_matches = []
                for db_author, affiliation in db_authors:
                    if are_names_similar(
                        input_author.strip(), db_author,
                        name1_style=input_name_style, name2_style=reference_name_style,
                        threshold=matching_threshold
                    ):
                        potential_matches.append(
                            {'db_author': db_author, 'affiliation': affiliation})

                if not potential_matches:
                    continue

                org_matching_affiliations = []
                if normalized_org_names:
                    for match in potential_matches:
                        affil = match.get('affiliation')
                        if affil:
                            normalized_affil = normalize_text(affil)
                            if any(org_name in normalized_affil for org_name in normalized_org_names):
                                org_matching_affiliations.append(match)

                final_match_author = None
                final_affiliation = None
                linkage_status = None

                if org_matching_affiliations:
                    linkage_status = 'org_match_found'
                    first_match = org_matching_affiliations[0]
                    final_match_author = first_match['db_author']
                    final_affiliation = first_match['affiliation']
                else:
                    first_potential = potential_matches[0]
                    final_match_author = first_potential['db_author']
                    final_affiliation = first_potential['affiliation']

                    if not organization_names:
                        linkage_status = 'first_available'
                    else:
                        linkage_status = 'name_match_no_org_affiliation'

                if final_match_author:
                    matches.append({
                        'input_doi': doi if doi else '',
                        'input_work_id': work_id if work_id else '',
                        'input_author_name': input_author.strip(),
                        'ref_author_name': final_match_author,
                        'ref_affiliation': final_affiliation,
                        'linkage_status': linkage_status
                    })

        print(f"Found {len(matches)} author-affiliation linkages")

        print(f"Writing linkage results to '{linkage_output_file}'...")
        with open(linkage_output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['input_doi', 'input_work_id', 'input_author_name',
                                                   'ref_author_name', 'ref_affiliation',
                                                   'linkage_status'])
            writer.writeheader()
            writer.writerows(matches)

        sql_query_full_log = f"""
        COPY (
            WITH linkage_data AS (
                SELECT * FROM read_csv_auto('{linkage_output_file}', HEADER=TRUE)
            )
            SELECT
                ld.input_doi,
                ld.input_work_id,
                ld.input_author_name,
                ld.ref_affiliation AS linking_affiliation,
                collab.work_id AS discovered_work_id,
                collab.doi AS discovered_doi,
                collab.author_name AS discovered_author,
                collab.affiliation_name AS discovered_author_affiliation,
                collab.affiliation_ror AS discovered_ror_id
            FROM linkage_data AS ld
            JOIN author_references AS collab
                ON lower(trim(ld.ref_affiliation)) = collab.normalized_affiliation_key
            LEFT JOIN all_input_ids AS exclude_ids 
                ON (collab.doi = exclude_ids.doi AND collab.doi IS NOT NULL AND exclude_ids.doi IS NOT NULL) 
                OR (collab.work_id = exclude_ids.work_id AND collab.work_id IS NOT NULL AND exclude_ids.work_id IS NOT NULL)
            WHERE (ld.linkage_status = 'org_match_found' OR ld.linkage_status = 'first_available')
            AND COALESCE(exclude_ids.doi, exclude_ids.work_id) IS NULL
            ORDER BY ld.input_doi, ld.input_author_name, discovered_doi
        ) TO '{full_log_output_file}' (HEADER, DELIMITER ',');
        """

        sql_query_deduplicated_works = f"""
        COPY (
            WITH distinct_linking_affiliations AS (
                SELECT DISTINCT
                    lower(trim(ref_affiliation)) AS normalized_affiliation_key
                FROM read_csv_auto('{linkage_output_file}', HEADER=TRUE)
                WHERE ref_affiliation IS NOT NULL
                AND (linkage_status = 'org_match_found' OR linkage_status = 'first_available')
            )
            SELECT DISTINCT
                collab.work_id AS work_id,
                collab.doi AS doi,
                collab.author_name AS author,
                collab.affiliation_name AS author_affiliation,
                collab.affiliation_ror AS ror_id
            FROM distinct_linking_affiliations AS dla
            JOIN author_references AS collab
                ON dla.normalized_affiliation_key = collab.normalized_affiliation_key
            LEFT JOIN all_input_ids AS exclude_ids 
                ON (collab.doi = exclude_ids.doi AND collab.doi IS NOT NULL AND exclude_ids.doi IS NOT NULL) 
                OR (collab.work_id = exclude_ids.work_id AND collab.work_id IS NOT NULL AND exclude_ids.work_id IS NOT NULL)
            WHERE COALESCE(exclude_ids.doi, exclude_ids.work_id) IS NULL
            ORDER BY doi, author
        ) TO '{discovery_output_file}' (HEADER, DELIMITER ',');
        """

        print(f"Generating full discovery log... saving to '{full_log_output_file}'")
        con.execute(sql_query_full_log)

        print(f"Generating deduplicated works list... saving to '{discovery_output_file}'")
        con.execute(sql_query_deduplicated_works)

    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()

    print(f"\nProcessing complete.")
    print(f"-> Linkage results saved to '{linkage_output_file}'")
    print(f"-> Full discovery log saved to '{full_log_output_file}'")
    print(f"-> Deduplicated discovered works saved to '{discovery_output_file}'")


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
        con.create_function('normalize_affiliation_udf',
                            normalize_text, [str], str)

        sql_query = f"""
        COPY (
            WITH input_data AS (
                SELECT
                    *,
                    normalize_affiliation_udf("{search_col}") AS normalized_search_key
                FROM read_csv_auto('{input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
            )
            SELECT
                inp."{search_col}" AS input_search_term,
                ref.work_id AS ref_work_id,
                ref.doi AS ref_doi,
                ref.author_name AS ref_author_name,
                ref.normalized_affiliation_name AS ref_affiliation,
                ref.normalized_affiliation_key AS ref_affiliation_normalized_key
            FROM author_references AS ref
            JOIN input_data AS inp
                ON ref.normalized_affiliation_key = inp.normalized_search_key
            ORDER BY input_search_term, ref.doi, ref.author_name
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


def search_by_id_and_org(db_file, input_file, output_file, memory_limit, config):
    print("--- Running in ID Discovery Mode ---")
    if not os.path.exists(db_file):
        print("Error: Database file not found. Run build_db.py first.")
        sys.exit(1)
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        sys.exit(1)

    try:
        # Try new id_search_columns first, fall back to doi_search_columns for compatibility
        if 'id_search_columns' in config:
            doi_col = config['id_search_columns'].get('doi')
            work_id_col = config['id_search_columns'].get('work_id')
        else:
            # Legacy support
            doi_col = config.get('doi_search_columns', {}).get('doi')
            work_id_col = None
            
        if not doi_col and not work_id_col:
            print("Error: Config must specify at least one of 'doi' or 'work_id' in id_search_columns.")
            sys.exit(1)
            
        organization_names = config.get('organization_names', [])
        if not organization_names:
            print(
                "Error: 'organization_names' list in config must not be empty for this mode.")
            sys.exit(1)
    except KeyError as e:
        print(f"Error: Config file missing required key: {e}")
        sys.exit(1)

    base_path, _ = os.path.splitext(output_file)
    discovered_works_file = f"{base_path}_discovered_works.csv"
    linking_affiliations_file = f"{base_path}_linking_affiliations.csv"
    unmatched_ids_file = f"{base_path}_unmatched_ids.csv"

    normalized_org_names = [normalize_text(
        name) for name in organization_names]

    con = duckdb.connect(database=db_file, read_only=True)
    con.execute(f"SET memory_limit='{memory_limit}';")

    try:
        print(f"Loading IDs from '{input_file}'...")
        
        # Build query based on available columns
        id_columns = []
        if doi_col:
            id_columns.append(f'"{doi_col}" AS doi')
        else:
            id_columns.append('NULL AS doi')
            
        if work_id_col:
            id_columns.append(f'"{work_id_col}" AS work_id')
        else:
            id_columns.append('NULL AS work_id')
            
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW input_ids AS
            SELECT DISTINCT {', '.join(id_columns)}
            FROM read_csv_auto('{input_file}', HEADER=TRUE);
        """)

        con.execute("CREATE OR REPLACE TEMP TABLE org_names (name TEXT)")
        for name in normalized_org_names:
            con.execute("INSERT INTO org_names VALUES (?)", (name,))

        print("Phase 1: Identifying linking affiliations from input IDs...")
        find_linking_affiliations_query = f"""
        CREATE OR REPLACE TEMP VIEW linking_affiliations AS
        SELECT DISTINCT
            ar.normalized_affiliation_key
        FROM author_references AS ar
        INNER JOIN input_ids AS id 
            ON (ar.doi = id.doi AND id.doi IS NOT NULL) 
            OR (ar.work_id = id.work_id AND id.work_id IS NOT NULL)
        WHERE EXISTS (
            SELECT 1
            FROM org_names AS onames
            WHERE CONTAINS(ar.normalized_affiliation_key, onames.name)
        );
        
        COPY (SELECT * FROM linking_affiliations) TO '{linking_affiliations_file}' (HEADER, DELIMITER ',');
        """
        con.execute(find_linking_affiliations_query)
        print(f"-> Found linking affiliations. Log saved to '{linking_affiliations_file}'.")

        print("Phase 2: Discovering new works from linking affiliations...")
        discover_works_query = f"""
        COPY (
            SELECT
                ar.work_id,
                ar.doi,
                ar.author_name,
                ar.affiliation_name,
                ar.affiliation_ror
            FROM author_references AS ar
            INNER JOIN linking_affiliations AS la ON ar.normalized_affiliation_key = la.normalized_affiliation_key
            LEFT JOIN input_ids AS id 
                ON (ar.doi = id.doi AND id.doi IS NOT NULL) 
                OR (ar.work_id = id.work_id AND id.work_id IS NOT NULL)
            WHERE id.doi IS NULL AND id.work_id IS NULL
            ORDER BY ar.work_id, ar.doi, ar.author_name
        ) TO '{discovered_works_file}' (HEADER, DELIMITER ',');
        """
        con.execute(discover_works_query)
        print(f"-> Found new works. Results saved to '{discovered_works_file}'.")

        # Phase 3: Identify input IDs that did not contribute to the linking affiliations list
        print("Phase 3: Identifying unmatched input IDs...")
        find_unmatched_ids_query = f"""
        COPY (
            SELECT
                id.work_id,
                id.doi
            FROM input_ids AS id
            LEFT JOIN (
                SELECT DISTINCT work_id, doi
                FROM author_references ar
                WHERE EXISTS (
                    SELECT 1
                    FROM org_names AS onames
                    WHERE CONTAINS(ar.normalized_affiliation_key, onames.name)
                )
            ) AS matched_from_input 
                ON (id.doi = matched_from_input.doi AND id.doi IS NOT NULL) 
                OR (id.work_id = matched_from_input.work_id AND id.work_id IS NOT NULL)
            WHERE matched_from_input.doi IS NULL AND matched_from_input.work_id IS NULL
            ORDER BY id.work_id, id.doi
        ) TO '{unmatched_ids_file}' (HEADER, DELIMITER ',');
        """
        con.execute(find_unmatched_ids_query)
        print(f"-> Found unmatched IDs. List saved to '{unmatched_ids_file}'.")

    except Exception as e:
        print(f"An error occurred during ID discovery: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()

    print("\nID discovery complete.")
    print(f"-> Discovered works saved to '{discovered_works_file}'")
    print(f"-> Linking affiliations log saved to '{linking_affiliations_file}'")
    print(f"-> Unmatched input IDs saved to '{unmatched_ids_file}'")


if __name__ == '__main__':
    args = parse_arguments()
    config = load_config(args.config)

    if args.process_file:
        process_publications_enhanced(args.db_file, args.input_file,
                                      args.output_file, args.memory_limit, config)
    elif args.search_affiliation:
        search_by_affiliation(
            args.db_file, args.input_file, args.output_file, args.memory_limit, config)
    elif args.id_search:
        search_by_id_and_org(
            args.db_file, args.input_file, args.output_file, args.memory_limit, config)
