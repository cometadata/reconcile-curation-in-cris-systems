import os
import re
import csv
import sys
import yaml
import argparse
import unicodedata
import duckdb
import jellyfish
import pandas as pd
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

def extract_doi(text):
    if not text or not isinstance(text, str):
        return None
    
    text = text.strip().strip('<>').strip('"').strip("'")
    
    url_prefixes = [
        'https://doi.org/', 'http://doi.org/', 'https://dx.doi.org/',
        'http://dx.doi.org/', 'https://www.doi.org/', 'http://www.doi.org/',
        'doi.org/', 'dx.doi.org/', 'www.doi.org/', 'doi:', 'DOI:',
    ]
    
    text_lower = text.lower()
    for prefix in url_prefixes:
        if text_lower.startswith(prefix.lower()):
            text = text[len(prefix):]
            break
    
    if '?' in text: text = text.split('?')[0]
    if '#' in text: text = text.split('#')[0]
    text = text.strip()
    
    doi_pattern = r'^(10\.\d{4,}(?:\.\d+)?/[-._;()\/:a-zA-Z0-9]+)(?:\s|$)'
    match = re.match(doi_pattern, text)
    if match:
        return match.group(1).strip()
    
    if text.startswith('10.') and '/' in text:
        return text.rstrip('.,;:')
    
    return None

def parse_name_by_style(name: str, style: str) -> dict:
    name = name.strip()

    if style == 'last_initial':
        parts = name.split()
        if len(parts) >= 2:
            last_name = ' '.join(parts[:-1])
            initials = parts[-1]
            first_initial = initials[0].lower() if initials else ''
            return {'first': first_initial, 'last': last_name.lower(), 'middle': '', 'normalized': f"{last_name.lower()} {first_initial}", 'original': name, 'style': style}
        else:
            return {'first': '', 'last': name.lower(), 'middle': '', 'normalized': name.lower(), 'original': name, 'style': style}

    elif style == 'last_comma_first':
        if ',' in name:
            parts = name.split(',', 1)
            last = parts[0].strip()
            rest = parts[1].strip() if len(parts) > 1 else ''
            rest_parts = rest.split()
            first = rest_parts[0].lower() if rest_parts else ''
            middle = ' '.join(rest_parts[1:]).lower() if len(rest_parts) > 1 else ''
            return {'first': first, 'last': last.lower(), 'middle': middle, 'normalized': f"{first} {middle} {last.lower()}".strip(), 'original': name, 'style': style}

    elif style == 'last_first':
        parts = name.split()
        if len(parts) >= 2:
            last = parts[0]
            first = parts[1] if len(parts) > 1 else ''
            middle = ' '.join(parts[2:]) if len(parts) > 2 else ''
            return {'first': first.lower(), 'last': last.lower(), 'middle': middle.lower(), 'normalized': f"{first.lower()} {middle.lower()} {last.lower()}".strip(), 'original': name, 'style': style}

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
            return {'first': first, 'last': last.lower(), 'middle': middle, 'normalized': f"{first} {middle} {last.lower()}".strip(), 'original': name, 'style': style}
    
    parsed = HumanName(name)
    first = (parsed.first or '').strip()
    last = (parsed.last or '').strip()
    middle = (parsed.middle or '').strip()
    clean = f"{first} {middle} {last}".strip()
    clean = unicodedata.normalize('NFKD', clean).encode('ascii', 'ignore').decode()
    normalized = re.sub(r'[-.,]', ' ', clean.lower()).strip()
    return {'first': first.lower(), 'last': last.lower(), 'middle': middle.lower(), 'normalized': normalized, 'original': name, 'style': 'first_last'}

def are_names_similar(name1_str, name2_str, name1_style='auto', name2_style='auto', threshold=0.85):
    name1 = parse_name_by_style(name1_str, name1_style)
    name2 = parse_name_by_style(name2_str, name2_style)
    if not name1['last'] or not name2['last']:
        return name1['normalized'] == name2['normalized']
    last_similarity = jellyfish.jaro_winkler_similarity(name1['last'], name2['last'])
    if last_similarity < threshold:
        return False
    if name1['first'] and name2['first']:
        if len(name1['first']) == 1 or len(name2['first']) == 1:
            if name1['first'][0] == name2['first'][0]:
                return True
        else:
            first_similarity = jellyfish.jaro_winkler_similarity(name1['first'], name2['first'])
            if first_similarity >= threshold:
                return True
    if last_similarity >= 0.95:
        return True
    return False

def load_config(config_path):
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)

def process_publications(db_file, input_file, output_file, memory_limit, config):
    print("--- Running in Processing Mode (with Chunking & Direct-to-Disk Writing) ---")
    if not os.path.exists(db_file):
        print(f"Error: Database file not found. Run build_db.py first.")
        sys.exit(1)
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        sys.exit(1)

    try:
        input_doi_col = config['input_columns'].get('doi')
        input_work_id_col = config['input_columns'].get('work_id')
        if not input_doi_col and not input_work_id_col:
            raise KeyError("Config must specify 'doi' or 'work_id' in input_columns")
        authors_col = config['input_columns']['authors']
        author_sep = config['input_columns']['author_separator']
        chunk_size = config.get('chunk_size', 100000)
        organization_names = config.get('organization_names', [])
        normalized_org_names = [normalize_text(name) for name in organization_names] if organization_names else []
        input_name_style = config.get('input_name_style', 'auto')
        reference_name_style = config.get('reference_name_style', 'first_last')
        matching_threshold = config.get('name_matching_threshold', 0.85)
    except KeyError as e:
        print(f"Error: Config file missing required key: {e}")
        sys.exit(1)

    base_path, _ = os.path.splitext(output_file)
    linkage_output_file = f"{base_path}_linkage.csv"
    full_log_output_file = f"{base_path}_full_discovery_log.csv"
    discovery_output_file = f"{base_path}_discovered_works.csv"

    for f in [linkage_output_file, full_log_output_file, discovery_output_file]:
        if os.path.exists(f):
            os.remove(f)

    con = duckdb.connect(database=db_file, read_only=False)
    con.execute(f"SET memory_limit='{memory_limit}';")

    try:
        print("Phase 1: Pre-scanning input file for all DOIs and Work IDs...")
        id_cols_to_read = [col for col in [input_doi_col, input_work_id_col] if col]
        
        con.execute("CREATE OR REPLACE TEMP TABLE all_input_ids (doi VARCHAR, work_id VARCHAR);")
        id_reader = pd.read_csv(input_file, usecols=id_cols_to_read, chunksize=chunk_size, dtype=str, keep_default_na=False)
        
        for id_chunk_df in id_reader:
            rename_map = {}
            if input_doi_col: rename_map[input_doi_col] = 'doi'
            if input_work_id_col: rename_map[input_work_id_col] = 'work_id'
            id_chunk_df = id_chunk_df.rename(columns=rename_map)
            
            if 'doi' not in id_chunk_df: id_chunk_df['doi'] = None
            if 'work_id' not in id_chunk_df: id_chunk_df['work_id'] = None
            
            con.execute("INSERT INTO all_input_ids SELECT DISTINCT doi, work_id FROM id_chunk_df;")

        con.execute("CREATE TEMP VIEW unique_input_ids AS SELECT DISTINCT * FROM all_input_ids;")
        id_count = con.execute("SELECT COUNT(*) FROM unique_input_ids;").fetchone()[0]
        print(f"-> Found {id_count} unique IDs for exclusion.")

        print(f"\nPhase 2: Processing input file in chunks of {chunk_size} rows...")
        
        linkage_fieldnames = ['input_doi', 'input_work_id', 'input_author_name', 'ref_author_name', 'ref_affiliation', 'linkage_status']
        total_linkages = 0

        with open(linkage_output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=linkage_fieldnames)
            writer.writeheader()
            
            chunk_reader = pd.read_csv(input_file, chunksize=chunk_size, dtype=str, keep_default_na=False)
            for i, chunk_df in enumerate(chunk_reader):
                print(f"  Processing chunk {i+1}...")
                con.register('input_chunk_df', chunk_df)
                
                id_selection = []
                if input_doi_col: id_selection.append(f'"{input_doi_col}" AS input_doi')
                else: id_selection.append('NULL AS input_doi')
                if input_work_id_col: id_selection.append(f'"{input_work_id_col}" AS input_work_id')
                else: id_selection.append('NULL AS input_work_id')

                escaped_authors_col = authors_col.replace('"', '""')
                author_query_base = f"FROM input_chunk_df WHERE \"{escaped_authors_col}\" IS NOT NULL"
                if author_sep == '':
                    input_authors_query = f"""
                        SELECT DISTINCT {', '.join(id_selection)}, trim("{escaped_authors_col}") AS input_author_raw
                        {author_query_base} AND trim("{escaped_authors_col}") != ''
                    """
                else:
                    input_authors_query = f"""
                        SELECT DISTINCT {', '.join(id_selection)},
                               UNNEST(string_split(trim("{escaped_authors_col}"), $${author_sep}$$)) AS input_author_raw
                        {author_query_base}
                    """
                input_authors = con.execute(input_authors_query).fetchall()

                matches_in_chunk = []
                processed_ids = set()
                
                for row in input_authors:
                    doi, work_id, author_name = row
                    clean_doi = extract_doi(doi) if doi else None
                    id_key = (clean_doi, work_id)
                    if id_key in processed_ids: continue

                    where_conditions = []
                    if clean_doi: where_conditions.append(f"doi = '{clean_doi.replace("'", "''")}'")
                    if work_id: where_conditions.append(f"work_id = '{work_id.replace("'", "''")}'")
                    if not where_conditions: continue

                    db_authors_query = f"SELECT DISTINCT author_name, normalized_affiliation_name FROM author_references WHERE {' OR '.join(where_conditions)}"
                    db_authors = con.execute(db_authors_query).fetchall()
                    processed_ids.add(id_key)

                    work_input_authors_raw = [r for r in input_authors if (extract_doi(r[0]) if r[0] else None, r[1]) == id_key]

                    for _, _, input_author in work_input_authors_raw:
                        potential_matches = []
                        for db_author, affiliation in db_authors:
                            if are_names_similar(input_author.strip(), db_author, name1_style=input_name_style, name2_style=reference_name_style, threshold=matching_threshold):
                                potential_matches.append({'db_author': db_author, 'affiliation': affiliation})

                        if not potential_matches: continue

                        org_matching_affiliations = []
                        if normalized_org_names:
                            for match in potential_matches:
                                affil = match.get('affiliation')
                                if affil and any(org_name in normalize_text(affil) for org_name in normalized_org_names):
                                    org_matching_affiliations.append(match)
                        
                        final_match = org_matching_affiliations[0] if org_matching_affiliations else potential_matches[0]
                        linkage_status = 'org_match_found' if org_matching_affiliations else ('first_available' if not organization_names else 'name_match_no_org_affiliation')

                        matches_in_chunk.append({
                            'input_doi': clean_doi or '', 'input_work_id': work_id or '',
                            'input_author_name': input_author.strip(),
                            'ref_author_name': final_match['db_author'], 'ref_affiliation': final_match['affiliation'],
                            'linkage_status': linkage_status
                        })

                if matches_in_chunk:
                    writer.writerows(matches_in_chunk)
                    total_linkages += len(matches_in_chunk)
                print(f"    -> Found and wrote {len(matches_in_chunk)} linkages.")
        
        print(f"\nFinished processing all chunks. Total linkages found: {total_linkages}")

        print("\nPhase 3: Discovering new works using linkage data (in chunks)...")
        
        linkage_chunk_reader = pd.read_csv(linkage_output_file, chunksize=chunk_size, dtype=str, keep_default_na=False)
        is_first_log_write = True
        
        for i, linkage_chunk_df in enumerate(linkage_chunk_reader):
            print(f"  Processing linkage chunk {i+1}...")
            con.register('linkage_chunk_df', linkage_chunk_df)

            chunk_query_full_log = f"""
            SELECT
                ld.input_doi, ld.input_work_id, ld.input_author_name,
                ld.ref_affiliation AS linking_affiliation,
                collab.work_id AS discovered_work_id, collab.doi AS discovered_doi,
                collab.author_name AS discovered_author,
                collab.affiliation_name AS discovered_author_affiliation,
                collab.affiliation_ror AS discovered_ror_id
            FROM linkage_chunk_df AS ld
            JOIN author_references AS collab ON lower(trim(ld.ref_affiliation)) = collab.normalized_affiliation_key
            LEFT JOIN unique_input_ids AS exclude_ids 
                ON (collab.doi = exclude_ids.doi AND collab.doi IS NOT NULL AND exclude_ids.doi IS NOT NULL) 
                OR (collab.work_id = exclude_ids.work_id AND collab.work_id IS NOT NULL AND exclude_ids.work_id IS NOT NULL)
            WHERE (ld.linkage_status = 'org_match_found' OR ld.linkage_status = 'first_available')
            AND COALESCE(exclude_ids.doi, exclude_ids.work_id) IS NULL
            """
            
            results_df = con.execute(chunk_query_full_log).fetch_df()
            
            if not results_df.empty:
                results_df.to_csv(full_log_output_file, mode='a', header=is_first_log_write, index=False)
                print(f"    -> Discovered and wrote {len(results_df)} new work entries.")
                is_first_log_write = False
        
        print(f"\nFinished generating full discovery log: '{full_log_output_file}'")

        print(f"\nPhase 4: Generating deduplicated works list from full log...")

        if not os.path.exists(full_log_output_file):
            print("Full discovery log is empty or was not created. Skipping deduplication.")
        else:
            sql_query_deduplicated_works = f"""
            COPY (
                SELECT DISTINCT
                    discovered_work_id AS work_id, discovered_doi AS doi,
                    discovered_author AS author,
                    discovered_author_affiliation AS author_affiliation,
                    discovered_ror_id AS ror_id
                FROM read_csv_auto('{full_log_output_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
                ORDER BY doi, author
            ) TO '{discovery_output_file}' (HEADER, DELIMITER ',');
            """
            con.execute(sql_query_deduplicated_works)
            print(f"-> Deduplicated works saved to '{discovery_output_file}'")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()

    print("\nProcessing complete.")
    print(f"-> Linkage results saved to '{linkage_output_file}'")
    print(f"-> Full discovery log saved to '{full_log_output_file}'")
    print(f"-> Deduplicated discovered works saved to '{discovery_output_file}'")


def search_by_affiliation(db_file, input_file, output_file, memory_limit, config):
    print(f"--- Running in Batch Affiliation Search Mode ---")
    if not os.path.exists(db_file): sys.exit("Error: Database file not found.")
    if not os.path.exists(input_file): sys.exit(f"Error: Input file not found at '{input_file}'")

    try:
        search_col = config['affiliation_search_columns']['affiliation_name']
    except KeyError:
        sys.exit("Error: Config file must contain affiliation_search_columns.affiliation_name")

    con = duckdb.connect(database=db_file, read_only=True)
    con.execute(f"SET memory_limit='{memory_limit}';")

    try:
        con.create_function('normalize_affiliation_udf', normalize_text, [str], str)
        escaped_search_col = search_col.replace('"', '""')
        sql_query = f"""
        COPY (
            WITH input_data AS (
                SELECT *, normalize_affiliation_udf("{escaped_search_col}") AS normalized_search_key
                FROM read_csv_auto('{input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
            )
            SELECT
                inp."{escaped_search_col}" AS input_search_term,
                ref.work_id AS ref_work_id, ref.doi AS ref_doi,
                ref.author_name AS ref_author_name,
                ref.normalized_affiliation_name AS ref_affiliation,
                ref.normalized_affiliation_key AS ref_affiliation_normalized_key
            FROM author_references AS ref JOIN input_data AS inp
                ON ref.normalized_affiliation_key = inp.normalized_search_key
            ORDER BY input_search_term, ref.doi, ref.author_name
        ) TO '{output_file}' (HEADER, DELIMITER ',');
        """
        print(f"Searching for affiliations from '{input_file}'...")
        con.execute(sql_query)
        print(f"Search complete. Enriched results saved to '{output_file}'.")
    except Exception as e:
        sys.exit(f"An error occurred during search: {e}")
    finally:
        con.close()


def search_by_id_and_org(db_file, input_file, output_file, memory_limit, config):
    print("--- Running in ID Discovery Mode ---")
    if not os.path.exists(db_file): sys.exit("Error: Database file not found.")
    if not os.path.exists(input_file): sys.exit(f"Error: Input file not found at '{input_file}'")
    
    try:
        if 'id_search_columns' in config:
            doi_col = config['id_search_columns'].get('doi')
            work_id_col = config['id_search_columns'].get('work_id')
        else:
            doi_col = config.get('doi_search_columns', {}).get('doi')
            work_id_col = None
        if not doi_col and not work_id_col:
            sys.exit("Error: Config must specify 'doi' or 'work_id' in id_search_columns.")
        organization_names = config.get('organization_names', [])
        if not organization_names:
            sys.exit("Error: 'organization_names' list in config must not be empty for this mode.")
    except KeyError as e:
        sys.exit(f"Error: Config file missing required key: {e}")

    base_path, _ = os.path.splitext(output_file)
    discovered_works_file = f"{base_path}_discovered_works.csv"
    linking_affiliations_file = f"{base_path}_linking_affiliations.csv"
    unmatched_ids_file = f"{base_path}_unmatched_ids.csv"

    normalized_org_names = [normalize_text(name) for name in organization_names]
    con = duckdb.connect(database=db_file, read_only=True)
    con.execute(f"SET memory_limit='{memory_limit}';")
    
    try:
        print(f"Loading IDs from '{input_file}'...")
        id_columns = []
        if doi_col: id_columns.append(f'"{doi_col.replace("\"", "\"\"")}" AS doi')
        else: id_columns.append('NULL AS doi')
        if work_id_col: id_columns.append(f'"{work_id_col.replace("\"", "\"\"")}" AS work_id')
        else: id_columns.append('NULL AS work_id')
        con.execute(f"CREATE OR REPLACE TEMP VIEW input_ids AS SELECT DISTINCT {', '.join(id_columns)} FROM read_csv_auto('{input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE);")

        con.execute("CREATE OR REPLACE TEMP TABLE org_names (name TEXT)")
        for name in normalized_org_names:
            con.execute("INSERT INTO org_names VALUES (?)", (name,))

        print("Phase 1: Identifying linking affiliations from input IDs...")
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW linking_affiliations AS
            SELECT DISTINCT ar.normalized_affiliation_key FROM author_references AS ar
            INNER JOIN input_ids AS id ON (ar.doi = id.doi AND id.doi IS NOT NULL) OR (ar.work_id = id.work_id AND id.work_id IS NOT NULL)
            WHERE EXISTS (SELECT 1 FROM org_names AS onames WHERE CONTAINS(ar.normalized_affiliation_key, onames.name));
            COPY (SELECT * FROM linking_affiliations) TO '{linking_affiliations_file}' (HEADER, DELIMITER ',');
        """)
        print(f"-> Found linking affiliations. Log saved to '{linking_affiliations_file}'.")

        print("Phase 2: Discovering new works from linking affiliations...")
        con.execute(f"""
            COPY (
                SELECT ar.work_id, ar.doi, ar.author_name, ar.affiliation_name, ar.affiliation_ror
                FROM author_references AS ar
                INNER JOIN linking_affiliations AS la ON ar.normalized_affiliation_key = la.normalized_affiliation_key
                LEFT JOIN input_ids AS id ON (ar.doi = id.doi AND id.doi IS NOT NULL) OR (ar.work_id = id.work_id AND id.work_id IS NOT NULL)
                WHERE id.doi IS NULL AND id.work_id IS NULL
                ORDER BY ar.work_id, ar.doi, ar.author_name
            ) TO '{discovered_works_file}' (HEADER, DELIMITER ',');
        """)
        print(f"-> Found new works. Results saved to '{discovered_works_file}'.")

        print("Phase 3: Identifying unmatched input IDs...")
        con.execute(f"""
            COPY (
                SELECT id.work_id, id.doi FROM input_ids AS id
                LEFT JOIN (
                    SELECT DISTINCT work_id, doi FROM author_references ar
                    WHERE EXISTS (SELECT 1 FROM org_names AS onames WHERE CONTAINS(ar.normalized_affiliation_key, onames.name))
                ) AS matched_from_input ON (id.doi = matched_from_input.doi AND id.doi IS NOT NULL) OR (id.work_id = matched_from_input.work_id AND id.work_id IS NOT NULL)
                WHERE matched_from_input.doi IS NULL AND matched_from_input.work_id IS NULL
                ORDER BY id.work_id, id.doi
            ) TO '{unmatched_ids_file}' (HEADER, DELIMITER ',');
        """)
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
        process_publications(args.db_file, args.input_file,
                                      args.output_file, args.memory_limit, config)
    elif args.search_affiliation:
        search_by_affiliation(
            args.db_file, args.input_file, args.output_file, args.memory_limit, config)
    elif args.id_search:
        search_by_id_and_org(
            args.db_file, args.input_file, args.output_file, args.memory_limit, config)