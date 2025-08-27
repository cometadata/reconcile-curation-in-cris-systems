import os
import csv
import sys
import pandas as pd
import logging

from query_db.db import DatabaseManager
from query_db.repository import AuthorReferencesRepository
from query_db.services import LinkageService, DiscoveryService
from query_db.analysis.entity_extraction import EntityExtractor
from query_db.utils import normalize_text, sanitize_file_path_for_sql, validate_column_name
from query_db.constants import *
from query_db.udf import register_all_udfs

logger = logging.getLogger(__name__)


class FileProcessor:
    def __init__(self, db_manager: DatabaseManager, config: dict, entity_extractor: EntityExtractor = None):
        self.db = db_manager
        self.config = config
        self.entity_extractor = entity_extractor
        
        logger.info("Registering UDFs for FileProcessor")
        register_all_udfs(db_manager)
        logger.info("UDF registration successful")
        self.repository = AuthorReferencesRepository(db_manager)
        self.linkage_service = LinkageService(self.repository, config)
        self.discovery_service = DiscoveryService(self.repository, config)
        
        self._setup_config()
    
    def _setup_config(self):
        try:
            self.input_doi_col = self.config['input_columns'].get('doi')
            self.input_work_id_col = self.config['input_columns'].get('work_id')
            if not self.input_doi_col and not self.input_work_id_col:
                raise KeyError("Config must specify 'doi' or 'work_id' in input_columns")
            
            self.authors_col = self.config['input_columns']['authors']
            self.author_sep = self.config['input_columns']['author_separator']
            self.chunk_size = self.config.get('chunk_size', DEFAULT_CHUNK_SIZE)
            self.organization_names = self.config.get('organization_names', [])
            self.normalized_org_names = [normalize_text(name) for name in self.organization_names]
            self.input_name_style = self.config.get('input_name_style', 'auto')
            self.reference_name_style = self.config.get('reference_name_style', 'first_last')
            self.matching_threshold = self.config.get('name_matching_threshold', DEFAULT_NAME_THRESHOLD)
            
            self.entity_extraction_enabled = self.config.get('entity_extraction_enabled', True)
            self.entity_matching_threshold = self.config.get('entity_matching_threshold', DEFAULT_ENTITY_THRESHOLD)
            self.use_entity_discovery = self.config.get('use_entity_discovery', True)
        except KeyError as e:
            print(f"Error: Config file missing required key: {e}")
            sys.exit(1)
    
    def run(self, input_file: str, output_file: str):
        print("--- Running in Processing Mode ---")
        
        if not os.path.exists(self.db.db_file):
            print(f"Error: Database file not found. Run build_db.py first.")
            sys.exit(1)
        if not os.path.exists(input_file):
            print(f"Error: Input file not found at '{input_file}'")
            sys.exit(1)
        
        base_path, _ = os.path.splitext(output_file)
        self.linkage_output_file = f"{base_path}{LINKAGE_SUFFIX}"
        self.full_log_output_file = f"{base_path}{FULL_LOG_SUFFIX}"
        self.discovery_output_file = f"{base_path}{DISCOVERED_WORKS_SUFFIX}"
        self.entity_mappings_file = f"{base_path}{ENTITY_MAPPINGS_SUFFIX}"
        
        for f in [self.linkage_output_file, self.discovery_output_file, self.entity_mappings_file]:
            if os.path.exists(f):
                os.remove(f)
        
        if os.path.exists(self.full_log_output_file):
            os.remove(self.full_log_output_file)
        
        try:
            self._prescan_ids(input_file)
            self._process_linkages(input_file)
            
            entity_mappings = {}
            if self.entity_extraction_enabled and self.entity_extractor and self.organization_names:
                entity_mappings = self._extract_entities()
            
            self._discover_works()
            
            if self.use_entity_discovery and entity_mappings:
                self._entity_discovery(entity_mappings)
            
            self._combine_results()
            
            self._print_summary()
            
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def _prescan_ids(self, input_file: str):
        print("Pre-scanning input file for all DOIs and Work IDs...")
        
        id_count = self.repository.create_input_ids_table(
            input_file, self.chunk_size, self.input_doi_col, self.input_work_id_col
        )
        print(f"-> Found {id_count} unique IDs for exclusion.")
    
    def _process_linkages(self, input_file: str):
        print(f"\nProcessing input file in chunks of {self.chunk_size} rows using UDF-based method...")
        
        total_linkages = 0
        
        self.repository.create_linkage_results_table()
        
        chunk_reader = pd.read_csv(input_file, chunksize=self.chunk_size, dtype=str, keep_default_na=False)
        for i, chunk_df in enumerate(chunk_reader):
            print(f"  Processing chunk {i+1}...")
            
            matches_in_chunk = self.linkage_service.find_linkages_udf(
                chunk_df, self.input_doi_col, self.input_work_id_col, 
                self.authors_col, self.author_sep
            )
            
            if matches_in_chunk:
                self.repository.insert_linkage_results(matches_in_chunk)
                total_linkages += len(matches_in_chunk)
            print(f"    -> Found and stored {len(matches_in_chunk)} linkages.")
        
        print(f"\nFinished processing all chunks. Total linkages found: {total_linkages}")
    
    
    def _extract_entities(self):
        print("\nExtracting organizational entities from matched affiliations...")
        
        print("  Collecting org-matched affiliations...")
        embl_linkage_df = self.repository.get_unique_affiliations_for_entity_extraction()
        
        if len(embl_linkage_df) == 0:
            print("No org-matched affiliations found for entity extraction.")
            return {}
        
        unique_affiliations = embl_linkage_df['ref_affiliation'].unique().tolist()
        print(f"  Found {len(unique_affiliations)} unique org affiliations.")
        
        print("  Fetching original affiliation names from database...")
        original_affiliations_map = self.repository.get_original_affiliation_names(unique_affiliations)
        
        if not original_affiliations_map:
            print("  No original affiliation names found.")
            return {}
        
        print(f"Found {len(original_affiliations_map)} original affiliation names.")
        
        print("  Extracting organizational entities...")
        entity_mappings = self.entity_extractor.extract_and_validate_from_affiliations(
            unique_affiliations, 
            original_affiliations_map
        )
        
        print(f"Extracted {len(entity_mappings)} entity-affiliation pairs.")
        
        if entity_mappings:
            affiliation_entities = {}
            for entity_text, source_affiliation in entity_mappings:
                if source_affiliation not in affiliation_entities:
                    affiliation_entities[source_affiliation] = []
                affiliation_entities[source_affiliation].append(entity_text)
            
            with open(self.entity_mappings_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['source_affiliation', 'extracted_entities'])
                writer.writeheader()
                
                for affiliation, entities in affiliation_entities.items():
                    writer.writerow({
                        'source_affiliation': affiliation,
                        'extracted_entities': '; '.join(entities)
                    })
            
            print(f"  Entity mappings saved to '{self.entity_mappings_file}'")
            print(f"  Using extracted entities for discovery.")
        
        return entity_mappings
    
    def _discover_works(self):
        print("\nDiscovering new works using linkage data (in chunks)...")
        
        self.repository.create_affiliation_discovery_table()
        
        print("Processing affiliation-based discovery...")
        total_discoveries = self.discovery_service.discover_by_affiliation()
        
        if total_discoveries > 0:
            print(f"-> Discovered {total_discoveries} new work entries.")
        
        print(f"\nFinished discovering works. Total discoveries: {total_discoveries}")
    
    def _entity_discovery(self, entity_mappings):
        print("\nDiscovering works using extracted entities...")
        
        entity_count = self.discovery_service.discover_by_entities(entity_mappings)
        print(f"Entity-based discovery found {entity_count} new work entries.")
    
    def _combine_results(self):
        print(f"\nGenerating combined discovered works list with match types...")
        
        print("Exporting linkage results to CSV...")
        self.repository.export_linkage_results_to_csv(self.linkage_output_file)
        
        combined_query, has_affiliation, has_entity = self.discovery_service.combine_and_deduplicate(None)
        
        if not combined_query:
            print("No discovery logs were created. Skipping deduplication.")
            return
        
        safe_discovery_output_file = sanitize_file_path_for_sql(self.discovery_output_file, is_output=True)
        
        sql_query_combined_works = f"""
        COPY (
            SELECT DISTINCT * FROM ({combined_query})
            ORDER BY match_type, doi, author
        ) TO '{safe_discovery_output_file}' (HEADER, DELIMITER ',');
        """
        self.db.execute(sql_query_combined_works)
        
        counts = self.discovery_service.get_discovery_counts(combined_query)
        
        print(f"-> Combined discovered works saved to '{self.discovery_output_file}'")
        for match_type, count in counts:
            print(f"   - {match_type}: {count} unique works")
    
    def _print_summary(self):
        print("\nProcessing complete.")
        print(f"-> Linkage results saved to '{self.linkage_output_file}'")
        
        if os.path.exists(self.full_log_output_file):
            print(f"-> Full discovery log saved to '{self.full_log_output_file}'")
        else:
            print("-> Discovery results processed in memory (intermediate log not saved)")
            
        if self.entity_extraction_enabled and os.path.exists(self.entity_mappings_file):
            print(f"-> Entity mappings saved to '{self.entity_mappings_file}'")
        print(f"-> Combined discovered works (with match types) saved to '{self.discovery_output_file}'")


class AffiliationSearchProcessor:
    
    def __init__(self, db_manager: DatabaseManager, config: dict):
        self.db = db_manager
        self.config = config
    
    def run(self, input_file: str, output_file: str):
        print(f"--- Running in Batch Affiliation Search Mode ---")
        
        if not os.path.exists(self.db.db_file):
            sys.exit("Error: Database file not found.")
        if not os.path.exists(input_file):
            sys.exit(f"Error: Input file not found at '{input_file}'")
        
        try:
            search_col = self.config['affiliation_search_columns']['affiliation_name']
        except KeyError:
            sys.exit("Error: Config file must contain affiliation_search_columns.affiliation_name")
        
        try:
            self.db.create_function('normalize_affiliation_udf', normalize_text, [str], str)
            additional_valid_cols = {search_col}
            validated_search_col = validate_column_name(search_col, additional_valid_cols)
            escaped_search_col = validated_search_col.replace('"', '""')
            
            safe_input_file = sanitize_file_path_for_sql(input_file, is_output=False)
            safe_output_file = sanitize_file_path_for_sql(output_file, is_output=True)
            
            sql_query = f"""
            COPY (
                WITH input_data AS (
                    SELECT *, normalize_affiliation_udf("{escaped_search_col}") AS normalized_search_key
                    FROM read_csv_auto('{safe_input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
                )
                SELECT
                    inp."{escaped_search_col}" AS input_search_term,
                    ref.work_id AS ref_work_id, ref.doi AS ref_doi,
                    ref.author_name AS ref_author_name,
                    ref.normalized_affiliation_name AS ref_affiliation,
                    ref.normalized_affiliation_key AS ref_affiliation_normalized_key
                FROM {TABLE_AUTHOR_REFERENCES} AS ref 
                JOIN input_data AS inp ON ref.normalized_affiliation_key = inp.normalized_search_key
                ORDER BY input_search_term, ref.doi, ref.author_name
            ) TO '{safe_output_file}' (HEADER, DELIMITER ',');
            """
            
            print(f"Searching for affiliations from '{input_file}'...")
            self.db.execute(sql_query)
            print(f"Search complete. Enriched results saved to '{output_file}'.")
            
        except Exception as e:
            sys.exit(f"An error occurred during search: {e}")