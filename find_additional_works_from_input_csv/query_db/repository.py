"""Repository class for all database operations related to author references."""

import pandas as pd
from query_db.db import DatabaseManager
from query_db.constants import *
from query_db.utils import extract_doi, normalize_text, sanitize_file_path_for_sql, validate_column_name


class AuthorReferencesRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def create_input_ids_table(self, input_file: str, chunk_size: int, 
                              input_doi_col: str = None, input_work_id_col: str = None):
        if not input_doi_col and not input_work_id_col:
            raise ValueError("Either input_doi_col or input_work_id_col must be specified")
        
        try:
            safe_input_file = sanitize_file_path_for_sql(input_file, is_output=False)
            
            id_cols = []
            additional_valid_columns = set()
            
            if input_doi_col:
                additional_valid_columns.add(input_doi_col)
                validated_doi_col = validate_column_name(input_doi_col, additional_valid_columns)
                id_cols.append(f'"{validated_doi_col}" AS doi')
            else:
                id_cols.append('NULL AS doi')
            
            if input_work_id_col:
                additional_valid_columns.add(input_work_id_col)
                validated_work_id_col = validate_column_name(input_work_id_col, additional_valid_columns)
                id_cols.append(f'"{validated_work_id_col}" AS work_id')
            else:
                id_cols.append('NULL AS work_id')
            
            select_clause = ', '.join(id_cols)
            
            query = f"""
            CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_INPUT_IDS} AS
            SELECT DISTINCT {select_clause}
            FROM read_csv_auto('{safe_input_file}', HEADER=TRUE, ALL_VARCHAR=TRUE);
            """
            self.db.execute(query)
            
            self.db.execute(f"CREATE OR REPLACE TEMP VIEW {TEMP_VIEW_UNIQUE_IDS} AS SELECT * FROM {TEMP_TABLE_INPUT_IDS};")
            
            return self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_VIEW_UNIQUE_IDS};")[0]
            
        except Exception as e:
            raise RuntimeError(f"Failed to create input IDs table: {e}")
    
    def query_authors_for_linkage_udf(self, chunk_df: pd.DataFrame, input_doi_col: str = None, 
                                     input_work_id_col: str = None, authors_col: str = None, 
                                     author_sep: str = '', input_name_style: str = 'first_last',
                                     reference_name_style: str = 'first_last', name_threshold: float = 0.85):
        if not authors_col:
            raise ValueError("authors_col must be specified")
        
        try:
            processed_chunk_df = chunk_df.copy()
            
            if input_doi_col and input_doi_col in processed_chunk_df.columns:
                processed_chunk_df['clean_doi'] = processed_chunk_df[input_doi_col].apply(
                    lambda x: extract_doi(x) if x else None
                )
            else:
                processed_chunk_df['clean_doi'] = None
            
            temp_input_table = "temp_input_chunk_udf"
            self.db.register_df(temp_input_table, processed_chunk_df)
            
            id_selection = []
            id_selection.append('clean_doi AS input_doi')
            
            if input_work_id_col: 
                id_selection.append(f'"{input_work_id_col}" AS input_work_id')
            else: 
                id_selection.append('NULL AS input_work_id')
            
            additional_valid_cols = {authors_col}
            if input_doi_col:
                additional_valid_cols.add(input_doi_col)
            if input_work_id_col:
                additional_valid_cols.add(input_work_id_col)
            validated_authors_col = validate_column_name(authors_col, additional_valid_cols)
            escaped_authors_col = validated_authors_col.replace('"', '""')
            escaped_input_style = input_name_style.replace("'", "''")
            escaped_ref_style = reference_name_style.replace("'", "''")
            
            author_filter = f'WHERE "{escaped_authors_col}" IS NOT NULL AND trim("{escaped_authors_col}") != \'\''
            
            if author_sep == '':
                input_authors_subquery = f"""
                    SELECT DISTINCT 
                        {', '.join(id_selection)}, 
                        trim("{escaped_authors_col}") AS input_author
                    FROM {temp_input_table} 
                    {author_filter}
                """
            else:
                escaped_separator = author_sep.replace('$', '$$')
                input_authors_subquery = f"""
                    SELECT DISTINCT 
                        {', '.join(id_selection)},
                        trim(UNNEST(string_split(trim("{escaped_authors_col}"), $${escaped_separator}$$))) AS input_author
                    FROM {temp_input_table} 
                    {author_filter}
                """
            
            udf_linkage_query = f"""
                SELECT DISTINCT
                    inp.input_doi,
                    inp.input_work_id, 
                    inp.input_author,
                    ref.author_name AS ref_author_name,
                    ref.normalized_affiliation_name AS ref_affiliation
                FROM ({input_authors_subquery}) AS inp
                JOIN {TABLE_AUTHOR_REFERENCES} AS ref ON (
                    -- Match by DOI if both are available and not empty
                    (inp.input_doi IS NOT NULL AND inp.input_doi != '' 
                     AND ref.doi IS NOT NULL AND ref.doi != ''
                     AND inp.input_doi = ref.doi)
                    OR
                    -- Match by work_id if both are available and not empty  
                    (inp.input_work_id IS NOT NULL AND inp.input_work_id != ''
                     AND ref.work_id IS NOT NULL AND ref.work_id != ''
                     AND inp.input_work_id = ref.work_id)
                ) 
                AND are_names_similar_udf(
                    inp.input_author, 
                    ref.author_name, 
                    '{escaped_input_style}', 
                    '{escaped_ref_style}', 
                    {name_threshold}
                )
                WHERE inp.input_author IS NOT NULL 
                AND inp.input_author != ''
                AND ref.author_name IS NOT NULL 
                AND ref.author_name != ''
            """
            
            return self.db.query(udf_linkage_query)
            
        except Exception as e:
            raise RuntimeError(f"Failed to query authors for linkage using UDF: {e}")
    
    def create_linkage_results_table(self):
        """Creates the temporary table to store linkage results."""
        self.db.execute(f"""
            CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_LINKAGE_RESULTS} (
                input_doi VARCHAR,
                input_work_id VARCHAR,
                input_author_name VARCHAR,
                ref_author_name VARCHAR,
                ref_affiliation VARCHAR,
                linkage_status VARCHAR
            )
        """)
    
    def insert_linkage_results(self, linkage_data: list):
        """Bulk inserts linkage results into the temporary table."""
        if not linkage_data:
            return
        linkage_df = pd.DataFrame(linkage_data)
        self.db.register_df('linkage_chunk_df', linkage_df)
        self.db.execute(f"INSERT INTO {TEMP_TABLE_LINKAGE_RESULTS} SELECT * FROM linkage_chunk_df")
    
    def export_linkage_results_to_csv(self, output_file: str):
        """Exports linkage results from temp table to CSV file.
        
        Args:
            output_file: Path to output CSV file for linkage results
            
        Raises:
            RuntimeError: If export fails
        """
        try:
            safe_output_file = sanitize_file_path_for_sql(output_file, is_output=True)
            
            export_query = f"""
                COPY (
                    SELECT * FROM {TEMP_TABLE_LINKAGE_RESULTS}
                    ORDER BY input_doi, input_work_id, input_author_name
                ) TO '{safe_output_file}' (HEADER, DELIMITER ',')
            """
            
            self.db.execute(export_query)
            
        except Exception as e:
            raise RuntimeError(f"Failed to export linkage results to CSV: {e}")
    
    def get_unique_affiliations_for_entity_extraction(self):
        """Get unique org-matched affiliations from temp linkage table for entity extraction.
            
        Returns:
            pd.DataFrame: DataFrame with columns ['input_doi', 'ref_affiliation']
        """
        try:
            query = f"""
                SELECT DISTINCT input_doi, ref_affiliation 
                FROM {TEMP_TABLE_LINKAGE_RESULTS}
                WHERE ref_affiliation IS NOT NULL AND ref_affiliation != ''
                AND input_doi IS NOT NULL AND input_doi != ''
                AND linkage_status = '{STATUS_ORG_MATCH}'
            """
            
            return self.db.query_df(query)
            
        except Exception as e:
            raise RuntimeError(f"Failed to get unique affiliations: {e}")
    
    def get_original_affiliation_names(self, normalized_affiliations: list):
        """Get original affiliation names from normalized affiliation names.
        
        Args:
            normalized_affiliations: List of normalized affiliation names
            
        Returns:
            dict: Mapping from normalized to original affiliation names
        """
        if not normalized_affiliations:
            return {}
        
        try:
            placeholders = ', '.join(['?' for _ in normalized_affiliations])
            
            affiliations_df = self.db.query_df(f"""
                SELECT DISTINCT 
                    normalized_affiliation_name,
                    FIRST(affiliation_name) as original_affiliation
                FROM {TABLE_AUTHOR_REFERENCES}
                WHERE normalized_affiliation_name IN ({placeholders})
                AND affiliation_name IS NOT NULL
                GROUP BY normalized_affiliation_name
            """, normalized_affiliations)
            
            return dict(zip(
                affiliations_df['normalized_affiliation_name'],
                affiliations_df['original_affiliation']
            ))
            
        except Exception as e:
            raise RuntimeError(f"Failed to get original affiliation names: {e}")
    
    def create_entity_keys_table(self, entity_to_sources: dict):
        """Create temporary table with entity keys and their source affiliations.
        
        Args:
            entity_to_sources: Dictionary mapping entity keys to source affiliations
            
        Returns:
            int: Number of entities inserted
        """
        if not entity_to_sources:
            return 0
        
        try:
            self.db.execute(f"CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ENTITY_KEYS} (entity_key VARCHAR, source_affiliations VARCHAR)")

            records = []
            for entity, sources in entity_to_sources.items():
                source_affiliation = sources[0] if isinstance(sources, list) else sources
                records.append({'entity_key': entity, 'source_affiliations': source_affiliation})

            entities_df = pd.DataFrame(records)
            self.db.register_df('entities_df', entities_df)

            self.db.execute(f"INSERT INTO {TEMP_TABLE_ENTITY_KEYS} SELECT entity_key, source_affiliations FROM entities_df")
            
            return len(entity_to_sources)
            
        except Exception as e:
            raise RuntimeError(f"Failed to create entity keys table: {e}")
    
    def create_affiliation_discovery_table(self):
        try:
            self.db.execute(f"""
                CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_AFFILIATION_DISCOVERED} (
                    input_doi VARCHAR,
                    input_work_id VARCHAR,
                    input_author_name VARCHAR,
                    linking_affiliation VARCHAR,
                    discovered_work_id VARCHAR,
                    discovered_doi VARCHAR,
                    discovered_author VARCHAR,
                    discovered_author_affiliation VARCHAR,
                    discovered_ror_id VARCHAR
                )
            """)
        except Exception as e:
            raise RuntimeError(f"Failed to create affiliation discovery table: {e}")
    
    def discover_works_by_affiliation(self, linkage_table_name: str = TEMP_TABLE_LINKAGE_RESULTS, exclude_ids_view: str = TEMP_VIEW_UNIQUE_IDS):
        try:
            query = f"""
            INSERT INTO {TEMP_TABLE_AFFILIATION_DISCOVERED}
            SELECT
                ld.input_doi, ld.input_work_id, ld.input_author_name,
                ld.ref_affiliation AS linking_affiliation,
                collab.work_id AS discovered_work_id, collab.doi AS discovered_doi,
                collab.author_name AS discovered_author,
                collab.affiliation_name AS discovered_author_affiliation,
                collab.affiliation_ror AS discovered_ror_id
            FROM {linkage_table_name} AS ld
            JOIN {TABLE_AUTHOR_REFERENCES} AS collab 
                ON lower(trim(ld.ref_affiliation)) = collab.normalized_affiliation_key
            LEFT JOIN {exclude_ids_view} AS exclude_ids 
                ON (collab.doi = exclude_ids.doi AND collab.doi IS NOT NULL AND exclude_ids.doi IS NOT NULL) 
                OR (CAST(collab.work_id AS VARCHAR) = CAST(exclude_ids.work_id AS VARCHAR) AND collab.work_id IS NOT NULL AND exclude_ids.work_id IS NOT NULL)
            WHERE (ld.linkage_status = '{STATUS_ORG_MATCH}' OR ld.linkage_status = '{STATUS_FIRST_AVAILABLE}')
            AND (exclude_ids.doi IS NULL AND exclude_ids.work_id IS NULL)
            """

            before_count = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}")
            before_count = before_count[0] if before_count else 0
            
            self.db.execute(query)

            after_count = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}")
            after_count = after_count[0] if after_count else 0
            
            return after_count - before_count
            
        except Exception as e:
            raise RuntimeError(f"Failed to discover works by affiliation: {e}")
    
    def discover_works_by_entities(self, entity_keys_table: str, org_names: list, 
                                  exclude_ids_view: str = TEMP_VIEW_UNIQUE_IDS):
        if not org_names:
            return 0
        
        try:
            temp_known_orgs = "temp_known_orgs"
            self.db.execute(f"CREATE OR REPLACE TEMP TABLE {temp_known_orgs} (name VARCHAR)")
            
            for org_name in org_names:
                escaped_org = org_name.lower().replace("'", "''")
                self.db.execute(f"INSERT INTO {temp_known_orgs} VALUES (?)", (escaped_org,))

            self.db.execute(f"""
                CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ALREADY_DISCOVERED} AS
                SELECT DISTINCT discovered_work_id, discovered_doi
                FROM (
                    SELECT NULL as discovered_work_id, NULL as discovered_doi
                    WHERE FALSE  -- Empty table if no discovery log exists
                )
            """)

            entity_discovery_query = f"""
                CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ENTITY_DISCOVERED} AS
                SELECT DISTINCT
                    ek.source_affiliations AS source_embl_affiliation,
                    ek.entity_key AS extracted_entity,
                    ar.work_id AS discovered_work_id,
                    ar.doi AS discovered_doi,
                    ar.author_name AS discovered_author,
                    ar.affiliation_name AS discovered_author_affiliation,
                    ar.normalized_affiliation_name AS discovered_normalized_affiliation,
                    ar.affiliation_ror AS discovered_ror_id
                FROM {entity_keys_table} AS ek
                JOIN {TABLE_AUTHOR_REFERENCES} AS ar 
                    ON ar.normalized_affiliation_key LIKE '%' || ek.entity_key || '%'
                    AND EXISTS (
                        SELECT 1 FROM {temp_known_orgs} AS org_names
                        WHERE ar.normalized_affiliation_key LIKE '%' || org_names.name || '%'
                    )
                LEFT JOIN {TEMP_TABLE_ALREADY_DISCOVERED} adw
                    ON (CAST(ar.work_id AS VARCHAR) = CAST(adw.discovered_work_id AS VARCHAR) AND ar.work_id IS NOT NULL)
                    OR (ar.doi = adw.discovered_doi AND ar.doi IS NOT NULL)
                LEFT JOIN {exclude_ids_view} AS exclude_ids 
                    ON (ar.doi = exclude_ids.doi AND ar.doi IS NOT NULL AND exclude_ids.doi IS NOT NULL) 
                    OR (CAST(ar.work_id AS VARCHAR) = CAST(exclude_ids.work_id AS VARCHAR) AND ar.work_id IS NOT NULL AND exclude_ids.work_id IS NOT NULL)
                WHERE COALESCE(adw.discovered_work_id, adw.discovered_doi) IS NULL
                AND (exclude_ids.doi IS NULL AND exclude_ids.work_id IS NULL)
                ORDER BY ar.work_id, ar.doi
            """
            
            self.db.execute(entity_discovery_query)

            return self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_ENTITY_DISCOVERED}")[0]
            
        except Exception as e:
            raise RuntimeError(f"Failed to discover works by entities: {e}")
    
    def update_already_discovered_from_log(self, log_file: str):
        try:
            safe_log_file = sanitize_file_path_for_sql(log_file, is_output=False)
            
            self.db.execute(f"""
                CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ALREADY_DISCOVERED} AS
                SELECT DISTINCT discovered_work_id, discovered_doi
                FROM read_csv_auto('{safe_log_file}', HEADER=TRUE, ALL_VARCHAR=TRUE)
            """)
        except Exception as e:
            raise RuntimeError(f"Failed to update already discovered table: {e}")
    
    def update_already_discovered_from_temp_tables(self):
        try:
            union_parts = []

            try:
                affiliation_check = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}")[0]
                if affiliation_check > 0:
                    union_parts.append(f"""
                        SELECT DISTINCT discovered_work_id, discovered_doi
                        FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}
                    """)
            except:
                pass
            
            try:
                entity_check = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_ENTITY_DISCOVERED}")[0]
                if entity_check > 0:
                    union_parts.append(f"""
                        SELECT DISTINCT discovered_work_id, discovered_doi
                        FROM {TEMP_TABLE_ENTITY_DISCOVERED}
                    """)
            except:
                pass
            
            if union_parts:
                union_query = " UNION ".join(union_parts)
                self.db.execute(f"""
                    CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ALREADY_DISCOVERED} AS
                    {union_query}
                """)
            else:
                self.db.execute(f"""
                    CREATE OR REPLACE TEMP TABLE {TEMP_TABLE_ALREADY_DISCOVERED} AS
                    SELECT NULL as discovered_work_id, NULL as discovered_doi
                    WHERE FALSE
                """)
                
        except Exception as e:
            raise RuntimeError(f"Failed to update already discovered table from temp tables: {e}")
    
    def combine_discovered_works(self, log_file: str = None, exclude_ids_view: str = TEMP_VIEW_UNIQUE_IDS):
        try:
            has_standard_discovery = False
            has_entity_discovery = False
            
            try:
                affiliation_check = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}")[0]
                has_standard_discovery = affiliation_check > 0
            except:
                has_standard_discovery = False
            
            try:
                entity_check = self.db.query_one(f"SELECT COUNT(*) FROM {TEMP_TABLE_ENTITY_DISCOVERED}")[0]
                has_entity_discovery = entity_check > 0
            except:
                has_entity_discovery = False
            
            if not has_standard_discovery and not has_entity_discovery:
                return None, False, False
            
            union_parts = []
            
            if has_standard_discovery:
                union_parts.append(f"""
                    SELECT 
                        discovered_work_id AS work_id, 
                        discovered_doi AS doi,
                        discovered_author AS author,
                        discovered_author_affiliation AS author_affiliation,
                        discovered_ror_id AS ror_id,
                        linking_affiliation AS matching_affiliation,
                        '{MATCH_TYPE_AFFILIATION}' AS match_type
                    FROM {TEMP_TABLE_AFFILIATION_DISCOVERED}
                """)
            
            if has_entity_discovery:
                union_parts.append(f"""
                    SELECT 
                        discovered_work_id AS work_id, 
                        discovered_doi AS doi,
                        discovered_author AS author,
                        discovered_author_affiliation AS author_affiliation,
                        discovered_ror_id AS ror_id,
                        extracted_entity AS matching_affiliation,
                        '{MATCH_TYPE_ENTITY}' AS match_type
                    FROM {TEMP_TABLE_ENTITY_DISCOVERED}
                """)
            
            if len(union_parts) == 1:
                combined_query = union_parts[0]
            else:
                union_query = " UNION ALL ".join(union_parts)
                combined_query = f"""
                    WITH all_discoveries AS (
                        {union_query}
                    ),
                    prioritized AS (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY work_id, doi, author 
                                   ORDER BY CASE match_type 
                                       WHEN '{MATCH_TYPE_AFFILIATION}' THEN 1 
                                       WHEN '{MATCH_TYPE_ENTITY}' THEN 2 
                                   END
                               ) as priority
                        FROM all_discoveries
                    )
                    SELECT work_id, doi, author, author_affiliation, ror_id, matching_affiliation, match_type
                    FROM prioritized
                    WHERE priority = 1
                """
            
            return combined_query, has_standard_discovery, has_entity_discovery
            
        except Exception as e:
            raise RuntimeError(f"Failed to combine discovered works: {e}")
    
    def get_match_type_counts(self, combined_query: str):
        try:
            count_query = f"""
                SELECT match_type, COUNT(DISTINCT COALESCE(work_id, doi)) as work_count
                FROM ({combined_query})
                GROUP BY match_type
            """
            return self.db.query(count_query)
        except Exception as e:
            raise RuntimeError(f"Failed to get match type counts: {e}")
    
    def validate_entities_in_db(self, extracted_entities: list, organization_names: list, threshold: float = 0.85) -> dict:
        if not extracted_entities:
            raise ValueError("extracted_entities cannot be empty")
        if not organization_names:
            raise ValueError("organization_names cannot be empty")
        if not (0.0 <= threshold <= 1.0):
            raise ValueError("threshold must be between 0.0 and 1.0")
        
        try:
            temp_entities_table = "temp_extracted_entities"
            self.db.execute(f"""
                CREATE OR REPLACE TEMP TABLE {temp_entities_table} (
                    entity_text VARCHAR,
                    source_affiliation VARCHAR
                )
            """)
            
            temp_orgs_table = "temp_org_names"
            self.db.execute(f"""
                CREATE OR REPLACE TEMP TABLE {temp_orgs_table} (
                    org_name VARCHAR
                )
            """)
            
            entities_df = pd.DataFrame(extracted_entities, columns=['entity_text', 'source_affiliation'])
            
            self.db.register_df('temp_entities_view', entities_df)
            
            self.db.execute(f"""
                INSERT INTO {temp_entities_table} 
                SELECT entity_text, source_affiliation FROM temp_entities_view
            """)
            
            orgs_df = pd.DataFrame(organization_names, columns=['org_name'])

            self.db.register_df('temp_orgs_view', orgs_df)
            
            self.db.execute(f"""
                INSERT INTO {temp_orgs_table} 
                SELECT org_name FROM temp_orgs_view
            """)

            matching_query = f"""
                SELECT DISTINCT 
                    ee.entity_text,
                    ee.source_affiliation,
                    o.org_name as matched_org,
                    partial_ratio_udf(ee.entity_text, o.org_name) as score
                FROM {temp_entities_table} ee
                CROSS JOIN {temp_orgs_table} o
                WHERE partial_ratio_udf(ee.entity_text, o.org_name) > ?
                ORDER BY ee.entity_text, score DESC
            """
            
            results = self.db.query(matching_query, (threshold,))

            validated_entities = {}
            for row in results:
                entity_text, source_affiliation, matched_org, score = row

                if entity_text not in validated_entities:
                    validated_entities[entity_text] = {
                        'source_affiliation': source_affiliation,
                        'matched_org': matched_org,
                        'score': float(score)
                    }
            
            return validated_entities
            
        except Exception as e:
            raise RuntimeError(f"Failed to validate entities in database: {e}")