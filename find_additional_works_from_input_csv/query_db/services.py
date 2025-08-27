
import os
import csv
import logging
import pandas as pd

from query_db.analysis.name_matching import are_names_similar
from query_db.utils import extract_doi, normalize_text, is_likely_acronym
from query_db.constants import *


class LinkageService:
    def __init__(self, repository, config):
        self.repository = repository
        self.config = config
        self._setup_config()

    def _setup_config(self):
        self.organization_names = self.config.get('organization_names', [])
        self.normalized_org_names = [normalize_text(
            name) for name in self.organization_names]
        self.input_name_style = self.config.get('input_name_style', 'auto')
        self.reference_name_style = self.config.get(
            'reference_name_style', 'first_last')
        self.matching_threshold = self.config.get(
            'name_matching_threshold', DEFAULT_NAME_THRESHOLD)

    def find_linkages_udf(self, chunk_df, input_doi_col=None, input_work_id_col=None,
                          authors_col=None, author_sep=''):
        if not authors_col:
            raise ValueError("authors_col parameter is required")

        try:
            matched_results = self.repository.query_authors_for_linkage_udf(
                chunk_df=chunk_df,
                input_doi_col=input_doi_col,
                input_work_id_col=input_work_id_col,
                authors_col=authors_col,
                author_sep=author_sep,
                input_name_style=self.input_name_style,
                reference_name_style=self.reference_name_style,
                name_threshold=self.matching_threshold
            )

            matches_in_chunk = []

            for row in matched_results:
                input_doi, input_work_id, input_author, ref_author_name, ref_affiliation = row

                status = self._determine_linkage_status(ref_affiliation)

                matches_in_chunk.append({
                    'input_doi': input_doi or '',
                    'input_work_id': input_work_id or '',
                    'input_author_name': input_author.strip() if input_author else '',
                    'ref_author_name': ref_author_name or '',
                    'ref_affiliation': ref_affiliation or '',
                    'linkage_status': status
                })

            return matches_in_chunk

        except Exception as e:
            raise RuntimeError(f"Failed to find linkages using UDF: {e}")

    def _determine_linkage_status(self, affiliation):
        if not self.organization_names or not self.normalized_org_names:
            return STATUS_FIRST_AVAILABLE

        if affiliation:
            normalized_affiliation = normalize_text(affiliation)
            for org_name in self.normalized_org_names:
                if org_name in normalized_affiliation:
                    return STATUS_ORG_MATCH

        return STATUS_NAME_MATCH_NO_ORG


class DiscoveryService:
    def __init__(self, repository, config):
        self.repository = repository
        self.config = config
        self._setup_config()

    def _setup_config(self):
        self.organization_names = self.config.get('organization_names', [])
        self.entity_matching_threshold = self.config.get(
            'entity_matching_threshold', DEFAULT_ENTITY_THRESHOLD)
        self.use_entity_discovery = self.config.get(
            'use_entity_discovery', True)

    def discover_by_affiliation(self, linkage_table_name: str = TEMP_TABLE_LINKAGE_RESULTS, exclude_ids_view=TEMP_VIEW_UNIQUE_IDS):
        try:
            discovered_count = self.repository.discover_works_by_affiliation(
                linkage_table_name, exclude_ids_view)
            return discovered_count

        except Exception as e:
            raise RuntimeError(f"Failed to discover works by affiliation: {e}")

    def discover_by_entities(self, entity_mappings, exclude_ids_view=TEMP_VIEW_UNIQUE_IDS):
        if not entity_mappings or not self.organization_names:
            return 0

        try:
            entity_to_sources = self._process_entity_mappings(entity_mappings)

            if not entity_to_sources:
                return 0

            num_entities = self.repository.create_entity_keys_table(
                entity_to_sources)

            discovered_count = self.repository.discover_works_by_entities(
                TEMP_TABLE_ENTITY_KEYS, self.organization_names, exclude_ids_view
            )

            return discovered_count

        except Exception as e:
            raise RuntimeError(f"Failed to discover works by entities: {e}")

    def _process_entity_mappings(self, entity_mappings):
        if not entity_mappings or not self.organization_names:
            return {}

        try:
            filtered_entities = []
            entity_source_map = {}

            for entity_text, source_affiliation in entity_mappings:
                if (not is_likely_acronym(entity_text) and len(entity_text) > 15 and len(normalize_text(entity_text)) > 15):
                    filtered_entities.append((entity_text, source_affiliation))

                if entity_text not in entity_source_map:
                    entity_source_map[entity_text] = []
                entity_source_map[entity_text].append(source_affiliation)

            if not filtered_entities:
                return {}

            threshold = self.entity_matching_threshold / 100.0

            validated_entities = self.repository.validate_entities_in_db(
                filtered_entities, self.organization_names, threshold
            )

            entity_to_sources = {}

            for entity_text, validation_info in validated_entities.items():
                normalized_entity = normalize_text(entity_text)

                if entity_text in entity_source_map:
                    entity_to_sources[normalized_entity] = entity_source_map[entity_text]

            return entity_to_sources

        except Exception as e:
            logging.error(f"Error processing entity mappings: {e}")
            return {}

    def combine_and_deduplicate(self, log_file=None, exclude_ids_view=TEMP_VIEW_UNIQUE_IDS):
        try:
            result = self.repository.combine_discovered_works(
                log_file, exclude_ids_view)
            combined_query, has_affiliation, has_entity = result

            return combined_query, has_affiliation, has_entity

        except Exception as e:
            raise RuntimeError(f"Failed to combine and deduplicate results: {e}")

    def get_discovery_counts(self, combined_query):
        if not combined_query:
            return []

        try:
            return self.repository.get_match_type_counts(combined_query)

        except Exception as e:
            raise RuntimeError(f"Failed to get discovery counts: {e}")
