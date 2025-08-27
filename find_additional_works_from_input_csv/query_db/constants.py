# File suffixes for output files
LINKAGE_SUFFIX = "_linkage.csv"
FULL_LOG_SUFFIX = "_full_discovery_log.csv"
DISCOVERED_WORKS_SUFFIX = "_discovered_works.csv"
ENTITY_MAPPINGS_SUFFIX = "_entity_mappings.csv"
LINKING_AFFILIATIONS_SUFFIX = "_linking_affiliations.csv"
UNMATCHED_IDS_SUFFIX = "_unmatched_ids.csv"

# Linkage status types
STATUS_ORG_MATCH = "org_match_found"
STATUS_FIRST_AVAILABLE = "first_available"
STATUS_NAME_MATCH_NO_ORG = "name_match_no_org_affiliation"

# Match types for discovered works
MATCH_TYPE_AFFILIATION = "affiliation_exact"
MATCH_TYPE_ENTITY = "entity_extracted"

# Default configuration values
DEFAULT_CHUNK_SIZE = 100000
DEFAULT_MEMORY_LIMIT = "8GB"
DEFAULT_NAME_THRESHOLD = 0.85
DEFAULT_ENTITY_THRESHOLD = 85

# Database table names
TABLE_AUTHOR_REFERENCES = "author_references"
TEMP_TABLE_INPUT_IDS = "all_input_ids"
TEMP_VIEW_UNIQUE_IDS = "unique_input_ids"
TEMP_TABLE_ENTITY_KEYS = "entity_keys"
TEMP_TABLE_ENTITY_DISCOVERED = "entity_discovered_works"
TEMP_TABLE_AFFILIATION_DISCOVERED = "affiliation_discovered_works"
TEMP_TABLE_ALREADY_DISCOVERED = "already_discovered_works"
TEMP_TABLE_KNOWN_ORGS = "temp_known_orgs"
TEMP_TABLE_LINKAGE_RESULTS = "temp_linkage_results"

# CSV field names
LINKAGE_FIELDNAMES = ['input_doi', 'input_work_id', 'input_author_name', 
                      'ref_author_name', 'ref_affiliation', 'linkage_status']

ENTITY_MAPPING_FIELDNAMES = ['normalized_affiliation', 'original_affiliation', 
                             'extracted_entities', 'all_extracted_entities']