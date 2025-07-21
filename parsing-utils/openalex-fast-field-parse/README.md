# OpenAlex Fast Field Parser

Tool for efficiently extracting field-level data from OpenAlex works data files.

## Usage

```bash
openalex-fast-field-parse -i <input_dir> -f <fields> [-o <output>]
```

## Required Arguments

- `-i, --input` - Directory containing JSONL.gz files
- `-f, --fields` - Comma-separated fields to extract (e.g., `authorships.author.display_name,title,ids.pmid`)

## Optional Arguments

- `-o, --output` - Output CSV file or directory (default: `field_data.csv`)
- `-g, --organize` - Organize output by source ID into separate files
- `--source-id` - Filter by specific OpenAlex source ID
- `--doi-prefix` - Filter by DOI prefix
- `-t, --threads` - Number of threads (0 for auto-detect)
- `-b, --batch-size` - Records per batch (default: 10000)
- `-l, --log-level` - Logging level: DEBUG, INFO, WARN, ERROR (default: INFO)
- `--max-open-files` - Max open files when organizing (default: 100)

## Examples

Extract author names and titles:
```bash
openalex-fast-field-parse -i /data/openalex -f "authorships.author.display_name,title" -o authors.csv
```

Organize by source with filtering:
```bash
openalex-fast-field-parse -i /data/openalex -f "doi,publication_year,cited_by_count" --organize -o output_dir/ --source-id S12345678
```

## Output Format

CSV with columns:
- `doi` - Document DOI
- `field_name` - Requested field name
- `subfield_path` - Full path including array indices
- `value` - Extracted value
- `source_id` - OpenAlex source ID
- `doi_prefix` - DOI prefix
- `source_file_path` - Source file path

## Available Fields

All OpenAlex metadata fields can be extracted using dot notation. Below are the available fields:

### Basic Metadata
- `id` - OpenAlex ID
- `doi` - Digital Object Identifier
- `doi_registration_agency` - DOI registration agency
- `display_name` - Display name
- `title` - Work title
- `publication_year` - Publication year
- `publication_date` - Publication date
- `language` - Language code
- `language_id` - Language ID
- `type` - Work type
- `type_id` - Work type ID
- `type_crossref` - Crossref work type
- `is_retracted` - Retraction status
- `is_paratext` - Paratext status
- `cited_by_count` - Citation count
- `countries_distinct_count` - Distinct countries count
- `institutions_distinct_count` - Distinct institutions count
- `locations_count` - Number of locations
- `referenced_works_count` - Number of referenced works
- `authors_count` - Number of authors
- `concepts_count` - Number of concepts
- `topics_count` - Number of topics
- `has_fulltext` - Fulltext availability
- `cited_by_api_url` - Cited by API URL
- `updated_date` - Last update date
- `created_date` - Creation date
- `updated` - Timestamp of last update

### Identifiers
- `ids` - Identifiers object
- `ids.openalex` - OpenAlex ID
- `ids.mag` - Microsoft Academic Graph ID
- `ids.pmid` - PubMed ID

### Locations
Location fields are available for `primary_location`, `best_oa_location`, and `locations`:
- `primary_location.is_oa` - Open access status
- `primary_location.version` - Version
- `primary_location.license` - License
- `primary_location.doi` - Location DOI
- `primary_location.is_accepted` - Accepted version status
- `primary_location.is_published` - Published version status
- `primary_location.pdf_url` - PDF URL
- `primary_location.landing_page_url` - Landing page URL
- `primary_location.source` - Source information
- `primary_location.source.id` - Source ID
- `primary_location.source.issn_l` - Linking ISSN
- `primary_location.source.issn` - ISSN list
- `primary_location.source.display_name` - Source display name
- `primary_location.source.publisher` - Publisher
- `primary_location.source.host_organization` - Host organization ID
- `primary_location.source.host_organization_name` - Host organization name
- `primary_location.source.is_oa` - Source OA status
- `primary_location.source.is_in_doaj` - DOAJ inclusion
- `primary_location.source.type` - Source type
- `primary_location.source.type_id` - Source type ID
- `primary_location.source.host_organization_lineage` - Organization hierarchy
- `primary_location.source.host_organization_lineage_names` - Organization names

### Open Access
- `open_access` - Open access information
- `open_access.is_oa` - OA status
- `open_access.oa_status` - OA status type
- `open_access.oa_url` - OA URL
- `open_access.any_repository_has_fulltext` - Repository fulltext availability

### Authorships
- `authorships` - Author list (array)
- `authorships.author_position` - Author position
- `authorships.is_corresponding` - Corresponding author status
- `authorships.raw_author_name` - Raw author name
- `authorships.raw_affiliation_string` - Raw affiliation string
- `authorships.raw_affiliation_strings` - Raw affiliation strings (array)
- `authorships.countries` - Countries (array)
- `authorships.country_ids` - Country IDs (array)
- `authorships.author` - Author information
- `authorships.author.id` - Author ID
- `authorships.author.display_name` - Author display name
- `authorships.author.orcid` - ORCID
- `authorships.institutions` - Institutions (array)
- `authorships.institutions.id` - Institution ID
- `authorships.institutions.display_name` - Institution name
- `authorships.institutions.ror` - ROR ID
- `authorships.institutions.country_code` - Country code
- `authorships.institutions.type` - Institution type
- `authorships.institutions.lineage` - Institution hierarchy
- `corresponding_author_ids` - Corresponding author IDs (array)
- `corresponding_institution_ids` - Corresponding institution IDs (array)

### References and Citations
- `referenced_works` - Referenced work IDs (array)
- `related_works` - Related work IDs (array)
- `indexed_in` - Indexes (array)
- `summary_stats` - Summary statistics
- `summary_stats.cited_by_count` - Citation count
- `summary_stats.2yr_cited_by_count` - 2-year citation count

### Bibliographic Data
- `biblio` - Bibliographic information
- `biblio.volume` - Volume
- `biblio.issue` - Issue
- `biblio.first_page` - First page
- `biblio.last_page` - Last page

### Concepts and Topics
- `concepts` - Concepts (array)
- `concepts.id` - Concept ID
- `concepts.wikidata` - Wikidata ID
- `concepts.display_name` - Concept name
- `concepts.level` - Concept level
- `concepts.score` - Concept score
- `topics` - Topics (array)
- `topics.id` - Topic ID
- `topics.display_name` - Topic name
- `topics.score` - Topic score
- `topics.subfield` - Subfield information
- `topics.subfield.id` - Subfield ID
- `topics.subfield.display_name` - Subfield name
- `topics.field` - Field information
- `topics.field.id` - Field ID
- `topics.field.display_name` - Field name
- `topics.domain` - Domain information
- `topics.domain.id` - Domain ID
- `topics.domain.display_name` - Domain name
- `primary_topic` - Primary topic
- `primary_topic.id` - Primary topic ID
- `primary_topic.display_name` - Primary topic name
- `primary_topic.score` - Primary topic score
- `primary_topic.subfield` - Primary topic subfield
- `primary_topic.field` - Primary topic field
- `primary_topic.domain` - Primary topic domain

### Medical Subject Headings
- `mesh` - MeSH terms (array)
- `mesh.is_major_topic` - Major topic status
- `mesh.descriptor_ui` - Descriptor UI
- `mesh.descriptor_name` - Descriptor name
- `mesh.qualifier_ui` - Qualifier UI
- `mesh.qualifier_name` - Qualifier name

### Keywords and SDGs
- `keywords` - Keywords (array)
- `keywords.keyword` - Keyword text
- `keywords.score` - Keyword score
- `sustainable_development_goals` - SDGs (array)
- `sustainable_development_goals.id` - SDG ID
- `sustainable_development_goals.display_name` - SDG name
- `sustainable_development_goals.score` - SDG score

### Metrics and Counts
- `counts_by_year` - Yearly citation counts (array)
- `counts_by_year.year` - Year
- `counts_by_year.cited_by_count` - Citations in year
- `cited_by_percentile_year` - Citation percentile
- `cited_by_percentile_year.min` - Minimum percentile
- `cited_by_percentile_year.max` - Maximum percentile

### Abstract
- `abstract_inverted_index` - Inverted index of abstract
- `abstract_inverted_index.*` - Word positions (dynamic keys)

### Other
- `versions` - Version information (array)
- `datasets` - Associated datasets (array)
- `grants` - Grant information (array)
- `apc_list` - Article processing charges list
- `apc_paid` - Article processing charges paid