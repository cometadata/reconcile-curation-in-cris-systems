# Crossref Fast Field Parser

Tool for efficiently extracting field-level data from Crossref's public data file.

## Usage

```bash
crossref-fast-field-parse -i <input_dir> -f <fields> [-o <output>]
```

## Required Arguments

- `-i, --input` - Directory containing JSONL.gz files
- `-f, --fields` - Comma-separated fields to extract (e.g., `author.family,title,ISSN`)

## Optional Arguments

- `-o, --output` - Output CSV file or directory (default: `field_data.csv`)
- `-g, --organize` - Organize output by member ID into separate files
- `--member` - Filter by specific member ID
- `--doi-prefix` - Filter by DOI prefix
- `-t, --threads` - Number of threads (0 for auto-detect)
- `-b, --batch-size` - Records per batch (default: 10000)
- `-l, --log-level` - Logging level: DEBUG, INFO, WARN, ERROR (default: INFO)
- `--max-open-files` - Max open files when organizing (default: 100)

## Examples

Extract author names and titles:
```bash
crossref-fast-field-parse -i /data/crossref -f "author.family,author.given,title" -o authors.csv
```

Organize by member with filtering:
```bash
crossref-fast-field-parse -i /data/crossref -f "DOI,publisher,issued.date-parts" --organize -o output_dir/ --member 78
```

## Output Format

CSV with columns:
- `doi` - Document DOI
- `field_name` - Requested field name
- `subfield_path` - Full path including array indices
- `value` - Extracted value
- `member_id` - Crossref member ID
- `doi_prefix` - DOI prefix

## Available Fields

All Crossref metadata fields can be extracted using dot notation. Below are the available fields::

### Basic Metadata
- `DOI` - Digital Object Identifier
- `URL` - Resource URL
- `title` - Article title (array)
- `subtitle` - Article subtitle (array)
- `short-title` - Short title (array)
- `original-title` - Original title (array)
- `container-title` - Journal/container title (array)
- `short-container-title` - Short container title (array)
- `abstract` - Abstract text
- `type` - Publication type
- `subtype` - Publication subtype
- `source` - Data source
- `publisher` - Publisher name
- `publisher-location` - Publisher location
- `language` - Language code
- `edition-number` - Edition number
- `volume` - Volume number
- `issue` - Issue number
- `page` - Page range
- `article-number` - Article number
- `part-number` - Part number

### Identifiers
- `ISSN` - International Standard Serial Number (array)
- `issn-type` - ISSN type information (array)
- `issn-type.type` - Type of ISSN
- `issn-type.value` - ISSN value
- `ISBN` - International Standard Book Number (array)
- `isbn-type` - ISBN type information (array)
- `isbn-type.type` - Type of ISBN
- `isbn-type.value` - ISBN value
- `alternative-id` - Alternative identifiers (array)

### Authors and Contributors
- `author` - Author list (array)
- `author.given` - Author given name
- `author.family` - Author family name
- `author.sequence` - Author sequence
- `author.affiliation` - Author affiliations (array)
- `author.affiliation.name` - Affiliation name
- `author.affiliation.department` - Department (array)
- `author.affiliation.place` - Location (array)
- `author.ORCID` - ORCID identifier
- `editor` - Editor list (array)
- `translator` - Translator list (array)
- `chair` - Chair list (array)

### Dates
- `created` - Creation date
- `created.date-parts` - Date parts (array)
- `created.date-time` - DateTime string
- `created.timestamp` - Unix timestamp
- `deposited` - Deposit date
- `indexed` - Index date
- `issued` - Issue date
- `published` - Publication date
- `published-print` - Print publication date
- `published-online` - Online publication date
- `posted` - Posted date
- `accepted` - Acceptance date
- `approved` - Approval date

### References and Citations
- `reference` - Reference list (array)
- `reference.key` - Reference key
- `reference.DOI` - Reference DOI
- `reference.author` - Reference author
- `reference.year` - Reference year
- `reference.journal-title` - Reference journal
- `reference.article-title` - Reference article title
- `reference.volume` - Reference volume
- `reference-count` - Number of references
- `references-count` - Alternative reference count
- `is-referenced-by-count` - Citation count

### Funding
- `funder` - Funder list (array)
- `funder.name` - Funder name
- `funder.DOI` - Funder DOI
- `funder.award` - Award numbers (array)

### Licensing
- `license` - License information (array)
- `license.URL` - License URL
- `license.content-version` - Content version
- `license.delay-in-days` - Embargo period

### Administrative
- `member` - Crossref member ID
- `prefix` - DOI prefix
- `score` - Relevance score
- `update-policy` - Update policy URL
- `archive` - Archive locations (array)
- `event` - Event information
- `assertion` - Assertions (array)

### Relations
- `relation` - Related works
- `relation.*` - Relation types (dynamic)
- `update-to` - Updates information (array)
- `updated-by` - Updated by information (array)

### Content and Versions
- `content-domain` - Content domain info
- `content-created` - Content creation date
- `content-updated` - Content update date
- `version` - Version information

### Clinical Trials
- `clinical-trial-number` - Clinical trial registrations (array)
- `clinical-trial-number.clinical-trial-number` - Trial number
- `clinical-trial-number.registry` - Registry name

### Projects and Grants
- `project` - Project information (array)
- `project.project-title` - Project titles (array)
- `project.funding` - Project funding (array)
- `project.investigator` - Investigators (array)
- `project.lead-investigator` - Lead investigators (array)

### Institutions
- `institution` - Institution information (array)
- `institution.name` - Institution name
- `institution.place` - Institution location (array)
- `institution.department` - Department (array)

### Standards
- `standards-body` - Standards body info
- `standards-body.name` - Standards body name
- `standards-body.acronym` - Standards body acronym

### Reviews
- `review` - Review information
- `review.type` - Review type
- `review.stage` - Review stage
- `review.recommendation` - Review recommendation

### Resource Links
- `link` - Resource links (array)
- `link.URL` - Link URL
- `link.content-type` - Content type
- `link.content-version` - Content version
- `resource` - Resource information
- `resource.primary` - Primary resource
- `resource.primary.URL` - Primary resource URL