use anyhow::{Context, Result};
use clap::Parser;
use csv::Writer;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::{DashMap, DashSet};
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde_json::Value;
use simple_logger::SimpleLogger;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use time::macros::format_description;
#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

#[derive(Parser)]
#[command(name = "OpenAlex Works Field Extractor")]
#[command(about = "Extract field data from the OpenAlex works data files in their compressed/JSONL.gz format")]
#[command(version = "1.0")]
struct Cli {
    #[arg(short, long, help = "Directory containing JSONL.gz files", required = true)]
    input: String,

    #[arg(short, long, default_value = "field_data.csv", help = "Output CSV file or directory")]
    output: String,

    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,

    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,

    #[arg(short, long, default_value = "10000", help = "Target number of records per batch sent to writer")]
    batch_size: usize,


    #[arg(short = 'g', long, help = "Organize output by source ID")]
    organize: bool,

    #[arg(long, help = "Filter by OpenAlex source ID")]
    source_id: Option<String>,

    #[arg(long, help = "Filter by DOI prefix")]
    doi_prefix: Option<String>,

    #[arg(long, default_value = "100", help = "Maximum number of open files when using --organize")]
    max_open_files: usize,

    #[arg(short, long, help = "Comma-separated list of fields to extract (e.g., 'authorships.author.display_name,title,ids.pmid')")]
    fields: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Doi(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DoiPrefix(String);

#[derive(Debug, Clone)]
struct FieldData {
    work_id: WorkId,
    doi: Option<Doi>,
    field_name: String,
    subfield_path: String,
    value: String,
    source_id: Option<SourceId>,
    doi_prefix: DoiPrefix,
    source_file_path: PathBuf,
}

impl Default for FieldData {
    fn default() -> Self {
        Self {
            work_id: WorkId(String::new()),
            doi: None,
            field_name: String::new(),
            subfield_path: String::new(),
            value: String::new(),
            source_id: None,
            doi_prefix: DoiPrefix(String::new()),
            source_file_path: PathBuf::new(),
        }
    }
}

#[derive(Debug, Default)]
struct FileStats {
    unique_work_ids: HashSet<WorkId>,
    unique_dois: HashSet<Doi>,
    field_counts: HashMap<String, usize>,
    source_counts: HashMap<SourceId, usize>,
    prefix_counts: HashMap<DoiPrefix, usize>,
    total_fields_extracted: usize,
}

struct ProcessedFileResult {
    stats: FileStats,
    error: Option<anyhow::Error>,
    filepath: PathBuf,
}

struct IncrementalStats {
    total_field_records: AtomicUsize,
    processed_files_ok: AtomicUsize,
    processed_files_error: AtomicUsize,

    unique_records: DashSet<String>,
    sources: DashMap<SourceId, AtomicUsize>,
    prefixes: DashMap<DoiPrefix, AtomicUsize>,
    unique_fields: DashMap<String, AtomicUsize>,
}

impl IncrementalStats {
    fn new() -> Self {
        Self {
            total_field_records: AtomicUsize::new(0),
            processed_files_ok: AtomicUsize::new(0),
            processed_files_error: AtomicUsize::new(0),
            unique_records: DashSet::new(),
            sources: DashMap::new(),
            prefixes: DashMap::new(),
            unique_fields: DashMap::new(),
        }
    }

    fn aggregate_file_stats(&self, file_stats: FileStats) {
        self.processed_files_ok.fetch_add(1, Ordering::Relaxed);
        self.total_field_records.fetch_add(file_stats.total_fields_extracted, Ordering::Relaxed);

        for work_id in file_stats.unique_work_ids {
            self.unique_records.insert(work_id.0);
        }

        for (field_name, count) in file_stats.field_counts {
             self.unique_fields.entry(field_name)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }

        for (source_id, count) in file_stats.source_counts {
             self.sources.entry(source_id)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }

         for (prefix, count) in file_stats.prefix_counts {
             self.prefixes.entry(prefix)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(count, Ordering::Relaxed);
        }
    }

    fn increment_error_files(&self) {
        self.processed_files_error.fetch_add(1, Ordering::Relaxed);
    }



    fn get_final_stats(&self) -> FinalStats {
        let final_fields: HashMap<String, usize> = self.unique_fields
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        let final_sources: HashMap<SourceId, usize> = self.sources
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        let final_prefixes: HashMap<DoiPrefix, usize> = self.prefixes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect();

        FinalStats {
            total_field_records: self.total_field_records.load(Ordering::Relaxed),
            processed_files_ok: self.processed_files_ok.load(Ordering::Relaxed),
            processed_files_error: self.processed_files_error.load(Ordering::Relaxed),
            unique_work_ids: self.unique_records.len(),
            unique_sources: final_sources,
            unique_prefixes: final_prefixes,
            unique_fields: final_fields,
        }
    }
}

struct FinalStats {
    total_field_records: usize,
    processed_files_ok: usize,
    processed_files_error: usize,
    unique_work_ids: usize,
    unique_sources: HashMap<SourceId, usize>,
    unique_prefixes: HashMap<DoiPrefix, usize>,
    unique_fields: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum FieldType {
    Array,
    Object,
    Value,
}

lazy_static! {
    static ref SCHEMA_STRUCTURE: HashMap<String, FieldType> = {
        let mut schema = HashMap::new();

        // --- Top-level Fields ---
        schema.insert("id".to_string(), FieldType::Value);
        schema.insert("doi".to_string(), FieldType::Value);
        schema.insert("doi_registration_agency".to_string(), FieldType::Value);
        schema.insert("display_name".to_string(), FieldType::Value);
        schema.insert("title".to_string(), FieldType::Value);
        schema.insert("publication_year".to_string(), FieldType::Value);
        schema.insert("publication_date".to_string(), FieldType::Value);
        schema.insert("language".to_string(), FieldType::Value);
        schema.insert("language_id".to_string(), FieldType::Value);
        schema.insert("type".to_string(), FieldType::Value);
        schema.insert("type_id".to_string(), FieldType::Value);
        schema.insert("type_crossref".to_string(), FieldType::Value);
        schema.insert("is_retracted".to_string(), FieldType::Value);
        schema.insert("is_paratext".to_string(), FieldType::Value);
        schema.insert("cited_by_count".to_string(), FieldType::Value);
        schema.insert("countries_distinct_count".to_string(), FieldType::Value);
        schema.insert("institutions_distinct_count".to_string(), FieldType::Value);
        schema.insert("locations_count".to_string(), FieldType::Value);
        schema.insert("referenced_works_count".to_string(), FieldType::Value);
        schema.insert("authors_count".to_string(), FieldType::Value);
        schema.insert("concepts_count".to_string(), FieldType::Value);
        schema.insert("topics_count".to_string(), FieldType::Value);
        schema.insert("has_fulltext".to_string(), FieldType::Value);
        schema.insert("cited_by_api_url".to_string(), FieldType::Value);
        schema.insert("updated_date".to_string(), FieldType::Value);
        schema.insert("created_date".to_string(), FieldType::Value);
        schema.insert("updated".to_string(), FieldType::Value);


        // --- ids Object ---
        schema.insert("ids".to_string(), FieldType::Object);
        schema.insert("ids.openalex".to_string(), FieldType::Value);
        schema.insert("ids.mag".to_string(), FieldType::Value);
        schema.insert("ids.pmid".to_string(), FieldType::Value);

        // --- Location Objects (primary_location, best_oa_location, locations) ---
        let location_fields = [
            "is_oa", "version", "license", "doi", "is_accepted", "is_published",
            "pdf_url", "landing_page_url"
        ];
        let source_fields = [
            "id", "issn_l", "issn", "display_name", "publisher", "host_organization",
            "host_organization_name", "is_oa", "is_in_doaj", "type", "type_id"
        ];
        let location_prefixes = ["primary_location", "best_oa_location", "locations"];

        for prefix in location_prefixes.iter() {
            schema.insert(prefix.to_string(), if *prefix == "locations" { FieldType::Array } else { FieldType::Object });
            for field in location_fields.iter() {
                schema.insert(format!("{}.{}", prefix, field), FieldType::Value);
            }
            schema.insert(format!("{}.source", prefix), FieldType::Object);
            for field in source_fields.iter() {
                schema.insert(format!("{}.source.{}", prefix, field), FieldType::Value);
            }
            schema.insert(format!("{}.source.host_organization_lineage", prefix), FieldType::Array);
            schema.insert(format!("{}.source.host_organization_lineage_names", prefix), FieldType::Array);
        }
        schema.insert("open_access".to_string(), FieldType::Object);
        schema.insert("open_access.is_oa".to_string(), FieldType::Value);
        schema.insert("open_access.oa_status".to_string(), FieldType::Value);
        schema.insert("open_access.oa_url".to_string(), FieldType::Value);
        schema.insert("open_access.any_repository_has_fulltext".to_string(), FieldType::Value);
        schema.insert("authorships".to_string(), FieldType::Array);
        schema.insert("authorships.author_position".to_string(), FieldType::Value);
        schema.insert("authorships.is_corresponding".to_string(), FieldType::Value);
        schema.insert("authorships.raw_author_name".to_string(), FieldType::Value);
        schema.insert("authorships.raw_affiliation_string".to_string(), FieldType::Value);
        schema.insert("authorships.raw_affiliation_strings".to_string(), FieldType::Array);
        schema.insert("authorships.countries".to_string(), FieldType::Array);
        schema.insert("authorships.country_ids".to_string(), FieldType::Array);
        schema.insert("authorships.author".to_string(), FieldType::Object);
        schema.insert("authorships.author.id".to_string(), FieldType::Value);
        schema.insert("authorships.author.display_name".to_string(), FieldType::Value);
        schema.insert("authorships.author.orcid".to_string(), FieldType::Value);
        schema.insert("authorships.affiliations".to_string(), FieldType::Array);
        schema.insert("authorships.affiliations.raw_affiliation_string".to_string(), FieldType::Value);
        schema.insert("authorships.affiliations.institution_ids".to_string(), FieldType::Array);
        schema.insert("authorships.institutions".to_string(), FieldType::Array);
        schema.insert("authorships.institutions.id".to_string(), FieldType::Value);
        schema.insert("authorships.institutions.display_name".to_string(), FieldType::Value);
        schema.insert("authorships.institutions.ror".to_string(), FieldType::Value);
        schema.insert("authorships.institutions.country_code".to_string(), FieldType::Value);
        schema.insert("authorships.institutions.type".to_string(), FieldType::Value);
        schema.insert("authorships.institutions.lineage".to_string(), FieldType::Array);
        schema.insert("corresponding_author_ids".to_string(), FieldType::Array);
        schema.insert("corresponding_institution_ids".to_string(), FieldType::Array);
        schema.insert("referenced_works".to_string(), FieldType::Array);
        schema.insert("related_works".to_string(), FieldType::Array);
        schema.insert("indexed_in".to_string(), FieldType::Array);
        schema.insert("summary_stats".to_string(), FieldType::Object);
        schema.insert("summary_stats.cited_by_count".to_string(), FieldType::Value);
        schema.insert("summary_stats.2yr_cited_by_count".to_string(), FieldType::Value);
        schema.insert("biblio".to_string(), FieldType::Object);
        schema.insert("biblio.volume".to_string(), FieldType::Value);
        schema.insert("biblio.issue".to_string(), FieldType::Value);
        schema.insert("biblio.first_page".to_string(), FieldType::Value);
        schema.insert("biblio.last_page".to_string(), FieldType::Value);
        schema.insert("concepts".to_string(), FieldType::Array);
        schema.insert("concepts.id".to_string(), FieldType::Value);
        schema.insert("concepts.wikidata".to_string(), FieldType::Value);
        schema.insert("concepts.display_name".to_string(), FieldType::Value);
        schema.insert("concepts.level".to_string(), FieldType::Value);
        schema.insert("concepts.score".to_string(), FieldType::Value);
        schema.insert("topics".to_string(), FieldType::Array);
        schema.insert("topics.id".to_string(), FieldType::Value);
        schema.insert("topics.display_name".to_string(), FieldType::Value);
        schema.insert("topics.score".to_string(), FieldType::Value);
        schema.insert("topics.subfield".to_string(), FieldType::Object);
        schema.insert("topics.subfield.id".to_string(), FieldType::Value);
        schema.insert("topics.subfield.display_name".to_string(), FieldType::Value);
        schema.insert("topics.field".to_string(), FieldType::Object);
        schema.insert("topics.field.id".to_string(), FieldType::Value);
        schema.insert("topics.field.display_name".to_string(), FieldType::Value);
        schema.insert("topics.domain".to_string(), FieldType::Object);
        schema.insert("topics.domain.id".to_string(), FieldType::Value);
        schema.insert("topics.domain.display_name".to_string(), FieldType::Value);
        schema.insert("primary_topic".to_string(), FieldType::Object);
        schema.insert("primary_topic.id".to_string(), FieldType::Value);
        schema.insert("primary_topic.display_name".to_string(), FieldType::Value);
        schema.insert("primary_topic.score".to_string(), FieldType::Value);
        schema.insert("primary_topic.subfield".to_string(), FieldType::Object);
        schema.insert("primary_topic.subfield.id".to_string(), FieldType::Value);
        schema.insert("primary_topic.subfield.display_name".to_string(), FieldType::Value);
        schema.insert("primary_topic.field".to_string(), FieldType::Object);
        schema.insert("primary_topic.field.id".to_string(), FieldType::Value);
        schema.insert("primary_topic.field.display_name".to_string(), FieldType::Value);
        schema.insert("primary_topic.domain".to_string(), FieldType::Object);
        schema.insert("primary_topic.domain.id".to_string(), FieldType::Value);
        schema.insert("primary_topic.domain.display_name".to_string(), FieldType::Value);
        schema.insert("mesh".to_string(), FieldType::Array);
        schema.insert("mesh.is_major_topic".to_string(), FieldType::Value);
        schema.insert("mesh.descriptor_ui".to_string(), FieldType::Value);
        schema.insert("mesh.descriptor_name".to_string(), FieldType::Value);
        schema.insert("mesh.qualifier_ui".to_string(), FieldType::Value);
        schema.insert("mesh.qualifier_name".to_string(), FieldType::Value);
        schema.insert("keywords".to_string(), FieldType::Array);
        schema.insert("keywords.keyword".to_string(), FieldType::Value);
        schema.insert("keywords.score".to_string(), FieldType::Value);
        schema.insert("sustainable_development_goals".to_string(), FieldType::Array);
        schema.insert("sustainable_development_goals.id".to_string(), FieldType::Value);
        schema.insert("sustainable_development_goals.display_name".to_string(), FieldType::Value);
        schema.insert("sustainable_development_goals.score".to_string(), FieldType::Value);
        schema.insert("counts_by_year".to_string(), FieldType::Array);
        schema.insert("counts_by_year.year".to_string(), FieldType::Value);
        schema.insert("counts_by_year.cited_by_count".to_string(), FieldType::Value);
        schema.insert("cited_by_percentile_year".to_string(), FieldType::Object);
        schema.insert("cited_by_percentile_year.min".to_string(), FieldType::Value);
        schema.insert("cited_by_percentile_year.max".to_string(), FieldType::Value);
        schema.insert("abstract_inverted_index".to_string(), FieldType::Object);
        schema.insert("abstract_inverted_index.*".to_string(), FieldType::Array);
        schema.insert("versions".to_string(), FieldType::Array);
        schema.insert("datasets".to_string(), FieldType::Array);
        schema.insert("grants".to_string(), FieldType::Array);
        schema.insert("apc_list".to_string(), FieldType::Object);
        schema.insert("apc_paid".to_string(), FieldType::Object);

        schema
    };
}

#[derive(Debug, Default)]
struct PatternTrieNode {
    children: HashMap<String, PatternTrieNode>,
    terminating_patterns: Vec<String>,
}

#[derive(Debug)]
struct PatternTrie {
    root: PatternTrieNode,
}

impl PatternTrie {
    fn new(field_specs: &[Vec<String>]) -> Self {
        let mut root = PatternTrieNode::default();
        
        for spec in field_specs {
            if spec.is_empty() {
                warn!("Skipping invalid empty field path specification.");
                continue;
            }

            let full_pattern_name = spec.join(".");
            let mut current_node = &mut root;
            let mut current_schema_path = String::new();

            for part in spec {
                if !current_schema_path.is_empty() {
                    current_schema_path.push('.');
                }
                current_schema_path.push_str(part);

                current_node = current_node.children.entry(part.clone()).or_default();
                
                // When a field is defined as FieldType::Array in the schema, we automatically
                // insert a special '[]' node as a child. This serves as a traversal marker:
                // - During extraction, when we encounter a JSON array, we look for this '[]' node
                // - If found, we iterate over array elements and continue traversal from there
                // - This allows patterns like "author.family" to match all authors in an array
                // Example: "author" -> "[]" -> "family" matches author[0].family, author[1].family, etc.
                if SCHEMA_STRUCTURE.get(&current_schema_path) == Some(&FieldType::Array) {
                    current_node = current_node.children.entry("[]".to_string()).or_default();
                }
            }
            // Mark the final node as a termination point for this pattern.
            current_node.terminating_patterns.push(full_pattern_name);
        }
        Self { root }
    }
    
    fn extract(&self, record: &Value) -> Vec<(String, String, String)> {
        let mut results = Vec::new();
        self.traverse(record, &self.root, String::new(), &mut results);
        results
    }

    fn traverse<'a>(
        &self,
        json_node: &'a Value,
        trie_node: &'a PatternTrieNode,
        current_path: String,
        results: &mut Vec<(String, String, String)>,
    ) {
        // Check if the current path corresponds to any requested patterns.
        if !trie_node.terminating_patterns.is_empty() {
            let value_str = match json_node {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "".to_string(),
                _ => serde_json::to_string(json_node).unwrap_or_else(|e| {
                    warn!("Failed to serialize complex value at path '{}': {}", current_path, e);
                    "[serialization error]".to_string()
                }),
            };

            for pattern_name in &trie_node.terminating_patterns {
                results.push((pattern_name.clone(), current_path.clone(), value_str.clone()));
            }
        }

        // Decide how to proceed with traversal based on JSON and Trie node types.
        match json_node {
            Value::Object(obj) => {
                for (key, value) in obj {
                    // Traverse using a specific key if it exists in the trie
                    if let Some(child_trie_node) = trie_node.children.get(key) {
                        let new_path = if current_path.is_empty() { key.clone() } else { format!("{}.{}", current_path, key) };
                        self.traverse(value, child_trie_node, new_path, results);
                    }
                    // Also check for a wildcard "*" (e.g., for `abstract_inverted_index.*`)
                    if let Some(wildcard_node) = trie_node.children.get("*") {
                        let new_path = if current_path.is_empty() { key.clone() } else { format!("{}.{}", current_path, key) };
                        self.traverse(value, wildcard_node, new_path, results);
                    }
                }
            }
            Value::Array(arr) => {
                // Check if the trie expects an array at this point
                if let Some(array_child_node) = trie_node.children.get("[]") {
                     for (i, item) in arr.iter().enumerate() {
                        let new_path = format!("{}[{}]", current_path, i);
                        self.traverse(item, array_child_node, new_path, results);
                    }
                }
            }
            _ => {
                // It's a primitive value, and we already handled extraction at the start of the function,
                // so there's nothing further to traverse.
            }
        }
    }
}


fn parse_field_specifications(field_specs: &str) -> Vec<Vec<String>> {
     field_specs
        .split(',')
        .filter(|spec| !spec.trim().is_empty())
        .map(|spec| {
            spec.trim()
                .split('.')
                .map(|part| part.trim().to_string())
                .filter(|part| !part.is_empty())
                .collect::<Vec<String>>()
        })
        .filter(|parts| !parts.is_empty())
        .collect()
}

fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.gz");
    let pattern_str = pattern.to_string_lossy();
    info!("Searching for files matching pattern: {}", pattern_str);
    let paths: Vec<PathBuf> = glob(&pattern_str)?
        .filter_map(Result::ok)
        .collect();
    if paths.is_empty() {
        warn!("No files found matching the pattern: {}", pattern_str);
    }
    Ok(paths)
}

trait FileProcessor {
    fn process(
        &self, 
        filepath: &Path, 
        sender: &Sender<Vec<FieldData>>, 
        batch_size: usize
    ) -> ProcessedFileResult;
}

struct JsonlProcessor {
    extractor: Arc<PatternTrie>,
    filter_source_id: Option<String>,
    filter_doi_prefix: Option<String>,
}

impl FileProcessor for JsonlProcessor {
    fn process(
        &self, 
        filepath: &Path, 
        sender: &Sender<Vec<FieldData>>, 
        batch_size: usize
    ) -> ProcessedFileResult {
        let mut batch_buffer = Vec::with_capacity(batch_size); 
        let mut file_stats = FileStats::default();

        let file = match File::open(filepath) {
            Ok(f) => f,
            Err(e) => {
                let err = anyhow::Error::new(e).context(format!("Failed to open file: {}", filepath.display()));
                return ProcessedFileResult { stats: file_stats, error: Some(err), filepath: filepath.to_path_buf() };
            }
        };

        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        let mut lines_processed = 0;
        let mut records_processed = 0;
        let mut records_missing_work_id = 0;
        let mut records_missing_source = 0;
        let mut records_filtered_out = 0;
        let mut json_parsing_errors = 0;

        for (line_num, line_result) in reader.lines().enumerate() {
            lines_processed += 1;
            let line_str = match line_result {
                Ok(s) => s,
                Err(e) => {
                    warn!("Error reading line {} from {}: {}", line_num + 1, filepath.display(), e);
                    continue;
                }
            };

            if line_str.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<Value>(&line_str) {
                Ok(record) => {
                    records_processed += 1;

                    let work_id_opt = extract_work_id(&record);
                    let source_id_opt = extract_source_id(&record);
                    let doi_opt = extract_doi(&record);
                    let doi_prefix_opt = extract_doi_prefix(doi_opt.as_ref());

                    if let Some(filter_s) = &self.filter_source_id {
                        if source_id_opt.as_ref().is_none_or(|s| &s.0 != filter_s) {
                            records_filtered_out += 1;
                            continue;
                        }
                    }
                     if let Some(filter_p) = &self.filter_doi_prefix {
                         if doi_prefix_opt.as_ref().is_none_or(|p| &p.0 != filter_p) {
                             records_filtered_out += 1;
                              continue;
                         }
                     }

                     let work_id = match work_id_opt {
                         Some(id) => id,
                         None => {
                             records_missing_work_id += 1;
                             continue;
                         }
                     };
                     if source_id_opt.is_none() {
                         records_missing_source += 1;
                     }
                     let doi_prefix = doi_prefix_opt.unwrap_or_else(|| DoiPrefix("".to_string()));

                    let extracted_fields = self.extractor.extract(&record);

                    if !extracted_fields.is_empty() {
                        file_stats.unique_work_ids.insert(work_id.clone());
                        if let Some(ref doi) = doi_opt {
                            file_stats.unique_dois.insert(doi.clone());
                        }
                        if let Some(ref source_id) = source_id_opt {
                            *file_stats.source_counts.entry(source_id.clone()).or_insert(0) += extracted_fields.len();
                        }
                        *file_stats.prefix_counts.entry(doi_prefix.clone()).or_insert(0) += extracted_fields.len();

                        for (field_name, subfield_path, value) in extracted_fields {
                            *file_stats.field_counts.entry(field_name.clone()).or_insert(0) += 1;
                            file_stats.total_fields_extracted += 1;

                            batch_buffer.push(FieldData {
                                work_id: work_id.clone(),
                                doi: doi_opt.clone(),
                                field_name,
                                subfield_path,
                                value,
                                source_id: source_id_opt.clone(),
                                doi_prefix: doi_prefix.clone(),
                                source_file_path: filepath.to_path_buf(),
                            });

                            if batch_buffer.len() >= batch_size {
                                if sender.send(std::mem::take(&mut batch_buffer)).is_err() {
                                    let err = anyhow::anyhow!("Writer thread channel closed unexpectedly on file {}", filepath.display());
                                    return ProcessedFileResult { stats: file_stats, error: Some(err), filepath: filepath.to_path_buf() };
                                }
                                batch_buffer = Vec::with_capacity(batch_size);
                            }
                        }
                    }
                }
                Err(e) => {
                    json_parsing_errors += 1;
                    warn!("Error parsing JSON from {}:{}: {}", filepath.display(), line_num + 1, e);
                }
            }
        }
        
        if !batch_buffer.is_empty() && sender.send(batch_buffer).is_err() {
            let err = anyhow::anyhow!("Writer thread channel closed unexpectedly on final batch for {}", filepath.display());
            return ProcessedFileResult { stats: file_stats, error: Some(err), filepath: filepath.to_path_buf() };
        }

        debug!(
            "Finished processing {}: {} lines read, {} records parsed ({} JSON errors), {} fields extracted. Skipped: {} missing work ID, {} missing Source, {} filtered out.",
            filepath.display(),
            lines_processed,
            records_processed,
            json_parsing_errors,
            file_stats.total_fields_extracted,
            records_missing_work_id,
            records_missing_source,
            records_filtered_out
        );

        ProcessedFileResult { stats: file_stats, error: None, filepath: filepath.to_path_buf() }
    }
}


fn extract_work_id(record: &Value) -> Option<WorkId> {
    record.get("id")
        .and_then(Value::as_str)
        .map(|s| WorkId(s.to_string()))
}

fn extract_doi(record: &Value) -> Option<Doi> {
    record.get("doi")
        .and_then(Value::as_str)
        .map(|s| s.strip_prefix("https://doi.org/").unwrap_or(s))
        .map(|s| Doi(s.to_string()))
}

fn extract_source_id(record: &Value) -> Option<SourceId> {
    record.get("primary_location")
        .and_then(|loc| loc.get("source"))
        .and_then(|src| src.get("id"))
        .and_then(Value::as_str)
        .map(|s| SourceId(s.to_string()))
}

fn extract_doi_prefix(doi: Option<&Doi>) -> Option<DoiPrefix> {
    doi.and_then(|doi_val| {
        doi_val.0.split_once('/').map(|(pfx, _)| DoiPrefix(pfx.to_string()))
    })
}

mod memory_usage {
    use log::info;

    #[derive(Debug)]
    pub struct MemoryStats {
        pub rss_mb: f64,
        pub vm_size_mb: f64,
        pub percent: Option<f64>,
    }

    #[cfg(target_os = "linux")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::fs::read_to_string;

        let pid = std::process::id();
        let status_file = format!("/proc/{}/status", pid);
        let content = read_to_string(status_file).ok()?;

        let mut vm_rss_kb = None;
        let mut vm_size_kb = None;

        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
            } else if line.starts_with("VmSize:") {
                vm_size_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok());
            }
            if vm_rss_kb.is_some() && vm_size_kb.is_some() {
                break;
            }
        }

        let rss_mb = vm_rss_kb? / 1024.0;
        let vm_size_mb = vm_size_kb? / 1024.0;
        let mut percent = None;

        if let Ok(meminfo) = read_to_string("/proc/meminfo") {
            if let Some(mem_total_kb) = meminfo.lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|s| s.parse::<f64>().ok()) {
                if mem_total_kb > 0.0 {
                    percent = Some((vm_rss_kb? / mem_total_kb) * 100.0);
                }
            }
        }


        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }

    #[cfg(target_os = "macos")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let ps_output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &pid.to_string()])
            .output().ok()?;
        let rss_kb = String::from_utf8_lossy(&ps_output.stdout).trim().parse::<f64>().ok()?;

         let vsz_output = Command::new("ps")
            .args(&["-o", "vsz=", "-p", &pid.to_string()])
            .output().ok()?;
         let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout).trim().parse::<f64>().ok()?;


        let rss_mb = rss_kb / 1024.0;
        let vm_size_mb = vsz_kb / 1024.0;
        let mut percent = None;

         if let Ok(hw_mem_output) = Command::new("sysctl").args(&["-n", "hw.memsize"]).output() {
             if let Ok(total_bytes_str) = String::from_utf8(hw_mem_output.stdout) {
                 if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                     let total_kb = total_bytes / 1024.0;
                     if total_kb > 0.0 {
                          percent = Some((rss_kb / total_kb) * 100.0);
                      }
                 }
             }
         }


        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }

    #[cfg(target_os = "windows")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command;

        let pid = std::process::id();
        let wmic_output = Command::new("wmic")
            .args(&[
                "process",
                "where",
                &format!("ProcessId={}", pid),
                "get",
                "WorkingSetSize,",
                "PageFileUsage",
                "/value",
            ])
            .output()
            .ok()?;

        let output_str = String::from_utf8_lossy(&wmic_output.stdout);
        let mut rss_bytes = None;
        let mut vm_kb = None;

        for line in output_str.lines() {
            if line.starts_with("PageFileUsage=") {
                vm_kb = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok());
            } else if line.starts_with("WorkingSetSize=") {
                 rss_bytes = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok());
             }
        }

        let rss_mb = rss_bytes? / (1024.0 * 1024.0);
        let vm_size_mb = vm_kb? / 1024.0;
        let mut percent = None;

         if let Ok(mem_output) = Command::new("wmic")
                .args(&["ComputerSystem", "get", "TotalPhysicalMemory", "/value"])
                .output() {
                let mem_str = String::from_utf8_lossy(&mem_output.stdout);
                 if let Some(total_bytes_str) = mem_str.lines()
                    .find(|line| line.starts_with("TotalPhysicalMemory="))
                    .and_then(|line| line.split('=').nth(1)) {
                      if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() {
                          if total_bytes > 0.0 {
                              percent = Some((rss_bytes? / total_bytes) * 100.0);
                          }
                    }
                 }
         }


        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        None
    }

    pub fn log_memory_usage(note: &str) {
        if let Some(stats) = get_memory_usage() {
            let percent_str = stats.percent.map_or_else(|| "N/A".to_string(), |p| format!("{:.1}%", p));
            info!(
                "Memory usage ({}): {:.1} MB physical (RSS), {:.1} MB virtual/commit, {} of system memory",
                note, stats.rss_mb, stats.vm_size_mb, percent_str
            );
        } else {
            info!("Memory usage tracking not available or failed on this platform ({})", std::env::consts::OS);
        }
    }
}

fn format_elapsed(elapsed: Duration) -> String {
    let total_secs = elapsed.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = elapsed.subsec_millis();

    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}.{:03}s", seconds, millis)
    }
}

trait OutputStrategy: Send {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn report_files_created(&self) -> usize;
}

struct SingleFileOutput {
    writer: Writer<File>,
    #[allow(dead_code)]
    headers: Vec<String>,
    file_path: PathBuf,
}

impl SingleFileOutput {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        info!("Initializing single output file: {}", file_path.display());
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory structure for: {}", file_path.display()))?;
        }

        let headers = vec![
            "work_id".to_string(),
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "source_id".to_string(),
            "doi_prefix".to_string(),
            "source_file_path".to_string(),
        ];

        let file = File::create(&file_path)
            .with_context(|| format!("Failed to create output file: {}", file_path.display()))?;

        let mut writer = Writer::from_writer(file);
        writer.write_record(&headers)
            .context("Failed to write header to single output file")?;
        writer.flush()
            .context("Failed to flush header to single output file")?;

        Ok(Self {
            writer,
            headers,
            file_path,
        })
    }
}

impl OutputStrategy for SingleFileOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        for field_data in batch {
            let doi_str = field_data.doi.as_ref().map(|d| d.0.as_str()).unwrap_or("");
            let source_id_str = field_data.source_id.as_ref().map(|s| s.0.as_str()).unwrap_or("");
            self.writer.write_record(&[
                &field_data.work_id.0,
                doi_str,
                &field_data.field_name,
                &field_data.subfield_path,
                &field_data.value,
                source_id_str,
                &field_data.doi_prefix.0,
                &field_data.source_file_path.display().to_string(),
            ])?;
        }
        Ok(())
    }

     fn flush(&mut self) -> Result<()> {
        info!("Flushing final data to: {}", self.file_path.display());
        self.writer.flush()
            .context(format!("Failed to flush single output file: {}", self.file_path.display()))?;
        Ok(())
    }

    fn report_files_created(&self) -> usize {
        1
    }
}

struct OrganizedOutput {
    base_output_dir: PathBuf,
    current_writers: HashMap<SourceId, Writer<File>>,
    created_files: HashSet<PathBuf>,
    max_open_files: usize,
    headers: Vec<String>,
    open_file_lru: VecDeque<SourceId>,
}

impl OrganizedOutput {
    fn new<P: AsRef<Path>>(output_path: P, max_open_files: usize) -> Result<Self> {
        let path = output_path.as_ref();
        if path.exists() && !path.is_dir() {
            return Err(anyhow::anyhow!("Output path for organized output must be a directory: {}", path.display()));
        }
        fs::create_dir_all(path)
            .with_context(|| format!("Failed to create base output directory: {}", path.display()))?;
        info!("Initializing organized output in directory: {}", path.display());
        info!("Using a maximum of {} open files at once", max_open_files);

        let headers = vec![
            "work_id".to_string(),
            "doi".to_string(),
            "field_name".to_string(),
            "subfield_path".to_string(),
            "value".to_string(),
            "source_id".to_string(),
            "doi_prefix".to_string(),
            "source_file_path".to_string(),
        ];

        Ok(Self {
            base_output_dir: path.to_path_buf(),
            current_writers: HashMap::with_capacity(max_open_files.min(1024)),
            created_files: HashSet::new(),
            max_open_files: max_open_files.max(1),
            headers,
            open_file_lru: VecDeque::with_capacity(max_open_files),
        })
    }

    fn get_writer(&mut self, source_id: &SourceId) -> Result<&mut Writer<File>> {
        let key = source_id.clone();

        if self.current_writers.contains_key(&key) {
            if let Some(pos) = self.open_file_lru.iter().position(|x| x == &key) {
                self.open_file_lru.remove(pos);
            }
            self.open_file_lru.push_front(key.clone());
            
            return self.current_writers.get_mut(&key)
                .ok_or_else(|| anyhow::anyhow!("Writer unexpectedly missing for source {}", key.0));
        }

        while self.current_writers.len() >= self.max_open_files {
            if let Some(lru_key) = self.open_file_lru.pop_back() {
                info!("Closing LRU file for source {} to maintain max open files limit.", lru_key.0);
                 if let Some(mut writer_to_close) = self.current_writers.remove(&lru_key) {
                     if let Err(e) = writer_to_close.flush() {
                         warn!("Error flushing file for source {} before closing: {}", lru_key.0, e);
                     }
                 }
            } else {
                 error!("LRU queue empty while trying to close files. Limit: {}", self.max_open_files);
                 break;
             }
        }

        let source_file_path = self.base_output_dir.join(format!("{}.csv", key.0));
        let file_needs_header = !self.created_files.contains(&source_file_path);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&source_file_path)
            .with_context(|| format!("Failed to open/create output file for source {}: {}", key.0, source_file_path.display()))?;

        let mut csv_writer = Writer::from_writer(file);

        if file_needs_header {
             csv_writer.write_record(&self.headers)
                .with_context(|| format!("Failed to write header to: {}", source_file_path.display()))?;
            csv_writer.flush()
                .with_context(|| format!("Failed to flush header to: {}", source_file_path.display()))?;
            self.created_files.insert(source_file_path.clone());
            debug!("Created new file with header: {}", source_file_path.display());
        } else {
             debug!("Opened existing file in append mode: {}", source_file_path.display());
         }

        self.current_writers.insert(key.clone(), csv_writer);
        self.open_file_lru.push_front(key.clone());

        self.current_writers.get_mut(&key)
            .ok_or_else(|| anyhow::anyhow!("Writer unexpectedly missing after insert for source {}", key.0))
    }
}


impl OutputStrategy for OrganizedOutput {
    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut grouped_records: HashMap<Option<SourceId>, Vec<&FieldData>> = HashMap::new();
        for field_data in batch {
             grouped_records
                .entry(field_data.source_id.clone())
                .or_default()
                .push(field_data);
        }

        for (source_id_opt, records) in grouped_records {
            let source_id = source_id_opt.unwrap_or_else(|| SourceId("unknown".to_string()));
            let writer = self.get_writer(&source_id)
                .with_context(|| format!("Failed to get writer for source {}", source_id.0))?;

            for field_data in records {
                 let doi_str = field_data.doi.as_ref().map(|d| d.0.as_str()).unwrap_or("");
                 let source_id_str = field_data.source_id.as_ref().map(|s| s.0.as_str()).unwrap_or("");
                 writer.write_record(&[
                     &field_data.work_id.0,
                     doi_str,
                     &field_data.field_name,
                     &field_data.subfield_path,
                     &field_data.value,
                     source_id_str,
                     &field_data.doi_prefix.0,
                     &field_data.source_file_path.display().to_string(),
                 ])?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!("Flushing {} open CSV files...", self.current_writers.len());
        let mut flush_errors = Vec::new();
        for (source_id, writer) in self.current_writers.iter_mut() {
            if let Err(e) = writer.flush() {
                flush_errors.push(format!("Failed to flush file for source {}: {}", source_id.0, e));
            }
        }
        self.current_writers.clear();
        self.open_file_lru.clear();


        info!(
            "Total unique files created/opened during run: {}",
            self.created_files.len()
        );

        if !flush_errors.is_empty() {
            Err(anyhow::anyhow!("Errors occurred during final flush:\n - {}", flush_errors.join("\n - ")))
        } else {
            Ok(())
        }
    }

    fn report_files_created(&self) -> usize {
        self.created_files.len()
    }
}

struct CsvWriterManager {
    output_strategy: Box<dyn OutputStrategy>,
}

impl CsvWriterManager {
    fn new<P: AsRef<Path>>(output_path: P, organize: bool, max_open_files: usize) -> Result<Self> {
        let strategy: Box<dyn OutputStrategy> = if organize {
            Box::new(OrganizedOutput::new(output_path, max_open_files)?)
        } else {
            Box::new(SingleFileOutput::new(output_path)?)
        };

        Ok(Self {
            output_strategy: strategy,
        })
    }

    fn write_batch(&mut self, batch: &[FieldData]) -> Result<()> {
        self.output_strategy.write_batch(batch)
            .context("Error writing batch via CsvWriterManager")
    }

    fn flush_all(&mut self) -> Result<()> {
        self.output_strategy.flush()
            .context("Error flushing all files via CsvWriterManager")
    }

    fn report_files_created(&self) -> usize {
        self.output_strategy.report_files_created()
    }
}

impl Drop for CsvWriterManager {
    fn drop(&mut self) {
        info!("CsvWriterManager dropping. Attempting final flush...");
        if let Err(e) = self.flush_all() {
            error!("Error flushing CSV writers during cleanup: {}", e);
        } else {
            info!("Final flush completed successfully.");
        }
    }
}

fn setup_logging(log_level_str: &str) -> Result<()> {
    let log_level = match log_level_str.to_uppercase().as_str() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" | "WARNING" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        other => {
            eprintln!("Invalid log level '{}', defaulting to INFO.", other);
            LevelFilter::Info
        }
    };

    SimpleLogger::new()
        .with_level(log_level)
        .with_timestamp_format(format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
        .init()?;
    
    Ok(())
}

fn setup_thread_pool(thread_count: usize) -> Result<usize> {
    let num_threads = if thread_count == 0 {
        let cores = num_cpus::get();
        info!("Auto-detected {} CPU cores. Using {} threads.", cores, cores);
        cores
    } else {
        info!("Using specified {} threads.", thread_count);
        thread_count
    };
    
    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global() {
        error!("Failed to build global thread pool: {}. Proceeding with default.", e);
    }
    
    Ok(num_threads)
}

fn prepare_extractor(fields_spec: &str) -> Result<(Vec<Vec<String>>, PatternTrie)> {
    let field_specifications = parse_field_specifications(fields_spec);
    if field_specifications.is_empty() {
        return Err(anyhow::anyhow!("No fields specified for extraction"));
    }
    
    info!("Fields to extract:");
    for spec in &field_specifications {
        info!("  - {}", spec.join("."));
    }
    
    info!("Building efficient pattern extractor (Trie)...");
    let extractor = PatternTrie::new(&field_specifications);
    debug!("Extractor Trie structure: {:?}", extractor.root);
    
    Ok((field_specifications, extractor))
}

fn find_input_files(input_dir: &str) -> Result<Vec<PathBuf>> {
    info!("Searching for input files in: {}", input_dir);
    let files = find_jsonl_gz_files(input_dir)?;
    info!("Found {} files to process.", files.len());
    Ok(files)
}

fn run_extraction_pipeline(
    cli: &Cli,
    files: Vec<PathBuf>,
    extractor: PatternTrie,
    num_threads: usize,
) -> Result<(FinalStats, Option<usize>, Vec<PathBuf>)> {
    info!("Using target batch size for writer: {} records.", cli.batch_size);
    if let Some(source_filter) = &cli.source_id {
        info!("Filtering by source ID: {}", source_filter);
    }
    if let Some(prefix_filter) = &cli.doi_prefix {
        info!("Filtering by DOI prefix: {}", prefix_filter);
    }
    if cli.organize {
        info!("Output will be organized by source ID in directory: {}", cli.output);
        info!("Using max {} open output files.", cli.max_open_files);
    } else {
        info!("Output will be written to single file: {}", cli.output);
    }

    let progress_bar = ProgressBar::new(files.len() as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec}) {msg}")
            .expect("Failed to create progress bar template")
            .progress_chars("=> "),
    );
    progress_bar.set_message("Starting processing...");

    let stats = Arc::new(IncrementalStats::new());

    let channel_capacity = (num_threads * 4).max(8);
    let (batch_sender, batch_receiver): (Sender<Vec<FieldData>>, Receiver<Vec<FieldData>>) = bounded(channel_capacity);
    info!("Using writer channel with capacity: {}", channel_capacity);

    let output_path_clone = cli.output.clone();
    let organize_clone = cli.organize;
    let max_open_files_clone = cli.max_open_files;
    let writer_thread = thread::spawn(move || -> Result<usize> {
        info!("Writer thread started.");
        let mut csv_writer_manager = CsvWriterManager::new(
            &output_path_clone,
            organize_clone,
            max_open_files_clone
        )?;

        let mut batches_written = 0;
        let mut records_written = 0;

        for batch in batch_receiver {
            if !batch.is_empty() {
                 let count = batch.len();
                 if let Err(e) = csv_writer_manager.write_batch(&batch) {
                     error!("Writer thread error writing batch: {}", e);
                 } else {
                      batches_written += 1;
                      records_written += count;
                      debug!("Writer thread wrote batch {} ({} records)", batches_written, count);
                  }
            }
        }

        info!("Writer thread finished receiving. Wrote {} records in {} batches.", records_written, batches_written);
         Ok(csv_writer_manager.report_files_created())
    });

    info!("Starting parallel file processing...");
    let extractor_arc = Arc::new(extractor);

    let processor = Arc::new(JsonlProcessor {
        extractor: extractor_arc,
        filter_source_id: cli.source_id.clone(),
        filter_doi_prefix: cli.doi_prefix.clone(),
    });

    let processing_results: Vec<ProcessedFileResult> = files
        .par_iter()
        .map(|filepath| {
            let processor_ref = Arc::clone(&processor);
            let sender_clone = batch_sender.clone();
            let pb_clone = progress_bar.clone();
            let target_batch_size = cli.batch_size;

            let process_start_time = Instant::now();

            let result = processor_ref.process(filepath, &sender_clone, target_batch_size);
            let duration = process_start_time.elapsed();

            let file_name_msg = filepath.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| filepath.display().to_string());

            pb_clone.inc(1);

            if result.error.is_some() {
                pb_clone.set_message(format!("ERR: {} ({})", file_name_msg, format_elapsed(duration)));
            } else {
                let num_extracted = result.stats.total_fields_extracted;
                pb_clone.set_message(format!("OK: {} ({} fields, {})", file_name_msg, num_extracted, format_elapsed(duration)));
            }
            
            result
        })
        .collect();

    info!("File processing complete. Aggregating final stats...");
    progress_bar.set_message("Aggregating stats...");

    drop(batch_sender);

    let mut files_with_errors = Vec::new();
    for result in processing_results {
        if let Some(e) = result.error {
            error!("Error processing file {}: {}", result.filepath.display(), e);
            stats.increment_error_files();
            files_with_errors.push(result.filepath);
        } else {
            stats.aggregate_file_stats(result.stats);
        }
    }

    progress_bar.finish_with_message(format!(
        "Processing finished. {} files OK, {} errors.",
        stats.processed_files_ok.load(Ordering::Relaxed),
        stats.processed_files_error.load(Ordering::Relaxed)
    ));

    info!("Waiting for writer thread to finish writing remaining batches...");
    let files_created_result = writer_thread.join();

    let files_created = match files_created_result {
         Ok(Ok(count)) => {
            info!("Writer thread finished successfully.");
            Some(count)
         },
         Ok(Err(e)) => {
              error!("Writer thread returned an error: {}", e);
              None
          }
         Err(e) => {
              error!("Writer thread panicked: {:?}", e);
              None
         }
    };

    let final_stats = stats.get_final_stats();
    Ok((final_stats, files_created, files_with_errors))
}

fn print_final_summary(
    start_time: Instant,
    final_stats: &FinalStats,
    cli: &Cli,
    files_created: Option<usize>,
    files_count: usize,
    files_with_errors: &[PathBuf],
) -> Result<()> {
    info!("-------------------- FINAL SUMMARY --------------------");
    let total_runtime = start_time.elapsed();
    info!("Total execution time: {}", format_elapsed(total_runtime));
    info!("Input files found: {}", files_count);

    info!("Files processed successfully: {}", final_stats.processed_files_ok);
    if final_stats.processed_files_error > 0 {
        warn!("Files with processing errors: {}", final_stats.processed_files_error);
        if !files_with_errors.is_empty() {
            for err_file in files_with_errors.iter().take(10) {
                warn!("  - {}", err_file.display());
            }
            if files_with_errors.len() > 10 {
                warn!("  ... (and {} more)", files_with_errors.len() - 10);
            }
        }
    }
    info!("Total field records extracted: {}", final_stats.total_field_records);
    info!("Unique work IDs encountered: {}", final_stats.unique_work_ids);
    info!("Unique Sources encountered: {}", final_stats.unique_sources.len());
    info!("Unique DOI Prefixes encountered: {}", final_stats.unique_prefixes.len());

    info!("Final Field breakdown:");
    let mut final_sorted_fields: Vec<_> = final_stats.unique_fields.iter().collect();
    final_sorted_fields.sort_by_key(|&(_, count)| std::cmp::Reverse(*count));
    for (field, count) in final_sorted_fields.iter().take(20) {
        info!("  - {}: {} records", field, count);
    }
    if final_sorted_fields.len() > 20 {
        info!("  ... ({} more fields)", final_sorted_fields.len() - 20);
    }

    if !final_stats.unique_sources.is_empty() && final_stats.unique_sources.len() < 50 {
        info!("Final Source statistics:");
        let mut sorted_sources: Vec<_> = final_stats.unique_sources.iter().collect();
        sorted_sources.sort_by_key(|&(_, count)| std::cmp::Reverse(*count));
        for (source, count) in sorted_sources {
            info!("  - Source {}: {} records", source.0, count);
        }
    } else if final_stats.unique_sources.len() >= 50 {
        info!("(Skipping detailed stats for {} sources)", final_stats.unique_sources.len());
    }

    if let Some(count) = files_created {
         if cli.organize {
            info!("Total unique output files created/opened: {}", count);
         } else {
             info!("Output written to: {}", cli.output);
         }
    } else {
         error!("Could not determine number of files created by writer thread.");
     }

    Ok(())
}

fn main() -> Result<()> {
    let start_time = Instant::now();
    let cli = Cli::parse();

    setup_logging(&cli.log_level)?;
    info!("Starting Field Extractor");
    memory_usage::log_memory_usage("initial");

    let num_threads = setup_thread_pool(cli.threads)?;
    
    let (_field_specifications, extractor) = prepare_extractor(&cli.fields)?;
    let files = find_input_files(&cli.input)?;
    
    if files.is_empty() {
        warn!("No .jsonl.gz files found in the specified directory. Exiting.");
        return Ok(());
    }

    let files_count = files.len();
    let (final_stats, files_created, files_with_errors) = run_extraction_pipeline(&cli, files, extractor, num_threads)?;
    
    print_final_summary(start_time, &final_stats, &cli, files_created, files_count, &files_with_errors)?;
    
    memory_usage::log_memory_usage("final");
    info!("Extraction process finished.");
    info!("-------------------------------------------------------");

    Ok(())
}