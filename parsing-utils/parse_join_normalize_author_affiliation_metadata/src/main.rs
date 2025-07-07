use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use deunicode::deunicode;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use log::{error, info};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

// Use lazy_static to compile regexes once at program start. This is a major
// performance boost compared to compiling them inside a loop.
lazy_static! {
    static ref AUTHOR_INDEX_RE: Regex = Regex::new(r"author\[(\d+)\]").unwrap();
    static ref AFFILIATION_INDEX_RE: Regex = Regex::new(r"affiliation\[(\d+)\]").unwrap();
    static ref NORMALIZE_RE: Regex = Regex::new(r"[^\w\s]").unwrap();
}

/// Command-line arguments parsed by Clap. The `short` attribute enables -i and -o.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = "A memory-efficient Rust script to process and normalize author affiliation data from a large, sorted CSV file.")]
struct Cli {
    /// Path to the input CSV file. MUST be sorted by the 'doi' column.
    #[arg(short = 'i', long)]
    input: PathBuf,

    /// Path for the output CSV file.
    /// If not provided, defaults to <input_filename>_processed.csv
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,
}

/// Represents a single row from the input CSV file.
/// Deriving Deserialize allows `csv` crate to automatically convert rows into this struct.
#[derive(Debug, Deserialize)]
struct InputRecord {
    doi: String,
    field_name: String,
    subfield_path: String,
    value: String,
}

/// Represents an author's affiliation with its sequence number.
#[derive(Debug, Clone)]
struct Affiliation {
    name: String,
    sequence: u32,
}

/// Aggregates all information related to a single author for a given DOI.
#[derive(Debug, Default, Clone)]
struct Author {
    given_name: Option<String>,
    family_name: Option<String>,
    full_name: Option<String>,
    affiliations: Vec<Affiliation>,
    sequence: u32,
}

/// Represents a single row in the final, processed output CSV file.
/// Deriving Serialize allows `csv` crate to automatically write this struct to a file.
#[derive(Debug, Serialize)]
struct OutputRecord {
    doi: String,
    author_sequence: u32,
    full_name: String,
    normalized_full_name: String,
    given_name: String,
    normalized_given_name: String,
    family_name: String,
    normalized_family_name: String,
    affiliation_sequence: String, // Use String to allow for "None"
    affiliation: String,
    normalized_affiliation: String,
}

/// Normalizes a text string by transliterating to ASCII, converting to lowercase,
/// removing punctuation, and trimming whitespace.
/// Takes a string slice to avoid unnecessary memory allocations.
fn normalize_text(text: &str) -> String {
    let unidecoded = deunicode(text);
    let lowercased = unidecoded.to_lowercase();
    let cleaned = NORMALIZE_RE.replace_all(&lowercased, "");
    cleaned.trim().to_string()
}

/// Helper function to process the aggregated data for a single DOI and write it to the CSV.
/// This contains the logic from the original "Transformation" step.
fn process_and_write_doi_data(
    doi: &str,
    authors: &HashMap<u32, Author>,
    wtr: &mut csv::Writer<File>,
) -> Result<usize, Box<dyn Error>> {
    let mut records_written = 0;
    for (_, author) in authors.iter() {
        let given_name = author.given_name.as_deref().unwrap_or("");
        let family_name = author.family_name.as_deref().unwrap_or("");
        let full_name = author.full_name.as_ref().map_or_else(
            || format!("{} {}", given_name, family_name).trim().to_string(),
            |name| name.clone(),
        );

        if author.affiliations.is_empty() {
            let record = OutputRecord {
                doi: doi.to_string(),
                author_sequence: author.sequence,
                full_name: full_name.clone(),
                normalized_full_name: normalize_text(&full_name),
                given_name: given_name.to_string(),
                normalized_given_name: normalize_text(given_name),
                family_name: family_name.to_string(),
                normalized_family_name: normalize_text(family_name),
                affiliation_sequence: "None".to_string(),
                affiliation: "".to_string(),
                normalized_affiliation: "".to_string(),
            };
            wtr.serialize(record)?;
            records_written += 1;
        } else {
            for affiliation in &author.affiliations {
                let record = OutputRecord {
                    doi: doi.to_string(),
                    author_sequence: author.sequence,
                    full_name: full_name.clone(),
                    normalized_full_name: normalize_text(&full_name),
                    given_name: given_name.to_string(),
                    normalized_given_name: normalize_text(given_name),
                    family_name: family_name.to_string(),
                    normalized_family_name: normalize_text(family_name),
                    affiliation_sequence: affiliation.sequence.to_string(),
                    affiliation: affiliation.name.clone(),
                    normalized_affiliation: normalize_text(&affiliation.name),
                };
                wtr.serialize(record)?;
                records_written += 1;
            }
        }
    }
    Ok(records_written)
}

fn main() -> Result<(), Box<dyn Error>> {
    // --- 1. Argument Parsing and Setup ---
    let start_time = Instant::now();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut cli = Cli::parse();
    info!("Starting data processing for: {}", cli.input.display());
    info!("IMPORTANT: This script assumes the input file is sorted by the 'doi' column.");


    if cli.output.is_none() {
        let input_path = Path::new(&cli.input);
        let stem = input_path.file_stem().unwrap().to_str().unwrap();
        let parent_dir = input_path.parent().unwrap_or_else(|| Path::new(""));
        let output_filename = format!("{}_processed.csv", stem);
        cli.output = Some(parent_dir.join(output_filename));
    }
    let output_path = cli.output.unwrap();
    info!("Output will be saved to: {}", output_path.display());

    // --- 2. Streaming Aggregation and Writing ---
    info!("Starting streaming aggregation from sorted input file...");
    let file = File::open(&cli.input)?;
    let file_size = file.metadata()?.len();
    
    // Setup progress bar for reading the input file
    let pb_read = ProgressBar::new(file_size);
    pb_read.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} Reading [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"),
    );

    let progress_reader = pb_read.wrap_read(file);
    let mut rdr = ReaderBuilder::new().from_reader(progress_reader);
    let mut wtr = WriterBuilder::new().from_path(output_path)?;

    // State for the streaming logic
    let mut current_doi: Option<String> = None;
    let mut authors_for_current_doi: HashMap<u32, Author> = HashMap::new();
    let mut total_records_written = 0;
    let mut total_dois_processed = 0;

    for (i, result) in rdr.deserialize::<InputRecord>().enumerate() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                error!("Error deserializing row {}: {}. Skipping.", i + 1, e);
                continue;
            }
        };
        
        // If current_doi exists and its value is different from the record's DOI,
        // it means we have finished collecting all data for the previous DOI.
        if current_doi.is_some() && current_doi.as_ref().unwrap() != &record.doi {
            // The DOI has changed, so we process the data we've collected.
            let doi_to_process = current_doi.clone().unwrap();
            let written_count = process_and_write_doi_data(&doi_to_process, &authors_for_current_doi, &mut wtr)?;
            total_records_written += written_count;
            total_dois_processed += 1;

            // Clear the map to free memory and prepare for the next DOI.
            authors_for_current_doi.clear();
        }

        // --- Update state with the current record's data ---
        current_doi = Some(record.doi.clone());

        // Aggregate data for the current DOI
        let author_index = match AUTHOR_INDEX_RE.captures(&record.subfield_path) {
            Some(caps) => caps.get(1).unwrap().as_str().parse::<u32>()?,
            None => continue,
        };

        let author_entry = authors_for_current_doi
            .entry(author_index)
            .or_insert_with(|| Author {
                sequence: author_index,
                ..Default::default()
            });

        match record.field_name.as_str() {
            "author.given" => author_entry.given_name = Some(record.value),
            "author.family" => author_entry.family_name = Some(record.value),
            "author.name" => author_entry.full_name = Some(record.value),
            "author.affiliation.name" => {
                if let Some(caps) = AFFILIATION_INDEX_RE.captures(&record.subfield_path) {
                    let affiliation_index = caps.get(1).unwrap().as_str().parse::<u32>()?;
                    author_entry.affiliations.push(Affiliation {
                        name: record.value,
                        sequence: affiliation_index,
                    });
                }
            }
            _ => {}
        }
    }

    // --- Process the very last DOI group after the loop finishes ---
    if let Some(doi) = current_doi {
        if !authors_for_current_doi.is_empty() {
             let written_count = process_and_write_doi_data(&doi, &authors_for_current_doi, &mut wtr)?;
             total_records_written += written_count;
             total_dois_processed += 1;
        }
    }
    pb_read.finish_with_message("Reading complete.");
    
    wtr.flush()?;
    info!(
        "Streaming process complete. Processed {} unique DOIs and wrote {} records.",
        total_dois_processed, total_records_written
    );
    info!(
        "Total time elapsed: {:.2?}",
        start_time.elapsed()
    );

    Ok(())
}