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
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;


mod external_sort {
    use super::{Cli, InputRecord};
    use csv::{ReaderBuilder, WriterBuilder};
    use indicatif::{ProgressBar, ProgressStyle};
    use log::{error, info};
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;
    use std::error::Error;
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    const MERGE_WIDTH: usize = 100;

    #[derive(Debug, Eq, PartialEq)]
    struct HeapEntry {
        record: InputRecord,
        reader_index: usize,
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.record.doi.cmp(&self.record.doi)
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    fn create_sorted_chunks(
        input_path: &Path,
        chunks_dir: &Path,
        chunk_size: usize,
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        info!("Phase 1: Creating sorted chunks...");
        let rdr = ReaderBuilder::new().from_path(input_path)?;
        let mut chunk = Vec::with_capacity(chunk_size);
        let mut chunk_files = Vec::new();
        let mut chunk_index = 0;

        let pb = ProgressBar::new(fs::metadata(input_path)?.len());
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} Sorting Chunks [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"));

        let mut reader_wrapper = pb.wrap_read(rdr.into_inner());
        let mut rdr = ReaderBuilder::new().from_reader(&mut reader_wrapper);

        for result in rdr.deserialize::<InputRecord>() {
            chunk.push(result?);
            if chunk.len() >= chunk_size {
                chunk.sort_by(|a, b| a.doi.cmp(&b.doi));
                let temp_path = chunks_dir.join(format!("chunk_{}.csv.zst", chunk_index));
                write_chunk(&chunk, &temp_path)?;
                chunk_files.push(temp_path);
                chunk.clear();
                chunk_index += 1;
            }
        }

        if !chunk.is_empty() {
            chunk.sort_by(|a, b| a.doi.cmp(&b.doi));
            let temp_path = chunks_dir.join(format!("chunk_{}.csv.zst", chunk_index));
            write_chunk(&chunk, &temp_path)?;
            chunk_files.push(temp_path);
        }

        pb.finish_with_message("Chunking complete.");
        Ok(chunk_files)
    }

    fn write_chunk(chunk: &[InputRecord], path: &Path) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        let encoder = zstd::Encoder::new(file, 10)?.auto_finish();
        let mut wtr = WriterBuilder::new().from_writer(encoder);
        for record in chunk {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }
    
    fn merge_chunks(
        chunk_files: &[PathBuf],
        output_path: &Path,
    ) -> Result<(), Box<dyn Error>> {
        info!("Phase 2: Merging {} chunks...", chunk_files.len());
        let mut readers: Vec<_> = chunk_files
            .iter()
            .map(|path| {
                let file = File::open(path)?;
                let decoder = zstd::Decoder::new(file)?;
                Ok(ReaderBuilder::new().from_reader(decoder))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

        let output_file = File::create(output_path)?;
        let writer: Box<dyn Write> = if output_path.extension().and_then(|s| s.to_str()) == Some("zst") {
            info!("-> Writing compressed intermediate file: {}", output_path.display());
            Box::new(zstd::Encoder::new(output_file, 10)?.auto_finish())
        } else {
            info!("-> Writing final uncompressed file: {}", output_path.display());
            Box::new(output_file)
        };
        
        let mut wtr = WriterBuilder::new().from_writer(writer);
        let mut heap = BinaryHeap::new();

        for (i, reader) in readers.iter_mut().enumerate() {
            if let Some(result) = reader.deserialize().next() {
                let record: InputRecord = result?;
                heap.push(HeapEntry { record, reader_index: i });
            }
        }

        let pb = ProgressBar::new_spinner();
        pb.set_message("Merging records...");

        while let Some(entry) = heap.pop() {
            let HeapEntry { record, reader_index } = entry;
            wtr.serialize(record)?;
            pb.inc(1);

            if let Some(result) = readers[reader_index].deserialize().next() {
                let next_record: InputRecord = result?;
                heap.push(HeapEntry { record: next_record, reader_index });
            }
        }

        pb.finish_with_message("Merging complete.");
        wtr.flush()?;
        Ok(())
    }
    
    pub fn sort_csv(cli: &Cli, output_path: &Path, chunks_dir: &Path) -> Result<(), Box<dyn Error>> {
        let mut current_pass_dir = chunks_dir.join("pass_0");
        fs::create_dir_all(&current_pass_dir)?;
        let mut chunk_files = create_sorted_chunks(&cli.input, &current_pass_dir, cli.chunk_size)?;

        let mut pass_num = 0;
        while chunk_files.len() > MERGE_WIDTH {
            pass_num += 1;
            info!(
                "Starting merge pass {}: merging {} chunks in groups of {}",
                pass_num,
                chunk_files.len(),
                MERGE_WIDTH
            );

            let next_pass_dir = chunks_dir.join(format!("pass_{}", pass_num));
            fs::create_dir_all(&next_pass_dir)?;
            let mut next_level_chunks = Vec::new();

            for (i, group) in chunk_files.chunks(MERGE_WIDTH).enumerate() {
                let intermediate_output_path =
                    next_pass_dir.join(format!("intermediate_chunk_{}.csv.zst", i));
                
                merge_chunks(group, &intermediate_output_path)?;
                next_level_chunks.push(intermediate_output_path);

                for chunk_to_delete in group {
                    if let Err(e) = fs::remove_file(chunk_to_delete) {
                        error!("Failed to delete intermediate chunk {}: {}", chunk_to_delete.display(), e);
                    }
                }
            }
            
            info!("Cleaning up directory: {}", current_pass_dir.display());
            if let Err(e) = fs::remove_dir_all(&current_pass_dir) {
                error!("Could not remove pass directory {}: {}", current_pass_dir.display(), e);
            }

            chunk_files = next_level_chunks;
            current_pass_dir = next_pass_dir;
        }

        info!("Starting final merge of {} chunks...", chunk_files.len());
        merge_chunks(&chunk_files, output_path)?;

        info!("Cleaning up final chunks directory: {}", current_pass_dir.display());
        if let Err(e) = fs::remove_dir_all(&current_pass_dir) {
            error!("Could not remove final chunks directory {}: {}", current_pass_dir.display(), e);
        }

        Ok(())
    }
}

lazy_static! {
    static ref AUTHORSHIP_INDEX_RE: Regex = Regex::new(r"authorships\[(\d+)\]").unwrap();
    static ref AFFILIATION_INDEX_RE: Regex = Regex::new(r"affiliations\[(\d+)\]").unwrap();
    static ref INSTITUTION_INDEX_RE: Regex = Regex::new(r"institutions\[(\d+)\]").unwrap();
    static ref NORMALIZE_RE: Regex = Regex::new(r"[^\w\s]").unwrap();
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = "A memory-efficient Rust script that first sorts a large CSV by 'doi' and then processes it.")]
struct Cli {
    #[arg(short = 'i', long)]
    input: PathBuf,

    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    #[arg(long, default_value_t = 500_000)]
    chunk_size: usize,

    #[arg(long)]
    temp_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
struct InputRecord {
    doi: String,
    field_name: String,
    subfield_path: String,
    value: String,
    #[allow(dead_code)]
    source_id: String,
    #[allow(dead_code)]
    doi_prefix: String,
}

#[derive(Debug, Default, Clone)]
struct Author {
    display_name: Option<String>,
    sequence: u32,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    doi: String,
    author_sequence: u32,
    author_name: String,
    normalized_author_name: String,
    affiliation_sequence: u32,
    affiliation_name: String,
    normalized_affiliation_name: String,
    affiliation_ror: String,
}

fn normalize_text(text: &str) -> String {
    let unidecoded = deunicode(text);
    let lowercased = unidecoded.to_lowercase();
    let cleaned = NORMALIZE_RE.replace_all(&lowercased, "");
    cleaned.trim().to_string()
}

#[derive(Debug, Default)]
struct TempAffiliation {
    raw_string: Option<String>,
    institution_ids: Vec<String>,
    sequence: u32,
}

#[derive(Debug, Default)]
struct TempInstitution {
    id: Option<String>,
    ror: Option<String>,
}


fn process_doi_group(
    doi: &str,
    records: &[InputRecord],
    wtr: &mut csv::Writer<File>,
) -> Result<usize, Box<dyn Error>> {
    let mut records_written = 0;

    let mut authors: HashMap<u32, Author> = HashMap::new();
    let mut affiliations: HashMap<(u32, u32), TempAffiliation> = HashMap::new();
    let mut institutions: HashMap<(u32, u32), TempInstitution> = HashMap::new();

    for record in records {
        let author_caps = match AUTHORSHIP_INDEX_RE.captures(&record.subfield_path) {
            Some(caps) => caps,
            None => continue,
        };
        let author_idx: u32 = author_caps.get(1).unwrap().as_str().parse()?;

        authors
            .entry(author_idx)
            .or_insert_with(|| Author { sequence: author_idx, ..Default::default() });
        
        match record.field_name.as_str() {
            "authorships.author.display_name" => {
                if let Some(author) = authors.get_mut(&author_idx) {
                    author.display_name = Some(record.value.clone());
                }
            }
            "authorships.affiliations.raw_affiliation_string" => {
                if let Some(aff_caps) = AFFILIATION_INDEX_RE.captures(&record.subfield_path) {
                    let aff_idx: u32 = aff_caps.get(1).unwrap().as_str().parse()?;
                    let entry = affiliations.entry((author_idx, aff_idx)).or_default();
                    entry.raw_string = Some(record.value.clone());
                    entry.sequence = aff_idx;
                }
            }
            "authorships.affiliations.institution_ids" => {
                if let Some(aff_caps) = AFFILIATION_INDEX_RE.captures(&record.subfield_path) {
                    let aff_idx: u32 = aff_caps.get(1).unwrap().as_str().parse()?;
                    affiliations
                        .entry((author_idx, aff_idx))
                        .or_default()
                        .institution_ids
                        .push(record.value.clone());
                }
            }
            "authorships.institutions.id" => {
                if let Some(inst_caps) = INSTITUTION_INDEX_RE.captures(&record.subfield_path) {
                    let inst_idx: u32 = inst_caps.get(1).unwrap().as_str().parse()?;
                    institutions
                        .entry((author_idx, inst_idx))
                        .or_default()
                        .id = Some(record.value.clone());
                }
            }
            "authorships.institutions.ror" => {
                if let Some(inst_caps) = INSTITUTION_INDEX_RE.captures(&record.subfield_path) {
                    let inst_idx: u32 = inst_caps.get(1).unwrap().as_str().parse()?;
                    institutions
                        .entry((author_idx, inst_idx))
                        .or_default()
                        .ror = Some(record.value.clone());
                }
            }
            _ => {}
        }
    }

    let mut ror_lookup: HashMap<String, String> = HashMap::new();
    for inst in institutions.values() {
        if let (Some(id), Some(ror)) = (&inst.id, &inst.ror) {
            ror_lookup.insert(id.clone(), ror.clone());
        }
    }

    let mut sorted_authors: Vec<_> = authors.values().cloned().collect();
    sorted_authors.sort_by_key(|a| a.sequence);

    for author in sorted_authors {
        let author_name = author.display_name.as_deref().unwrap_or("");
        let normalized_author_name = normalize_text(author_name);

        let mut author_affiliations: Vec<_> = affiliations
            .iter()
            .filter(|((auth_idx, _), _)| *auth_idx == author.sequence)
            .map(|(_, aff_data)| aff_data)
            .collect();
        author_affiliations.sort_by_key(|a| a.sequence);

        if author_affiliations.is_empty() {
            let record = OutputRecord {
                doi: doi.to_string(),
                author_sequence: author.sequence,
                author_name: author_name.to_string(),
                normalized_author_name,
                affiliation_sequence: 0,
                affiliation_name: "".to_string(),
                normalized_affiliation_name: "".to_string(),
                affiliation_ror: "".to_string(),
            };
            wtr.serialize(record)?;
            records_written += 1;
        } else {
            for affiliation in author_affiliations {
                let affiliation_name = affiliation.raw_string.as_deref().unwrap_or("");
                let normalized_affiliation_name = normalize_text(affiliation_name);

                let mut affiliation_ror = "".to_string();
                for inst_id in &affiliation.institution_ids {
                    if let Some(ror) = ror_lookup.get(inst_id) {
                        affiliation_ror = ror.clone();
                        break;
                    }
                }

                let record = OutputRecord {
                    doi: doi.to_string(),
                    author_sequence: author.sequence,
                    author_name: author_name.to_string(),
                    normalized_author_name: normalized_author_name.clone(),
                    affiliation_sequence: affiliation.sequence,
                    affiliation_name: affiliation_name.to_string(),
                    normalized_affiliation_name,
                    affiliation_ror,
                };
                wtr.serialize(record)?;
                records_written += 1;
            }
        }
    }
    Ok(records_written)
}

fn main() -> Result<(), Box<dyn Error>> {
    let overall_start_time = Instant::now();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut cli = Cli::parse();
    if cli.output.is_none() {
        let input_path = Path::new(&cli.input);
        let stem = input_path.file_stem().unwrap().to_str().unwrap();
        let parent_dir = input_path.parent().unwrap_or_else(|| Path::new(""));
        let output_filename = format!("{}_processed.csv", stem);
        cli.output = Some(parent_dir.join(output_filename));
    }
    let output_path = cli.output.as_ref().unwrap();

    let _main_temp_dir = if let Some(path) = &cli.temp_dir {
        tempfile::Builder::new().prefix("csv_proc_").tempdir_in(path)?
    } else {
        tempfile::Builder::new().prefix("csv_proc_").tempdir()?
    };
    let temp_dir_path = _main_temp_dir.path();
    info!("Using temporary directory: {}", temp_dir_path.display());

    let sort_start_time = Instant::now();
    info!("Starting external sort...");
    
    let chunks_dir = temp_dir_path.join("chunks");
    fs::create_dir_all(&chunks_dir)?;
    let temp_sorted_path = temp_dir_path.join("sorted_data.csv");
    
    external_sort::sort_csv(&cli, &temp_sorted_path, &chunks_dir)?;
    info!("External sort finished in {:.2?}.", sort_start_time.elapsed());

    info!("Starting streaming aggregation from sorted temporary file...");
    let process_start_time = Instant::now();

    let file = File::open(&temp_sorted_path)?;
    let file_size = file.metadata()?.len();
    let pb_read = ProgressBar::new(file_size);
    pb_read.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} Processing [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"),
    );

    let progress_reader = pb_read.wrap_read(file);
    let mut rdr = ReaderBuilder::new().from_reader(progress_reader);
    let mut wtr = WriterBuilder::new().from_path(output_path)?;

    let mut current_doi: Option<String> = None;
    let mut records_for_current_doi: Vec<InputRecord> = Vec::new();
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

        if current_doi.is_some() && current_doi.as_ref().unwrap() != &record.doi {
            let doi_to_process = current_doi.clone().unwrap();
            
            let written_count = process_doi_group(&doi_to_process, &records_for_current_doi, &mut wtr)?;
            total_records_written += written_count;
            total_dois_processed += 1;
            
            records_for_current_doi.clear();
        }

        current_doi = Some(record.doi.clone());
        records_for_current_doi.push(record);
    }

    if let Some(doi) = current_doi {
        if !records_for_current_doi.is_empty() {
            let written_count = process_doi_group(&doi, &records_for_current_doi, &mut wtr)?;
            total_records_written += written_count;
            total_dois_processed += 1;
        }
    }

    pb_read.finish_with_message("Processing complete.");
    wtr.flush()?;

    info!(
        "Streaming process complete in {:.2?}. Processed {} unique DOIs and wrote {} records.",
        process_start_time.elapsed(), total_dois_processed, total_records_written
    );
    info!(
        "Total time for all operations: {:.2?}",
        overall_start_time.elapsed()
    );

    Ok(())
}