import os
import re
import sys
import argparse
import duckdb
import time
from unidecode import unidecode


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


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="""
        Tool to setup a DuckDB database for linking authors to affiliations.

        Usage:
        python %(prog)s --reference-file /path/to/100gb.csv --db-file publications.duckdb
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-r", "--reference-file",
        required=True,
        help="Path to the reference CSV file with author affiliations."
    )
    parser.add_argument(
        "-d", "--db-file",
        required=True,
        help="Path to the DuckDB database file to be created. Default: works.db"
    )
    parser.add_argument(
        "-m", "--memory-limit",
        default="8GB",
        help="Memory limit for DB processing (e.g., '16GB', '2GB'). Default: 8GB."
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Temporary directory for disk spilling when memory limit is reached."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="If specified, process CSV in chunks of this many rows (fallback for very large files)."
    )
    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip index creation (useful for very large datasets where indexes can be created later)."
    )

    return parser.parse_args()


def get_file_size_gb(file_path):
    """Get file size in GB"""
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 ** 3)


def setup_database(db_file, reference_file, memory_limit, temp_dir=None, chunk_size=None, skip_indexes=False):
    print("--- Running Database Setup ---")
    
    if not os.path.exists(reference_file):
        print(f"Error: Reference file not found at '{reference_file}'")
        sys.exit(1)
    
    file_size_gb = get_file_size_gb(reference_file)
    print(f"Reference file size: {file_size_gb:.2f} GB")
    
    print(f"Creating and optimizing new DuckDB database at '{db_file}'...")
    con = duckdb.connect(database=db_file, read_only=False)
    
    print(f"Setting memory limit to {memory_limit}.")
    con.execute(f"SET memory_limit='{memory_limit}';")
    
    if temp_dir:
        print(f"Setting temporary directory to '{temp_dir}'.")
        con.execute(f"SET temp_directory='{temp_dir}';")
    
    # Set single-threaded mode for large file stability
    con.execute("SET threads=1;")
    # Disable insertion order preservation for better memory efficiency
    con.execute("SET preserve_insertion_order=false;")
    
    print(f"Loading and transforming reference data from '{reference_file}'.")
    print("This may take some time for large files...")
    
    try:
        start_time = time.time()
        
        # Step 1: Create error tracking table
        print("Creating error tracking table...")
        con.execute("""
            CREATE TABLE IF NOT EXISTS import_errors (
                error_message VARCHAR,
                row_content VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        if chunk_size:
            # Chunked processing for extremely large files
            print(f"Using chunked processing with chunk size: {chunk_size:,} rows")
            process_chunked(con, reference_file, chunk_size)
        else:
            # Standard processing with error handling
            process_standard(con, reference_file)
        
        # Step 4: Create indexes for fast lookups (optional)
        if skip_indexes:
            print("Skipping index creation as requested.")
            print("You can create indexes later with:")
            print("  CREATE INDEX idx_work_id ON author_references (work_id);")
            print("  CREATE INDEX idx_doi ON author_references (doi);")
            print("  CREATE INDEX idx_norm_name ON author_references (normalized_author_name);")
            print("  CREATE INDEX idx_norm_affil ON author_references (normalized_affiliation_key);")
            print("  CREATE INDEX idx_ror ON author_references (affiliation_ror);")
        else:
            print("Creating indexes for fast lookups...")
            print("Note: Index creation may take time for large datasets...")
            
            # Create indexes one at a time with memory cleanup between each
            indexes = [
                ("idx_work_id", "work_id"),
                ("idx_doi", "doi"),
                ("idx_norm_name", "normalized_author_name"),
                ("idx_norm_affil", "normalized_affiliation_key"),
                ("idx_ror", "affiliation_ror")
            ]
            
            for idx_name, column in indexes:
                try:
                    print(f"  Creating index on {column}...")
                    # Force checkpoint and memory cleanup before each index
                    con.execute("CHECKPOINT;")
                    con.execute(f"SET memory_limit='{memory_limit}';")  # Re-enforce memory limit
                    con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON author_references ({column});")
                    print(f"    ✓ Index {idx_name} created")
                except Exception as e:
                    print(f"    ⚠ Warning: Could not create index {idx_name}: {e}")
                    print(f"    Continuing without this index...")
            
            print("Index creation completed.")
        
        # Step 5: Display statistics
        elapsed_time = time.time() - start_time
        print(f"\nImport completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        
        # Get final statistics
        stats = con.execute("""
            SELECT 
                (SELECT COUNT(*) FROM author_references) as final_rows,
                (SELECT COUNT(*) FROM import_errors) as error_rows
        """).fetchone()
        
        print(f"Final statistics:")
        print(f"  - Successfully imported rows: {stats[0]:,}")
        print(f"  - Errors/skipped rows: {stats[1]:,}")
        
        if stats[1] > 0:
            print(f"\nNote: {stats[1]:,} rows had errors. Check 'import_errors' table for details.")
            sample_errors = con.execute("SELECT error_message FROM import_errors LIMIT 5").fetchall()
            if sample_errors:
                print("Sample errors:")
                for err in sample_errors:
                    print(f"  - {err[0][:100]}...")
        
        print("\nDatabase build is complete!")
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)
    finally:
        con.close()


def process_standard(con, reference_file):
    """Standard processing with robust error handling"""
    
    # Step 2: Import with error tolerance into staging table
    print("Stage 1: Importing raw data with error tolerance...")
    
    # First, try to create staging table with error handling
    staging_sql = f"""
        CREATE OR REPLACE TABLE author_references_staging AS
        SELECT * FROM read_csv_auto(
            '{reference_file}',
            header=true,
            delim=',',
            quote='"',
            escape='"',
            parallel=false,
            ignore_errors=true,
            maximum_line_size=10485760,
            sample_size=100000,
            null_padding=true,
            all_varchar=true
        );
    """
    
    try:
        con.execute(staging_sql)
        
        # Get row count
        staging_count = con.execute("SELECT COUNT(*) FROM author_references_staging").fetchone()[0]
        print(f"  - Loaded {staging_count:,} rows into staging table")
        
    except Exception as e:
        print(f"Warning: Standard import failed: {e}")
        print("Attempting fallback import method...")
        
        # Fallback: More conservative parameters
        fallback_sql = f"""
            CREATE OR REPLACE TABLE author_references_staging AS
            SELECT * FROM read_csv_auto(
                '{reference_file}',
                header=true,
                delim=',',
                parallel=false,
                all_varchar=true
            );
        """
        con.execute(fallback_sql)
        staging_count = con.execute("SELECT COUNT(*) FROM author_references_staging").fetchone()[0]
        print(f"  - Loaded {staging_count:,} rows into staging table (fallback method)")
    
    # Step 3: Clean and transform to final table
    print("Stage 2: Cleaning and transforming data...")
    
    transform_sql = """
        CREATE OR REPLACE TABLE author_references AS
        SELECT
            work_id,
            CASE 
                WHEN doi IS NULL OR doi = '' OR doi = 'null' THEN NULL
                ELSE doi
            END as doi,
            TRY_CAST(author_sequence AS INTEGER) as author_sequence,
            author_name,
            normalized_author_name,
            TRY_CAST(affiliation_sequence AS INTEGER) as affiliation_sequence,
            affiliation_name,
            normalized_affiliation_name,
            affiliation_ror,
            lower(trim(COALESCE(normalized_affiliation_name, ''))) as normalized_affiliation_key
        FROM author_references_staging
        WHERE work_id IS NOT NULL 
          AND work_id != ''
          AND work_id != 'null'
          AND author_name IS NOT NULL
          AND author_name != ''
          AND LENGTH(work_id) < 1000
          AND LENGTH(COALESCE(author_name, '')) < 500;
    """
    
    con.execute(transform_sql)
    
    # Get final count
    final_count = con.execute("SELECT COUNT(*) FROM author_references").fetchone()[0]
    print(f"  - Final table contains {final_count:,} valid rows")
    
    # Log any rows that were filtered out
    skipped = staging_count - final_count
    if skipped > 0:
        print(f"  - Filtered out {skipped:,} invalid/incomplete rows")
        
        # Log sample of filtered rows for debugging
        con.execute("""
            INSERT INTO import_errors (error_message, row_content)
            SELECT 
                'Row filtered during transformation: missing required fields or invalid data length',
                work_id || ',' || COALESCE(doi, '') || ',' || COALESCE(author_name, '')
            FROM author_references_staging
            WHERE work_id IS NULL 
               OR work_id = ''
               OR work_id = 'null'
               OR author_name IS NULL
               OR author_name = ''
               OR LENGTH(work_id) >= 1000
               OR LENGTH(COALESCE(author_name, '')) >= 500
            LIMIT 100;
        """)
    
    # Clean up staging table to save space
    con.execute("DROP TABLE author_references_staging;")
    print("  - Staging table cleaned up")


def process_chunked(con, reference_file, chunk_size):
    """Process CSV in chunks for extremely large files"""
    print(f"Processing CSV in chunks of {chunk_size:,} rows...")
    
    # Create the final table structure first
    con.execute("""
        CREATE OR REPLACE TABLE author_references (
            work_id VARCHAR,
            doi VARCHAR,
            author_sequence INTEGER,
            author_name VARCHAR,
            normalized_author_name VARCHAR,
            affiliation_sequence INTEGER,
            affiliation_name VARCHAR,
            normalized_affiliation_name VARCHAR,
            affiliation_ror VARCHAR,
            normalized_affiliation_key VARCHAR
        );
    """)
    
    offset = 0
    chunk_num = 1
    total_rows = 0
    
    while True:
        print(f"Processing chunk {chunk_num} (offset: {offset:,})...")
        
        try:
            # Read chunk with LIMIT and OFFSET
            chunk_sql = f"""
                INSERT INTO author_references
                SELECT
                    work_id,
                    CASE 
                        WHEN doi IS NULL OR doi = '' OR doi = 'null' THEN NULL
                        ELSE doi
                    END as doi,
                    TRY_CAST(author_sequence AS INTEGER) as author_sequence,
                    author_name,
                    normalized_author_name,
                    TRY_CAST(affiliation_sequence AS INTEGER) as affiliation_sequence,
                    affiliation_name,
                    normalized_affiliation_name,
                    affiliation_ror,
                    lower(trim(COALESCE(normalized_affiliation_name, ''))) as normalized_affiliation_key
                FROM read_csv_auto(
                    '{reference_file}',
                    header=true,
                    delim=',',
                    parallel=false,
                    ignore_errors=true,
                    all_varchar=true
                )
                WHERE work_id IS NOT NULL 
                  AND work_id != ''
                  AND work_id != 'null'
                  AND author_name IS NOT NULL
                  AND author_name != ''
                  AND LENGTH(work_id) < 1000
                  AND LENGTH(COALESCE(author_name, '')) < 500
                LIMIT {chunk_size}
                OFFSET {offset};
            """
            
            result = con.execute(chunk_sql)
            rows_inserted = result.rowcount if hasattr(result, 'rowcount') else chunk_size
            
            if rows_inserted == 0:
                print(f"No more rows to process. Total rows imported: {total_rows:,}")
                break
            
            total_rows += rows_inserted
            print(f"  - Chunk {chunk_num} completed: {rows_inserted:,} rows imported")
            
            offset += chunk_size
            chunk_num += 1
            
        except Exception as e:
            print(f"Error processing chunk {chunk_num}: {e}")
            # Log error and continue with next chunk
            con.execute(f"""
                INSERT INTO import_errors (error_message, row_content)
                VALUES ('Chunk {chunk_num} processing error', '{str(e)[:500]}');
            """)
            offset += chunk_size
            chunk_num += 1
            
            # Safety check to prevent infinite loop
            if chunk_num > 10000:  # Adjust based on expected file size
                print("Safety limit reached. Stopping chunk processing.")
                break


if __name__ == '__main__':
    args = parse_arguments()
    setup_database(
        args.db_file, 
        args.reference_file, 
        args.memory_limit,
        args.temp_dir,
        args.chunk_size,
        args.skip_indexes
    )