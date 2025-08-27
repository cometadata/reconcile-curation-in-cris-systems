import os
import sys
import argparse

from query_db.config import load_config
from query_db.db import DatabaseManager
from query_db.workflows import FileProcessor, AffiliationSearchProcessor
from query_db.analysis.entity_extraction import EntityExtractor
from query_db.constants import DEFAULT_MEMORY_LIMIT
from query_db.udf import register_all_udfs


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="""
        Query the author-affiliation database to find author-affiliation linkages and related works.

        Three modes:

        1. Process work and author file to find linked affiliations:
           python %(prog)s --process-file --input-file /path/to/input.csv --output-file results.csv --db-file publications.duckdb --config config.yaml

        2. Search for works by affiliation:
           python %(prog)s --search-affiliation --input-file affiliations.csv --output-file works.csv --db-file publications.duckdb --config config.yaml
           
        3. Discover works related to input IDs (DOIs/Work IDs) via shared affiliations:
           python %(prog)s --id-search --input-file ids.csv --output-file results.csv --db-file publications.duckdb --config config.yaml
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-i", "--input-file",
                        help="Path to the input CSV file.")
    parser.add_argument("-o", "--output-file",
                        help="Path to the output CSV file where results will be saved.")
    parser.add_argument("-d", "--db-file", required=True,
                        help="Path to the DuckDB database file to use.")
    parser.add_argument("-m", "--memory-limit", default=DEFAULT_MEMORY_LIMIT,
                        help=f"Memory limit for DB processing (e.g., '16GB', '2GB'). Default: {DEFAULT_MEMORY_LIMIT}")
    parser.add_argument("-c", "--config", required=True,
                        help="Path to the YAML configuration file.")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-p", "--process-file", action="store_true",
                      help="Run in file processing mode. Requires --input-file and --output-file.")
    mode.add_argument("-s", "--search-affiliation", action="store_true",
                      help="Search for works using a list of affiliations from an input file.")
    
    parser.add_argument("--no-udf", action="store_true",
                        help="Disable User-Defined Functions (UDF) for name matching. "
                             "UDFs provide significant performance improvements but can be disabled for compatibility.")

    args = parser.parse_args()

    if args.process_file or args.search_affiliation:
        if not args.input_file or not args.output_file:
            parser.error("Both --input-file and --output-file are required.")
    
    return args


def main():
    args = parse_arguments()
    config = load_config(args.config)
    
    use_udf = not args.no_udf
    
    db_manager = DatabaseManager(
        db_file=args.db_file,
        memory_limit=args.memory_limit,
        read_only=(args.search_affiliation)
    )
    
    if use_udf and not args.process_file:
        print("Registering database UDFs...")
        try:
            register_all_udfs(db_manager)
            print("UDF registration successful")
        except Exception as e:
            print(f"Warning: UDF registration failed, continuing without UDFs: {e}")
            use_udf = False
    
    try:
        if args.process_file:
            entity_extractor = None
            if config.get('entity_extraction_enabled', True):
                print("Initializing NER model for entity extraction...")
                entity_extractor = EntityExtractor()
            
            processor = FileProcessor(db_manager, config, entity_extractor)
            processor.run(args.input_file, args.output_file)
            
        elif args.search_affiliation:
            processor = AffiliationSearchProcessor(db_manager, config)
            processor.run(args.input_file, args.output_file)
            
    
    finally:
        db_manager.close()


if __name__ == '__main__':
    main()