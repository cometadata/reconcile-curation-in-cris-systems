import os
import sys
import time
import duckdb
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Create indexes on an existing DuckDB database with author_references table",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-d", "--db-file",
        required=True,
        help="Path to the existing DuckDB database file"
    )
    parser.add_argument(
        "-m", "--memory-limit",
        default="16GB",
        help="Memory limit for index creation (e.g., '32GB', '8GB'). Default: 16GB"
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Temporary directory for disk spilling during index creation"
    )
    parser.add_argument(
        "--indexes",
        nargs="+",
        choices=["work_id", "doi", "norm_name", "norm_affil", "ror", "all"],
        default=["all"],
        help="Which indexes to create (default: all)"
    )

    return parser.parse_args()


def create_indexes(db_file, memory_limit, temp_dir=None, selected_indexes=["all"]):

    if not os.path.exists(db_file):
        print(f"Error: Database file not found at '{db_file}'")
        sys.exit(1)

    print(f"Opening database: {db_file}")
    con = duckdb.connect(database=db_file, read_only=False)

    try:
        print(f"Configuring database for index creation...")
        print(f"  Memory limit: {memory_limit}")

        con.execute(f"SET memory_limit='{memory_limit}';")
        con.execute("SET preserve_insertion_order=false;")

        con.execute("SET threads=4;")

        if temp_dir:
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            print(f"  Temp directory: {temp_dir}")
            con.execute(f"SET temp_directory='{temp_dir}';")

        table_check = con.execute("""
            SELECT COUNT(*) as row_count 
            FROM information_schema.tables 
            WHERE table_name = 'author_references'
        """).fetchone()

        if not table_check or table_check[0] == 0:
            print("Error: Table 'author_references' not found in database")
            sys.exit(1)

        row_count = con.execute(
            "SELECT COUNT(*) FROM author_references").fetchone()[0]
        print(f"Table 'author_references' contains {row_count:,} rows")

        all_indexes = [
            ("idx_work_id", "work_id", "work_id"),
            ("idx_doi", "doi", "doi"),
            ("idx_norm_name", "normalized_author_name", "norm_name"),
            ("idx_norm_affil", "normalized_affiliation_key", "norm_affil"),
            ("idx_ror", "affiliation_ror", "ror")
        ]

        if "all" in selected_indexes:
            indexes_to_create = all_indexes
        else:
            indexes_to_create = [
                idx for idx in all_indexes if idx[2] in selected_indexes]

        print(f"\nCreating {len(indexes_to_create)} index(es)...")
        print("This may take considerable time for large datasets.\n")

        successful = 0
        failed = 0

        for idx_name, column, short_name in indexes_to_create:
            start_time = time.time()

            existing = con.execute(f"""
                SELECT COUNT(*) 
                FROM duckdb_indexes() 
                WHERE index_name = '{idx_name}'
            """).fetchone()[0]

            if existing > 0:
                print(f"✓ Index {idx_name} already exists, skipping...")
                successful += 1
                continue

            print(f"Creating index {idx_name} on column {column}...")

            try:
                con.execute("CHECKPOINT;")
                con.execute("PRAGMA force_checkpoint;")

                con.execute(f"CREATE INDEX {idx_name} ON author_references ({column});")

                elapsed = time.time() - start_time
                print(f"  Index {idx_name} created in {elapsed:.1f} seconds")
                successful += 1

            except Exception as e:
                print(f"  Failed to create index {idx_name}: {e}")
                failed += 1

                try:
                    con.execute("PRAGMA force_checkpoint;")
                except:
                    pass

        print(f"\n{'='*50}")
        print(f"Index creation complete:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")

        if failed > 0:
            print("\nFor failed indexes, you may need to:")
            print("  1. Increase memory limit (--memory-limit)")
            print("  2. Ensure sufficient disk space in temp directory")
            print("  3. Create indexes one at a time")

        print("\nFinalizing database...")
        con.execute("CHECKPOINT;")
        print("Done!")

    except Exception as e:
        print(f"Error during index creation: {e}")
        sys.exit(1)
    finally:
        con.close()


def main():
    args = parse_arguments()
    create_indexes(
        args.db_file,
        args.memory_limit,
        args.temp_dir,
        args.indexes
    )


if __name__ == "__main__":
    main()
