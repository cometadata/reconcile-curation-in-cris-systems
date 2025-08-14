import os
import sys
import argparse
import duckdb


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Verify and analyze DuckDB database with author_references table",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "-d", "--db-file",
        required=True,
        help="Path to the DuckDB database file to verify"
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=10,
        help="Number of sample rows to display (default: 10)"
    )

    return parser.parse_args()


def verify_database(db_file, sample_size=10):

    if not os.path.exists(db_file):
        print(f"Error: Database file not found at '{db_file}'")
        sys.exit(1)

    file_size_gb = os.path.getsize(db_file) / (1024**3)
    print(f"Database file: {db_file}")
    print(f"File size: {file_size_gb:.2f} GB\n")

    con = duckdb.connect(database=db_file, read_only=True)

    try:
        tables = con.execute("SHOW TABLES").fetchall()
        print("Tables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        print()

        if any('author_references' in t for t in tables):
            print("Table: author_references")
            print("-" * 50)

            row_count = con.execute(
                "SELECT COUNT(*) FROM author_references").fetchone()[0]
            print(f"Total rows: {row_count:,}")

            columns = con.execute("DESCRIBE author_references").fetchall()
            print("\nColumns:")
            for col in columns:
                print(f"  {col[0]:<30} {col[1]:<15} {col[2] if col[2] else ''}")

            print("\nIndexes:")
            indexes = con.execute("""
                SELECT index_name
                FROM duckdb_indexes() 
                WHERE table_name = 'author_references'
            """).fetchall()

            if indexes:
                for idx in indexes:
                    print(f"  {idx[0]}")
            else:
                print("  No indexes found")

            print("\nData Statistics:")
            stats = con.execute("""
                SELECT 
                    COUNT(DISTINCT work_id) as unique_works,
                    COUNT(DISTINCT author_name) as unique_authors,
                    COUNT(DISTINCT affiliation_name) as unique_affiliations,
                    COUNT(DISTINCT affiliation_ror) as unique_rors,
                    COUNT(*) FILTER (WHERE doi IS NOT NULL) as rows_with_doi,
                    COUNT(*) FILTER (WHERE affiliation_ror IS NOT NULL) as rows_with_ror
                FROM author_references
            """).fetchone()

            print(f"  Unique works: {stats[0]:,}")
            print(f"  Unique authors: {stats[1]:,}")
            print(f"  Unique affiliations: {stats[2]:,}")
            print(f"  Unique ROR IDs: {stats[3]:,}")
            print(f"  Rows with DOI: {stats[4]:,} ({stats[4]*100/row_count:.1f}%)")
            print(f"  Rows with ROR: {stats[5]:,} ({stats[5]*100/row_count:.1f}%)")

            print("\nData Quality Checks:")

            null_works = con.execute(
                "SELECT COUNT(*) FROM author_references WHERE work_id IS NULL").fetchone()[0]
            print(f"  Rows with NULL work_id: {null_works:,}")

            null_authors = con.execute(
                "SELECT COUNT(*) FROM author_references WHERE author_name IS NULL").fetchone()[0]
            print(f"  Rows with NULL author_name: {null_authors:,}")

            print(f"\nSample Data (first {sample_size} rows):")
            print("-" * 50)
            samples = con.execute(f"""
                SELECT 
                    work_id,
                    author_name,
                    affiliation_name,
                    affiliation_ror
                FROM author_references 
                LIMIT {sample_size}
            """).fetchall()

            for i, row in enumerate(samples, 1):
                print(f"\nRow {i}:")
                print(f"  Work ID: {row[0][:50]}..." if len(str(row[0])) > 50 else f"  Work ID: {row[0]}")
                print(f"  Author: {row[1]}")
                print(f"  Affiliation: {row[2][:50]}..." if row[2] and len(str(row[2])) > 50 else f"  Affiliation: {row[2]}")
                print(f"  ROR: {row[3]}")

        if any('import_errors' in t for t in tables):
            print("\n" + "="*50)
            print("Import Errors Table:")
            print("-" * 50)
            error_count = con.execute(
                "SELECT COUNT(*) FROM import_errors").fetchone()[0]
            print(f"Total errors logged: {error_count:,}")

            if error_count > 0:
                print("\nRecent errors (last 5):")
                errors = con.execute("""
                    SELECT error_message, timestamp 
                    FROM import_errors 
                    ORDER BY timestamp DESC 
                    LIMIT 5
                """).fetchall()
                for err in errors:
                    print(f"  [{err[1]}] {err[0][:100]}...")

        print("\n" + "="*50)
        print("Database verification complete!")

    except Exception as e:
        print(f"Error during verification: {e}")
        sys.exit(1)
    finally:
        con.close()


def main():
    args = parse_arguments()
    verify_database(args.db_file, args.sample)


if __name__ == "__main__":
    main()
