"""
PostgreSQL Bulk Insert Script - Append Mode
Inserts additional records without dropping existing data
"""

import psycopg2
from psycopg2.extras import execute_values
import random
import string
import time
from datetime import datetime, timedelta
from io import StringIO
import argparse

import os
from dotenv import load_dotenv
load_dotenv()

# Database connection string (set in .env file)
DB_URL = os.getenv("POSTGRES_URL", "postgres://user:pass@localhost:5432/db")

# Configuration
TOTAL_RECORDS = 2_000_000
BATCH_SIZE = 50_000


def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_email():
    return f"{random_string(8)}@{random_string(5)}.com"


def random_date(start_year=2020, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def random_decimal(min_val=0, max_val=10000):
    return round(random.uniform(min_val, max_val), 2)


def generate_record():
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support']
    return (
        random_string(12),
        random_email(),
        random_string(8),
        random_string(10),
        random.randint(18, 65),
        random_decimal(30000, 150000),
        random.choice([True, False]),
        random_date(),
        random.choice(departments),
        random_decimal(0, 100)
    )


def generate_batch(size):
    return [generate_record() for _ in range(size)]


def main():
    parser = argparse.ArgumentParser(description='Append records to PostgreSQL')
    parser.add_argument('--records', type=int, default=TOTAL_RECORDS,
                        help=f'Number of records to insert (default: {TOTAL_RECORDS:,})')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Batch size (default: {BATCH_SIZE:,})')
    args = parser.parse_args()

    print("=" * 60)
    print("PostgreSQL Bulk Insert - APPEND Mode")
    print("=" * 60)
    print(f"Records to add: {args.records:,}")
    print(f"Batch size: {args.batch_size:,}")

    print("\n🔌 Connecting to database...")
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return

    try:
        # Get current count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM test_users")
            initial_count = cur.fetchone()[0]
            print(f"📊 Current records: {initial_count:,}")

        # Insert using COPY
        print(f"\n📊 Inserting {args.records:,} new records...")
        print("-" * 50)

        start_time = time.time()
        inserted = 0

        with conn.cursor() as cur:
            while inserted < args.records:
                current_batch = min(args.batch_size, args.records - inserted)
                batch = generate_batch(current_batch)

                buffer = StringIO()
                for record in batch:
                    line = '\t'.join([
                        str(record[0]),
                        str(record[1]),
                        str(record[2]),
                        str(record[3]),
                        str(record[4]),
                        str(record[5]),
                        't' if record[6] else 'f',
                        record[7].strftime('%Y-%m-%d %H:%M:%S'),
                        str(record[8]),
                        str(record[9]),
                    ])
                    buffer.write(line + '\n')

                buffer.seek(0)

                cur.copy_from(
                    buffer,
                    'test_users',
                    columns=('username', 'email', 'first_name', 'last_name', 'age',
                             'salary', 'is_active', 'created_at', 'department', 'score'),
                    sep='\t'
                )

                inserted += current_batch
                elapsed = time.time() - start_time
                rate = inserted / elapsed if elapsed > 0 else 0

                print(f"\r  Progress: {inserted:,}/{args.records:,} ({100*inserted/args.records:.1f}%) | "
                      f"Rate: {rate:,.0f} rec/sec | "
                      f"Elapsed: {elapsed:.1f}s", end="", flush=True)

            conn.commit()

        total_time = time.time() - start_time
        print(f"\n  ✓ Completed in {total_time:.2f}s ({args.records/total_time:,.0f} records/sec)")

        # Verify
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM test_users")
            final_count = cur.fetchone()[0]
            print(f"\n✓ Records: {initial_count:,} → {final_count:,} (+{final_count - initial_count:,})")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        print("\n🔌 Connection closed")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
