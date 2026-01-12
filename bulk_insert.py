"""
Optimized PostgreSQL Bulk Insert Script
Inserts 1 million random records using batch operations and COPY command
"""

import psycopg2
from psycopg2 import sql
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
TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 10_000  # Records per batch for execute_values
COPY_BATCH_SIZE = 50_000  # Records per COPY batch


def random_string(length=10):
    """Generate random string"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_email():
    """Generate random email"""
    return f"{random_string(8)}@{random_string(5)}.com"


def random_date(start_year=2020, end_year=2024):
    """Generate random date"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def random_decimal(min_val=0, max_val=10000):
    """Generate random decimal"""
    return round(random.uniform(min_val, max_val), 2)


def create_table(conn):
    """Create the test table if it doesn't exist"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS test_users;
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                email VARCHAR(100) NOT NULL,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                age INTEGER,
                salary DECIMAL(12, 2),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                department VARCHAR(50),
                score DECIMAL(5, 2)
            );
        """)
        conn.commit()
        print("✓ Table 'test_users' created successfully")


def generate_record():
    """Generate a single random record"""
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support']
    return (
        random_string(12),                    # username
        random_email(),                       # email
        random_string(8),                     # first_name
        random_string(10),                    # last_name
        random.randint(18, 65),               # age
        random_decimal(30000, 150000),        # salary
        random.choice([True, False]),         # is_active
        random_date(),                        # created_at
        random.choice(departments),           # department
        random_decimal(0, 100)                # score
    )


def generate_batch(size):
    """Generate a batch of random records"""
    return [generate_record() for _ in range(size)]


def method_execute_values(conn, total_records, batch_size):
    """
    Method 1: Using execute_values (fast batch insert)
    """
    print(f"\n📊 Method: execute_values (batch_size={batch_size:,})")
    print("-" * 50)
    
    start_time = time.time()
    inserted = 0
    
    with conn.cursor() as cur:
        while inserted < total_records:
            current_batch = min(batch_size, total_records - inserted)
            batch = generate_batch(current_batch)
            
            execute_values(
                cur,
                """
                INSERT INTO test_users 
                (username, email, first_name, last_name, age, salary, is_active, created_at, department, score)
                VALUES %s
                """,
                batch,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            
            inserted += current_batch
            elapsed = time.time() - start_time
            rate = inserted / elapsed if elapsed > 0 else 0
            
            print(f"\r  Progress: {inserted:,}/{total_records:,} ({100*inserted/total_records:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/sec | "
                  f"Elapsed: {elapsed:.1f}s", end="", flush=True)
        
        conn.commit()
    
    total_time = time.time() - start_time
    print(f"\n  ✓ Completed in {total_time:.2f}s ({total_records/total_time:,.0f} records/sec)")
    return total_time


def method_copy(conn, total_records, batch_size):
    """
    Method 2: Using COPY command (fastest for bulk inserts)
    """
    print(f"\n📊 Method: COPY command (batch_size={batch_size:,})")
    print("-" * 50)
    
    start_time = time.time()
    inserted = 0
    
    with conn.cursor() as cur:
        while inserted < total_records:
            current_batch = min(batch_size, total_records - inserted)
            batch = generate_batch(current_batch)
            
            # Create CSV-like string buffer
            buffer = StringIO()
            for record in batch:
                # Convert record to tab-separated string
                line = '\t'.join([
                    str(record[0]),  # username
                    str(record[1]),  # email
                    str(record[2]),  # first_name
                    str(record[3]),  # last_name
                    str(record[4]),  # age
                    str(record[5]),  # salary
                    't' if record[6] else 'f',  # is_active
                    record[7].strftime('%Y-%m-%d %H:%M:%S'),  # created_at
                    str(record[8]),  # department
                    str(record[9]),  # score
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
            
            print(f"\r  Progress: {inserted:,}/{total_records:,} ({100*inserted/total_records:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/sec | "
                  f"Elapsed: {elapsed:.1f}s", end="", flush=True)
        
        conn.commit()
    
    total_time = time.time() - start_time
    print(f"\n  ✓ Completed in {total_time:.2f}s ({total_records/total_time:,.0f} records/sec)")
    return total_time


def verify_count(conn):
    """Verify the number of records inserted"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_users")
        count = cur.fetchone()[0]
        print(f"\n✓ Verified: {count:,} records in table")
        return count


def main():
    parser = argparse.ArgumentParser(description='Bulk insert random data into PostgreSQL')
    parser.add_argument('--records', type=int, default=TOTAL_RECORDS, 
                        help=f'Number of records to insert (default: {TOTAL_RECORDS:,})')
    parser.add_argument('--method', choices=['execute_values', 'copy', 'both'], 
                        default='copy', help='Insert method (default: copy)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Batch size (default: {BATCH_SIZE:,})')
    args = parser.parse_args()
    
    print("=" * 60)
    print("PostgreSQL Bulk Insert Script")
    print("=" * 60)
    print(f"Target: {args.records:,} records")
    print(f"Method: {args.method}")
    print(f"Batch size: {args.batch_size:,}")
    
    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return
    
    try:
        # Create table
        create_table(conn)
        
        # Run selected method(s)
        if args.method == 'execute_values':
            method_execute_values(conn, args.records, args.batch_size)
        elif args.method == 'copy':
            method_copy(conn, args.records, args.batch_size)
        elif args.method == 'both':
            # Test both methods with smaller dataset
            test_size = min(100_000, args.records)
            print(f"\n🧪 Benchmarking both methods with {test_size:,} records...")
            
            create_table(conn)
            t1 = method_execute_values(conn, test_size, args.batch_size)
            
            create_table(conn)
            t2 = method_copy(conn, test_size, args.batch_size)
            
            print("\n" + "=" * 60)
            print("Benchmark Results:")
            print(f"  execute_values: {t1:.2f}s ({test_size/t1:,.0f} rec/sec)")
            print(f"  COPY command:   {t2:.2f}s ({test_size/t2:,.0f} rec/sec)")
            print(f"  Winner: {'COPY' if t2 < t1 else 'execute_values'} ({abs(t1-t2)/max(t1,t2)*100:.1f}% faster)")
        
        # Verify count
        verify_count(conn)
        
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
