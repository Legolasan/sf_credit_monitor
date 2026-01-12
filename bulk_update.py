"""
Optimized PostgreSQL Bulk Update Script
Updates all records inserted by bulk_insert.py using batch operations
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, execute_batch
import random
import string
import time
from datetime import datetime, timedelta
import argparse

import os
from dotenv import load_dotenv
load_dotenv()

# Database connection string (set in .env file)
DB_URL = os.getenv("POSTGRES_URL", "postgres://user:pass@localhost:5432/db")

# Configuration
BATCH_SIZE = 5_000  # Records per batch


def random_string(length=10):
    """Generate random string"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_decimal(min_val=0, max_val=10000):
    """Generate random decimal"""
    return round(random.uniform(min_val, max_val), 2)


def get_record_count(conn):
    """Get total number of records in the table"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_users")
        return cur.fetchone()[0]


def get_id_range(conn):
    """Get min and max IDs from the table"""
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(id), MAX(id) FROM test_users")
        return cur.fetchone()


def method_batch_update_by_id_range(conn, batch_size):
    """
    Method 1: Update records in batches using ID ranges
    Most efficient for sequential IDs
    """
    print(f"\n📊 Method: Batch Update by ID Range (batch_size={batch_size:,})")
    print("-" * 50)
    
    min_id, max_id = get_id_range(conn)
    if min_id is None:
        print("  No records found!")
        return 0
    
    total_records = max_id - min_id + 1
    print(f"  ID range: {min_id:,} to {max_id:,}")
    
    start_time = time.time()
    updated = 0
    current_id = min_id
    
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support', 'R&D']
    
    with conn.cursor() as cur:
        while current_id <= max_id:
            end_id = min(current_id + batch_size - 1, max_id)
            
            # Generate random update values for this batch
            new_salary = random_decimal(35000, 175000)
            new_score = random_decimal(0, 100)
            new_dept = random.choice(departments)
            is_active = random.choice([True, False])
            
            cur.execute("""
                UPDATE test_users 
                SET salary = salary * %s,
                    score = %s,
                    department = %s,
                    is_active = %s,
                    first_name = first_name || '_updated'
                WHERE id BETWEEN %s AND %s
            """, (random.uniform(0.9, 1.2), new_score, new_dept, is_active, current_id, end_id))
            
            batch_updated = cur.rowcount
            updated += batch_updated
            current_id = end_id + 1
            
            elapsed = time.time() - start_time
            rate = updated / elapsed if elapsed > 0 else 0
            progress = (current_id - min_id) / total_records * 100
            
            print(f"\r  Progress: {updated:,} updated ({progress:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/sec | "
                  f"Elapsed: {elapsed:.1f}s", end="", flush=True)
        
        conn.commit()
    
    total_time = time.time() - start_time
    print(f"\n  ✓ Completed in {total_time:.2f}s ({updated/total_time:,.0f} records/sec)")
    return total_time


def method_execute_batch(conn, batch_size):
    """
    Method 2: Update using execute_batch with individual UPDATE statements
    Good for non-sequential updates or when each row needs different values
    """
    print(f"\n📊 Method: execute_batch (batch_size={batch_size:,})")
    print("-" * 50)
    
    total_records = get_record_count(conn)
    if total_records == 0:
        print("  No records found!")
        return 0
    
    min_id, max_id = get_id_range(conn)
    print(f"  Total records: {total_records:,}")
    
    start_time = time.time()
    updated = 0
    
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support', 'R&D']
    
    with conn.cursor() as cur:
        # Fetch IDs in batches and update
        cur.execute("SELECT id FROM test_users ORDER BY id")
        
        while True:
            ids = cur.fetchmany(batch_size)
            if not ids:
                break
            
            # Generate update data for each ID
            update_data = [
                (
                    random_decimal(35000, 175000),  # new salary
                    random_decimal(0, 100),         # new score
                    random.choice(departments),     # new department
                    random.choice([True, False]),   # is_active
                    id_tuple[0]                     # id
                )
                for id_tuple in ids
            ]
            
            execute_batch(
                cur,
                """
                UPDATE test_users 
                SET salary = %s,
                    score = %s,
                    department = %s,
                    is_active = %s
                WHERE id = %s
                """,
                update_data,
                page_size=batch_size
            )
            
            updated += len(ids)
            elapsed = time.time() - start_time
            rate = updated / elapsed if elapsed > 0 else 0
            
            print(f"\r  Progress: {updated:,}/{total_records:,} ({100*updated/total_records:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/sec | "
                  f"Elapsed: {elapsed:.1f}s", end="", flush=True)
        
        conn.commit()
    
    total_time = time.time() - start_time
    print(f"\n  ✓ Completed in {total_time:.2f}s ({updated/total_time:,.0f} records/sec)")
    return total_time


def method_temp_table_update(conn, batch_size):
    """
    Method 3: Update using temporary table and JOIN
    Fastest for updating many columns with different values per row
    """
    print(f"\n📊 Method: Temp Table + JOIN Update (batch_size={batch_size:,})")
    print("-" * 50)
    
    total_records = get_record_count(conn)
    if total_records == 0:
        print("  No records found!")
        return 0
    
    min_id, max_id = get_id_range(conn)
    print(f"  Total records: {total_records:,}")
    
    start_time = time.time()
    updated = 0
    current_id = min_id
    
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support', 'R&D']
    
    with conn.cursor() as cur:
        while current_id <= max_id:
            end_id = min(current_id + batch_size - 1, max_id)
            
            # Create temp table with update values
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS update_batch (
                    id INTEGER PRIMARY KEY,
                    new_salary DECIMAL(12, 2),
                    new_score DECIMAL(5, 2),
                    new_department VARCHAR(50),
                    new_is_active BOOLEAN
                ) ON COMMIT DROP
            """)
            
            # Generate batch data
            batch_data = [
                (
                    i,
                    random_decimal(35000, 175000),
                    random_decimal(0, 100),
                    random.choice(departments),
                    random.choice([True, False])
                )
                for i in range(current_id, end_id + 1)
            ]
            
            # Insert into temp table
            execute_values(
                cur,
                "INSERT INTO update_batch (id, new_salary, new_score, new_department, new_is_active) VALUES %s",
                batch_data
            )
            
            # Perform JOIN update
            cur.execute("""
                UPDATE test_users t
                SET salary = u.new_salary,
                    score = u.new_score,
                    department = u.new_department,
                    is_active = u.new_is_active
                FROM update_batch u
                WHERE t.id = u.id
            """)
            
            batch_updated = cur.rowcount
            updated += batch_updated
            current_id = end_id + 1
            
            # Drop temp table
            cur.execute("DROP TABLE IF EXISTS update_batch")
            
            elapsed = time.time() - start_time
            rate = updated / elapsed if elapsed > 0 else 0
            progress = (current_id - min_id) / (max_id - min_id + 1) * 100
            
            print(f"\r  Progress: {updated:,} updated ({progress:.1f}%) | "
                  f"Rate: {rate:,.0f} rec/sec | "
                  f"Elapsed: {elapsed:.1f}s", end="", flush=True)
        
        conn.commit()
    
    total_time = time.time() - start_time
    print(f"\n  ✓ Completed in {total_time:.2f}s ({updated/total_time:,.0f} records/sec)")
    return total_time


def verify_updates(conn):
    """Verify updates were applied"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE first_name LIKE '%_updated') as with_suffix,
                AVG(salary) as avg_salary,
                AVG(score) as avg_score
            FROM test_users
        """)
        result = cur.fetchone()
        print(f"\n✓ Verification:")
        print(f"  Total records: {result[0]:,}")
        print(f"  Records with '_updated' suffix: {result[1]:,}")
        print(f"  Average salary: ${result[2]:,.2f}")
        print(f"  Average score: {result[3]:.2f}")


def main():
    parser = argparse.ArgumentParser(description='Bulk update records in PostgreSQL')
    parser.add_argument('--method', choices=['range', 'batch', 'temp_table', 'all'], 
                        default='range', help='Update method (default: range)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Batch size (default: {BATCH_SIZE:,})')
    args = parser.parse_args()
    
    print("=" * 60)
    print("PostgreSQL Bulk Update Script")
    print("=" * 60)
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
        total_records = get_record_count(conn)
        print(f"\n📊 Records in table: {total_records:,}")
        
        if total_records == 0:
            print("⚠️  No records to update! Run bulk_insert.py first.")
            return
        
        # Run selected method(s)
        if args.method == 'range':
            method_batch_update_by_id_range(conn, args.batch_size)
        elif args.method == 'batch':
            method_execute_batch(conn, args.batch_size)
        elif args.method == 'temp_table':
            method_temp_table_update(conn, args.batch_size)
        elif args.method == 'all':
            # Benchmark all methods
            print(f"\n🧪 Benchmarking all methods...")
            
            t1 = method_batch_update_by_id_range(conn, args.batch_size)
            t2 = method_execute_batch(conn, args.batch_size)
            t3 = method_temp_table_update(conn, args.batch_size)
            
            print("\n" + "=" * 60)
            print("Benchmark Results:")
            print(f"  ID Range:    {t1:.2f}s ({total_records/t1:,.0f} rec/sec)")
            print(f"  execute_batch: {t2:.2f}s ({total_records/t2:,.0f} rec/sec)")
            print(f"  Temp Table:  {t3:.2f}s ({total_records/t3:,.0f} rec/sec)")
            winner = min([(t1, 'ID Range'), (t2, 'execute_batch'), (t3, 'Temp Table')])
            print(f"  Winner: {winner[1]}")
        
        # Verify updates
        verify_updates(conn)
        
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
