"""
PostgreSQL Bulk Update Script - 50K Records
Updates 50,000 records to reduce database load
"""

import psycopg2
import random
import time

import os
from dotenv import load_dotenv
load_dotenv()

# Database connection string (set in .env file)
DB_URL = os.getenv("POSTGRES_URL", "postgres://user:pass@localhost:5432/db")

# Configuration
RECORDS_TO_UPDATE = 50_000
BATCH_SIZE = 2_500  # Smaller batches for stability


def random_decimal(min_val=0, max_val=10000):
    """Generate random decimal"""
    return round(random.uniform(min_val, max_val), 2)


def main():
    print("=" * 60)
    print("PostgreSQL Bulk Update Script - 50K Records")
    print("=" * 60)
    print(f"Records to update: {RECORDS_TO_UPDATE:,}")
    print(f"Batch size: {BATCH_SIZE:,}")
    
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
        # Get ID range
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(id), MAX(id), COUNT(*) FROM test_users")
            min_id, max_id, total = cur.fetchone()
            print(f"\n📊 Table stats: {total:,} records (ID {min_id} to {max_id})")
        
        if total == 0:
            print("⚠️  No records to update!")
            return
        
        # Update first 50k records
        print(f"\n📊 Updating first {RECORDS_TO_UPDATE:,} records...")
        print("-" * 50)
        
        departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support', 'R&D']
        
        start_time = time.time()
        updated = 0
        current_id = min_id
        end_target = min_id + RECORDS_TO_UPDATE - 1
        
        with conn.cursor() as cur:
            while current_id <= end_target and current_id <= max_id:
                end_id = min(current_id + BATCH_SIZE - 1, end_target, max_id)
                
                # Generate random update values
                new_salary_factor = random.uniform(0.9, 1.2)
                new_score = random_decimal(0, 100)
                new_dept = random.choice(departments)
                is_active = random.choice([True, False])
                
                cur.execute("""
                    UPDATE test_users 
                    SET salary = salary * %s,
                        score = %s,
                        department = %s,
                        is_active = %s,
                        first_name = first_name || '_upd'
                    WHERE id BETWEEN %s AND %s
                """, (new_salary_factor, new_score, new_dept, is_active, current_id, end_id))
                
                batch_updated = cur.rowcount
                updated += batch_updated
                current_id = end_id + 1
                
                elapsed = time.time() - start_time
                rate = updated / elapsed if elapsed > 0 else 0
                progress = updated / RECORDS_TO_UPDATE * 100
                
                print(f"\r  Progress: {updated:,}/{RECORDS_TO_UPDATE:,} ({progress:.1f}%) | "
                      f"Rate: {rate:,.0f} rec/sec | "
                      f"Elapsed: {elapsed:.1f}s", end="", flush=True)
            
            conn.commit()
        
        total_time = time.time() - start_time
        print(f"\n  ✓ Completed in {total_time:.2f}s ({updated/total_time:,.0f} records/sec)")
        
        # Verify updates
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM test_users WHERE first_name LIKE '%_upd'
            """)
            updated_count = cur.fetchone()[0]
            print(f"\n✓ Verification: {updated_count:,} records have '_upd' suffix")
        
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
