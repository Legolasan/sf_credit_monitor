"""
Combined script: Insert 1M records, then update all of them
Runs bulk_insert followed by bulk_update
"""

import psycopg2
import time
import argparse
from bulk_insert import (
    DB_URL, create_table, method_copy, verify_count,
    TOTAL_RECORDS, COPY_BATCH_SIZE
)
from bulk_update import (
    method_batch_update_by_id_range, verify_updates, get_record_count,
    BATCH_SIZE as UPDATE_BATCH_SIZE
)


def main():
    parser = argparse.ArgumentParser(description='Insert and then update records')
    parser.add_argument('--records', type=int, default=TOTAL_RECORDS,
                        help=f'Number of records (default: {TOTAL_RECORDS:,})')
    parser.add_argument('--insert-batch', type=int, default=COPY_BATCH_SIZE,
                        help=f'Insert batch size (default: {COPY_BATCH_SIZE:,})')
    parser.add_argument('--update-batch', type=int, default=UPDATE_BATCH_SIZE,
                        help=f'Update batch size (default: {UPDATE_BATCH_SIZE:,})')
    args = parser.parse_args()
    
    print("=" * 70)
    print("PostgreSQL: INSERT then UPDATE Pipeline")
    print("=" * 70)
    print(f"Records: {args.records:,}")
    print(f"Insert batch size: {args.insert_batch:,}")
    print(f"Update batch size: {args.update_batch:,}")
    
    # Connect
    print("\n🔌 Connecting to database...")
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        print("✓ Connected successfully")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return
    
    total_start = time.time()
    
    try:
        # ============ PHASE 1: INSERT ============
        print("\n" + "=" * 70)
        print("PHASE 1: BULK INSERT")
        print("=" * 70)
        
        create_table(conn)
        insert_time = method_copy(conn, args.records, args.insert_batch)
        verify_count(conn)
        
        # ============ PHASE 2: UPDATE ============
        print("\n" + "=" * 70)
        print("PHASE 2: BULK UPDATE")
        print("=" * 70)
        
        update_time = method_batch_update_by_id_range(conn, args.update_batch)
        verify_updates(conn)
        
        # ============ SUMMARY ============
        total_time = time.time() - total_start
        
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"  Records processed: {args.records:,}")
        print(f"  Insert time:       {insert_time:.2f}s ({args.records/insert_time:,.0f} rec/sec)")
        print(f"  Update time:       {update_time:.2f}s ({args.records/update_time:,.0f} rec/sec)")
        print(f"  Total time:        {total_time:.2f}s")
        print(f"  Overall rate:      {(args.records * 2)/total_time:,.0f} operations/sec")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        print("\n🔌 Connection closed")
    
    print("\n" + "=" * 70)
    print("✅ Pipeline completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
