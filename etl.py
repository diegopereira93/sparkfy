import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries
from utils import (
    get_config, 
    connect_to_redshift,
    execute_query
)


def load_staging_tables(cur, conn, config):
    """
    Load data from S3 into staging tables.
    
    Args:
        cur: Database cursor
        conn: Database connection
        config: Configuration parser with S3 paths
    """
    try:
        print("\n" + "=" * 80)
        print("STARTING DATA LOADING TO STAGING")
        print("=" * 80)
        
        for i, query in enumerate(copy_table_queries):
            try:
                print(f"\nExecuting staging query {i+1}/{len(copy_table_queries)}")
                
                # Identify which staging is being loaded
                if "staging_events" in query.lower():
                    print(f"Loading staging_events...")
                elif "staging_songs" in query.lower():
                    print(f"Loading staging_songs...")
                
                execute_query(cur, conn, query, f"Staging query {i+1}")
                
            except Exception as e:
                print(f"Error in staging query {i+1}: {e}")
                conn.rollback()
                raise
        
        print("\nStaging data loading completed successfully!")
        
    except Exception as e:
        print(f"Error during data loading to staging: {e}")
        conn.rollback()
        raise


def insert_tables(cur, conn):
    """
    Insert data from staging tables into analytics tables.
    
    Args:
        cur: Database cursor
        conn: Database connection
    """
    print("\n" + "=" * 80)
    print("STARTING INSERTION INTO ANALYTICAL TABLES")
    print("=" * 80)
    
    for i, query in enumerate(insert_table_queries):
        try:
            table_name = query.split("INSERT INTO ")[1].split(" ")[0] if "INSERT INTO " in query else f"table {i+1}"
            print(f"Populating {table_name}...")
            
            execute_query(cur, conn, query, f"Populating table {table_name}")
            
        except Exception as e:
            print(f"Error populating {table_name}: {e}")
            conn.rollback()
            raise
    
    print("\nInsertion into analytical tables completed successfully!")


def run_etl():
    """
    Main function to run the complete ETL process.
    """
    print("\n" + "=" * 80)
    print("SPARKIFY ETL")
    print("=" * 80)
    
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        # Load configuration
        config = get_config()
        
        # Connect to database
        conn, cur = connect_to_redshift()
        
        # Load data into staging tables
        load_staging_tables(cur, conn, config)
        
        # Insert data into analytical tables
        insert_tables(cur, conn)
        
        # Display summary
        total_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("ETL PROCESSING SUMMARY")
        print("=" * 80)
        
        print(f"Total execution time: {total_time:.2f} seconds")
        print("\nETL process completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {e}")
        raise
    finally:
        # Close connections
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    run_etl()