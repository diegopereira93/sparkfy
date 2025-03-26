import time
from sql_queries import create_table_queries, drop_table_queries
from utils import (
    connect_to_redshift,
    execute_query
)


def drop_tables(cur, conn):
    """
    Drops all tables defined in drop_table_queries.
    
    Parameters:
        cur: cursor for executing SQL commands
        conn: database connection
    """
    print("Dropping existing tables...")
    
    for i, query in enumerate(drop_table_queries, 1):
        table_name = query.split("DROP TABLE")[1].split(";")[0].strip() if "DROP TABLE" in query else f"Table {i}"
        print(f"  Dropping {table_name}...")
        execute_query(cur, conn, query, f"Dropping {table_name}")


def create_tables(cur, conn):
    """
    Creates all tables defined in create_table_queries.
    
    Parameters:
        cur: cursor for executing SQL commands
        conn: database connection
    """
    print("\nCreating tables...")
    
    for i, query in enumerate(create_table_queries, 1):
        table_name = query.split("CREATE TABLE")[1].split("(")[0].strip() if "CREATE TABLE" in query else f"Table {i}"
        print(f"  Creating {table_name}...")
        execute_query(cur, conn, query, f"Creating {table_name}")


def run_setup():
    """
    Main function that sets up database tables.
    """
    print("\n" + "=" * 80)
    print("SPARKIFY TABLE SETUP")
    print("=" * 80)
    
    start_time = time.time()
    
    conn = None
    cur = None
    
    try:
        # Connect to the database
        conn, cur = connect_to_redshift()
        
        # Execute database operations
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        # Calculate and display total time
        elapsed_time = time.time() - start_time
        print(f"\nTable setup completed successfully in {elapsed_time:.2f} seconds!")
        
    except Exception as e:
        print(f"Error during table setup: {e}")
        raise
    finally:
        # Close connections directly
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    run_setup()