import pandas as pd
from sql_queries import analytics_queries
from utils import (
    connect_to_redshift,
    format_query_results
)


def execute_analytics_queries(conn, cur):
    """
    Executes predefined analytical queries.
    
    Args:
        conn: Database connection
        cur: Database cursor
        
    Returns:
        list: List of formatted results
    """
    results = []
    
    for query_name, query in analytics_queries.items():
        try:
            print(f"Executing query: {query_name}...")
            cur.execute(query)
            rows = cur.fetchall()
            
            # Format results using utility function
            formatted_result = format_query_results(cur, rows, query_name)
            results.append(formatted_result)
            
        except Exception as e:
            print(f"Error executing query '{query_name}': {e}")
            results.append(f"\n=== {query_name} ===\nERROR: {e}")
    
    return results


def run_analytics():
    """
    Main function that runs database analytics.
    """
    print("\n" + "=" * 80)
    print("SPARKIFY ANALYTICS")
    print("=" * 80)
    
    conn = None
    cur = None
    
    try:
        # Connect to database
        conn, cur = connect_to_redshift()
        
        # Execute analytical queries
        results = execute_analytics_queries(conn, cur)
        
        # Display all results
        for result in results:
            print(result)
            
        print("\n" + "=" * 80)
        print("ANALYTICS COMPLETED")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error executing analytics: {e}")
        raise
    finally:
        # Close connections directly
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    run_analytics()