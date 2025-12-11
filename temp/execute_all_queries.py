"""
Script to execute all 20 OLAP queries from Queries-DW.sql
and save their outputs for report inclusion.

This script:
1. Connects to the MySQL database
2. Parses the Queries-DW.sql file to extract individual queries
3. Executes each query and captures results
4. Saves results in both CSV and JSON formats
5. Records execution time for each query
6. Generates summary statistics

Author: Muhammad Abdullah Ali (i23-2523)
Date: November 17, 2025
"""

import mysql.connector
import pandas as pd
import json
import time
import re
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'DWH_Proj'
}

def parse_sql_file(sql_file_path):
    """
    Parse the SQL file and extract individual queries with their metadata.
    
    Returns:
        List of dictionaries containing query_number, title, description, and sql_code
    """
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by query sections (Q1, Q2, ..., Q20)
    queries = []
    
    # Pattern to match query sections
    pattern = r'-- Q(\d+):\s*(.*?)\n-- =+\n(.*?)(?=-- =+\n-- Q|\Z)'
    matches = re.findall(pattern, content, re.DOTALL)
    
    for match in matches:
        query_num = int(match[0])
        title = match[1].strip()
        section_content = match[2]
        
        # Extract description (lines starting with --)
        description_lines = []
        sql_lines = []
        in_description = True
        
        for line in section_content.split('\n'):
            if line.strip().startswith('--'):
                if in_description:
                    desc = line.strip().lstrip('--').strip()
                    if desc and not desc.startswith('='):
                        description_lines.append(desc)
            else:
                in_description = False
                sql_lines.append(line)
        
        sql_code = '\n'.join(sql_lines).strip()
        
        # Remove trailing comments and separators
        sql_code = re.sub(r'\n-- =+.*$', '', sql_code, flags=re.MULTILINE)
        
        if sql_code:
            queries.append({
                'query_number': query_num,
                'title': title,
                'description': ' '.join(description_lines),
                'sql_code': sql_code
            })
    
    return queries

def execute_query(cursor, sql_code, query_num):
    """
    Execute a single query and return results with timing.
    Handles multi-statement queries (like CREATE VIEW + SELECT).
    
    Returns:
        Tuple of (results_df, execution_time, error_message)
    """
    try:
        start_time = time.time()
        
        # Check if this is a multi-statement query (contains DROP/CREATE VIEW)
        if 'DROP VIEW' in sql_code.upper() or 'CREATE VIEW' in sql_code.upper():
            # Split by semicolons and execute each statement
            statements = [stmt.strip() for stmt in sql_code.split(';') if stmt.strip()]
            results = []
            column_names = []
            
            for stmt in statements:
                cursor.execute(stmt)
                if cursor.description:
                    # This statement returns results
                    results = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
        else:
            # Single statement query
            cursor.execute(sql_code)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
        
        execution_time = time.time() - start_time
        
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=column_names)
        
        return df, execution_time, None
    
    except mysql.connector.Error as err:
        return None, 0, str(err)
    except Exception as e:
        return None, 0, str(e)

def format_execution_time(seconds):
    """Format execution time in human-readable format."""
    if seconds < 1:
        return f"{seconds*1000:.2f}ms"
    else:
        return f"{seconds:.3f}s"

def save_results(query_info, df, execution_time, output_dir):
    """
    Save query results in multiple formats.
    """
    query_num = query_info['query_number']
    
    # Create output directory if it doesn't exist
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Save as CSV
    csv_path = output_dir / f"Q{query_num:02d}_results.csv"
    df.to_csv(csv_path, index=False)
    
    # Save as JSON with metadata
    json_path = output_dir / f"Q{query_num:02d}_metadata.json"
    metadata = {
        'query_number': query_num,
        'title': query_info['title'],
        'description': query_info['description'],
        'execution_time_seconds': execution_time,
        'execution_time_formatted': format_execution_time(execution_time),
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': list(df.columns),
        'timestamp': datetime.now().isoformat()
    }
    
    with open(json_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Save first 10 rows as preview JSON (convert Decimal to float)
    preview_path = output_dir / f"Q{query_num:02d}_preview.json"
    preview_data = df.head(10).to_dict(orient='records')
    
    # Convert Decimal types and dates to serializable formats for JSON
    def convert_decimals(obj):
        if isinstance(obj, list):
            return [convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: convert_decimals(value) for key, value in obj.items()}
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return obj
    
    preview_data = convert_decimals(preview_data)
    
    with open(preview_path, 'w') as f:
        json.dump(preview_data, f, indent=2)
    
    print(f"✓ Q{query_num}: {query_info['title'][:60]}... ({len(df)} rows, {format_execution_time(execution_time)})")

def generate_summary_report(all_results, output_dir):
    """
    Generate a summary report of all query executions.
    """
    summary = {
        'total_queries': len(all_results),
        'successful_queries': sum(1 for r in all_results if r['success']),
        'failed_queries': sum(1 for r in all_results if not r['success']),
        'total_execution_time': sum(r['execution_time'] for r in all_results if r['success']),
        'queries': all_results,
        'timestamp': datetime.now().isoformat()
    }
    
    # Save summary as JSON
    summary_path = Path(output_dir) / 'query_execution_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Create a readable text summary
    summary_text_path = Path(output_dir) / 'query_execution_summary.txt'
    with open(summary_text_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("OLAP QUERIES EXECUTION SUMMARY\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Total Queries: {summary['total_queries']}\n")
        f.write(f"Successful: {summary['successful_queries']}\n")
        f.write(f"Failed: {summary['failed_queries']}\n")
        f.write(f"Total Execution Time: {format_execution_time(summary['total_execution_time'])}\n\n")
        
        f.write("=" * 80 + "\n")
        f.write("INDIVIDUAL QUERY RESULTS\n")
        f.write("=" * 80 + "\n\n")
        
        for result in all_results:
            f.write(f"Q{result['query_number']:02d}: {result['title']}\n")
            f.write("-" * 80 + "\n")
            
            if result['success']:
                f.write(f"  Status: SUCCESS\n")
                f.write(f"  Execution Time: {format_execution_time(result['execution_time'])}\n")
                f.write(f"  Rows Returned: {result['row_count']}\n")
                f.write(f"  Columns: {result['column_count']}\n")
            else:
                f.write(f"  Status: FAILED\n")
                f.write(f"  Error: {result['error']}\n")
            
            f.write("\n")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY: {summary['successful_queries']}/{summary['total_queries']} queries executed successfully")
    print(f"Total execution time: {format_execution_time(summary['total_execution_time'])}")
    print(f"Results saved to: {output_dir}")
    print(f"{'='*80}\n")

def main():
    """
    Main execution function.
    """
    print("\n" + "="*80)
    print("OLAP QUERIES EXECUTION SCRIPT")
    print("="*80 + "\n")
    
    # Paths
    sql_file = Path(__file__).parent.parent / 'Queries-DW.sql'
    output_dir = Path(__file__).parent / 'query_results'
    
    print(f"SQL File: {sql_file}")
    print(f"Output Directory: {output_dir}\n")
    
    # Parse SQL file
    print("Parsing SQL file...")
    queries = parse_sql_file(sql_file)
    print(f"Found {len(queries)} queries\n")
    
    # Connect to database
    print("Connecting to database...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✓ Connected successfully\n")
    except mysql.connector.Error as err:
        print(f"✗ Database connection failed: {err}")
        return
    
    # Execute queries
    print("Executing queries...\n")
    all_results = []
    
    for query_info in queries:
        df, exec_time, error = execute_query(cursor, query_info['sql_code'], query_info['query_number'])
        
        if error:
            print(f"✗ Q{query_info['query_number']}: {query_info['title'][:60]}... FAILED")
            print(f"  Error: {error}\n")
            all_results.append({
                'query_number': query_info['query_number'],
                'title': query_info['title'],
                'success': False,
                'error': error,
                'execution_time': 0,
                'row_count': 0,
                'column_count': 0
            })
        else:
            save_results(query_info, df, exec_time, output_dir)
            all_results.append({
                'query_number': query_info['query_number'],
                'title': query_info['title'],
                'success': True,
                'error': None,
                'execution_time': exec_time,
                'row_count': len(df),
                'column_count': len(df.columns)
            })
    
    # Close database connection
    cursor.close()
    conn.close()
    
    # Generate summary report
    print("\nGenerating summary report...")
    generate_summary_report(all_results, output_dir)
    
    print("\n✓ All done!\n")

if __name__ == "__main__":
    main()
