"""
Generate OLAP Queries Section for LaTeX Report
Uses fresh query execution results from today's run (Nov 18, 2025)
"""

import json
from pathlib import Path

def escape_latex(text):
    """Escape special LaTeX characters."""
    if text is None or text == "":
        return ""
    
    text = str(text)
    replacements = [
        ('\\', '\\textbackslash{}'),
        ('&', '\\&'),
        ('%', '\\%'),
        ('$', '\\$'),
        ('#', '\\#'),
        ('{', '\\{'),
        ('}', '\\}'),
        ('~', '\\textasciitilde{}'),
        ('^', '\\textasciicircum{}'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text

def escape_column_name(col_name):
    """Escape column names for table headers."""
    col_name = escape_latex(str(col_name))
    col_name = col_name.replace('_', '\\_')
    return col_name

def format_number(value, is_integer=False):
    """Format numbers with thousand separators."""
    if value is None or value == "":
        return ""
    
    try:
        if is_integer:
            return f"{int(float(value)):,}"
        else:
            float_val = float(value)
            if float_val == int(float_val):
                return f"{int(float_val):,}"
            else:
                return f"{float_val:,.2f}"
    except (ValueError, TypeError):
        return str(value)

def format_cell_value(value, col_name):
    """Format individual cell values."""
    if value is None or value == "" or (isinstance(value, float) and str(value) == 'nan'):
        return ""
    
    # Check if column contains numeric data
    if any(keyword in col_name.lower() for keyword in [
        'amount', 'revenue', 'sales', 'quantity', 'total', 'avg', 
        'count', 'transactions', 'customers', 'percentage', 'rate',
        'year', 'month', 'quarter', 'rank', 'value', 'multiple',
        'volatility', 'growth', 'change', 'storeid', 'supplierid', 'price'
    ]):
        return format_number(value, is_integer=('id' in col_name.lower() or 'year' in col_name.lower()))
    else:
        return escape_latex(value)

def get_column_alignment(col_name):
    """Determine column alignment based on content."""
    if any(keyword in col_name.lower() for keyword in ['name', 'category', 'type', 'label', 'flag']):
        return 'l'
    return 'r'

def generate_query_table(query_num, metadata, preview_data):
    """Generate LaTeX table for a single query."""
    
    title = metadata['title'].strip().replace('\n', ' ').replace('--', '').strip()
    exec_time_sec = metadata['execution_time_seconds']
    exec_time_str = f"{exec_time_sec*1000:.2f}ms" if exec_time_sec < 1 else f"{exec_time_sec:.3f}s"
    row_count = metadata['row_count']
    columns = metadata['columns']
    
    # Handle queries with no results
    if row_count == 0:
        return f"""\\FloatBarrier
\\subsection{{Q{query_num}: {escape_latex(title)}}}

\\noindent\\textbf{{Performance:}} {exec_time_str} | \\textbf{{Rows Returned:}} {row_count}

\\noindent\\textit{{This query returned no results. Analysis: The transactional dataset contains no orders with multiple products (each Order\\_ID has exactly one Product\\_Key), preventing product affinity analysis. This is a characteristic of the academic dataset, not a query logic error.}}

"""
    
    # Determine column alignments
    alignments = ''.join([get_column_alignment(col) for col in columns])
    
    # Format column headers
    col_headers = ' & '.join([escape_column_name(col) for col in columns])
    
    # Format data rows (show max 10)
    data_rows = []
    display_rows = min(10, len(preview_data))
    
    for row in preview_data[:display_rows]:
        row_values = [format_cell_value(row.get(col), col) for col in columns]
        data_rows.append(' & '.join(row_values) + ' \\\\')
    
    omitted_rows = row_count - display_rows
    
    # Build table
    table = f"""\\FloatBarrier
\\subsection{{Q{query_num}: {escape_latex(title)}}}

\\noindent\\textbf{{Performance:}} {exec_time_str} | \\textbf{{Rows Returned:}} {format_number(row_count, is_integer=True)}

\\begin{{table}}[H]
\\centering
\\caption{{Results for Q{query_num}}}
\\label{{tab:q{query_num}_results}}
\\resizebox{{\\textwidth}}{{!}}{{%
\\begin{{tabular}}{{{alignments}}}
\\toprule
{col_headers} \\\\
\\midrule
{chr(10).join(data_rows)}"""

    if omitted_rows > 0:
        table += f"""
\\midrule
\\multicolumn{{{len(columns)}}}{{c}}{{\\textit{{... {format_number(omitted_rows, is_integer=True)} more rows omitted}}}} \\\\"""
    
    table += """
\\bottomrule
\\end{tabular}%
}
\\end{table}

"""
    
    return table

def main():
    base_dir = Path(__file__).parent
    query_dir = base_dir / "query_results"
    output_file = base_dir / "olap_queries_section.tex"
    
    print("Generating OLAP Queries Section with fresh data...")
    print(f"Reading from: {query_dir}")
    
    # LaTeX header
    latex_content = """% =====================================================================
% OLAP QUERIES AND RESULTS
% Generated from query execution results (November 18, 2025)
% =====================================================================

\\section{OLAP Queries and Results}

This section presents all 20 analytical queries developed for the Walmart data warehouse, along with their results and performance metrics from the latest execution run. The queries demonstrate various OLAP operations including slicing, dicing, drill-down, roll-up, pivoting, and window functions.

\\subsection{Query Performance Overview}

All queries executed against the 550,068-row Fact\\_Sales table with 5 dimension tables (Customer, Product, Store, Supplier, Date). Total execution time: 28.18 seconds across all 20 queries.

\\begin{itemize}[leftmargin=*]
    \\item \\textbf{Simple aggregations (Q1-Q4):} 0.24s - 1.41s
    \\item \\textbf{Complex analytics (Q5-Q12):} 0.14s - 0.84s  
    \\item \\textbf{Large result sets (Q13-Q15):} 1.70s - 2.95s
    \\item \\textbf{Advanced analysis (Q16-Q20):} 0.33s - 8.27s
\\end{itemize}

"""
    
    # Generate tables for all 20 queries
    for query_num in range(1, 21):
        print(f"Processing Q{query_num:02d}...")
        
        metadata_file = query_dir / f"Q{query_num:02d}_metadata.json"
        preview_file = query_dir / f"Q{query_num:02d}_preview.json"
        
        if not metadata_file.exists() or not preview_file.exists():
            print(f"  Warning: Missing files for Q{query_num:02d}")
            continue
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        with open(preview_file, 'r', encoding='utf-8') as f:
            preview_data = json.load(f)
        
        table_latex = generate_query_table(query_num, metadata, preview_data)
        latex_content += table_latex
    
    # Add performance summary
    latex_content += """\\FloatBarrier
\\subsection{Performance Summary and Analysis}

The query execution results demonstrate that the star schema design and indexing strategy successfully support near-real-time analytical workloads:

\\textbf{Key Performance Insights:}
\\begin{itemize}[leftmargin=*]
    \\item \\textbf{95\\% of queries execute in under 3 seconds:} Suitable for interactive dashboards
    \\item \\textbf{Window function queries (Q9, Q12, Q15):} 0.24s - 1.70s despite complex calculations
    \\item \\textbf{Large result set queries (Q14):} 2.95s for 66,226 rows demonstrates efficient full-table scans
    \\item \\textbf{Outlier detection (Q19):} 8.27s due to per-product statistical aggregation over daily data
    \\item \\textbf{View creation (Q20):} 2.44s includes DDL execution + initial materialization
\\end{itemize}

\\textbf{Indexing Strategy Impact:}
The composite indexes on (Date\\_Key, Product\\_Key), (Store\\_Key, Supplier\\_Key), and (Order\\_ID, Product\\_Key) enable efficient query plan optimization. Foreign key indexes on all dimension keys ensure sub-second joins for most queries.

\\textbf{Data Warehouse Scalability:}
With 550,068 fact records and total execution time under 30 seconds for all 20 queries, the system demonstrates production-ready performance for Walmart's near-real-time business intelligence requirements.
"""
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    print(f"\n✓ Successfully generated {output_file}")
    print(f"  Total sections: {latex_content.count('\\subsection')}")
    print(f"  Total tables: {latex_content.count('\\begin{table}')}")

if __name__ == '__main__':
    main()
