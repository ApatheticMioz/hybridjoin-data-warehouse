"""
Regenerate LaTeX OLAP Queries Section with Correctly Formatted Tables
Fixes issues with:
- Special character escaping (& symbol)
- Number formatting with proper thousand separators
- Column alignment
- Proper line breaks for long category names
"""

import json
import os
from pathlib import Path

def escape_latex(text):
    """Escape special LaTeX characters properly."""
    if text is None or text == "":
        return ""
    
    text = str(text)
    # Handle specific replacements - order matters!
    # Replace backslash first to avoid double-escaping
    text = text.replace('\\', '\\textbackslash{}')
    text = text.replace('&', '\\&')
    text = text.replace('%', '\\%')
    text = text.replace('$', '\\$')
    text = text.replace('#', '\\#')
    text = text.replace('{', '\\{')
    text = text.replace('}', '\\}')
    text = text.replace('~', '\\textasciitilde{}')
    text = text.replace('^', '\\textasciicircum{}')
    # Don't escape underscores - we'll handle them separately
    
    return text

def escape_column_name(col_name):
    """Escape column names for table headers - underscores need \_ """
    col_name = str(col_name)
    col_name = escape_latex(col_name)
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
            # Check if it's actually an integer value
            float_val = float(value)
            if float_val == int(float_val):
                return f"{int(float_val):,}"
            else:
                return f"{float_val:,.2f}"
    except (ValueError, TypeError):
        return str(value)

def get_column_alignment(col_name, sample_values):
    """Determine column alignment based on content."""
    # Text columns
    if any(keyword in col_name.lower() for keyword in ['name', 'category', 'type', 'label', 'flag', 'id']):
        return 'l'
    # Numeric columns
    return 'r'

def format_cell_value(value, col_name):
    """Format individual cell values."""
    if value is None or value == "" or (isinstance(value, float) and str(value) == 'nan'):
        return ""
    
    # Check if column contains numeric data
    if any(keyword in col_name.lower() for keyword in [
        'amount', 'revenue', 'sales', 'quantity', 'total', 'avg', 
        'count', 'transactions', 'customers', 'percentage', 'rate',
        'year', 'month', 'quarter', 'rank', 'value', 'multiple',
        'volatility', 'growth', 'change', 'storeid', 'supplierid'
    ]):
        return format_number(value, is_integer='id' in col_name.lower() or 'year' in col_name.lower())
    else:
        return escape_latex(value)

def generate_table(query_num, metadata, preview_data):
    """Generate LaTeX table code for a query."""
    
    # Get basic info
    title = metadata['title'].strip().replace('\n', ' ').replace('--', '').strip()
    description = metadata.get('description', '').strip().replace('\n', ' ')
    exec_time = metadata['execution_time_formatted']
    row_count = metadata['row_count']
    columns = metadata['columns']
    
    # Handle queries with no results
    if row_count == 0:
        return f"""\\FloatBarrier
\\subsection{{Q{query_num}: {escape_latex(title)}}}

\\noindent {escape_latex(description)}.

\\noindent\\textbf{{Performance:}} {exec_time} | \\textbf{{Rows Returned:}} {row_count}

\\noindent\\textit{{This query returned no results (see Data-Specific Findings in Section 5.3).}}
"""
    
    # Determine column alignments
    alignments = []
    for col in columns:
        sample_vals = [row.get(col) for row in preview_data[:5]]
        alignments.append(get_column_alignment(col, sample_vals))
    
    alignment_string = ''.join(alignments)
    
    # Format column headers - use special escaping for column names
    col_headers = ' & '.join([escape_column_name(col) for col in columns])
    
    # Format data rows
    data_rows = []
    display_rows = min(10, len(preview_data))  # Show max 10 rows
    
    for row in preview_data[:display_rows]:
        row_values = []
        for col in columns:
            value = row.get(col)
            row_values.append(format_cell_value(value, col))
        data_rows.append(' & '.join(row_values) + ' \\\\')
    
    # Calculate omitted rows
    omitted_rows = row_count - display_rows
    
    # Build table
    table = f"""\\FloatBarrier
\\subsection{{Q{query_num}: {escape_latex(title)}}}

\\noindent {escape_latex(description)}.

\\noindent\\textbf{{Performance:}} {exec_time} | \\textbf{{Rows Returned:}} {format_number(row_count, is_integer=True)}


\\begin{{table}}[H]
\\centering
\\caption{{Results for Q{query_num}: {escape_latex(title)}}}
\\label{{tab:q{query_num}_results}}
\\resizebox{{\\textwidth}}{{!}}{{%
\\begin{{tabular}}{{{alignment_string}}}
\\toprule
{col_headers} \\\\
\\midrule
{chr(10).join(data_rows)}"""

    # Add omitted rows note if applicable
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
    """Main function to regenerate the LaTeX section."""
    
    base_dir = Path(__file__).parent
    query_dir = base_dir / "query_results"
    output_file = base_dir / "olap_queries_section.tex"
    
    # LaTeX header
    latex_content = """% =====================================================================
% OLAP QUERIES AND RESULTS
% Auto-generated from query execution results
% =====================================================================

\\section{OLAP Queries and Results}

This section presents all 20 analytical queries developed for the Walmart data warehouse, along with their results and performance metrics. The queries demonstrate various OLAP operations including slicing, dicing, drill-down, roll-up, pivoting, and window functions.

\\subsection{Query Categories}

The 20 queries are organized into the following categories:

\\begin{enumerate}[leftmargin=*]
    \\item \\textbf{Q1-Q4:} Basic aggregations and temporal analysis
    \\item \\textbf{Q5-Q8:} Cross-dimensional analysis and ranking
    \\item \\textbf{Q9-Q11:} Time-series analysis with window functions
    \\item \\textbf{Q12-Q15:} Advanced analytics (growth rates, volatility)
    \\item \\textbf{Q16-Q18:} Multi-dimensional analysis and ROLLUP
    \\item \\textbf{Q19-Q20:} Outlier detection and views
\\end{enumerate}

"""
    
    # Generate tables for all 20 queries
    for query_num in range(1, 21):
        print(f"Processing Q{query_num:02d}...")
        
        # Load metadata
        metadata_file = query_dir / f"Q{query_num:02d}_metadata.json"
        preview_file = query_dir / f"Q{query_num:02d}_preview.json"
        
        if not metadata_file.exists() or not preview_file.exists():
            print(f"  Warning: Missing files for Q{query_num:02d}")
            continue
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        with open(preview_file, 'r', encoding='utf-8') as f:
            preview_data = json.load(f)
        
        # Generate table
        table_latex = generate_table(query_num, metadata, preview_data)
        latex_content += table_latex
    
    # Add performance summary
    latex_content += """\\FloatBarrier
\\subsection{Performance Summary}

The results demonstrate that the star schema and indexing strategy successfully support near-real-time analytics, with 95\\% of queries executing in under 3 seconds.

\\textbf{Key Performance Insights:}
\\begin{itemize}[leftmargin=*]
    \\item \\textbf{Simple aggregations (Q1-Q4):} Average 0.5s execution time
    \\item \\textbf{Complex window functions (Q9, Q12, Q15):} 0.2-1.7s execution time
    \\item \\textbf{Large result sets (Q13-Q14):} 1.1-2.4s despite returning 3,600+ rows
    \\item \\textbf{Outlier detection (Q19):} 9.0s due to per-product statistical calculations
    \\item \\textbf{View creation (Q20):} 2.5s for aggregating quarterly data across stores
\\end{itemize}

The query performance confirms that the data warehouse successfully supports interactive business intelligence dashboards and ad-hoc analysis.
"""
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    print(f"\n✓ Successfully regenerated {output_file}")
    print(f"  Total sections: {latex_content.count('\\subsection')}")

if __name__ == '__main__':
    main()
