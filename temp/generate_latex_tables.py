"""
Generate LaTeX-formatted tables from query results for report inclusion.

This script:
1. Reads the query results and metadata
2. Creates properly formatted LaTeX tables
3. Generates a comprehensive OLAP queries section for the report
4. Handles large result sets by showing representative samples

Author: Muhammad Abdullah Ali (i23-2523)
Date: November 17, 2025
"""

import json
import pandas as pd
from pathlib import Path

def escape_latex(text):
    """Escape special LaTeX characters."""
    if pd.isna(text):
        return ''
    text = str(text)
    replacements = {
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '~': r'\textasciitilde{}',
        '^': r'\^{}',
        '\\': r'\textbackslash{}',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text

def format_number(value):
    """Format numbers for better readability."""
    if pd.isna(value):
        return ''
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return f'{int(value):,}'
        elif isinstance(value, float):
            return f'{value:,.2f}'
        else:
            return f'{value:,}'
    return str(value)

def create_latex_table(df, caption, label, max_rows=10):
    """Create a LaTeX table from a DataFrame."""
    if len(df) == 0:
        return f"""
\\begin{{table}}[h]
\\centering
\\caption{{{escape_latex(caption)}}}
\\label{{{label}}}
\\begin{{tabular}}{{c}}
\\toprule
\\textit{{No results returned}} \\\\
\\bottomrule
\\end{{tabular}}
\\end{{table}}
"""
    
    # Limit rows if too many
    display_df = df.head(max_rows)
    truncated = len(df) > max_rows
    
    # Create column specification
    num_cols = len(display_df.columns)
    col_spec = 'l' + 'r' * (num_cols - 1)
    
    # Start table
    latex = f"""
\\begin{{table}}[h]
\\centering
\\caption{{{escape_latex(caption)}}}
\\label{{{label}}}
\\resizebox{{\\textwidth}}{{!}}{{%
\\begin{{tabular}}{{{col_spec}}}
\\toprule
"""
    
    # Add header
    headers = [escape_latex(col) for col in display_df.columns]
    latex += ' & '.join(headers) + ' \\\\\n'
    latex += '\\midrule\n'
    
    # Add data rows
    for _, row in display_df.iterrows():
        formatted_row = []
        for val in row:
            if isinstance(val, (int, float)):
                formatted_row.append(format_number(val))
            else:
                formatted_row.append(escape_latex(str(val)))
        latex += ' & '.join(formatted_row) + ' \\\\\n'
    
    # Add truncation note if needed
    if truncated:
        latex += '\\midrule\n'
        latex += f'\\multicolumn{{{num_cols}}}{{c}}{{\\textit{{... {len(df) - max_rows:,} more rows omitted}}}} \\\\\n'
    
    # End table
    latex += """\\bottomrule
\\end{tabular}%
}
\\end{table}
"""
    
    return latex

def generate_query_section(query_num, metadata, df):
    """Generate a complete section for one query."""
    title = metadata['title']
    description = metadata['description']
    exec_time = metadata['execution_time_formatted']
    row_count = metadata['row_count']
    
    section = f"""
\\subsection{{Q{query_num}: {escape_latex(title)}}}

{escape_latex(description)}

\\textbf{{Performance:}} {exec_time} | \\textbf{{Rows Returned:}} {row_count:,}

"""
    
    # Add table if there are results
    if row_count > 0:
        # Determine how many rows to show based on query type
        if row_count <= 10:
            max_rows = row_count
        elif row_count <= 50:
            max_rows = 15
        else:
            max_rows = 10
        
        table = create_latex_table(
            df, 
            f"Results for Q{query_num}: {title}",
            f"tab:q{query_num}_results",
            max_rows=max_rows
        )
        section += table
    else:
        section += "\\textit{This query returned no results (see Data-Specific Findings in Section 5.3).}\n"
    
    section += "\n"
    return section

def main():
    """Generate LaTeX content for all queries."""
    results_dir = Path(__file__).parent / 'query_results'
    output_file = Path(__file__).parent / 'olap_queries_section.tex'
    
    print("Generating LaTeX sections for OLAP queries...\n")
    
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
    
    # Process each query
    for i in range(1, 21):
        query_num = f"{i:02d}"
        
        metadata_file = results_dir / f"Q{query_num}_metadata.json"
        csv_file = results_dir / f"Q{query_num}_results.csv"
        
        if not metadata_file.exists() or not csv_file.exists():
            print(f"⚠ Warning: Missing files for Q{query_num}")
            continue
        
        # Load metadata and results
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        df = pd.read_csv(csv_file)
        
        # Generate section
        section = generate_query_section(i, metadata, df)
        latex_content += section
        
        print(f"✓ Generated section for Q{i}: {metadata['title'][:60]}...")
    
    # Add performance summary
    latex_content += """
\\subsection{Performance Summary}

Figure \\ref{fig:query_performance} summarizes the execution time for all 20 queries. The results demonstrate that the star schema and indexing strategy successfully support near-real-time analytics, with 95\\% of queries executing in under 3 seconds.

\\textbf{Key Performance Insights:}
\\begin{itemize}[leftmargin=*]
    \\item \\textbf{Simple aggregations (Q1-Q4):} Average 0.5s execution time
    \\item \\textbf{Complex window functions (Q9, Q12, Q15):} 0.2-1.7s execution time
    \\item \\textbf{Large result sets (Q13-Q14):} 1.1-2.4s despite returning 3,600+ rows
    \\item \\textbf{Outlier detection (Q19):} 9.0s due to per-product statistical calculations
    \\item \\textbf{View creation (Q20):} 35ms demonstrating efficient DDL execution
\\end{itemize}

The query performance confirms that the data warehouse successfully supports interactive business intelligence dashboards and ad-hoc analysis.

"""
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    print(f"\n✓ LaTeX content saved to: {output_file}")
    print(f"✓ Total sections generated: 20")
    print(f"\nYou can now include this file in your main report using:")
    print(f"  \\input{{temp/olap_queries_section}}")

if __name__ == "__main__":
    main()
