"""Compare ETL timings across the six implementation iterations."""

import matplotlib.pyplot as plt
import numpy as np

def generate_etl_evolution():
    # Set professional style
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['font.size'] = 11
    
    # Data
    iterations = ['Iteration 1\nNaive RAM', 'Iteration 2\nRow-by-Row I/O', 
                  'Iteration 3\nUnique Key I/O', 'Iteration 4\nBatch Lookups', 
                  'Iteration 5\nChained HJ', 'Iteration 6\nChained HJ (Final)']
    times = [25, 310, 120, 30, 31.14, 36.78]
    colors = ['#EF5350', '#EF5350', '#EF5350', '#4CAF50', '#2196F3', '#1E88E5']
    
    # Create figure with specific size for academic papers
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # Create bars with enhanced styling
    bars = ax.bar(iterations, times, color=colors, edgecolor='black', linewidth=1.8, 
                   alpha=0.85, width=0.6)
    
    # Add value labels on bars with better positioning
    for i, (bar, time) in enumerate(zip(bars, times)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 8,
                f'{time}s',
                ha='center', va='bottom', fontweight='bold', fontsize=12,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                         edgecolor='black', linewidth=1, alpha=0.8))
    
    # Add "Unbounded Memory" annotations with professional arrows
    ax.annotate('Unbounded\nMemory', xy=(0, 25), xytext=(0, 220),
                ha='center', fontsize=11, color='#C62828', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#FFEBEE', 
                         edgecolor='#C62828', linewidth=2),
                arrowprops=dict(arrowstyle='->', color='#C62828', lw=2.5,
                              connectionstyle='arc3,rad=0'))
    
    ax.annotate('Unbounded\nMemory', xy=(2, 120), xytext=(2, 220),
                ha='center', fontsize=11, color='#C62828', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#FFEBEE',
                         edgecolor='#C62828', linewidth=2),
                arrowprops=dict(arrowstyle='->', color='#C62828', lw=2.5,
                              connectionstyle='arc3,rad=0'))
    
    # Add "OPTIMAL" labels with professional styling
    ax.annotate('Practical\nOptimum', xy=(3, 30), xytext=(3, 95),
                ha='center', fontsize=10, color='#2E7D32', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='#E8F5E9',
                         edgecolor='#2E7D32', linewidth=2),
                arrowprops=dict(arrowstyle='->', color='#2E7D32', lw=2,
                              connectionstyle='arc3,rad=0'))
    
    ax.annotate('Scalable\nSolution', xy=(4, 32), xytext=(4, 95),
                ha='center', fontsize=10, color='#1565C0', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='#E3F2FD',
                         edgecolor='#1565C0', linewidth=2),
                arrowprops=dict(arrowstyle='->', color='#1565C0', lw=2,
                              connectionstyle='arc3,rad=0'))

    ax.annotate('Submission\nRun (Iteration 6)', xy=(5, 36.78), xytext=(5, 110),
                ha='center', fontsize=10, color='#0D47A1', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.4', facecolor='#E3F2FD',
                         edgecolor='#0D47A1', linewidth=2),
                arrowprops=dict(arrowstyle='->', color='#0D47A1', lw=2,
                              connectionstyle='arc3,rad=0'))
    
    # Labels and title with professional formatting
    ax.set_xlabel('Implementation Iteration', fontsize=14, fontweight='bold', labelpad=10)
    ax.set_ylabel('Total ETL Time (seconds)', fontsize=14, fontweight='bold', labelpad=10)
    ax.set_title('ETL Performance Evolution by Implementation\nProcessing 550,068 Transactions (Customer+Product+Supplier enrichment)', 
                 fontsize=15, fontweight='bold', pad=20)
    
    # Enhanced grid
    ax.yaxis.grid(True, linestyle='--', alpha=0.5, linewidth=1)
    ax.set_axisbelow(True)
    
    # Set y-axis limit with padding
    ax.set_ylim(0, 340)
    
    # Add professional legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#EF5350', edgecolor='black', label='Disqualified (Unbounded Memory)', linewidth=1.5),
        Patch(facecolor='#4CAF50', edgecolor='black', label='Iteration 4: Production-Ready', linewidth=1.5),
        Patch(facecolor='#2196F3', edgecolor='black', label='Iteration 5: Academically Scalable', linewidth=1.5),
        Patch(facecolor='#1E88E5', edgecolor='black', label='Iteration 6: Submission Run (Final QA)', linewidth=1.5)
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=11, framealpha=0.95,
             edgecolor='black', fancybox=True, shadow=True)
    
    # Enhance tick labels
    ax.tick_params(axis='both', which='major', labelsize=11)
    
    # Add border
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
    
    # Tight layout
    plt.tight_layout()
    
    # Save with high quality
    import os
    output_path = os.path.join(os.path.dirname(__file__), '..', 'figures', 'graph3_etl_evolution.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print("[OK] Graph 3 generated: figures/graph3_etl_evolution.png")
    plt.close()

if __name__ == '__main__':
    generate_etl_evolution()
