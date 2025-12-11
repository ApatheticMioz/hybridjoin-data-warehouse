"""Show how the chained HYBRIDJOIN ETL feeds the MySQL warehouse."""

from graphviz import Digraph


def generate_etl_pipeline():
    dot = Digraph(comment='Chained HYBRIDJOIN Pipeline', format='png')
    dot.attr(rankdir='TB', bgcolor='white', dpi='300', size='8,11!', ratio='fill', pad='0.2')
    dot.attr('node', fontname='Helvetica-Bold', fontsize='14', shape='box',
             style='filled,rounded', margin='0.45,0.25')
    dot.attr('edge', fontname='Helvetica-Bold', fontsize='13', penwidth='3')

    # Pipeline stages (top-to-bottom flow)
    dot.node('CSV', 'CSV SOURCE\n\ntransactional_data.csv\n550,068 lines\n24-hour ingest window',
             fillcolor='#FFCDD2', color='#C62828', penwidth='3', fontsize='15')

    dot.node('Producer', 'PRODUCER THREAD\n\nStreams CSV tuples\nBatch=5,000 | STREAM buffer 512MB\nPublishes metrics + lag alerts',
             fillcolor='#FFF59D', color='#F57F17', penwidth='3', fontsize='15')

    dot.node('StreamBuf', 'stream_buffer\n(Queue)\n5,000 slot ring buffer\nBack-pressure aware',
             fillcolor='#E1BEE7', color='#6A1B9A', penwidth='3', shape='cylinder', fontsize='14')

    dot.node('Consumer1', 'CONSUMER 1\nHYBRIDJOIN\n\nCustomer enrichment\nStore lookup + validation\n\nhS=10K | FIFO | vP=500',
             fillcolor='#B3E5FC', color='#0277BD', penwidth='3.5', fontsize='15')

    dot.node('IntQueue', 'intermediate_queue\n(Customer+Store enriched)\n5,000 slot ring buffer',
             fillcolor='#E1BEE7', color='#6A1B9A', penwidth='3', shape='cylinder', fontsize='14')

    dot.node('Consumer2', 'CONSUMER 2\nHYBRIDJOIN\n\nProduct enrichment\nSupplier lookups + net sales\n\nhS=10K | FIFO | vP=500',
             fillcolor='#FFCC80', color='#EF6C00', penwidth='3.5', fontsize='15')

    # Warehouse destination cluster
    with dot.subgraph(name='cluster_dw') as dw:
        dw.attr(label='MySQL Warehouse (schema: DWH_Proj)', fontsize='15', fontname='Helvetica-Bold',
                color='#283593', style='rounded', penwidth='2', bgcolor='#EEF0FF')
        dw.node_attr.update(style='filled,rounded', fontname='Helvetica-Bold')
        dw.node('FactTable', 'Fact_Sales\nDWH_Proj.fact_sales (InnoDB)\nPK: Sales_Key | FKs: Date/Product/Customer/Store/Supplier\nNet/Gross/Discount metrics\n550,068 rows from Iteration 6 load',
                shape='cylinder', fillcolor='#C5CAE9', color='#283593', fontsize='14')
        dw.node('DimTables', 'Dimension Tables\nPre-populated in PHASE 1 (batch loading)\nDim_Customer | Dim_Product | Dim_Store\nDim_Supplier | Dim_Date\n5,891 + 3,631 + 8 + 7 + 2,192 rows',
                shape='box', fillcolor='#E8EAF6', color='#283593', fontsize='12')
        dw.edge('FactTable', 'DimTables', style='dashed', color='#3949AB', penwidth='2',
                label='FK refs (lookups)',
                dir='both')

    # External lookup stores (side nodes) - DISK RELATIONS for HYBRIDJOIN
    dot.node('MasterCust', 'Master_Customer\nDisk Relation R1 (sorted by Customer_ID)\nPartitioned reads (vP=500)\n5,891 rows',
             fillcolor='#C8E6C9', color='#2E7D32', penwidth='3', shape='cylinder', fontsize='13')
    dot.node('MasterProd', 'Master_Product\nDisk Relation R2 (sorted by Product_ID)\nPartitioned reads (vP=500)\nStoreID + SupplierID fields\n3,631 rows',
             fillcolor='#C8E6C9', color='#2E7D32', penwidth='3', shape='cylinder', fontsize='13')

    # Main pipeline flow (blue arrows)
    dot.edge('CSV', 'Producer', label='  read  ', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')
    dot.edge('Producer', 'StreamBuf', label='  write  ', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')
    dot.edge('StreamBuf', 'Consumer1', label='  consume  ', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')
    dot.edge('Consumer1', 'IntQueue', label='  emit  ', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')
    dot.edge('IntQueue', 'Consumer2', label='  consume  ', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')
    dot.edge('Consumer2', 'FactTable', label='  INSERT batches\n(1000 rows)\n+ surrogate key lookups', color='#0D47A1', fontcolor='#0D47A1', penwidth='4')

    # Lookup edges (green dashed) - HYBRIDJOIN disk partition reads
    dot.edge('MasterCust', 'Consumer1', label='partition reads\n(vP=500)', color='#1B5E20', fontcolor='#1B5E20',
             style='dashed', penwidth='3', constraint='false')
    dot.edge('MasterProd', 'Consumer2', label='partition reads\n(vP=500)\nenriches with Store+Supplier', color='#1B5E20', fontcolor='#1B5E20',
             style='dashed', penwidth='3', constraint='false')

    # Title
    dot.attr(label=r'\n\nChained HYBRIDJOIN ETL Pipeline (Iteration 6)\nStream enrichment → Wide table → Surrogate key lookup → Fact load',
             fontsize='18', fontname='Helvetica-Bold', labelloc='t')

    # Render
    import os
    output_path = os.path.join(os.path.dirname(__file__), '..', 'figures', 'graph2_etl_pipeline')
    dot.render(output_path, cleanup=True, format='png', engine='dot')
    print("[OK] Graph 2 generated: figures/graph2_etl_pipeline.png")


if __name__ == '__main__':
    generate_etl_pipeline()
