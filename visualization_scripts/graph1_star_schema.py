import os
import sys
from pathlib import Path
from graphviz import Digraph

def _ensure_graphviz_path() -> None:
    if sys.platform != 'win32':
        return
    candidates = [
        Path(r"C:\\Program Files\\Graphviz\\bin"),
        Path(r"C:\\Program Files (x86)\\Graphviz\\bin"),
    ]
    for candidate in candidates:
        if candidate.exists() and str(candidate) not in os.environ['PATH']:
            os.environ['PATH'] = os.environ['PATH'] + os.pathsep + str(candidate)
            break

def generate_star_schema():
    _ensure_graphviz_path()

    # Use 'dot' for the hierarchical layout that worked
    dot = Digraph(comment='Star Schema Design', format='png', engine='dot')
    
    dot.graph_attr.update({
        'bgcolor': 'white',
        'dpi': '300',
        'splines': 'polyline', # sharp, clean lines
        'nodesep': '0.6',
        'ranksep': '0.8',
        'rankdir': 'TB',       # Top-to-Bottom
        'pad': '0.5'
    })
    
    dot.attr('node', shape='plain', fontname='Helvetica')
    dot.attr('edge', fontname='Helvetica', fontsize='11', penwidth='2.0')

    # --- NODES ---
    # FIX: Added ALIGN="LEFT" and BALIGN="LEFT" to all attribute cells.
    # ALIGN="LEFT": Puts the text block on the left side of the cell.
    # BALIGN="LEFT": Aligns the individual lines of text within that block to the left.

    dot.node('Fact', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#FFD700" ALIGN="CENTER"><B><FONT POINT-SIZE="16">Fact_Sales</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#FFF8E1" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="14">
                <B>Sales_Key (PK)</B><BR/>
                Customer_Key (FK)<BR/>
                Product_Key (FK)<BR/>
                Date_Key (FK)<BR/>
                Store_Key (FK)<BR/>
                Supplier_Key (FK)
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="14">
                Order_ID<BR/>
                Order_Line_Number<BR/>
                Quantity<BR/>
                Unit_Price<BR/>
                Total_Purchase_Amount<BR/>
                Discount_Amount<BR/>
                Net_Sales_Amount<BR/>
                Weekend_Flag<BR/>
                Order_Channel
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>550,068 rows</I></TD></TR>
        </TABLE>>''')

    dot.node('DimProduct', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#C8E6C9" ALIGN="CENTER"><B><FONT POINT-SIZE="14">Dim_Product</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#E8F5E9" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                <B>Product_Key (PK)</B><BR/>
                Product_ID (NK)
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                Product_Category<BR/>
                Unit_Price<BR/>
                Price_Band<BR/>
                Is_Premium
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>3,631 rows</I></TD></TR>
        </TABLE>>''')

    dot.node('DimCustomer', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#BBDEFB" ALIGN="CENTER"><B><FONT POINT-SIZE="14">Dim_Customer</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#E3F2FD" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                <B>Customer_Key (PK)</B><BR/>
                Customer_ID (NK)
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                Gender / Age<BR/>
                Occupation_Bucket<BR/>
                City_Tier<BR/>
                Stay_Bucket<BR/>
                Loyalty_Segment<BR/>
                Marital_Status
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>5,891 rows</I></TD></TR>
        </TABLE>>''')

    dot.node('DimDate', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#F8BBD0" ALIGN="CENTER"><B><FONT POINT-SIZE="14">Dim_Date</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#FCE4EC" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                <B>Date_Key (PK)</B><BR/>
                Full_Date
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                Day_Name<BR/>
                Is_Weekend<BR/>
                Month / Quarter<BR/>
                Fiscal_Year<BR/>
                Season
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>2,192 rows</I></TD></TR>
        </TABLE>>''')

    dot.node('DimStore', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#E1BEE7" ALIGN="CENTER"><B><FONT POINT-SIZE="14">Dim_Store</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#F3E5F5" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                <B>Store_Key (PK)</B><BR/>
                Store_ID (NK)
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                Store_Name<BR/>
                Store_Channel<BR/>
                Store_Tier<BR/>
                SKU_Count<BR/>
                Is_Flagship
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>8 rows</I></TD></TR>
        </TABLE>>''')

    dot.node('DimSupplier', '''<
        <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
            <TR><TD BGCOLOR="#FFE0B2" ALIGN="CENTER"><B><FONT POINT-SIZE="14">Dim_Supplier</FONT></B></TD></TR>
            <TR><TD BGCOLOR="#FFF3E0" ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                <B>Supplier_Key (PK)</B><BR/>
                Supplier_ID (NK)
            </FONT></TD></TR>
            <TR><TD ALIGN="LEFT" BALIGN="LEFT"><FONT POINT-SIZE="12">
                Supplier_Name<BR/>
                Supplier_Tier<BR/>
                Primary_Category<BR/>
                SKU_Count<BR/>
                Reliability_Score
            </FONT></TD></TR>
            <TR><TD ALIGN="RIGHT"><I>7 rows</I></TD></TR>
        </TABLE>>''')
    
    # --- LAYOUT ---
    # Grouping nodes to force the Top -> Bottom structure
    
    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('DimProduct')
        s.node('DimDate')

    with dot.subgraph() as s:
        s.attr(rank='same')
        s.node('DimCustomer')
        s.node('DimStore')
        s.node('DimSupplier')

    # --- EDGES ---
    # 1. Top Dimensions -> Fact (Arrow points to Fact)
    top_edge_defaults = {'dir': 'forward', 'arrowhead': 'crow'}
    dot.edge('DimProduct', 'Fact', label='Product_Key', color='#2E7D32', fontcolor='#2E7D32', **top_edge_defaults)
    dot.edge('DimDate', 'Fact', label='Date_Key', color='#AD1457', fontcolor='#AD1457', **top_edge_defaults)
    
    # 2. Fact -> Bottom Dimensions
    # Using dir='back' to point arrow at Fact, but keep Fact visually "above" the dimensions
    bottom_edge_defaults = {'dir': 'back', 'arrowtail': 'crow', 'arrowhead': 'none'}
    dot.edge('Fact', 'DimCustomer', label='Customer_Key', color='#1565C0', fontcolor='#1565C0', **bottom_edge_defaults)
    dot.edge('Fact', 'DimStore', label='Store_Key', color='#6A1B9A', fontcolor='#6A1B9A', **bottom_edge_defaults)
    dot.edge('Fact', 'DimSupplier', label='Supplier_Key', color='#BF360C', fontcolor='#BF360C', **bottom_edge_defaults)

    # Title
    dot.attr(label=r'\n\nWalmart Data Warehouse: Star Schema (Iteration 6)', 
             fontsize='20', fontname='Helvetica-Bold', labelloc='t')
    
    output_path = os.path.join(os.path.dirname(__file__), '..', 'figures', 'graph1_star_schema')
    dot.render(output_path, cleanup=True, format='png')
    print(f"[OK] Graph generated: {output_path}.png")

if __name__ == '__main__':
    generate_star_schema()