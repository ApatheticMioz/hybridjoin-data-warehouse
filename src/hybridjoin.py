# HYBRIDJOIN Implementation for Near-Real-Time Data Warehouse
# 3-thread pipeline: Producer -> Consumer1(Customer) -> Consumer2(Product) -> DB

import threading
import queue
import time
import pandas as pd
import os
import getpass
import math
from collections import defaultdict
from datetime import datetime
from doubly_linked_list import DoublyLinkedList
from db_utils import (
    get_database_connection,
    insert_fact_sales_batch,
    get_customer_partition,
    get_product_partition,
    load_master_data_tables,
    populate_all_dimensions,
    load_dimension_lookups
)

HS = 10000
VP = 500
STREAM_BUFFER_SIZE = 5000
INTERMEDIATE_QUEUE_SIZE = 5000
BATCH_INSERT_SIZE = 1000
TOTAL_MEMORY_TUPLES = (2 * HS) + (2 * VP) + STREAM_BUFFER_SIZE + INTERMEDIATE_QUEUE_SIZE + BATCH_INSERT_SIZE

stream_buffer = queue.Queue(maxsize=STREAM_BUFFER_SIZE)
intermediate_queue = queue.Queue(maxsize=INTERMEDIATE_QUEUE_SIZE)
fact_batch = []
fact_batch_lock = threading.Lock()

producer_finished = threading.Event()
consumer1_finished = threading.Event()

class StreamMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.total_streamed = 0
        self.stream_start_time = None
        
metrics = StreamMetrics()

class StreamProducer(threading.Thread):
    
    def __init__(self, csv_file_path, delay=0.0001):
        super().__init__(name="StreamProducer")
        self.csv_file_path = csv_file_path
        self.delay = delay
        self.daemon = False
        
    def run(self):
        print("\n[PRODUCER] Starting stream producer thread...")
        
        try:
            print(f"[PRODUCER] Reading from {self.csv_file_path}...")
            df = pd.read_csv(self.csv_file_path)
            
            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])
            
            df['date'] = pd.to_datetime(df['date'])
            
            total_records = len(df)
            print(f"[PRODUCER] Loaded {total_records:,} transactions")
            print("[PRODUCER] Streaming records into buffer...")
            print(f"[PRODUCER] Buffer capacity: {STREAM_BUFFER_SIZE:,} records\n")
            
            with metrics.lock:
                metrics.stream_start_time = time.time()
            
            last_report_time = time.time()
            report_interval = 5.0
            
            for idx, row in enumerate(df.itertuples(index=False, name=None)):
                stream_tuple = {
                    'Order_ID': int(row[0]),
                    'Customer_ID': int(row[1]),
                    'Product_ID': str(row[2]),
                    'quantity': int(row[3]),
                    'date': row[4]
                }
                
                stream_buffer.put(stream_tuple)
                
                with metrics.lock:
                    metrics.total_streamed = idx + 1
                
                current_time = time.time()
                if current_time - last_report_time >= report_interval:
                    elapsed = current_time - metrics.stream_start_time
                    rate = metrics.total_streamed / elapsed if elapsed > 0 else 0
                    buffer_size = stream_buffer.qsize()
                    buffer_pct = (buffer_size / STREAM_BUFFER_SIZE) * 100
                    
                    print(f"[STREAM] {metrics.total_streamed:>8,}/{total_records:<8,} records | "
                          f"Rate: {rate:>7,.0f} rec/s | "
                          f"Buffer: {buffer_size:>5,}/{STREAM_BUFFER_SIZE:,} ({buffer_pct:>5.1f}%)")
                    last_report_time = current_time
                
                if self.delay > 0:
                    time.sleep(self.delay)
            
            print(f"\n[PRODUCER] Finished streaming {total_records:,} records")
            
        except Exception as e:
            print(f"[PRODUCER] Error: {e}")
            raise
        finally:
            producer_finished.set()
            print("[PRODUCER] Producer thread terminated\n")

class HybridJoinConsumer(threading.Thread):
    
    def __init__(self, name, input_queue, output_queue, join_key_field, 
                 partition_loader_func, upstream_finished_event, 
                 connection=None,
                 dim_lookups=None,
                 final_load_batch=None, final_load_batch_lock=None,
                 is_final_stage=False):
        super().__init__(name=name)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.join_key_field = join_key_field
        self.partition_loader_func = partition_loader_func
        self.upstream_finished_event = upstream_finished_event
        self.connection = connection
        self.dim_lookups = dim_lookups
        self.final_load_batch = final_load_batch
        self.final_load_batch_lock = final_load_batch_lock
        self.is_final_stage = is_final_stage
        self.daemon = False
        
        self.hash_table = defaultdict(list)
        self.queue = DoublyLinkedList()
        self.disk_buffer = []
        self.w = HS
        
        self.total_processed = 0
        self.total_joined = 0
        self.total_output = 0
        self.total_loaded = 0
        self.dropped_records = 0
        self._drop_log_samples = 0
        self.start_time = None
        self.last_report_time = None
        self.last_report_output = 0
        self.disk_io_count = 0
        
        print(f"\n[{self.name}] Initialized - Join on {self.join_key_field}")
    
    def hash_function(self, key):
        return hash(key) % HS
    
    def run(self):
        print(f"[{self.name}] Starting join processing...")
        
        iteration = 0
        self.start_time = time.time()
        self.last_report_time = self.start_time
        report_interval = 5.0
        
        try:
            while True:
                iteration += 1
                
                tuples_loaded_this_iteration = 0
                
                if self.w > 0:
                    while tuples_loaded_this_iteration < self.w and not self.input_queue.empty():
                        try:
                            stream_tuple = self.input_queue.get(timeout=0.001)
                            join_key = stream_tuple[self.join_key_field]
                            queue_node = self.queue.append(join_key)
                            self.hash_table[join_key].append((stream_tuple, queue_node))
                            tuples_loaded_this_iteration += 1
                            self.total_processed += 1
                        except queue.Empty:
                            break
                    
                    if tuples_loaded_this_iteration > 0:
                        self.w = 0
                
                if self.queue.is_empty():
                    if self.upstream_finished_event.is_set() and self.input_queue.empty():
                        print(f"\n[{self.name}] Finished processing")
                        break
                    else:
                        time.sleep(0.01)
                        continue
                
                oldest_key = self.queue.peek_front()
                
                if oldest_key is None:
                    self.w = HS - len(self.queue)
                    continue
                
                try:
                    self.disk_buffer = self.partition_loader_func(
                        self.connection,
                        oldest_key,
                        VP
                    )
                    self.disk_io_count += 1
                    
                    if not self.disk_buffer:
                        if oldest_key in self.hash_table:
                            matching_tuples = self.hash_table[oldest_key][:]
                            for stream_tuple, queue_node in matching_tuples:
                                self.hash_table[oldest_key].remove((stream_tuple, queue_node))
                                self.queue.remove(queue_node)
                                self.w += 1
                        continue
                        
                except Exception as e:
                    if oldest_key in self.hash_table:
                        matching_tuples = self.hash_table[oldest_key][:]
                        for stream_tuple, queue_node in matching_tuples:
                            self.hash_table[oldest_key].remove((stream_tuple, queue_node))
                            self.queue.remove(queue_node)
                            self.w += 1
                    continue
                
                for disk_record in self.disk_buffer:
                    join_key_value = disk_record[self.join_key_field]
                    
                    if join_key_value in self.hash_table:
                        matching_tuples = self.hash_table[join_key_value][:]
                        
                        for stream_tuple, queue_node in matching_tuples:
                            self.hash_table[join_key_value].remove((stream_tuple, queue_node))
                            self.queue.remove(queue_node)
                            self.w += 1
                            
                            enriched_tuple = stream_tuple.copy()
                            enriched_tuple.update(disk_record)
                            
                            self.total_joined += 1
                            
                            if self.is_final_stage:
                                self._write_fact_to_batch(enriched_tuple)
                            else:
                                self.output_queue.put(enriched_tuple)
                                self.total_output += 1
                        
                        if not self.hash_table[join_key_value]:
                            del self.hash_table[join_key_value]
                
                current_time = time.time()
                if current_time - self.last_report_time >= report_interval:
                    elapsed = current_time - self.start_time
                    elapsed_since_report = current_time - self.last_report_time
                    
                    output_metric = self.total_loaded if self.is_final_stage else self.total_output
                    recent_rate = (output_metric - self.last_report_output) / elapsed_since_report if elapsed_since_report > 0 else 0
                    
                    input_size = self.input_queue.qsize()
                    hash_keys = len(self.hash_table)
                    elapsed_str = f"{int(elapsed//60):02d}:{int(elapsed%60):02d}"
                    
                    print(f"[{self.name}] {elapsed_str:<8} | {iteration:>6,} | {len(self.queue):>8,} | "
                          f"{hash_keys:>7,} | {self.w:>6,} | "
                          f"{input_size:>10,} | "
                          f"{self.disk_io_count:>8,} | {self.total_joined:>10,} | "
                          f"{output_metric:>10,} | {recent_rate:>8,.0f} r/s")
                    
                    self.last_report_time = current_time
                    self.last_report_output = output_metric
            
            if self.is_final_stage:
                with self.final_load_batch_lock:
                    if self.final_load_batch:
                        inserted = insert_fact_sales_batch(self.connection, self.final_load_batch)
                        self.total_loaded += inserted
                        self.final_load_batch.clear()
            
            elapsed = time.time() - self.start_time
            print(f"\n[{self.name}] Complete - Processed {self.total_processed:,} tuples, {self.total_joined:,} joined")
            print(f"Successful Joins:            {self.total_joined:,}")
            
            if self.is_final_stage:
                print(f"Records Loaded to DW:        {self.total_loaded:,}")
                print(f"Join Success Rate:           {(self.total_joined/self.total_processed*100):.2f}%")
                print(f"Average Throughput:          {self.total_loaded/elapsed:,.0f} records/sec")
            else:
                print(f"Records Output to Next Stage: {self.total_output:,}")
                print(f"Join Success Rate:           {(self.total_joined/self.total_processed*100):.2f}%")
                print(f"Average Throughput:          {self.total_output/elapsed:,.0f} records/sec")
            
            print(f"Avg Joins per Disk I/O:      {self.total_joined/self.disk_io_count:.1f}" if self.disk_io_count > 0 else "N/A")
            print(f"Memory Bounded:              ~{TOTAL_MEMORY_TUPLES:,} tuples (2*hS + 2*vP + buffers + batch)")
            print(f"{'='*85}\n")
            
        except Exception as e:
            print(f"\n[{self.name}] Error: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            print(f"[{self.name}] Consumer thread terminated\n")
    
    def _write_fact_to_batch(self, enriched_tuple):
        """Convert natural keys to surrogate keys using dimension lookups, then insert to fact"""
        try:
            # Lookup surrogate keys from dimensions
            customer_id = int(enriched_tuple['Customer_ID'])
            customer_key = self.dim_lookups['customer'].get(customer_id)
            
            product_id = str(enriched_tuple['Product_ID'])
            product_key = self.dim_lookups['product'].get(product_id)
            
            store_id = str(enriched_tuple.get('StoreID', 'UNKNOWN'))
            store_key = self.dim_lookups['store'].get(store_id)
            
            supplier_id = str(enriched_tuple.get('SupplierID', 'UNKNOWN'))
            supplier_key = self.dim_lookups['supplier'].get(supplier_id)
            
            date_obj = pd.to_datetime(enriched_tuple['date'])
            date_key = int(date_obj.strftime('%Y%m%d'))
            
            # Skip if any lookup fails
            if not all([customer_key, product_key, store_key, supplier_key, date_key]):
                self.dropped_records += 1
                return
            
            unit_price = float(enriched_tuple['Price'])
            total_purchase = enriched_tuple['quantity'] * unit_price
            discount_amount = 0
            weekend_flag = 1 if date_obj.weekday() >= 5 else 0
            
            fact_record = (
                enriched_tuple['Order_ID'],
                1,
                customer_key,
                product_key,
                store_key,
                supplier_key,
                date_key,
                enriched_tuple['quantity'],
                unit_price,
                total_purchase,
                discount_amount,
                weekend_flag,
                'In-Store'
            )

            with self.final_load_batch_lock:
                self.final_load_batch.append(fact_record)

                if len(self.final_load_batch) >= BATCH_INSERT_SIZE:
                    inserted = insert_fact_sales_batch(self.connection, self.final_load_batch)
                    self.total_loaded += inserted
                    self.final_load_batch.clear()
                    
        except Exception as e:
            self.dropped_records += 1
            if self._drop_log_samples < 3:
                print(f"[{self.name}] Error processing order {enriched_tuple.get('Order_ID')}: {e}")
                self._drop_log_samples += 1

def main():
    print("\n" + "="*70)
    print(" "*10 + "HYBRIDJOIN ETL Process for Walmart Data Warehouse")
    print("="*70)
    
    print("\n--- Database Configuration ---")
    host = input("Enter MySQL host [localhost]: ").strip() or "localhost"
    user = input("Enter MySQL username [root]: ").strip() or "root"
    password = getpass.getpass("Enter MySQL password: ")
    database = input("Enter database name [DWH_Proj]: ").strip() or "DWH_Proj"
    
    print(f"\nConnecting to: {user}@{host}/{database}")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    customer_master_path = os.path.join(project_root, "data", "customer_master_data.csv")
    product_master_path = os.path.join(project_root, "data", "product_master_data.csv")
    transactional_path = os.path.join(project_root, "data", "transactional_data.csv")
    
    connection = get_database_connection(host, user, password, database)
    
    try:
        # Step 1: Populate ALL dimension tables from master data (batch loading)
        populate_all_dimensions(connection, customer_master_path, product_master_path)
        
        # Step 2: Load dimension lookups into memory (for surrogate key conversion)
        print("\n" + "="*70)
        print("PHASE 2: Loading dimension lookups into memory")
        print("="*70 + "\n")
        dim_lookups = load_dimension_lookups(connection)
        
        # Step 3: Load master data tables (disk-based relations for HYBRIDJOIN)
        print("\n" + "="*70)
        print("PHASE 3: Loading Master Data Tables for HYBRIDJOIN")
        print("="*70 + "\n")
        load_master_data_tables(connection, customer_master_path, product_master_path)
        
        # Step 4: Run HYBRIDJOIN to populate fact table
        print("\n" + "="*70)
        print("PHASE 4: Running HYBRIDJOIN Pipeline for Fact Table")
        print("="*70)
        print("Starting 3-thread pipeline...\n")
        
        connection_consumer1 = get_database_connection(host, user, password, database)
        connection_consumer2 = get_database_connection(host, user, password, database)
        
        producer = StreamProducer(transactional_path, delay=0)
        
        consumer_customer = HybridJoinConsumer(
            name="Consumer-Customer",
            input_queue=stream_buffer,
            output_queue=intermediate_queue,
            join_key_field='Customer_ID',
            partition_loader_func=get_customer_partition,
            upstream_finished_event=producer_finished,
            connection=connection_consumer1,
            is_final_stage=False
        )
        
        consumer_product = HybridJoinConsumer(
            name="Consumer-Product",
            input_queue=intermediate_queue,
            output_queue=None,
            join_key_field='Product_ID',
            partition_loader_func=get_product_partition,
            upstream_finished_event=consumer1_finished,
            connection=connection_consumer2,
            dim_lookups=dim_lookups,
            final_load_batch=fact_batch,
            final_load_batch_lock=fact_batch_lock,
            is_final_stage=True
        )
        
        start_time = time.time()
        producer.start()
        consumer_customer.start()
        consumer_product.start()
        
        producer.join()
        consumer_customer.join()
        consumer1_finished.set()
        consumer_product.join()
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 85)
        print("CHAINED HYBRIDJOIN ETL PROCESS COMPLETED SUCCESSFULLY")
        print("=" * 85)
        print(f"Total Execution Time:        {int(elapsed_time//60):02d}:{int(elapsed_time%60):02d} ({elapsed_time:.2f} seconds)")
        print(f"\nPipeline Stage Performance:")
        print(f"  Producer:")
        print(f"    - Records Streamed:      {metrics.total_streamed:,}")
        print(f"  Consumer 1 (Customer Join):")
        print(f"    - Tuples Processed:      {consumer_customer.total_processed:,}")
        print(f"    - Successful Joins:      {consumer_customer.total_joined:,}")
        print(f"    - Join Rate:             {(consumer_customer.total_joined/consumer_customer.total_processed*100):.2f}%")
        print(f"    - Disk I/O Operations:   {consumer_customer.disk_io_count:,}")
        print(f"    - Output to Stage 2:     {consumer_customer.total_output:,}")
        print(f"  Consumer 2 (Product Join):")
        print(f"    - Tuples Processed:      {consumer_product.total_processed:,}")
        print(f"    - Successful Joins:      {consumer_product.total_joined:,}")
        print(f"    - Join Rate:             {(consumer_product.total_joined/consumer_product.total_processed*100):.2f}%")
        print(f"    - Disk I/O Operations:   {consumer_product.disk_io_count:,}")
        print(f"    - Records Loaded to DW:  {consumer_product.total_loaded:,}")
        if consumer_product.dropped_records:
            print(f"    - Dropped Records:       {consumer_product.dropped_records:,} (missing dimension keys)")
        print(f"\nOverall Metrics:")
        
        if consumer_product.total_loaded > 0:
            print(f"  - Final Records in DW:     {consumer_product.total_loaded:,}")
            print(f"  - End-to-End Throughput:   {consumer_product.total_loaded / elapsed_time:.0f} records/second")
            print(f"  - Pipeline Efficiency:     {(consumer_product.total_loaded / metrics.total_streamed * 100):.2f}% (output/input)")
            print(f"  - Total Disk I/O Ops:      {consumer_customer.disk_io_count + consumer_product.disk_io_count:,}")
            print(f"  - Memory Footprint:        ~{TOTAL_MEMORY_TUPLES:,} tuples (queues + batches) - BOUNDED")
            num_batches = math.ceil(consumer_product.total_loaded / BATCH_INSERT_SIZE)
            if num_batches > 0:
                print(f"  - Database Batch Inserts:  {num_batches:,}")
        else:
            print(f"  - Throughput:              N/A (no records loaded)")
        
        print("=" * 85)
        
        # Verify final count
        cursor = connection_consumer2.cursor()
        cursor.execute("SELECT COUNT(*) FROM Fact_Sales")
        fact_count = cursor.fetchone()[0]
        cursor.close()
        print(f"\nFact_Sales: {fact_count:,} records")
        print("=" * 85 + "\n")
        
    except Exception as e:
        print(f"\nETL failed: {e}")
        raise
    finally:
        if connection.is_connected():
            connection.close()
        if 'connection_consumer1' in locals() and connection_consumer1.is_connected():
            connection_consumer1.close()
        if 'connection_consumer2' in locals() and connection_consumer2.is_connected():
            connection_consumer2.close()

if __name__ == "__main__":
    main()
