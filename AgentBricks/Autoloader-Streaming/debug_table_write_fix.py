# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ” Auto Loader Table Write Fix - CRITICAL ISSUE
# MAGIC
# MAGIC **PROBLEM IDENTIFIED**: Auto Loader processes 15 rows but writes 0 to table
# MAGIC **ROOT CAUSE**: Table write operation failing despite successful stream processing
# MAGIC **SOLUTION**: Direct table write verification and fix

# COMMAND ----------

print("ðŸ” AUTO LOADER TABLE WRITE DIAGNOSTIC")
print("=" * 60)
print("ðŸŽ¯ ISSUE: Stream processes 15 rows but 0 records in table")
print("ðŸŽ¯ FOCUS: Fix the table write disconnect")

import time
from datetime import datetime
from pyspark.sql.functions import *

# Same configuration as failing script
LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming/"
CHECKPOINT_PATH = "/FileStore/checkpoints/table_write_fix/"

# Adaptive table configuration
def get_table_config():
    try:
        UC_CATALOG = "network_fault_detection"
        UC_SCHEMA = "processed_data"
        spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}")
        
        return {
            'mode': 'unity_catalog',
            'raw_table': f"{UC_CATALOG}.{UC_SCHEMA}.raw_network_logs_streaming",
            'severity_table': f"{UC_CATALOG}.{UC_SCHEMA}.severity_classifications_streaming"
        }
    except:
        return {
            'mode': 'default_database',
            'raw_table': 'default.raw_network_logs_streaming',
            'severity_table': 'default.severity_classifications_streaming'
        }

config = get_table_config()
TABLE_RAW_LOGS = config['raw_table']
TABLE_SEVERITY = config['severity_table']

print(f"ðŸ—ƒï¸ Configuration: {config['mode']}")
print(f"ðŸ“Š Raw table: {TABLE_RAW_LOGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 1: Reproduce and Fix the Exact Issue

# COMMAND ----------

def create_test_log_files():
    """Create the exact same log files"""
    
    print("ðŸ“ CREATING TEST LOG FILES")
    print("-" * 40)
    
    try:
        # Clean and recreate
        try:
            dbutils.fs.rm(LOG_SOURCE_PATH, recurse=True)
        except:
            pass
        
        dbutils.fs.mkdirs(LOG_SOURCE_PATH)
        
        # Same 3 files with 5 lines each = 15 total lines
        sample_logs = [
            {
                'filename': 'critical_network_001.log',
                'content': """2025-09-12 10:15:23 CRITICAL [NetworkGateway] Primary gateway offline - all traffic down
2025-09-12 10:15:24 ERROR [System] Network outage detected on all interfaces  
2025-09-12 10:15:25 CRITICAL [Monitor] Complete network failure - escalating to P1
2025-09-12 10:15:26 ERROR [Router] Cannot reach upstream providers
2025-09-12 10:15:27 CRITICAL [Alert] Service unavailable - customer impact high"""
            },
            {
                'filename': 'high_severity_002.log',
                'content': """2025-09-12 10:20:15 ERROR [LoadBalancer] Backend servers not responding
2025-09-12 10:20:16 WARN [Monitor] Response time degraded to 5000ms
2025-09-12 10:20:17 ERROR [Database] Connection pool exhausted
2025-09-12 10:20:18 WARN [System] CPU usage at 95% on critical servers
2025-09-12 10:20:19 ERROR [Service] API endpoints returning 500 errors"""
            },
            {
                'filename': 'medium_severity_003.log',
                'content': """2025-09-12 10:25:10 WARN [System] Disk usage at 80% on log server
2025-09-12 10:25:11 INFO [Monitor] Backup process completed successfully
2025-09-12 10:25:12 WARN [Network] Intermittent packet loss detected
2025-09-12 10:25:13 INFO [Service] Scheduled maintenance window approaching
2025-09-12 10:25:14 WARN [Security] Unusual login attempts from foreign IP"""
            }
        ]
        
        total_lines = 0
        for log_file in sample_logs:
            file_path = f"{LOG_SOURCE_PATH}{log_file['filename']}"
            dbutils.fs.put(file_path, log_file['content'], overwrite=True)
            
            # Count lines
            content = dbutils.fs.head(file_path, max_bytes=1000)
            lines = len(content.strip().split('\n')) if content.strip() else 0
            total_lines += lines
            
            print(f"âœ… {log_file['filename']}: {lines} lines")
        
        print(f"ðŸ“Š Total lines available: {total_lines}")
        return total_lines == 15
        
    except Exception as e:
        print(f"âŒ Error creating files: {e}")
        return False

files_created = create_test_log_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 2: Create Tables with Write Verification

# COMMAND ----------

def create_tables_with_verification():
    """Create tables and verify write permissions"""
    
    print("ðŸ—ƒï¸ TABLE CREATION WITH WRITE VERIFICATION")
    print("-" * 50)
    
    try:
        # Clean existing tables
        print("ðŸ§¹ Dropping existing tables...")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_RAW_LOGS}")
        print(f"âœ… Dropped {TABLE_RAW_LOGS}")
        
        # Create simple raw logs table
        print("ðŸ“‹ Creating raw logs table...")
        spark.sql(f"""
        CREATE TABLE {TABLE_RAW_LOGS} (
            log_content STRING,
            file_path STRING, 
            ingestion_timestamp TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """)
        print(f"âœ… Created table: {TABLE_RAW_LOGS}")
        
        # CRITICAL: Test direct write to verify table permissions
        print("ðŸ§ª Testing direct write to table...")
        test_data = [
            ("Test log content 1", "/test/file1.log", datetime.now()),
            ("Test log content 2", "/test/file2.log", datetime.now()),
            ("Test log content 3", "/test/file3.log", datetime.now())
        ]
        
        test_df = spark.createDataFrame(test_data, ["log_content", "file_path", "ingestion_timestamp"])
        test_df.write.mode("append").saveAsTable(TABLE_RAW_LOGS)
        
        # Verify write
        test_count = spark.table(TABLE_RAW_LOGS).count()
        print(f"âœ… Direct write test: {test_count} records written successfully")
        
        if test_count > 0:
            print("âœ… TABLE WRITE PERMISSIONS: WORKING")
            
            # Clean test data
            spark.sql(f"DELETE FROM {TABLE_RAW_LOGS} WHERE file_path LIKE '/test/%'")
            final_count = spark.table(TABLE_RAW_LOGS).count()
            print(f"âœ… Test data cleaned, table ready: {final_count} records")
            
            return True
        else:
            print("âŒ TABLE WRITE PERMISSIONS: FAILED")
            return False
            
    except Exception as e:
        print(f"âŒ Table creation/verification failed: {e}")
        import traceback
        traceback.print_exc()
        return False

table_ready = create_tables_with_verification()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŒŠ Step 3: Fixed Auto Loader with Write Debugging

# COMMAND ----------

def run_autoloader_with_write_debugging():
    """Run Auto Loader with enhanced table write debugging"""
    
    print("\nðŸŒŠ AUTO LOADER WITH WRITE DEBUGGING")
    print("=" * 60)
    
    if not files_created or not table_ready:
        print("âŒ Prerequisites failed - cannot run test")
        return False
    
    try:
        # Force clean checkpoint
        print("ðŸ§¹ Cleaning checkpoint completely...")
        try:
            dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
            print("âœ… Checkpoint cleaned")
        except:
            print("ðŸ“ No checkpoint to clean")
        
        # Auto Loader configuration (same as working debug script)
        autoloader_options = {
            "cloudFiles.format": "text",
            "cloudFiles.schemaLocation": f"{CHECKPOINT_PATH}schema/",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.maxFilesPerTrigger": "5"
        }
        
        print("ðŸ”§ Auto Loader configuration:")
        for key, value in autoloader_options.items():
            print(f"   {key}: {value}")
        
        # Create streaming DataFrame
        print("\nðŸ“Š Creating Auto Loader stream...")
        stream_df = (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .load(LOG_SOURCE_PATH)
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .selectExpr("value as log_content", 
                               "_metadata.file_path as file_path",
                               "ingestion_timestamp"))
        
        print("âœ… Stream DataFrame created")
        print(f"ðŸ“‹ Schema: {[field.name for field in stream_df.schema.fields]}")
        
        # ENHANCED: Add write debugging with explicit error handling
        def debug_write_function(batch_df, batch_id):
            """Enhanced write function with comprehensive debugging"""
            
            try:
                print(f"\nðŸ“ WRITE DEBUG: Batch {batch_id}")
                print("-" * 40)
                
                # Check batch data
                batch_count = batch_df.count()
                print(f"ðŸ“Š Batch received: {batch_count} records")
                
                if batch_count == 0:
                    print("âš ï¸ Empty batch - nothing to write")
                    return
                
                # Show sample data
                print("ðŸ“‹ Sample batch data:")
                sample_data = batch_df.limit(3).collect()
                for i, row in enumerate(sample_data):
                    log_content = row['log_content'][:50] + "..." if len(row['log_content']) > 50 else row['log_content']
                    print(f"   Row {i}: {log_content}")
                
                # CRITICAL: Direct table write with error capture
                print("ðŸ’¾ Attempting table write...")
                
                write_start_time = time.time()
                batch_df.write.mode("append").saveAsTable(TABLE_RAW_LOGS)
                write_time = time.time() - write_start_time
                
                print(f"âœ… Write completed in {write_time:.2f} seconds")
                
                # Immediate verification
                print("ðŸ” Verifying write...")
                current_count = spark.table(TABLE_RAW_LOGS).count()
                print(f"âœ… Table now contains: {current_count} total records")
                
                # Show latest records
                if current_count > 0:
                    print("ðŸ“‹ Latest records in table:")
                    latest = spark.table(TABLE_RAW_LOGS).orderBy(desc("ingestion_timestamp")).limit(2)
                    for row in latest.collect():
                        content_preview = row['log_content'][:40] + "..."
                        print(f"   {content_preview}")
                
                print(f"âœ… Batch {batch_id}: Successfully processed {batch_count} records")
                
            except Exception as write_error:
                print(f"âŒ WRITE ERROR in batch {batch_id}: {str(write_error)}")
                import traceback
                traceback.print_exc()
        
        # Start streaming with debug write function
        print("\nðŸš€ Starting Auto Loader stream with write debugging...")
        
        debug_query = (stream_df.writeStream
                      .foreachBatch(debug_write_function)
                      .outputMode("append") 
                      .option("checkpointLocation", f"{CHECKPOINT_PATH}debug/")
                      .option("queryName", "debug_table_write_stream")
                      .trigger(processingTime="5 seconds")  # Faster for debugging
                      .start())
        
        print(f"âœ… Debug stream started: {debug_query.id}")
        
        # Enhanced monitoring for 2 minutes
        test_duration = 120  # 2 minutes should be enough
        print(f"â° Enhanced monitoring for {test_duration} seconds...")
        
        start_time = time.time()
        last_count = 0
        check_interval = 10  # Check every 10 seconds
        next_check = start_time + check_interval
        
        while time.time() - start_time < test_duration:
            current_time = time.time()
            
            try:
                if debug_query.isActive:
                    # Check stream progress
                    progress = debug_query.lastProgress
                    if progress:
                        batch_id = progress.get('batchId', 'unknown')
                        input_rows = progress.get('numInputRows', 0)
                        processing_time = progress.get('durationMs', {}).get('triggerExecution', 0)
                        
                        if input_rows > 0:
                            print(f"ðŸ“Š [{datetime.now().strftime('%H:%M:%S')}] Batch {batch_id}: {input_rows} rows, {processing_time}ms")
                    else:
                        elapsed = int(current_time - start_time)
                        remaining = test_duration - elapsed
                        print(f"â³ [{datetime.now().strftime('%H:%M:%S')}] Waiting... ({elapsed}s/{test_duration}s)")
                    
                    # Table count verification every 10 seconds
                    if current_time >= next_check:
                        current_count = spark.table(TABLE_RAW_LOGS).count() if spark.catalog.tableExists(TABLE_RAW_LOGS) else 0
                        new_records = current_count - last_count
                        
                        print(f"    ðŸ“Š Table verification: {current_count} total records (+{new_records} new)")
                        
                        last_count = current_count
                        next_check = current_time + check_interval
                else:
                    print("âŒ Stream stopped unexpectedly")
                    break
                    
            except Exception as monitor_error:
                print(f"âš ï¸ Monitoring error: {monitor_error}")
            
            time.sleep(5)
        
        # Stop stream
        print("\nðŸ›‘ Stopping debug stream...")
        debug_query.stop()
        
        print("âœ… Stream stopped")
        return True
        
    except Exception as e:
        print(f"âŒ Auto Loader with write debugging failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Run the write debugging test
write_debug_success = run_autoloader_with_write_debugging()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“Š Step 4: Final Verification

# COMMAND ----------

print("ðŸ“Š FINAL VERIFICATION - TABLE WRITE FIX")
print("=" * 60)

try:
    # Check final results
    if spark.catalog.tableExists(TABLE_RAW_LOGS):
        final_count = spark.table(TABLE_RAW_LOGS).count()
        print(f"ðŸ“„ Final table count: {final_count}")
        
        if final_count >= 15:
            print(f"\nðŸŽ‰ SUCCESS! TABLE WRITE ISSUE FIXED!")
            print(f"âœ… Expected: 15 log lines")
            print(f"âœ… Actual: {final_count} records in table")
            print(f"âœ… Auto Loader â†’ Table write pipeline: WORKING")
            
            # Show sample records
            print(f"\nðŸ“‹ Sample records from table:")
            sample_records = spark.table(TABLE_RAW_LOGS).select("log_content", "file_path").limit(5)
            
            for i, row in enumerate(sample_records.collect()):
                file_name = row['file_path'].split('/')[-1] if row['file_path'] else 'unknown'
                content_preview = row['log_content'][:60] + "..."
                print(f"  {i+1}. {file_name}: {content_preview}")
                
            print(f"\nðŸ”§ ROOT CAUSE RESOLVED:")
            print(f"   - Table write permissions: âœ… Working")
            print(f"   - Auto Loader configuration: âœ… Working")
            print(f"   - Checkpoint management: âœ… Working")
            print(f"   - Stream processing: âœ… Working")
            
        elif final_count > 0:
            print(f"\nâš ï¸ PARTIAL SUCCESS:")
            print(f"âœ… Table writes working: {final_count} records")
            print(f"âš ï¸ Expected more records (should be ~15)")
            print(f"ðŸ”§ May need longer processing time or file detection issues")
            
        else:
            print(f"\nâŒ TABLE WRITE ISSUE PERSISTS:")
            print(f"âŒ Records in table: {final_count}")
            print(f"ðŸ”§ Check the detailed write debugging logs above")
            print(f"ðŸ”§ Possible issues:")
            print(f"   - Table permissions")
            print(f"   - Delta table conflicts") 
            print(f"   - Auto Loader file detection")
            
    else:
        print(f"âŒ Table {TABLE_RAW_LOGS} does not exist")
        print(f"ðŸ”§ Table creation failed")
        
except Exception as e:
    print(f"âŒ Verification failed: {e}")

print(f"\nâ° Diagnostic completed: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“‹ Summary & Next Steps
# MAGIC
# MAGIC This diagnostic script focuses specifically on the **table write disconnect** issue where:
# MAGIC - âœ… Auto Loader processes 15 rows successfully 
# MAGIC - âŒ 0 records appear in the destination table
# MAGIC
# MAGIC ### Key Debugging Features:
# MAGIC 1. **Direct table write verification** - Tests table permissions before streaming
# MAGIC 2. **Enhanced write debugging** - Shows exactly what happens during table writes  
# MAGIC 3. **Real-time table verification** - Checks record count every 10 seconds
# MAGIC 4. **Comprehensive error capture** - Catches and displays write failures
# MAGIC
# MAGIC ### Expected Resolution:
# MAGIC - If table writes work: Will show 15+ records in final verification
# MAGIC - If table writes fail: Will show specific error messages during write operations
# MAGIC - Root cause will be clearly identified in the detailed logs
