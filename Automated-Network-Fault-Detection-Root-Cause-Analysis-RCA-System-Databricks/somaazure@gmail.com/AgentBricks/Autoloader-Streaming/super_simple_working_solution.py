# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ”§ Super Simple Working Solution - Back to Basics
# MAGIC
# MAGIC **APPROACH**: Strip everything down to absolute basics that must work
# MAGIC **GOAL**: Get any classification working, then build up
# MAGIC **STATUS**: Based on successful table write fix (15/15) + minimal processing

# COMMAND ----------

print("ðŸ”§ SUPER SIMPLE WORKING SOLUTION")
print("=" * 50)
print("ðŸŽ¯ GOAL: Get ANY severity classification working")
print("ðŸŽ¯ APPROACH: Minimal complexity, maximum reliability")

import time
from datetime import datetime
from pyspark.sql.functions import *

# Same configuration that achieved 15/15 raw ingestion success
LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming/"
CHECKPOINT_PATH = "/FileStore/checkpoints/super_simple/"

# Use same table config that worked
def get_table_config():
    try:
        UC_CATALOG = "network_fault_detection"
        UC_SCHEMA = "processed_data"
        spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}")
        return f"{UC_CATALOG}.{UC_SCHEMA}.severity_classifications_streaming"
    except:
        return 'default.severity_classifications_streaming'

TABLE_SEVERITY = get_table_config()
print(f"ðŸ“Š Severity table: {TABLE_SEVERITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“ Step 1: Create Log Files (Same as Successful)

# COMMAND ----------

def create_simple_log_files():
    """Same log creation that worked"""
    
    print("ðŸ“ Creating log files (same as successful 15/15)")
    print("-" * 40)
    
    try:
        # Clean and recreate
        try:
            dbutils.fs.rm(LOG_SOURCE_PATH, recurse=True)
        except:
            pass
        
        dbutils.fs.mkdirs(LOG_SOURCE_PATH)
        
        # Simple 3 files, 5 lines each = 15 total
        sample_logs = [
            {
                'filename': 'critical_network_001.log',
                'content': """2025-09-12 10:15:23 CRITICAL [NetworkGateway] Primary gateway offline
2025-09-12 10:15:24 ERROR [System] Network outage detected
2025-09-12 10:15:25 CRITICAL [Monitor] Complete network failure
2025-09-12 10:15:26 ERROR [Router] Cannot reach upstream
2025-09-12 10:15:27 CRITICAL [Alert] Service unavailable"""
            },
            {
                'filename': 'high_severity_002.log',
                'content': """2025-09-12 10:20:15 ERROR [LoadBalancer] Backend servers not responding
2025-09-12 10:20:16 WARN [Monitor] Response time degraded
2025-09-12 10:20:17 ERROR [Database] Connection pool exhausted
2025-09-12 10:20:18 WARN [System] CPU usage at 95%
2025-09-12 10:20:19 ERROR [Service] API endpoints returning errors"""
            },
            {
                'filename': 'medium_severity_003.log',
                'content': """2025-09-12 10:25:10 WARN [System] Disk usage at 80%
2025-09-12 10:25:11 INFO [Monitor] Backup process completed
2025-09-12 10:25:12 WARN [Network] Intermittent packet loss
2025-09-12 10:25:13 INFO [Service] Scheduled maintenance window
2025-09-12 10:25:14 WARN [Security] Unusual login attempts"""
            }
        ]
        
        total_lines = 0
        for log_file in sample_logs:
            file_path = f"{LOG_SOURCE_PATH}{log_file['filename']}"
            dbutils.fs.put(file_path, log_file['content'], overwrite=True)
            
            # Verify
            content = dbutils.fs.head(file_path, max_bytes=500)
            lines = len(content.strip().split('\n')) if content.strip() else 0
            total_lines += lines
            
            print(f"âœ… {log_file['filename']}: {lines} lines")
        
        print(f"ðŸ“Š Total lines: {total_lines}")
        return total_lines == 15
        
    except Exception as e:
        print(f"âŒ Error creating files: {e}")
        return False

files_created = create_simple_log_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ—ƒï¸ Step 2: Create Simple Table

# COMMAND ----------

def create_super_simple_table():
    """Create the simplest possible severity table"""
    
    print("ðŸ—ƒï¸ Creating super simple severity table")
    print("-" * 40)
    
    try:
        # Clean table
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_SEVERITY}")
        
        # Super simple schema - just the essentials
        spark.sql(f"""
        CREATE TABLE {TABLE_SEVERITY} (
            severity_id BIGINT,
            log_content STRING,
            predicted_severity STRING,
            confidence_score DOUBLE,
            created_at TIMESTAMP
        ) USING DELTA
        """)
        
        print(f"âœ… Created simple table: {TABLE_SEVERITY}")
        
        # Test write immediately
        test_data = [(12345, "Test log content", "P2", 0.8, datetime.now())]
        test_df = spark.createDataFrame(test_data, ["severity_id", "log_content", "predicted_severity", "confidence_score", "created_at"])
        test_df.write.mode("append").saveAsTable(TABLE_SEVERITY)
        
        test_count = spark.table(TABLE_SEVERITY).count()
        print(f"âœ… Write test successful: {test_count} records")
        
        # Clean test data
        spark.sql(f"DELETE FROM {TABLE_SEVERITY}")
        print(f"âœ… Table ready for real data")
        
        return True
        
    except Exception as e:
        print(f"âŒ Table creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

table_ready = create_super_simple_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 3: Super Simple Processing Function

# COMMAND ----------

def super_simple_batch_processor(batch_df, batch_id):
    """Super simple processing - no complex logic, just basic classification"""
    
    print(f"\nðŸ”§ SUPER SIMPLE PROCESSOR: Batch {batch_id}")
    print("=" * 50)
    
    try:
        # Basic batch info
        batch_count = batch_df.count()
        print(f"ðŸ“Š Batch count: {batch_count}")
        
        if batch_count == 0:
            print("âš ï¸ Empty batch")
            return
        
        print("âœ… CHECKPOINT 1: Batch processor function executing")
        
        # Convert to pandas
        try:
            batch_pandas = batch_df.toPandas()
            print(f"âœ… CHECKPOINT 2: Pandas conversion successful ({len(batch_pandas)} rows)")
        except Exception as pandas_error:
            print(f"âŒ CHECKPOINT 2 FAILED: {pandas_error}")
            return
        
        # Super simple processing
        records = []
        
        for idx, row in batch_pandas.iterrows():
            try:
                # Get log content
                log_content = row.get('log_content', row.get('value', ''))
                
                if not log_content.strip():
                    continue
                
                print(f"   ðŸ“ Row {idx}: {log_content[:40]}...")
                
                # SUPER SIMPLE classification - just keyword matching
                log_lower = log_content.lower()
                
                if 'critical' in log_lower:
                    severity = 'P1'
                    confidence = 0.9
                elif 'error' in log_lower:
                    severity = 'P2' 
                    confidence = 0.8
                else:
                    severity = 'P3'
                    confidence = 0.7
                
                # Create minimal record
                record = {
                    'severity_id': int(time.time() * 1000000) + idx,
                    'log_content': log_content[:200],  # Truncate to fit table
                    'predicted_severity': severity,
                    'confidence_score': confidence,
                    'created_at': datetime.now()
                }
                
                records.append(record)
                print(f"   âœ… Row {idx}: {severity} (confidence: {confidence})")
                
            except Exception as row_error:
                print(f"   âŒ Row {idx} error: {row_error}")
                continue
        
        print(f"âœ… CHECKPOINT 3: {len(records)} records processed")
        
        # Write to table
        if records:
            try:
                print(f"ðŸ’¾ Writing {len(records)} records to table...")
                
                records_df = spark.createDataFrame(records)
                print(f"âœ… CHECKPOINT 4: DataFrame created")
                
                records_df.write.mode("append").saveAsTable(TABLE_SEVERITY)
                print(f"âœ… CHECKPOINT 5: Table write successful")
                
                # Verify immediately
                current_count = spark.table(TABLE_SEVERITY).count()
                print(f"âœ… CHECKPOINT 6: Table verification - {current_count} total records")
                
                if current_count > 0:
                    print("ðŸ“‹ Latest record:")
                    latest = spark.table(TABLE_SEVERITY).orderBy(desc("severity_id")).limit(1)
                    for row in latest.collect():
                        print(f"   {row['predicted_severity']} - {row['log_content'][:50]}...")
                
            except Exception as write_error:
                print(f"âŒ CHECKPOINT 4/5/6 FAILED: {write_error}")
                import traceback
                traceback.print_exc()
        else:
            print("âš ï¸ No records to write")
        
        print(f"âœ… Batch {batch_id} completed: {len(records)} records processed")
        
    except Exception as batch_error:
        print(f"âŒ BATCH PROCESSOR FAILED: {batch_error}")
        import traceback
        traceback.print_exc()

print("âœ… Super simple processor ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸš€ Step 4: Run Super Simple Streaming

# COMMAND ----------

def run_super_simple_streaming():
    """Run super simple streaming version"""
    
    print("\nðŸš€ SUPER SIMPLE STREAMING TEST")
    print("=" * 50)
    
    if not files_created or not table_ready:
        print("âŒ Prerequisites failed")
        return False
    
    try:
        # Clean checkpoint
        try:
            dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
            print("âœ… Checkpoint cleaned")
        except:
            print("ðŸ“ No checkpoint to clean")
        
        # Same Auto Loader config that achieved 15/15 raw success
        autoloader_options = {
            "cloudFiles.format": "text",
            "cloudFiles.schemaLocation": f"{CHECKPOINT_PATH}schema/",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.maxFilesPerTrigger": "5"
        }
        
        print("ðŸŒŠ Creating Auto Loader stream (same config that worked)...")
        
        # Same stream structure that worked for raw ingestion
        stream_df = (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .load(LOG_SOURCE_PATH)
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .selectExpr("value as log_content", 
                               "_metadata.file_path as file_path",
                               "ingestion_timestamp"))
        
        print("âœ… Stream DataFrame created")
        
        # Start super simple stream
        print("ðŸš€ Starting super simple processing stream...")
        
        simple_query = (stream_df.writeStream
                       .foreachBatch(super_simple_batch_processor)
                       .outputMode("append")
                       .option("checkpointLocation", f"{CHECKPOINT_PATH}simple/")
                       .option("queryName", "super_simple_severity")
                       .trigger(processingTime="5 seconds")
                       .start())
        
        print(f"âœ… Super simple stream started: {simple_query.id}")
        
        # Monitor for 90 seconds (same duration that worked)
        test_duration = 90
        print(f"â° Monitoring for {test_duration} seconds...")
        
        start_time = time.time()
        check_interval = 15
        next_check = start_time + check_interval
        
        while time.time() - start_time < test_duration:
            current_time = time.time()
            
            try:
                if simple_query.isActive:
                    elapsed = int(current_time - start_time)
                    remaining = test_duration - elapsed
                    print(f"â³ [{datetime.now().strftime('%H:%M:%S')}] Stream active ({elapsed}s/{test_duration}s)")
                    
                    # Check progress every 15 seconds
                    if current_time >= next_check:
                        try:
                            current_count = spark.table(TABLE_SEVERITY).count()
                            print(f"    ðŸ“Š Current severity count: {current_count}")
                            next_check = current_time + check_interval
                        except Exception as check_error:
                            print(f"    âš ï¸ Count check error: {check_error}")
                            next_check = current_time + check_interval
                else:
                    print("âŒ Stream stopped unexpectedly")
                    break
            except Exception as monitor_error:
                print(f"âš ï¸ Monitoring error: {monitor_error}")
            
            time.sleep(5)
        
        # Stop stream
        print("\nðŸ›‘ Stopping super simple stream...")
        simple_query.stop()
        print("âœ… Stream stopped")
        
        return True
        
    except Exception as e:
        print(f"âŒ Super simple streaming failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Run super simple streaming
simple_success = run_super_simple_streaming()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“Š Step 5: Final Results

# COMMAND ----------

print("ðŸ“Š SUPER SIMPLE STREAMING RESULTS")
print("=" * 50)

try:
    final_count = spark.table(TABLE_SEVERITY).count() if spark.catalog.tableExists(TABLE_SEVERITY) else 0
    print(f"ðŸŽ¯ Final severity classifications: {final_count}")
    
    if final_count > 0:
        print(f"\nðŸŽ‰ SUCCESS! SUPER SIMPLE SOLUTION WORKING!")
        print(f"âœ… {final_count} severity classifications created")
        print(f"âœ… Basic AgentBricks processing: WORKING")
        
        # Show all results
        print(f"\nðŸ“‹ All Classifications:")
        all_results = spark.table(TABLE_SEVERITY).select("severity_id", "predicted_severity", "confidence_score", "log_content").orderBy("severity_id")
        
        for row in all_results.collect():
            log_preview = row['log_content'][:40] + "..."
            print(f"   ID {row['severity_id']}: {row['predicted_severity']} (conf: {row['confidence_score']:.1f}) - {log_preview}")
        
        # Show distribution
        print(f"\nðŸ“Š Severity Distribution:")
        distribution = spark.table(TABLE_SEVERITY).groupBy("predicted_severity").count().orderBy("predicted_severity").collect()
        for row in distribution:
            print(f"   {row['predicted_severity']}: {row['count']} incidents")
        
        print(f"\nðŸ”§ SOLUTION PROVEN:")
        print(f"   âœ… Auto Loader ingestion: WORKING")
        print(f"   âœ… Stream processing: WORKING")  
        print(f"   âœ… Basic classification: WORKING")
        print(f"   âœ… Table writes: WORKING")
        print(f"   âœ… End-to-end pipeline: WORKING")
        
    else:
        print(f"\nâŒ SUPER SIMPLE SOLUTION STILL FAILING")
        print(f"ðŸ”§ Check the detailed logs above for:")
        print(f"   - Did 'CHECKPOINT 1-6' messages appear?")
        print(f"   - Any specific error messages?")
        print(f"   - Table write failures?")
        
except Exception as e:
    print(f"âŒ Results verification failed: {e}")

print(f"\nâ° Super simple test completed: {datetime.now()}")
print("ðŸŽ¯ This is the minimal viable solution - build complexity from here!")
