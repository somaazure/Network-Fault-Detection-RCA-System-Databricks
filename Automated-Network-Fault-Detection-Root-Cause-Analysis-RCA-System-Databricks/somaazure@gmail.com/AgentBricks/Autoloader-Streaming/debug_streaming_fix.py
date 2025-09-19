# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ”§ Auto Loader Streaming Debug & Fix
# MAGIC
# MAGIC This notebook provides step-by-step debugging and fixes for the Auto Loader streaming issue where no data is being processed.

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql.functions import *

print("ðŸ” AUTO LOADER STREAMING DIAGNOSTIC & FIX")
print("=" * 60)

# Configuration (match your existing notebook)
LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming/"
CHECKPOINT_PATH = "/FileStore/checkpoints/autoloader_severity/"
TABLE_SEVERITY = "default.severity_classifications_streaming"  # Adjust if using Unity Catalog
TABLE_RAW_LOGS = "default.raw_network_logs_streaming"          # Adjust if using Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 1: Verify and Recreate Log Files

# COMMAND ----------

def create_and_verify_log_files():
    """Create sample log files and verify they exist with content"""
    
    print("ðŸ“ STEP 1: LOG FILE VERIFICATION & CREATION")
    print("-" * 50)
    
    try:
        # Clean and recreate directory
        print("ðŸ§¹ Cleaning existing log directory...")
        try:
            dbutils.fs.rm(LOG_SOURCE_PATH, recurse=True)
        except:
            pass  # Directory might not exist
        
        # Create fresh directory
        dbutils.fs.mkdirs(LOG_SOURCE_PATH)
        print(f"âœ… Created directory: {LOG_SOURCE_PATH}")
        
        # Create substantial log files with multiple lines
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
        
        # Write files and verify
        files_created = 0
        for log_file in sample_logs:
            file_path = f"{LOG_SOURCE_PATH}{log_file['filename']}"
            
            # Write file content
            dbutils.fs.put(file_path, log_file['content'], overwrite=True)
            
            # Verify file was created and has content
            try:
                file_content = dbutils.fs.head(file_path, max_bytes=500)
                line_count = len(file_content.strip().split('\n'))
                file_size = len(file_content.encode('utf-8'))
                
                print(f"âœ… Created {log_file['filename']}: {line_count} lines, {file_size} bytes")
                files_created += 1
                
            except Exception as verify_error:
                print(f"âŒ Failed to verify {log_file['filename']}: {verify_error}")
        
        # Final verification - list all files
        print(f"\nðŸ“‚ Final verification - listing all files in {LOG_SOURCE_PATH}:")
        try:
            all_files = dbutils.fs.ls(LOG_SOURCE_PATH)
            for file_info in all_files:
                print(f"  ðŸ“„ {file_info.name} - {file_info.size} bytes - Modified: {file_info.modificationTime}")
                
                # Show first few lines of each file
                if file_info.size > 0:
                    sample_content = dbutils.fs.head(file_info.path, max_bytes=150)
                    first_line = sample_content.split('\n')[0] if sample_content else "No content"
                    print(f"      First line: {first_line}")
                    
        except Exception as list_error:
            print(f"âŒ Error listing files: {list_error}")
            return False
        
        print(f"\nâœ… Successfully created {files_created} log files")
        return files_created > 0
        
    except Exception as e:
        print(f"âŒ Error creating log files: {e}")
        return False

# Create and verify log files
log_creation_success = create_and_verify_log_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 2: Test Static Auto Loader Read (Before Streaming)

# COMMAND ----------

def test_files_readable():
    """Test if files are readable using regular Spark text format (not CloudFiles)"""
    
    print("\nðŸ“Š STEP 2: FILE READABILITY TEST")
    print("-" * 50)
    
    if not log_creation_success:
        print("âŒ Cannot test - log files were not created successfully")
        return False
    
    try:
        print(f"ðŸ“ Source: {LOG_SOURCE_PATH}")
        print("ðŸ“‹ Testing basic Spark text format (not CloudFiles)")
        
        # Test regular Spark text read (not CloudFiles) - Unity Catalog compatible
        static_df = (spark.read
                    .format("text")
                    .load(LOG_SOURCE_PATH)
                    .select("value", "_metadata.file_path")
                    .withColumnRenamed("_metadata.file_path", "file_path"))
        
        print("âœ… Spark text format read successful")
        
        # Check schema
        print(f"ðŸ“Š Schema: {static_df.schema}")
        
        # Count records
        record_count = static_df.count()
        print(f"ðŸ“„ Total records found: {record_count}")
        
        if record_count > 0:
            print("\nðŸ“‹ Sample records:")
            sample_data = static_df.select("value", "file_path").limit(5)
            
            for row in sample_data.collect():
                file_name = row['file_path'].split('/')[-1] if row['file_path'] else 'unknown'
                content_preview = row['value'][:80] + "..." if len(row['value']) > 80 else row['value']
                print(f"  ðŸ“„ {file_name}: {content_preview}")
            
            print("âœ… Files are readable - Auto Loader should work with readStream")
            return True
        else:
            print("âš ï¸ No records found - file format or path issue")
            return False
            
    except Exception as e:
        print(f"âŒ File readability test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_autoloader_config():
    """Test Auto Loader configuration (without executing streaming operations)"""
    
    print("\nðŸ”§ STEP 2.5: AUTO LOADER CONFIGURATION TEST")
    print("-" * 50)
    
    try:
        # Clean any existing schema
        try:
            dbutils.fs.rm(f"{CHECKPOINT_PATH}schema_test/", recurse=True)
        except:
            pass
        
        # Configure Auto Loader
        autoloader_options = {
            "cloudFiles.format": "text",
            "cloudFiles.schemaLocation": f"{CHECKPOINT_PATH}schema_test/",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.maxFilesPerTrigger": "5"
        }
        
        print(f"ðŸ“ Source: {LOG_SOURCE_PATH}")
        print(f"ðŸ“‹ Options: {autoloader_options}")
        
        # Create Auto Loader streaming DataFrame (but don't execute it)
        stream_df = (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .load(LOG_SOURCE_PATH))
        
        print("âœ… Auto Loader streaming DataFrame created successfully")
        print(f"ðŸ“Š Schema: {stream_df.schema}")
        print("âœ… Configuration is valid (ready for streaming)")
        
        return True
        
    except Exception as e:
        print(f"âŒ Auto Loader configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Test file readability
file_readability_success = test_files_readable()

# Test Auto Loader configuration
config_test_success = test_autoloader_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 3: Fixed Streaming Test (Extended Duration)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from datetime import datetime
import time

def run_extended_streaming_test():
    """Run Auto Loader streaming test with extended duration and better monitoring"""

    print("\nðŸŒŠ STEP 3: EXTENDED STREAMING TEST")
    print("-" * 50)

    if not file_readability_success:
        print("âŒ Cannot run streaming test - file readability test failed")
        return False

    if not config_test_success:
        print("âŒ Cannot run streaming test - Auto Loader configuration failed")
        return False

    try:
        # Clean checkpoint to force reprocessing
        print("ðŸ§¹ Cleaning checkpoint directory...")
        try:
            dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
        except:
            pass

        # Recreate tables to ensure clean state
        print("ðŸ—ƒï¸ Recreating tables...")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_RAW_LOGS}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_SEVERITY}")

        # Create simple raw logs table
        spark.sql(f"""
        CREATE TABLE {TABLE_RAW_LOGS} (
            log_content STRING,
            file_path STRING,
            ingestion_timestamp TIMESTAMP
        ) USING DELTA
        """)

        print(f"âœ… Created table: {TABLE_RAW_LOGS}")

        # Configure streaming Auto Loader
        autoloader_options = {
            "cloudFiles.format": "text",
            "cloudFiles.schemaLocation": f"{CHECKPOINT_PATH}schema/",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.maxFilesPerTrigger": "5"
        }

        # Create streaming DataFrame
        stream_df = (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .load(LOG_SOURCE_PATH)
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .selectExpr("value as log_content",
                               "_metadata.file_path as file_path",
                               "ingestion_timestamp"))

        # Simple streaming write to raw logs table
        print("ðŸš€ Starting streaming query...")
        stream_query = (stream_df.writeStream
                       .outputMode("append")
                       .format("delta")
                       .option("checkpointLocation", f"{CHECKPOINT_PATH}raw_logs/")
                       .option("queryName", "autoloader_raw_ingestion")
                       .trigger(processingTime="10 seconds")
                       .toTable(TABLE_RAW_LOGS))

        print(f"âœ… Stream started: {stream_query.id}")

        # Monitor for extended period
        test_duration = 90  # 90 seconds instead of 15
        print(f"â° Monitoring for {test_duration} seconds...")

        start_time = time.time()
        last_progress_timestamp = ""
        check_interval = 15  # Check intermediate results every 15 seconds
        next_check = start_time + check_interval

        while time.time() - start_time < test_duration:
            current_time = time.time()

            try:
                if stream_query.isActive:
                    progress = stream_query.lastProgress
                    current_timestamp_val = progress.get('timestamp', '') if progress else ''
                    if progress and current_timestamp_val != last_progress_timestamp:
                        batch_id = progress.get('batchId', 'unknown')
                        input_rows = progress.get('inputRowsPerSecond', 0)
                        num_input_rows = progress.get('numInputRows', 0)
                        processing_time = progress.get('durationMs', {}).get('triggerExecution', 0)

                        print(f"ðŸ“Š [{datetime.now().strftime('%H:%M:%S')}] Batch {batch_id}")
                        print(f"    ðŸ“¥ Input rows this batch: {num_input_rows}")
                        print(f"    âš¡ Rows/second: {input_rows}")
                        print(f"    â±ï¸  Processing time: {processing_time}ms")
                        last_progress_timestamp = current_timestamp_val
                    else:
                        print(f"â³ [{datetime.now().strftime('%H:%M:%S')}] Waiting for data...")

                    # Check intermediate results every 15 seconds
                    if current_time >= next_check:
                        try:
                            current_raw_count = spark.table(TABLE_RAW_LOGS).count()
                            print(f"    ðŸ“Š Intermediate check: {current_raw_count} raw records so far")
                            next_check = current_time + check_interval
                        except:
                            print("    ðŸ“Š Intermediate check: Tables not ready yet")
                            next_check = current_time + check_interval
                else:
                    print("âŒ Stream is no longer active")
                    break

            except Exception as monitor_error:
                print(f"âš ï¸ Monitoring error: {monitor_error}")

            time.sleep(5)  # Check every 5 seconds

        # Stop stream
        print("ðŸ›‘ Stopping stream...")
        stream_query.stop()

        # Verify results
        print("\nðŸ“Š VERIFICATION RESULTS:")
        raw_count = spark.table(TABLE_RAW_LOGS).count()
        print(f"ðŸ“„ Raw logs ingested: {raw_count}")

        if raw_count > 0:
            print("âœ… SUCCESS: Auto Loader is working!")

            # Show sample data
            print("\nðŸ“‹ Sample ingested data:")
            sample_df = spark.table(TABLE_RAW_LOGS).select("log_content", "file_path").limit(3)

            for row in sample_df.collect():
                file_name = row['file_path'].split('/')[-1] if row['file_path'] else 'unknown'
                content = row['log_content'][:100] + "..." if len(row['log_content']) > 100 else row['log_content']
                print(f"  ðŸ“„ {file_name}: {content}")

            return True
        else:
            print("âŒ FAILURE: No data was ingested")
            print("ðŸ”§ Possible issues:")
            print("   - File format or encoding problems")
            print("   - Auto Loader permissions")
            print("   - Checkpoint conflicts")
            return False

    except Exception as e:
        print(f"âŒ Extended streaming test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Run extended streaming test
streaming_success = run_extended_streaming_test()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 4: Manual Processing Test (If Auto Loader Fails)

# COMMAND ----------

def manual_processing_test():
    """Manual processing test to isolate the issue"""
    
    print("\nðŸ”§ STEP 4: MANUAL PROCESSING TEST")
    print("-" * 50)
    
    try:
        # Read files manually using Spark
        print("ðŸ“– Reading files manually with Spark text format...")
        
        manual_df = (spark.read
                    .format("text")
                    .load(LOG_SOURCE_PATH)
                    .withColumn("file_path", input_file_name())
                    .withColumn("manual_timestamp", current_timestamp()))
        
        manual_count = manual_df.count()
        print(f"ðŸ“„ Records read manually: {manual_count}")
        
        if manual_count > 0:
            print("âœ… Files are readable - Auto Loader configuration is the issue")
            
            # Show manual data
            print("\nðŸ“‹ Manual read sample:")
            manual_sample = manual_df.select("value", "file_path").limit(3)
            
            for row in manual_sample.collect():
                file_name = row['file_path'].split('/')[-1]
                content = row['value'][:80] + "..." if len(row['value']) > 80 else row['value']  
                print(f"  ðŸ“„ {file_name}: {content}")
                
            # Try saving manually
            print("\nðŸ’¾ Testing manual save...")
            test_table = "default.manual_test_logs"
            
            manual_df.write.mode("overwrite").saveAsTable(test_table)
            saved_count = spark.table(test_table).count()
            print(f"âœ… Manual save successful: {saved_count} records")
            
            return True
        else:
            print("âŒ Even manual read failed - file/path issue")
            return False
            
    except Exception as e:
        print(f"âŒ Manual processing test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Skip manual test if Auto Loader succeeded  
if streaming_success:
    print("\nðŸŽ‰ AUTO LOADER SUCCESS - SKIPPING MANUAL TEST")
    print("âœ… No need for manual processing test since Auto Loader is working!")
else:
    print("\nðŸ”§ AUTO LOADER FAILED - RUNNING MANUAL DIAGNOSTIC")
    manual_success = manual_processing_test()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“‹ Diagnostic Summary & Recommendations

# COMMAND ----------

print("\nðŸ“‹ DIAGNOSTIC SUMMARY")
print("=" * 60)

if streaming_success:
    print("ðŸŽ‰ SUCCESS: Auto Loader streaming is working!")
    print("\nâœ… Working components:")
    print("  - Log file creation âœ“")
    print("  - Auto Loader configuration âœ“") 
    print("  - Streaming ingestion âœ“")
    print("  - Delta table writes âœ“")
    
    print("\nðŸ”§ To apply this fix to your main notebook:")
    print("  1. Increase stream monitoring duration from 15s to 90s")
    print("  2. Add checkpoint cleanup before each test")
    print("  3. Use trigger(processingTime='10 seconds') for faster processing")
    print("  4. Add better progress monitoring")
    
else:
    print("ðŸ”´ ISSUE IDENTIFIED: Auto Loader streaming not working")
    print("\nâŒ Failed components:")
    if not log_creation_success:
        print("  - Log file creation âŒ")
    if not static_test_success:
        print("  - Auto Loader configuration âŒ")
    
    print("\nðŸ”§ RECOMMENDED FIXES:")
    print("  1. Check file permissions and DBFS access")
    print("  2. Verify Auto Loader format compatibility")
    print("  3. Clean all checkpoints and restart")
    print("  4. Consider using format('text') instead of cloudFiles")
    
    if 'manual_success' in locals() and manual_success:
        print("  5. âœ… Manual processing works - use direct file reading as fallback")
    else:
        print("  5. âŒ Even manual processing failed - check file paths and content")

print(f"\nâ° Diagnostic completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
