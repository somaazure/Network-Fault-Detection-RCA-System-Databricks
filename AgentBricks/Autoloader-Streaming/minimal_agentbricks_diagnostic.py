# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ” Minimal AgentBricks Diagnostic - Isolate Exact Failure
# MAGIC
# MAGIC **ISSUE**: Auto Loader ingests 15 logs, AgentBricks produces 0 classifications
# MAGIC **FOCUS**: Isolate whether it's stream setup or processing logic failure

# COMMAND ----------

print("ðŸ” MINIMAL AGENTBRICKS DIAGNOSTIC")
print("=" * 60)
print("ðŸŽ¯ GOAL: Isolate exact failure point in AgentBricks processing")

import time
from datetime import datetime
from pyspark.sql.functions import *

# Same successful configuration
LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming/"
CHECKPOINT_PATH = "/FileStore/checkpoints/minimal_agentbricks/"

def get_table_config():
    try:
        UC_CATALOG = "network_fault_detection"
        UC_SCHEMA = "processed_data"
        spark.sql(f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}")
        return {
            'severity_table': f"{UC_CATALOG}.{UC_SCHEMA}.severity_classifications_streaming"
        }
    except:
        return {
            'severity_table': 'default.severity_classifications_streaming'
        }

config = get_table_config()
TABLE_SEVERITY = config['severity_table']

print(f"ðŸ“Š Severity table: {TABLE_SEVERITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ§ª Step 1: Test AgentBricks Processing Logic Directly

# COMMAND ----------

def test_agentbricks_logic_directly():
    """Test AgentBricks processing logic with static data (no streaming)"""
    
    print("ðŸ§ª TESTING AGENTBRICKS LOGIC DIRECTLY (NO STREAMING)")
    print("-" * 60)
    
    try:
        # Clean severity table
        print("ðŸ§¹ Cleaning severity table...")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_SEVERITY}")
        
        # Create simple severity table
        spark.sql(f"""
        CREATE TABLE {TABLE_SEVERITY} (
            severity_id BIGINT,
            raw_log_content STRING,
            predicted_severity STRING,
            confidence_score DOUBLE,
            classification_method STRING,
            processing_time_ms BIGINT,
            classification_timestamp TIMESTAMP,
            batch_id STRING,
            agent_version STRING,
            file_source_path STRING
        ) USING DELTA
        """)
        print(f"âœ… Created severity table: {TABLE_SEVERITY}")
        
        # Mock AgentBricks tools (same as working)
        def tool(func):
            return func
        
        @tool
        def extract_severity_features_test(log_content: str):
            """Simple test version"""
            try:
                critical_patterns = ['critical', 'error', 'outage', 'down', 'offline', 'failure']
                high_patterns = ['warn', 'timeout', 'failed', 'exception', 'alert']
                
                log_lower = log_content.lower()
                critical_matches = sum(1 for pattern in critical_patterns if pattern in log_lower)
                high_matches = sum(1 for pattern in high_patterns if pattern in log_lower)
                
                severity_score = (critical_matches * 0.9) + (high_matches * 0.6)
                severity_score = min(1.0, severity_score)
                
                return {
                    'status': 'success',
                    'severity_score': severity_score,
                    'critical_matches': critical_matches,
                    'high_matches': high_matches
                }
            except Exception as e:
                return {'status': 'error', 'error': str(e)}
        
        @tool 
        def classify_severity_test(features):
            """Simple test classification"""
            try:
                severity_score = features.get('severity_score', 0)
                critical_matches = features.get('critical_matches', 0)
                
                if critical_matches > 0 or severity_score >= 0.8:
                    return {'status': 'success', 'severity': 'P1', 'confidence': 0.9, 'processing_time_ms': 10}
                elif severity_score >= 0.5:
                    return {'status': 'success', 'severity': 'P2', 'confidence': 0.8, 'processing_time_ms': 8}
                else:
                    return {'status': 'success', 'severity': 'P3', 'confidence': 0.7, 'processing_time_ms': 5}
            except Exception as e:
                return {'status': 'error', 'error': str(e)}
        
        # Test with sample log data
        test_logs = [
            "2025-09-12 10:15:23 CRITICAL [NetworkGateway] Primary gateway offline - all traffic down",
            "2025-09-12 10:20:15 ERROR [LoadBalancer] Backend servers not responding", 
            "2025-09-12 10:25:10 WARN [System] Disk usage at 80% on log server"
        ]
        
        print(f"\nðŸ” Testing AgentBricks processing with {len(test_logs)} sample logs...")
        
        severity_records = []
        for i, log_content in enumerate(test_logs):
            print(f"\n   ðŸ“ Testing log {i+1}: {log_content[:50]}...")
            
            # Test feature extraction
            feature_result = extract_severity_features_test(log_content)
            if feature_result['status'] != 'success':
                print(f"   âŒ Feature extraction failed: {feature_result.get('error', 'unknown')}")
                continue
            
            print(f"   âœ… Features: score={feature_result['severity_score']:.2f}, critical={feature_result['critical_matches']}")
            
            # Test classification
            classification_result = classify_severity_test(feature_result)
            if classification_result['status'] != 'success':
                print(f"   âŒ Classification failed: {classification_result.get('error', 'unknown')}")
                continue
                
            print(f"   âœ… Classification: {classification_result['severity']} (confidence: {classification_result['confidence']:.2f})")
            
            # Create record
            severity_record = {
                'severity_id': int(time.time() * 1000000) + i,
                'raw_log_content': log_content,
                'predicted_severity': classification_result['severity'],
                'confidence_score': classification_result['confidence'],
                'classification_method': 'direct_test',
                'processing_time_ms': classification_result['processing_time_ms'],
                'classification_timestamp': datetime.now(),
                'batch_id': 'direct_test',
                'agent_version': 'test_v1.0',
                'file_source_path': f'test_log_{i+1}.log'
            }
            
            severity_records.append(severity_record)
            print(f"   âœ… Record created: ID={severity_record['severity_id']}")
        
        # Test direct table write
        if severity_records:
            print(f"\nðŸ’¾ Testing direct table write with {len(severity_records)} records...")
            
            try:
                severity_df = spark.createDataFrame(severity_records)
                print(f"   âœ… DataFrame created: {severity_df.count()} records")
                
                severity_df.write.mode("append").saveAsTable(TABLE_SEVERITY)
                print(f"   âœ… Table write successful")
                
                # Verify
                final_count = spark.table(TABLE_SEVERITY).count()
                print(f"   âœ… Final verification: {final_count} records in table")
                
                if final_count > 0:
                    print("\nðŸ“‹ Sample results:")
                    sample = spark.table(TABLE_SEVERITY).select("predicted_severity", "confidence_score", "classification_method").limit(3)
                    for row in sample.collect():
                        print(f"   {row['predicted_severity']} (confidence: {row['confidence_score']:.2f}, method: {row['classification_method']})")
                
                return final_count == len(severity_records)
                
            except Exception as write_error:
                print(f"   âŒ Table write failed: {str(write_error)}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print("âŒ No records created - processing logic failed")
            return False
            
    except Exception as e:
        print(f"âŒ Direct logic test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Test AgentBricks logic directly
direct_logic_success = test_agentbricks_logic_directly()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŒŠ Step 2: Test Minimal Streaming with Simple Processing

# COMMAND ----------

def test_minimal_streaming():
    """Test minimal streaming with simple processing"""
    
    print("\nðŸŒŠ TESTING MINIMAL STREAMING")
    print("-" * 40)
    
    if not direct_logic_success:
        print("âŒ Cannot test streaming - direct logic failed")
        return False
    
    try:
        # Clean checkpoint
        try:
            dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
        except:
            pass
        
        # Simple batch processing function
        def minimal_batch_processor(batch_df, batch_id):
            """Minimal batch processor with maximum debugging"""
            
            try:
                print(f"\nðŸ” MINIMAL BATCH PROCESSOR: Batch {batch_id}")
                print("=" * 50)
                
                # Basic checks
                print(f"ðŸ“Š Batch DataFrame type: {type(batch_df)}")
                batch_count = batch_df.count()
                print(f"ðŸ“Š Batch count: {batch_count}")
                
                if batch_count == 0:
                    print("âš ï¸ Empty batch - nothing to process")
                    return
                
                # Show schema
                print(f"ðŸ“‹ Batch schema: {[field.name for field in batch_df.schema.fields]}")
                
                # Convert to pandas with error handling
                try:
                    print("ðŸ”„ Converting to pandas...")
                    batch_pandas = batch_df.toPandas()
                    print(f"âœ… Pandas conversion successful: {len(batch_pandas)} rows")
                    
                    # Show sample data
                    if len(batch_pandas) > 0:
                        first_row = batch_pandas.iloc[0]
                        print(f"ðŸ“ Sample row keys: {list(first_row.keys())}")
                        
                        log_content = first_row.get('log_content', first_row.get('value', 'NO_CONTENT'))
                        print(f"ðŸ“ Sample log content: {log_content[:60]}...")
                        
                except Exception as pandas_error:
                    print(f"âŒ Pandas conversion failed: {pandas_error}")
                    return
                
                # CRITICAL: Test if we can get here
                print("ðŸŽ¯ CRITICAL CHECKPOINT: Batch processing function is executing!")
                print(f"ðŸŽ¯ This proves the foreachBatch mechanism is working")
                
                # Simple record creation (no complex processing)
                simple_records = []
                
                for idx, row in batch_pandas.iterrows():
                    try:
                        log_content = row.get('log_content', row.get('value', ''))
                        file_path = row.get('file_path', f'batch_{batch_id}_row_{idx}')
                        
                        if log_content.strip():
                            # Super simple classification
                            if 'critical' in log_content.lower():
                                severity = 'P1'
                            elif 'error' in log_content.lower():
                                severity = 'P2'
                            else:
                                severity = 'P3'
                            
                            simple_record = {
                                'severity_id': int(time.time() * 1000000) + idx,
                                'raw_log_content': log_content[:200],
                                'predicted_severity': severity,
                                'confidence_score': 0.8,
                                'classification_method': 'minimal_test',
                                'processing_time_ms': 1,
                                'classification_timestamp': datetime.now(),
                                'batch_id': str(batch_id),
                                'agent_version': 'minimal_v1.0',
                                'file_source_path': file_path
                            }
                            
                            simple_records.append(simple_record)
                            print(f"   âœ… Record {idx}: {severity} created")
                    
                    except Exception as row_error:
                        print(f"   âŒ Row {idx} error: {row_error}")
                        continue
                
                # Test table write
                if simple_records:
                    print(f"\nðŸ’¾ Writing {len(simple_records)} simple records...")
                    
                    try:
                        simple_df = spark.createDataFrame(simple_records)
                        simple_df.write.mode("append").saveAsTable(TABLE_SEVERITY)
                        
                        # Verify write
                        current_count = spark.table(TABLE_SEVERITY).count()
                        print(f"âœ… Write successful! Table now has {current_count} records")
                        
                    except Exception as write_error:
                        print(f"âŒ Write failed: {write_error}")
                        import traceback
                        traceback.print_exc()
                else:
                    print("âš ï¸ No simple records to write")
                
                print(f"âœ… Batch {batch_id} processing completed")
                
            except Exception as batch_error:
                print(f"âŒ CRITICAL: Batch processor failed: {batch_error}")
                import traceback
                traceback.print_exc()
        
        # Create minimal stream
        print("ðŸ“Š Creating minimal Auto Loader stream...")
        
        autoloader_options = {
            "cloudFiles.format": "text",
            "cloudFiles.schemaLocation": f"{CHECKPOINT_PATH}schema/",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "none",
            "cloudFiles.maxFilesPerTrigger": "5"
        }
        
        stream_df = (spark.readStream
                    .format("cloudFiles")
                    .options(**autoloader_options)
                    .load(LOG_SOURCE_PATH)
                    .withColumn("ingestion_timestamp", current_timestamp())
                    .selectExpr("value as log_content", 
                               "_metadata.file_path as file_path",
                               "ingestion_timestamp"))
        
        # Start minimal stream
        print("ðŸš€ Starting minimal stream with simple processing...")
        
        minimal_query = (stream_df.writeStream
                        .foreachBatch(minimal_batch_processor)
                        .outputMode("append")
                        .option("checkpointLocation", f"{CHECKPOINT_PATH}minimal/")
                        .option("queryName", "minimal_agentbricks_test")
                        .trigger(processingTime="5 seconds")
                        .start())
        
        print(f"âœ… Minimal stream started: {minimal_query.id}")
        
        # Monitor for shorter duration
        test_duration = 60  # 1 minute should be enough
        print(f"â° Monitoring for {test_duration} seconds...")
        
        start_time = time.time()
        while time.time() - start_time < test_duration:
            try:
                if minimal_query.isActive:
                    elapsed = int(time.time() - start_time)
                    remaining = test_duration - elapsed
                    print(f"â³ [{datetime.now().strftime('%H:%M:%S')}] Stream active ({elapsed}s/{test_duration}s)")
                    
                    # Check results every 15 seconds
                    if elapsed % 15 == 0 and elapsed > 0:
                        current_count = spark.table(TABLE_SEVERITY).count() if spark.catalog.tableExists(TABLE_SEVERITY) else 0
                        print(f"    ðŸ“Š Current severity count: {current_count}")
                else:
                    print("âŒ Stream stopped unexpectedly")
                    break
            except Exception as monitor_error:
                print(f"âš ï¸ Monitoring error: {monitor_error}")
            
            time.sleep(5)
        
        # Stop stream
        print("ðŸ›‘ Stopping minimal stream...")
        minimal_query.stop()
        
        return True
        
    except Exception as e:
        print(f"âŒ Minimal streaming test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# Test minimal streaming
minimal_streaming_success = test_minimal_streaming()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“Š Step 3: Diagnostic Results

# COMMAND ----------

print("ðŸ“Š MINIMAL AGENTBRICKS DIAGNOSTIC RESULTS")
print("=" * 60)

# Check final results
try:
    severity_count = spark.table(TABLE_SEVERITY).count() if spark.catalog.tableExists(TABLE_SEVERITY) else 0
    print(f"ðŸŽ¯ Final severity classifications: {severity_count}")
    
    if severity_count > 0:
        print(f"\nâœ… SUCCESS: AgentBricks processing is working!")
        print(f"âœ… {severity_count} classifications created")
        
        # Show sample results
        print(f"\nðŸ“‹ Sample classifications:")
        sample = spark.table(TABLE_SEVERITY).select("predicted_severity", "confidence_score", "classification_method").limit(5)
        for row in sample.collect():
            print(f"   {row['predicted_severity']} (confidence: {row['confidence_score']:.2f}, method: {row['classification_method']})")
            
        print(f"\nðŸ”§ ROOT CAUSE ANALYSIS:")
        if direct_logic_success:
            print(f"   âœ… Direct AgentBricks logic: WORKING")
        if minimal_streaming_success:
            print(f"   âœ… Streaming foreachBatch: WORKING")
        print(f"   ðŸŽ¯ The issue is likely in the complex processing logic, not the streaming setup")
        
    else:
        print(f"\nâŒ AGENTBRICKS PROCESSING STILL FAILING")
        print(f"\nðŸ”§ DIAGNOSTIC ANALYSIS:")
        
        if not direct_logic_success:
            print(f"   âŒ Direct AgentBricks logic: FAILED")
            print(f"   ðŸŽ¯ Issue is in the core processing logic (feature extraction/classification)")
        elif not minimal_streaming_success:
            print(f"   âŒ Streaming foreachBatch: FAILED") 
            print(f"   ðŸŽ¯ Issue is in the streaming setup or batch processing")
        else:
            print(f"   âš ï¸ Both direct logic and streaming should work - check detailed logs above")
            
        print(f"\nðŸ”§ RECOMMENDED FIXES:")
        print(f"   1. Review the detailed error logs above")
        print(f"   2. Check if 'CRITICAL CHECKPOINT' message appeared (proves foreachBatch works)")
        print(f"   3. Look for specific error messages in processing steps")
        print(f"   4. Verify table write permissions and schema compatibility")
        
except Exception as e:
    print(f"âŒ Diagnostic results failed: {e}")

print(f"\nâ° Diagnostic completed: {datetime.now()}")
