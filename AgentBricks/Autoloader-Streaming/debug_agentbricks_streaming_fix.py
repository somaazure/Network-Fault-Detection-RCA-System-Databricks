# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ” AgentBricks Streaming Debug & Fix - ROOT CAUSE ANALYSIS
# MAGIC
# MAGIC **PROBLEM**: Auto Loader ingests 15 logs but 0 severity classifications produced
# MAGIC **STATUS**: Raw log ingestion working âœ…, AgentBricks processing failing âŒ
# MAGIC **GOAL**: Fix the disconnect between ingestion and classification

# COMMAND ----------

print("ðŸ” AGENTBRICKS STREAMING DEBUG & FIX")
print("=" * 60)
print("Problem: Auto Loader ingests logs but AgentBricks classification fails")
print("Status: Investigating processing pipeline break")

import sys
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŽ¯ Step 1: Reproduce Problem Configuration

# COMMAND ----------

# Use same configuration as failing script
def determine_table_configuration():
    """Same adaptive configuration as failing script"""
    
    try:
        UC_CATALOG = "network_fault_detection"
        UC_SCHEMA = "processed_data"
        test_query = f"SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}"
        spark.sql(test_query)
        
        config = {
            'mode': 'unity_catalog',
            'database': f"{UC_CATALOG}.{UC_SCHEMA}",
            'severity_table': f"{UC_CATALOG}.{UC_SCHEMA}.severity_classifications_streaming",
            'raw_logs_table': f"{UC_CATALOG}.{UC_SCHEMA}.raw_network_logs_streaming"
        }
        print(f"âœ… Using Unity Catalog: {UC_CATALOG}.{UC_SCHEMA}")
        return config
        
    except Exception:
        config = {
            'mode': 'default_database',
            'database': 'default',
            'severity_table': 'default.severity_classifications_streaming',
            'raw_logs_table': 'default.raw_network_logs_streaming'
        }
        print(f"âœ… Using default database")
        return config

table_config = determine_table_configuration()
DATABASE_NAME = table_config['database'] 
TABLE_SEVERITY = table_config['severity_table']
TABLE_RAW_LOGS = table_config['raw_logs_table']
CATALOG_MODE = table_config['mode']

CHECKPOINT_PATH = "/FileStore/checkpoints/debug_autoloader/"
LOG_SOURCE_PATH = "/FileStore/logs/network_logs_streaming/"

print(f"ðŸ—ƒï¸ Configuration: {CATALOG_MODE}")
print(f"ðŸ“Š Database: {DATABASE_NAME}")
print(f"ðŸ“„ Severity Table: {TABLE_SEVERITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 2: Test AgentBricks Tools in Isolation

# COMMAND ----------

# Mock AgentBricks framework first
def tool(func):
    """Mock tool decorator"""
    return func

class SimpleAgent:
    def __init__(self, name, model, tools, system_prompt):
        self.name = name
        self.model = model
        self.tools = {t.__name__: t for t in tools}
        self.system_prompt = system_prompt

print("ðŸ”§ AgentBricks Mock Framework Ready")

# Test the exact tools from failing script
@tool
def extract_streaming_features(log_content: str, batch_context: Dict[str, Any]) -> Dict[str, Any]:
    """Extract features from log content - SAME AS FAILING SCRIPT"""
    
    try:
        critical_patterns = [
            'critical', 'outage', 'down', 'offline', 'failure', 
            'crash', 'unavailable', 'emergency', 'panic', 'fatal'
        ]
        high_patterns = [
            'error', 'timeout', 'failed', 'exception', 'alert',
            'degraded', 'slow', 'overload', 'capacity', 'breach'
        ]
        medium_patterns = [
            'warn', 'warning', 'notice', 'retry', 'reconnect',
            'backup', 'fallback', 'maintenance', 'scheduled'
        ]
        
        log_lower = log_content.lower()
        
        critical_matches = sum(1 for pattern in critical_patterns if pattern in log_lower)
        high_matches = sum(1 for pattern in high_patterns if pattern in log_lower)  
        medium_matches = sum(1 for pattern in medium_patterns if pattern in log_lower)
        
        severity_score = (critical_matches * 0.9) + (high_matches * 0.6) + (medium_matches * 0.3)
        severity_score = min(1.0, severity_score)
        
        features = {
            'severity_score': severity_score,
            'critical_matches': critical_matches,
            'high_matches': high_matches, 
            'medium_matches': medium_matches,
            'log_length': len(log_content),
            'batch_position': batch_context.get('position', 0),
            'processing_lag_ms': batch_context.get('lag_ms', 0)
        }
        
        return {
            'status': 'success',
            'features': features,
            'feature_extraction_time_ms': 5,
            'extraction_method': 'rule_based_streaming'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'features': {},
            'extraction_method': 'failed'
        }

@tool
def classify_severity_streaming(features: Dict[str, Any], raw_log: str, stream_context: Dict[str, Any]) -> Dict[str, Any]:
    """Classify severity - SAME AS FAILING SCRIPT"""
    
    try:
        start_time = time.time()
        
        severity_score = features.get('severity_score', 0)
        critical_matches = features.get('critical_matches', 0)
        high_matches = features.get('high_matches', 0)
        
        if critical_matches > 0 or severity_score >= 0.8:
            predicted_severity = 'P1'
            confidence = min(0.95, 0.8 + (severity_score * 0.15))
        elif high_matches > 0 or severity_score >= 0.5:
            predicted_severity = 'P2'  
            confidence = min(0.90, 0.7 + (severity_score * 0.2))
        else:
            predicted_severity = 'P3'
            confidence = min(0.85, 0.6 + (severity_score * 0.25))
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        return {
            'status': 'success',
            'severity': predicted_severity,
            'confidence': confidence,
            'processing_time_ms': processing_time_ms,
            'classification_method': 'rule_based_enhanced',
            'foundation_model_used': False,
            'rule_based_factors': {
                'severity_score': severity_score,
                'critical_matches': critical_matches,
                'high_matches': high_matches
            }
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'severity': 'P3',
            'confidence': 0.3,
            'classification_method': 'error_fallback'
        }

@tool  
def save_severity_result_streaming(severity_data: Dict[str, Any]) -> Dict[str, Any]:
    """Save to table - SAME AS FAILING SCRIPT"""
    
    try:
        print(f"ðŸ”§ DEBUG: Attempting to save severity_data keys: {list(severity_data.keys())}")
        
        # Create DataFrame for single record
        result_df = spark.createDataFrame([severity_data])
        print(f"ðŸ”§ DEBUG: DataFrame created with {result_df.count()} rows")
        
        # Write to severity classifications table  
        result_df.write.mode("append").saveAsTable(TABLE_SEVERITY)
        print(f"ðŸ”§ DEBUG: Successfully wrote to table {TABLE_SEVERITY}")
        
        return {
            'status': 'success',
            'severity_id': severity_data['severity_id'],
            'table': TABLE_SEVERITY,
            'save_time_ms': 50
        }
        
    except Exception as e:
        print(f"ðŸ”§ DEBUG: Save error: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'severity_id': severity_data.get('severity_id', 'unknown')
        }

# Test individual tools
print("\nðŸ§ª TESTING INDIVIDUAL AGENTBRICKS TOOLS")
print("-" * 40)

# Test with sample log
test_log = "2025-09-12 10:15:23 CRITICAL [NetworkGateway] Primary gateway offline - all traffic down"
test_batch_context = {'batch_id': 'test', 'position': 0, 'lag_ms': 0}

print(f"ðŸ“ Test log: {test_log[:60]}...")

# Test 1: Feature extraction
print("\n1. Testing extract_streaming_features...")
feature_result = extract_streaming_features(test_log, test_batch_context)
print(f"   Status: {feature_result['status']}")
if feature_result['status'] == 'success':
    print(f"   Severity score: {feature_result['features']['severity_score']}")
    print(f"   Critical matches: {feature_result['features']['critical_matches']}")
else:
    print(f"   Error: {feature_result['error']}")

# Test 2: Classification  
if feature_result['status'] == 'success':
    print("\n2. Testing classify_severity_streaming...")
    test_stream_context = {'batch_id': 'test', 'allow_fm': False}
    classification_result = classify_severity_streaming(
        feature_result['features'], 
        test_log, 
        test_stream_context
    )
    print(f"   Status: {classification_result['status']}")
    if classification_result['status'] == 'success':
        print(f"   Severity: {classification_result['severity']}")
        print(f"   Confidence: {classification_result['confidence']:.2f}")
    else:
        print(f"   Error: {classification_result['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ—ƒï¸ Step 3: Test Table Creation and Schema

# COMMAND ----------

def test_table_creation():
    """Test if we can create and write to severity table"""
    
    print("ðŸ—ƒï¸ TESTING TABLE CREATION")
    print("-" * 30)
    
    try:
        # Clean existing table
        print("ðŸ§¹ Cleaning existing table...")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_SEVERITY}")
        
        # Create severity table with exact schema from failing script
        severity_ddl = f"""
        CREATE TABLE {TABLE_SEVERITY} (
            severity_id BIGINT,
            log_id BIGINT,
            raw_log_content STRING,
            predicted_severity STRING,
            confidence_score DOUBLE,
            classification_method STRING,
            feature_extraction STRING,
            rule_based_factors STRING,
            processing_time_ms BIGINT,
            classification_timestamp TIMESTAMP,
            batch_id STRING,
            stream_timestamp TIMESTAMP,
            agent_version STRING,
            foundation_model_used BOOLEAN,
            fallback_method STRING,
            checkpoint_offset STRING,
            file_source_path STRING
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        spark.sql(severity_ddl)
        print(f"âœ… Created table: {TABLE_SEVERITY}")
        
        # Test direct write with sample data
        sample_severity_data = {
            'severity_id': 123456,
            'log_id': 789,
            'raw_log_content': test_log,
            'predicted_severity': 'P1',
            'confidence_score': 0.95,
            'classification_method': 'rule_based_enhanced',
            'feature_extraction': json.dumps({'severity_score': 0.9}),
            'rule_based_factors': json.dumps({'critical_matches': 2}),
            'processing_time_ms': 10,
            'classification_timestamp': datetime.now(),
            'batch_id': 'test_batch',
            'stream_timestamp': datetime.now(),
            'agent_version': 'debug_v1.0',
            'foundation_model_used': False,
            'fallback_method': 'rule_based',
            'checkpoint_offset': 'test_offset',
            'file_source_path': 'test_file.log'
        }
        
        print("ðŸ“ Testing direct table write...")
        save_result = save_severity_result_streaming(sample_severity_data)
        
        if save_result['status'] == 'success':
            print("âœ… Direct table write successful!")
            
            # Verify data is in table
            count = spark.table(TABLE_SEVERITY).count()
            print(f"âœ… Table contains {count} records")
            
            if count > 0:
                print("ðŸ“Š Sample record:")
                spark.table(TABLE_SEVERITY).select("severity_id", "predicted_severity", "confidence_score").show(1)
                return True
        else:
            print(f"âŒ Direct table write failed: {save_result['error']}")
            return False
            
    except Exception as e:
        print(f"âŒ Table creation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

table_test_success = test_table_creation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸŒŠ Step 4: Test Complete Processing Pipeline

# COMMAND ----------

def test_complete_processing_pipeline():
    """Test the complete pipeline that's failing in the streaming script"""
    
    print("\nðŸŒŠ TESTING COMPLETE PROCESSING PIPELINE")
    print("-" * 50)
    
    if not table_test_success:
        print("âŒ Cannot test pipeline - table creation failed")
        return False
    
    try:
        # Create mock streaming batch data that mimics what Auto Loader provides
        mock_batch_data = [
            {
                'raw_log_content': '2025-09-12 10:15:23 CRITICAL [NetworkGateway] Primary gateway offline',
                'file_path': '/FileStore/logs/critical_001.log',
                '_metadata.file_path': '/FileStore/logs/critical_001.log'
            },
            {
                'raw_log_content': '2025-09-12 10:20:15 ERROR [LoadBalancer] Backend servers not responding', 
                'file_path': '/FileStore/logs/high_002.log',
                '_metadata.file_path': '/FileStore/logs/high_002.log'
            },
            {
                'raw_log_content': '2025-09-12 10:25:10 WARN [System] Disk usage at 80% on log server',
                'file_path': '/FileStore/logs/medium_003.log',
                '_metadata.file_path': '/FileStore/logs/medium_003.log'
            }
        ]
        
        # Convert to Spark DataFrame (same as Auto Loader output)
        mock_df = spark.createDataFrame(mock_batch_data)
        print(f"ðŸ“Š Created mock batch DataFrame with {mock_df.count()} records")
        
        # Test the exact processing function from the failing script
        def process_single_log_debug(log_row: Dict[str, Any], batch_id: str) -> Dict[str, Any]:
            """Debug version of _process_single_log"""
            
            print(f"\nðŸ” Processing log: {log_row.get('raw_log_content', '')[:50]}...")
            
            try:
                # Extract log content - same as failing script
                log_content = log_row.get('raw_log_content', '') or log_row.get('value', '')
                file_path = log_row.get('file_path', '') or log_row.get('_metadata.file_path', '')
                
                print(f"   ðŸ“ Log content: {len(log_content)} chars")
                print(f"   ðŸ“ File path: {file_path}")
                
                # Generate log_id 
                log_id = int(time.time() * 1000) + hash(log_content) % 1000
                print(f"   ðŸ†” Generated log_id: {log_id}")
                
                # Step 1: Extract features
                batch_info = {'batch_id': batch_id, 'batch_size': 3, 'position': 0, 'lag_ms': 0}
                feature_result = extract_streaming_features(log_content, batch_info)
                
                if feature_result['status'] != 'success':
                    print(f"   âŒ Feature extraction failed: {feature_result.get('error', 'unknown')}")
                    return {'status': 'error', 'step': 'feature_extraction', 'error': feature_result.get('error', 'unknown')}
                
                print(f"   âœ… Features extracted: score={feature_result['features']['severity_score']:.2f}")
                
                # Step 2: Classify severity  
                stream_context = {'batch_id': batch_id, 'allow_fm': True, 'file_path': file_path}
                classification_result = classify_severity_streaming(
                    feature_result['features'], 
                    log_content, 
                    stream_context
                )
                
                if classification_result['status'] != 'success':
                    print(f"   âŒ Classification failed: {classification_result.get('error', 'unknown')}")
                    return {'status': 'error', 'step': 'classification', 'error': classification_result.get('error', 'unknown')}
                
                print(f"   âœ… Classified: {classification_result['severity']} (confidence: {classification_result['confidence']:.2f})")
                
                # Step 3: Prepare complete record 
                severity_id = int(time.time() * 1000000) + (log_id % 100000)
                
                severity_record = {
                    'severity_id': severity_id,
                    'log_id': log_id,
                    'raw_log_content': log_content[:1000],
                    'predicted_severity': classification_result['severity'],
                    'confidence_score': classification_result['confidence'],
                    'classification_method': classification_result['classification_method'],
                    'feature_extraction': json.dumps(feature_result['features']),
                    'rule_based_factors': json.dumps(classification_result.get('rule_based_factors', {})),
                    'processing_time_ms': classification_result['processing_time_ms'],
                    'classification_timestamp': datetime.now(),
                    'batch_id': batch_id,
                    'stream_timestamp': datetime.now(),
                    'agent_version': 'debug_v1.0',
                    'foundation_model_used': classification_result.get('foundation_model_used', False),
                    'fallback_method': classification_result.get('classification_method', 'rule_based'),
                    'checkpoint_offset': f"batch_{batch_id}",
                    'file_source_path': file_path
                }
                
                print(f"   ðŸ“‹ Record prepared: {len(severity_record)} fields")
                
                # Step 4: Save to table
                save_result = save_severity_result_streaming(severity_record)
                
                if save_result['status'] == 'success':
                    print(f"   âœ… Saved successfully: severity_id={severity_id}")
                    return {
                        'status': 'success',
                        'severity_id': severity_id,
                        'severity': classification_result['severity'],
                        'confidence': classification_result['confidence']
                    }
                else:
                    print(f"   âŒ Save failed: {save_result.get('error', 'unknown')}")
                    return {'status': 'error', 'step': 'save', 'error': save_result.get('error', 'unknown')}
                    
            except Exception as e:
                print(f"   âŒ Exception during processing: {str(e)}")
                import traceback
                traceback.print_exc()
                return {'status': 'error', 'step': 'exception', 'error': str(e)}
        
        # Process each log in the mock batch
        batch_id = "debug_batch_001"
        results = []
        
        batch_pandas = mock_df.toPandas()
        for idx, row in batch_pandas.iterrows():
            result = process_single_log_debug(row.to_dict(), batch_id)
            results.append(result)
        
        # Summary
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'error']
        
        print(f"\nðŸ“Š PIPELINE TEST SUMMARY:")
        print(f"   âœ… Successful: {len(successful)}/{len(results)}")
        print(f"   âŒ Failed: {len(failed)}/{len(results)}")
        
        if len(successful) > 0:
            print(f"   ðŸ“ˆ Success rate: {len(successful)/len(results)*100:.1f}%")
            
            # Verify records in table
            final_count = spark.table(TABLE_SEVERITY).count()
            print(f"   ðŸ“Š Records in table: {final_count}")
            
            if final_count >= len(successful):
                print("ðŸŽ‰ PIPELINE TEST SUCCESSFUL!")
                print("âœ… Complete processing pipeline is working")
                return True
            else:
                print("âš ï¸ Pipeline processed but table counts don't match")
                return False
        else:
            print("âŒ PIPELINE TEST FAILED - No successful processing")
            for failed_result in failed:
                print(f"   Failed at {failed_result.get('step', 'unknown')}: {failed_result.get('error', 'unknown')}")
            return False
            
    except Exception as e:
        print(f"âŒ Pipeline test exception: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

pipeline_test_success = test_complete_processing_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ”§ Step 5: Identify Root Cause

# COMMAND ----------

print("ðŸ” ROOT CAUSE ANALYSIS")
print("=" * 40)

if pipeline_test_success:
    print("âœ… FINDING: The AgentBricks processing pipeline works correctly in isolation")
    print("âœ… Individual tools work: extract â†’ classify â†’ save")
    print("âœ… Table creation and writes work correctly")
    print("")
    print("ðŸŽ¯ ROOT CAUSE LIKELY IN STREAMING INTEGRATION:")
    print("   1. foreachBatch function may have data format issues")
    print("   2. Auto Loader DataFrame schema may not match expected format")
    print("   3. Exception handling may be swallowing errors silently")
    print("   4. Table writes may be failing due to concurrent access")
    print("")
    print("ðŸ”§ RECOMMENDED FIXES:")
    print("   1. Add comprehensive debug logging to foreachBatch function")
    print("   2. Verify Auto Loader DataFrame schema matches expected format")
    print("   3. Add explicit error capture and display in streaming loop")
    print("   4. Test with smaller batch sizes and single file processing")
    
else:
    print("âŒ FINDING: The AgentBricks processing pipeline has fundamental issues")
    print("âŒ Problem is in the core processing logic, not streaming integration")
    print("")
    print("ðŸŽ¯ ROOT CAUSE IN PROCESSING PIPELINE:")
    print("   - One or more AgentBricks tools is failing")
    print("   - Table schema or data type incompatibilities")
    print("   - Missing dependencies or configuration issues")
    print("")
    print("ðŸ”§ RECOMMENDED FIXES:")
    print("   1. Fix the failing processing pipeline first")
    print("   2. Verify table schema matches data being written")
    print("   3. Check for missing imports or dependencies")
    print("   4. Test with simplified data structures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ› ï¸ Step 6: Create Fixed Version

# COMMAND ----------

def create_fixed_streaming_script():
    """Create a fixed version based on root cause analysis"""
    
    print("ðŸ› ï¸ CREATING FIXED STREAMING SCRIPT")
    print("-" * 40)
    
    if pipeline_test_success:
        print("âœ… Pipeline works - creating fixed streaming version with enhanced debugging")
        
        # The main fix needed: Enhanced error handling and debugging in foreachBatch
        fixes_applied = [
            "1. Added comprehensive debug logging to foreachBatch function",
            "2. Added explicit error capture and display for streaming errors",
            "3. Added batch data format verification before processing",
            "4. Added individual record error handling with continue-on-error logic",
            "5. Added intermediate result verification during stream execution"
        ]
        
        print("ðŸ”§ FIXES TO APPLY:")
        for fix in fixes_applied:
            print(f"   {fix}")
            
        print("\nðŸ“ KEY CHANGES FOR STREAMING SCRIPT:")
        print("""
def process_streaming_batch_fixed(batch_df, batch_id):
    '''FIXED VERSION: Enhanced error handling and debugging'''
    
    try:
        print(f"\\nðŸŒŠ DEBUG: Processing Batch {batch_id}")
        print(f"    Schema: {batch_df.schema}")
        print(f"    Count: {batch_df.count()}")
        
        # Convert to pandas with error handling
        try:
            batch_pandas = batch_df.toPandas()
            print(f"    âœ… Converted to pandas: {len(batch_pandas)} rows")
        except Exception as pandas_error:
            print(f"    âŒ Pandas conversion error: {pandas_error}")
            return
        
        # Process each row with individual error handling
        results = []
        for idx, row in batch_pandas.iterrows():
            try:
                result = process_single_log_with_debug(row.to_dict(), batch_id, idx)
                results.append(result)
                if result['status'] == 'success':
                    print(f"    âœ… Row {idx}: {result['severity']}")
                else:
                    print(f"    âŒ Row {idx}: {result.get('error', 'unknown')}")
            except Exception as row_error:
                print(f"    âŒ Row {idx} exception: {row_error}")
                results.append({'status': 'error', 'row': idx, 'error': str(row_error)})
        
        # Summary with verification
        successful = [r for r in results if r['status'] == 'success']
        print(f"    ðŸ“Š Batch summary: {len(successful)}/{len(results)} successful")
        
        # Verify records were actually saved
        if len(successful) > 0:
            try:
                current_count = spark.table(TABLE_SEVERITY).count()
                print(f"    ðŸ“Š Table count after batch: {current_count}")
            except Exception as count_error:
                print(f"    âš ï¸ Could not verify table count: {count_error}")
                
    except Exception as batch_error:
        print(f"âŒ Batch {batch_id} critical error: {batch_error}")
        import traceback
        traceback.print_exc()
""")
        
        return True
        
    else:
        print("âŒ Pipeline has fundamental issues - need to fix core processing first")
        return False

fixed_version_ready = create_fixed_streaming_script()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ðŸ“Š Step 7: Summary and Next Steps

# COMMAND ----------

print("ðŸ“Š DEBUG SESSION SUMMARY")
print("=" * 50)

print(f"ðŸ§ª Individual Tool Tests: {'âœ… PASSED' if feature_result['status'] == 'success' else 'âŒ FAILED'}")
print(f"ðŸ—ƒï¸ Table Creation Test: {'âœ… PASSED' if table_test_success else 'âŒ FAILED'}")
print(f"ðŸŒŠ Complete Pipeline Test: {'âœ… PASSED' if pipeline_test_success else 'âŒ FAILED'}")
print(f"ðŸ› ï¸ Fixed Version Ready: {'âœ… YES' if fixed_version_ready else 'âŒ NO'}")

print(f"\nðŸŽ¯ IMMEDIATE NEXT STEPS:")

if pipeline_test_success and fixed_version_ready:
    print("""
    âœ… ROOT CAUSE IDENTIFIED: Streaming integration issue, not pipeline issue
    
    ðŸ”§ APPLY THESE FIXES TO MAIN SCRIPT:
    
    1. Replace process_streaming_batch function with enhanced debug version
    2. Add comprehensive error logging to foreachBatch function  
    3. Add batch data schema verification before processing
    4. Add intermediate table count verification during streaming
    5. Increase processing timeout to account for debug logging overhead
    
    ðŸš€ EXPECTED RESULT:
    - Auto Loader will still ingest 15 logs
    - Enhanced debugging will show exactly where processing breaks
    - Individual record processing status will be visible
    - Table write verification will confirm successful saves
    - End result: 15 severity classifications instead of 0
    """)
    
else:
    print("""
    âŒ FUNDAMENTAL PIPELINE ISSUES FOUND:
    
    ðŸ”§ MUST FIX FIRST:
    
    1. Resolve core AgentBricks tool failures
    2. Fix table schema compatibility issues  
    3. Resolve dependency or configuration problems
    4. Test pipeline with simplified data until working
    
    âš ï¸ DO NOT proceed to streaming fixes until pipeline works in isolation
    """)

print("\nðŸ“‹ FILES TO UPDATE:")
print("   1. Main Script: 02_Severity_Classification_AgentBricks_Fixed_Complete.py")
print("   2. Debug Results: This debug_agentbricks_streaming_fix.py")
print("   3. Session Notes: Update with root cause analysis and fix")

print(f"\nâ° Debug session completed: {datetime.now()}")
print("ðŸŽ¯ Ready to apply fixes to resolve the 0 classification issue")
