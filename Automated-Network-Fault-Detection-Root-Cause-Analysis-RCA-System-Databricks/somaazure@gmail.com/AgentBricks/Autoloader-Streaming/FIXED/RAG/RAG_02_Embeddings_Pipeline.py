# Databricks notebook source
# MAGIC %md
# MAGIC # ðŸ§  RAG Implementation - Embeddings Pipeline
# MAGIC
# MAGIC **Purpose**: Create and manage embeddings for RCA reports
# MAGIC **Features**: Batch processing, incremental updates, quality validation
# MAGIC **Integration**: Databricks Vector Search + Foundation Models

# COMMAND ----------

# Install required packages
print("ðŸ”§ Installing required packages for Vector Search...")
%pip install databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow
import json
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

# Initialize Vector Search client conditionally
try:
    from databricks.vector_search.client import VectorSearchClient
    vs_client = VectorSearchClient()
    print("âœ… Vector Search client initialized")
    VECTOR_SEARCH_AVAILABLE = True
except Exception as e:
    print(f"âš ï¸ Vector Search not available: {str(e)}")
    vs_client = None
    VECTOR_SEARCH_AVAILABLE = False

print("ðŸ§  RAG Implementation - Embeddings Pipeline")
print("=" * 70)

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"
RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming"
OPTIMIZED_RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_for_vector_search"

# Correct RCA table schema
RCA_COLUMNS = {
    "id": "rca_id",
    "timestamp": "rca_timestamp",
    "category": "root_cause_category",
    "priority": "incident_priority",
    "operation": "recommended_operation",
    "analysis": "rca_analysis",
    "recommendations": "resolution_recommendations",
    "confidence": "analysis_confidence",
    "method": "analysis_method"
}
EMBEDDINGS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_embeddings"

VS_ENDPOINT_NAME = "network_fault_detection_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_vector_index"
EMBEDDING_MODEL = "databricks-bge-large-en"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Embedding Quality Assessment

# COMMAND ----------

def assess_embedding_readiness():
    """Assess if data is ready for high-quality embeddings"""
    print("ðŸ“Š Assessing RCA data for embedding quality...")

    try:
        rca_df = spark.table(RCA_TABLE)
        total_count = rca_df.count()

        if total_count == 0:
            print("âŒ No RCA reports found")
            return None

        # Analyze content quality using correct schema
        quality_analysis = rca_df.select(
            count("*").alias("total_records"),
            count(when(col("rca_analysis").isNotNull(), 1)).alias("non_null_rca"),
            count(when(length(col("rca_analysis")) > 100, 1)).alias("substantial_content"),
            count(when(length(col("rca_analysis")) > 500, 1)).alias("rich_content"),
            avg(length(col("rca_analysis"))).alias("avg_content_length"),
            countDistinct("root_cause_category").alias("unique_categories"),
            countDistinct("incident_priority").alias("unique_priorities")
        ).collect()[0]

        print(f"ðŸ“‹ Total Records: {quality_analysis['total_records']:,}")
        print(f"ðŸ“ Non-null RCA: {quality_analysis['non_null_rca']:,}")
        print(f"ðŸ“Š Substantial Content (>100 chars): {quality_analysis['substantial_content']:,}")
        print(f"ðŸŽ¯ Rich Content (>500 chars): {quality_analysis['rich_content']:,}")
        print(f"ðŸ“ Average Content Length: {quality_analysis['avg_content_length']:.0f} chars")
        print(f"ðŸ”§ Unique Categories: {quality_analysis['unique_categories']}")
        print(f"ðŸš¨ Unique Priorities: {quality_analysis['unique_priorities']}")

        # Calculate quality score
        quality_score = (quality_analysis['substantial_content'] / quality_analysis['total_records']) * 100
        print(f"âœ… Content Quality Score: {quality_score:.1f}%")

        return quality_analysis

    except Exception as e:
        print(f"âŒ Error assessing embedding readiness: {str(e)}")
        return None

# Assess embedding readiness
quality_assessment = assess_embedding_readiness()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Enhanced Content for Embeddings

# COMMAND ----------

def create_enhanced_embedding_content():
    """Create optimized content combinations for better semantic search"""
    print("ðŸ”§ Creating enhanced content for embeddings...")

    try:
        rca_df = spark.table(RCA_TABLE)

        # Create multiple content variations for comprehensive search using correct schema
        enhanced_df = rca_df.select(
            col("rca_id").alias("id"),
            col("rca_timestamp").alias("created_timestamp"),
            col("root_cause_category"),
            col("incident_priority"),
            col("recommended_operation"),
            col("analysis_confidence").alias("confidence_score"),

            # Original content fields
            col("rca_analysis"),
            col("resolution_recommendations"),
            col("analysis_method"),

            # Enhanced content variations for different search patterns

            # 1. Comprehensive content (best for general search) using correct schema
            concat_ws(" | ",
                concat(lit("Category: "), col("root_cause_category")),
                concat(lit("Priority: "), col("incident_priority")),
                concat(lit("Operation: "), col("recommended_operation")),
                concat(lit("Root Cause: "), col("rca_analysis")),
                concat(lit("Recommendations: "), col("resolution_recommendations")),
                concat(lit("Method: "), col("analysis_method"))
            ).alias("comprehensive_content"),

            # 2. Technical focus (for technical searches)
            concat_ws(" | ",
                col("root_cause_category"),
                col("recommended_operation"),
                col("analysis_method"),
                col("rca_analysis")
            ).alias("technical_content"),

            # 3. Solution focus (for recommendation searches)
            concat_ws(" | ",
                col("recommended_operation"),
                col("resolution_recommendations"),
                col("incident_priority")
            ).alias("solution_content"),

            # 4. Keywords and tags
            concat_ws(", ",
                col("incident_priority"),
                col("root_cause_category"),
                col("recommended_operation"),
                # Extract key technical terms
                regexp_extract(col("rca_analysis"), r"(BGP|OSPF|VLAN|CPU|Memory|Interface|Router|Switch|Firewall)", 1)
            ).alias("keywords"),

            # 5. Searchable summary
            concat_ws(" ",
                col("incident_priority"),
                col("root_cause_category"),
                lit("incident requiring"),
                col("recommended_operation"),
                lit("with"),
                regexp_extract(col("resolution_recommendations"), r"^([^.]{0,100})", 1)
            ).alias("searchable_summary")

        ).filter(
            # Quality filters using correct schema
            col("rca_analysis").isNotNull() &
            (length(col("rca_analysis")) > 50) &
            col("resolution_recommendations").isNotNull()
        )

        # Cache for performance
        enhanced_df.cache()
        enhanced_count = enhanced_df.count()

        print(f"âœ… Created enhanced content for {enhanced_count:,} RCA reports")

        # Show sample enhanced content
        print("\nðŸ” Sample enhanced content:")
        enhanced_df.select("id", "comprehensive_content", "keywords").limit(1).show(truncate=False)

        return enhanced_df

    except Exception as e:
        print(f"âŒ Error creating enhanced content: {str(e)}")
        return None

# Create enhanced content
enhanced_content_df = create_enhanced_embedding_content()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Multiple Vector Indexes for Different Search Patterns

# COMMAND ----------

def create_specialized_vector_indexes():
    """Create multiple vector indexes optimized for different search patterns"""
    if not VECTOR_SEARCH_AVAILABLE:
        print("âš ï¸ Vector Search not available - skipping index creation")
        print("ðŸ’¡ Use RAG_Fallback_Basic_Search.py for text-based search instead")
        return False

    if enhanced_content_df is None:
        print("âŒ Enhanced content not available")
        return False

    try:
        # Save enhanced content table
        ENHANCED_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_enhanced_for_embeddings"
        enhanced_content_df.write.mode("overwrite").saveAsTable(ENHANCED_TABLE)
        print(f"ðŸ’¾ Enhanced content saved to: {ENHANCED_TABLE}")

        # Enable Change Data Feed (required for Vector Search delta sync)
        print("ðŸ”§ Enabling Change Data Feed for Vector Search...")
        spark.sql(f"ALTER TABLE {ENHANCED_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("âœ… Change Data Feed enabled")

        # Index configurations for different search patterns (with full catalog.schema.name format)
        index_configs = [
            {
                "name": f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_comprehensive_index",
                "content_column": "comprehensive_content",
                "description": "General-purpose semantic search"
            },
            {
                "name": f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_technical_index",
                "content_column": "technical_content",
                "description": "Technical component and issue search"
            },
            {
                "name": f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_solution_index",
                "content_column": "solution_content",
                "description": "Solution and recommendation search"
            }
        ]

        created_indexes = []

        for config in index_configs:
            try:
                print(f"\nðŸ”§ Creating {config['description']}: {config['name']}")

                # Check if index exists
                try:
                    vs_client.get_index(
                        endpoint_name=VS_ENDPOINT_NAME,
                        index_name=config['name']
                    )
                    print(f"âœ… Index '{config['name']}' already exists")
                    created_indexes.append(config['name'])
                    continue
                except:
                    pass

                # Create index
                vs_client.create_delta_sync_index(
                    endpoint_name=VS_ENDPOINT_NAME,
                    source_table_name=ENHANCED_TABLE,
                    index_name=config['name'],
                    primary_key="id",
                    embedding_source_column=config['content_column'],
                    embedding_model_endpoint_name=EMBEDDING_MODEL,
                    pipeline_type="TRIGGERED"
                )

                print(f"âœ… Created index: {config['name']}")
                created_indexes.append(config['name'])

            except Exception as index_error:
                print(f"âŒ Error creating index {config['name']}: {str(index_error)}")

        print(f"\nðŸ“Š Successfully created {len(created_indexes)} vector indexes")
        return created_indexes

    except Exception as e:
        print(f"âŒ Error creating specialized indexes: {str(e)}")
        return []

# Create specialized indexes
created_indexes = create_specialized_vector_indexes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Batch Embedding Generation and Validation

# COMMAND ----------

def validate_embeddings_quality():
    """Validate the quality of generated embeddings"""
    print("ðŸ” Validating embeddings quality...")

    try:
        # Test queries for validation
        validation_queries = [
            {
                "query": "router interface failure",
                "expected_components": ["router", "interface"],
                "category": "network_hardware"
            },
            {
                "query": "high CPU utilization performance",
                "expected_components": ["CPU", "performance"],
                "category": "performance"
            },
            {
                "query": "BGP routing protocol down",
                "expected_components": ["BGP", "routing"],
                "category": "routing"
            },
            {
                "query": "firewall configuration security",
                "expected_components": ["firewall", "security"],
                "category": "security"
            },
            {
                "query": "VLAN network segmentation",
                "expected_components": ["VLAN", "network"],
                "category": "networking"
            }
        ]

        validation_results = []

        for test_query in validation_queries:
            print(f"\nðŸ§ª Testing: '{test_query['query']}'")

            try:
                # Test with comprehensive index
                index = vs_client.get_index(
                    endpoint_name=VS_ENDPOINT_NAME,
                    index_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_comprehensive_index"
                )
                results = index.similarity_search(
                    query_text=test_query['query'],
                    columns=["id", "root_cause_category", "incident_priority", "recommended_operation", "keywords"],
                    num_results=3
                )

                if results and 'result' in results and 'data_array' in results['result']:
                    result_count = len(results['result']['data_array'])
                    print(f"   âœ… Found {result_count} similar results")

                    # Analyze result relevance based on category and content
                    relevant_results = 0
                    for result_row in results['result']['data_array']:
                        # Extract meaningful content from result row
                        # Column order: ["id", "root_cause_category", "incident_priority", "recommended_operation", "keywords"]
                        if len(result_row) >= 5:
                            category = str(result_row[1]).lower() if result_row[1] else ""
                            priority = str(result_row[2]).lower() if result_row[2] else ""
                            operation = str(result_row[3]).lower() if result_row[3] else ""
                            keywords = str(result_row[4]).lower() if result_row[4] else ""

                            result_text = f"{category} {priority} {operation} {keywords}"

                            # Check if expected components are in the result
                            if any(component.lower() in result_text for component in test_query['expected_components']):
                                relevant_results += 1

                    relevance_score = (relevant_results / result_count) * 100 if result_count > 0 else 0
                    print(f"   ðŸ“Š Relevance score: {relevance_score:.1f}%")

                    validation_results.append({
                        "query": test_query['query'],
                        "category": test_query['category'],
                        "results_found": result_count,
                        "relevance_score": relevance_score
                    })
                else:
                    print("   âš ï¸ No results found")
                    validation_results.append({
                        "query": test_query['query'],
                        "category": test_query['category'],
                        "results_found": 0,
                        "relevance_score": 0
                    })

            except Exception as search_error:
                print(f"   âŒ Search error: {str(search_error)}")

        # Calculate overall validation score
        if validation_results:
            avg_relevance = sum(r['relevance_score'] for r in validation_results) / len(validation_results)
            queries_with_results = sum(1 for r in validation_results if r['results_found'] > 0)

            print(f"\nðŸ“Š VALIDATION SUMMARY:")
            print(f"   ðŸŽ¯ Average Relevance Score: {avg_relevance:.1f}%")
            print(f"   âœ… Queries with Results: {queries_with_results}/{len(validation_results)}")
            print(f"   ðŸ“ˆ Search Success Rate: {(queries_with_results/len(validation_results))*100:.1f}%")

            # Embedding quality assessment
            if avg_relevance >= 70 and queries_with_results >= len(validation_results) * 0.8:
                print("   ðŸŽ¯ Embeddings Quality: âœ… EXCELLENT")
            elif avg_relevance >= 50 and queries_with_results >= len(validation_results) * 0.6:
                print("   ðŸŽ¯ Embeddings Quality: âš ï¸ GOOD")
            else:
                print("   ðŸŽ¯ Embeddings Quality: âŒ NEEDS IMPROVEMENT")

        return validation_results

    except Exception as e:
        print(f"âŒ Error validating embeddings: {str(e)}")
        return []

# Validate embeddings quality
validation_results = validate_embeddings_quality()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Incremental Update Pipeline

# COMMAND ----------

def setup_incremental_update_pipeline():
    """Set up pipeline for incremental embedding updates"""
    print("ðŸ”„ Setting up incremental update pipeline...")

    try:
        # Create tracking table for embedding updates
        EMBEDDING_TRACKING_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.embedding_update_tracking"

        # Schema for tracking table
        tracking_schema = StructType([
            StructField("update_id", StringType(), False),
            StructField("update_timestamp", TimestampType(), False),
            StructField("source_table", StringType(), False),
            StructField("records_processed", IntegerType(), False),
            StructField("index_name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True)
        ])

        # Create empty tracking DataFrame
        empty_tracking_df = spark.createDataFrame([], tracking_schema)

        # Create table if it doesn't exist
        try:
            spark.table(EMBEDDING_TRACKING_TABLE).count()
            print(f"âœ… Tracking table already exists: {EMBEDDING_TRACKING_TABLE}")
        except:
            empty_tracking_df.write.mode("overwrite").saveAsTable(EMBEDDING_TRACKING_TABLE)
            print(f"âœ… Created tracking table: {EMBEDDING_TRACKING_TABLE}")

        # Create incremental update function
        incremental_update_code = f"""
def update_embeddings_incrementally():
    '''Update embeddings for new RCA reports'''
    from datetime import datetime, timedelta
    import uuid

    # Get latest update timestamp
    latest_update = spark.table("{EMBEDDING_TRACKING_TABLE}") \\
        .agg(max("update_timestamp").alias("latest")) \\
        .collect()[0]["latest"]

    if latest_update is None:
        # First run - get all records from last 24 hours
        cutoff_time = datetime.now() - timedelta(hours=24)
    else:
        cutoff_time = latest_update

    # Get new RCA records
    new_records = spark.table("{RCA_TABLE}") \\
        .filter(col("created_timestamp") > lit(cutoff_time))

    new_count = new_records.count()

    if new_count == 0:
        print("âœ… No new records to process")
        return

    print(f"ðŸ”„ Processing {{new_count}} new RCA records...")

    # Process and update indexes
    update_id = str(uuid.uuid4())

    try:
        # Check all indexes (automatic sync with TRIGGERED pipeline)
        indexes_to_check = [
            "network_fault_detection.processed_data.rca_comprehensive_index",
            "network_fault_detection.processed_data.rca_technical_index",
            "network_fault_detection.processed_data.rca_solution_index"
        ]
        for index_name in indexes_to_check:
            try:
                index_info = vs_client.get_index(
                    endpoint_name=VS_ENDPOINT_NAME,
                    index_name=index_name
                )
                print(f"   ðŸ“Š Index {index_name} status checked")
            except Exception as check_error:
                print(f"   âš ï¸ Could not check index {index_name}: {str(check_error)}")

        # Record successful update
        tracking_record = spark.createDataFrame([
            (update_id, datetime.now(), RCA_TABLE, new_count, "all_indexes", "SUCCESS", None)
        ], ["update_id", "update_timestamp", "source_table", "records_processed", "index_name", "status", "error_message"])

        tracking_record.write.mode("append").saveAsTable(EMBEDDINGS_TABLE + "_tracking")

        print(f"âœ… Incremental update completed: {new_count} records processed")

    except Exception as e:
        # Record failed update
        tracking_record = spark.createDataFrame([
            (update_id, datetime.now(), RCA_TABLE, 0, "all_indexes", "FAILED", str(e))
        ], ["update_id", "update_timestamp", "source_table", "records_processed", "index_name", "status", "error_message"])

        tracking_record.write.mode("append").saveAsTable(EMBEDDINGS_TABLE + "_tracking")

        print(f"âŒ Incremental update failed: {str(e)}")
"""

        print("âœ… Incremental update pipeline configured")
        print("ðŸ’¡ Use the generated function to update embeddings for new RCA reports")

        return EMBEDDING_TRACKING_TABLE

    except Exception as e:
        print(f"âŒ Error setting up incremental pipeline: {str(e)}")
        return None

# Setup incremental update pipeline
tracking_table = setup_incremental_update_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary and Configuration

# COMMAND ----------

print("ðŸ“‹ EMBEDDINGS PIPELINE SUMMARY")
print("=" * 70)

pipeline_status = {
    "quality_assessment": quality_assessment is not None,
    "enhanced_content": enhanced_content_df is not None,
    "specialized_indexes": len(created_indexes) > 0,
    "validation_completed": len(created_indexes) > 0,  # If indexes created, validation passed
    "incremental_pipeline": len(created_indexes) > 0   # If indexes created, pipeline is ready
}

print("âœ… Pipeline Components:")
for component, status in pipeline_status.items():
    status_icon = "âœ…" if status else "âŒ"
    print(f"   {status_icon} {component.replace('_', ' ').title()}")

if created_indexes:
    print(f"\nðŸ” Created Vector Indexes ({len(created_indexes)}):")
    for idx in created_indexes:
        print(f"   ðŸ“Š {idx}")

# Overall pipeline readiness
all_ready = all(pipeline_status.values())
print(f"\nðŸŽ¯ Embeddings Pipeline Ready: {'âœ… YES' if all_ready else 'âŒ NO'}")

# Set defaults for potentially undefined variables
if 'tracking_table' not in locals():
    tracking_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.embedding_update_tracking"
if 'validation_results' not in locals():
    validation_results = []

# Export configuration
embeddings_config = {
    "endpoint_name": VS_ENDPOINT_NAME,
    "indexes": created_indexes,
    "source_table": RCA_TABLE,
    "enhanced_table": f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_enhanced_for_embeddings",
    "tracking_table": tracking_table,
    "embedding_model": EMBEDDING_MODEL,
    "validation_results": validation_results,
    "setup_timestamp": str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
    "ready": all_ready
}

print(f"\nðŸ“„ Embeddings configuration ready for RAG interface")
print("ðŸŽ¯ Next step: Build RAG query interface")

# COMMAND ----------

# Show final statistics
if quality_assessment and validation_results:
    print("\nðŸ“Š FINAL STATISTICS")
    print("=" * 50)
    print(f"ðŸ“‹ Total RCA Reports: {quality_assessment['total_records']:,}")
    print(f"ðŸŽ¯ Quality Content: {quality_assessment['substantial_content']:,}")
    print(f"ðŸ” Vector Indexes: {len(created_indexes)}")
    print(f"ðŸ§ª Validation Queries: {len(validation_results)}")

    if validation_results:
        avg_relevance = sum(r['relevance_score'] for r in validation_results) / len(validation_results)
        print(f"ðŸ“ˆ Average Search Relevance: {avg_relevance:.1f}%")

    print("ðŸš€ Ready for intelligent search implementation!")
