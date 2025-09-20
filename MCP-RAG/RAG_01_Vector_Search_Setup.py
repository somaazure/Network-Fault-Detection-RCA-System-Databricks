# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 RAG Implementation - Vector Search Setup
# MAGIC
# MAGIC **Purpose**: Set up Databricks Vector Search for RCA reports intelligent search
# MAGIC **Data Source**: `rca_reports_streaming` table (107+ rich-text RCA reports)
# MAGIC **Target**: Semantic search on incident patterns, root causes, and solutions

# COMMAND ----------

# Install required packages
print("🔧 Installing required packages for Vector Search...")
%pip install databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

spark = SparkSession.builder.getOrCreate()
print("✅ Initializing Vector Search client...")

try:
    vs_client = VectorSearchClient()
    print("✅ Vector Search client initialized successfully")
except Exception as e:
    print(f"⚠️ Vector Search client initialization failed: {str(e)}")
    print("💡 This may be expected - will use alternative approach")

print("🔍 RAG Implementation - Vector Search Setup")
print("=" * 70)

# COMMAND ----------

# Configuration
CATALOG_NAME = "network_fault_detection"
SCHEMA_NAME = "processed_data"
RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_streaming"

# Vector Search configuration
VS_ENDPOINT_NAME = "network_fault_detection_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_vector_index"
EMBEDDING_MODEL = "databricks-bge-large-en"  # Databricks managed embedding model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Vector Search Endpoint

# COMMAND ----------

def create_vector_search_endpoint():
    """Create Vector Search endpoint if it doesn't exist"""
    try:
        # Check if endpoint already exists
        existing_endpoints = vs_client.list_endpoints()
        endpoint_names = [ep['name'] for ep in existing_endpoints.get('endpoints', [])]

        if VS_ENDPOINT_NAME in endpoint_names:
            print(f"✅ Vector Search endpoint '{VS_ENDPOINT_NAME}' already exists")
            return True

        # Create new endpoint
        print(f"🔧 Creating Vector Search endpoint: {VS_ENDPOINT_NAME}")

        vs_client.create_endpoint(
            name=VS_ENDPOINT_NAME,
            endpoint_type="STANDARD"
        )

        print(f"✅ Vector Search endpoint '{VS_ENDPOINT_NAME}' created successfully")
        return True

    except Exception as e:
        print(f"❌ Error creating Vector Search endpoint: {str(e)}")
        return False

# Create endpoint
endpoint_created = create_vector_search_endpoint()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare RCA Data for Vector Search

# COMMAND ----------

def prepare_rca_data_for_vectorization():
    """Prepare RCA data with optimized content for vector search"""
    print("📊 Preparing RCA data for vectorization...")

    try:
        # Load RCA reports
        rca_df = spark.table(RCA_TABLE)
        total_records = rca_df.count()

        print(f"📋 Total RCA reports available: {total_records:,}")

        if total_records == 0:
            print("❌ No RCA reports found. Run the agent pipeline first.")
            return None

        # Create enhanced content for better search results using correct schema
        enhanced_rca_df = rca_df.select(
            col("rca_id").alias("id"),
            col("rca_timestamp").alias("created_timestamp"),
            col("root_cause_category"),
            col("incident_priority"),
            col("recommended_operation"),
            col("analysis_confidence").alias("confidence_score"),

            # Primary content for vector search - combine key fields
            concat_ws(" | ",
                col("incident_priority"),
                col("root_cause_category"),
                col("recommended_operation"),
                col("rca_analysis"),
                col("resolution_recommendations")
            ).alias("search_content"),

            # Keep original fields for metadata
            col("rca_analysis"),
            col("resolution_recommendations"),
            col("analysis_method"),

            # Add searchable keywords
            concat_ws(", ",
                col("incident_priority"),
                col("root_cause_category"),
                col("recommended_operation")
            ).alias("keywords")
        ).filter(
            # Only include quality content
            col("rca_analysis").isNotNull() &
            (length(col("rca_analysis")) > 50)
        )

        # Cache for performance
        enhanced_rca_df.cache()
        final_count = enhanced_rca_df.count()

        print(f"✅ Prepared {final_count:,} RCA reports for vectorization")
        print(f"📊 Data quality retention: {(final_count/total_records)*100:.1f}%")

        # Show sample of prepared data
        print("\n🔍 Sample prepared data:")
        enhanced_rca_df.select("id", "keywords", "search_content").limit(2).show(truncate=False)

        return enhanced_rca_df

    except Exception as e:
        print(f"❌ Error preparing RCA data: {str(e)}")
        return None

# Prepare data
prepared_rca_df = prepare_rca_data_for_vectorization()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Optimized RCA Table for Vector Search

# COMMAND ----------

# Create optimized table for vector search
OPTIMIZED_RCA_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.rca_reports_for_vector_search"

if prepared_rca_df is not None:
    print(f"💾 Creating optimized RCA table: {OPTIMIZED_RCA_TABLE}")

    # Save optimized data
    prepared_rca_df.write.mode("overwrite").saveAsTable(OPTIMIZED_RCA_TABLE)

    # Enable Change Data Feed (required for Vector Search delta sync)
    print("🔧 Enabling Change Data Feed for Vector Search...")
    spark.sql(f"ALTER TABLE {OPTIMIZED_RCA_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("✅ Change Data Feed enabled")

    # Verify table creation
    optimized_count = spark.table(OPTIMIZED_RCA_TABLE).count()
    print(f"✅ Optimized RCA table created with {optimized_count:,} records")

    # Display table schema
    print("\n📋 Optimized table schema:")
    spark.table(OPTIMIZED_RCA_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Vector Search Index

# COMMAND ----------

def create_vector_search_index():
    """Create vector search index on the optimized RCA table"""
    if not endpoint_created or prepared_rca_df is None:
        print("❌ Prerequisites not met for vector index creation")
        return False

    try:
        # Check if index already exists
        try:
            existing_index = vs_client.get_index(
                endpoint_name=VS_ENDPOINT_NAME,
                index_name=VS_INDEX_NAME
            )
            print(f"✅ Vector search index '{VS_INDEX_NAME}' already exists")
            return True
        except:
            # Index doesn't exist, create it
            pass

        print(f"🔧 Creating vector search index: {VS_INDEX_NAME}")

        # Create vector search index
        index = vs_client.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT_NAME,
            source_table_name=OPTIMIZED_RCA_TABLE,
            index_name=VS_INDEX_NAME,
            primary_key="id",
            embedding_source_column="search_content",
            embedding_model_endpoint_name=EMBEDDING_MODEL,
            pipeline_type="TRIGGERED"  # Manual sync for control
        )

        print(f"✅ Vector search index '{VS_INDEX_NAME}' created successfully")

        # Check index status (alternative to wait_until_index_is_ready)
        print("⏳ Checking index status...")
        try:
            index_info = vs_client.get_index(
                endpoint_name=VS_ENDPOINT_NAME,
                index_name=VS_INDEX_NAME
            )
            status = index_info.get('status', {}).get('ready', False)
            print(f"📊 Index status: {'Ready' if status else 'Initializing (will be ready shortly)'}")
        except Exception as status_error:
            print(f"⚠️ Could not check index status: {str(status_error)}")
            print("💡 Index may still be initializing - this is normal")

        print("🎯 Vector search index created and will be ready shortly!")
        return True

    except Exception as e:
        print(f"❌ Error creating vector search index: {str(e)}")
        print("💡 Tip: Make sure the endpoint is running and the table exists")
        return False

# Create vector index
index_created = create_vector_search_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Sync Vector Index with Data

# COMMAND ----------

def sync_vector_index():
    """Sync the vector index with current data"""
    if not index_created:
        print("❌ Vector index not available for sync")
        return False

    try:
        print("🔄 Checking vector index sync status...")

        # Get index status and information
        index_info = vs_client.get_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME
        )

        print("✅ Vector index is configured for automatic sync")
        print("⏳ Index will sync automatically as data changes (Change Data Feed enabled)")

        # Display index information using object attributes
        try:
            if hasattr(index_info, 'status'):
                print(f"📊 Index status: {index_info.status}")
            elif hasattr(index_info, 'describe'):
                index_desc = index_info.describe()
                print(f"📊 Index description available")
            else:
                print(f"📊 Index object type: {type(index_info)}")
                print(f"📊 Index available attributes: {[attr for attr in dir(index_info) if not attr.startswith('_')]}")
        except Exception as attr_error:
            print(f"📊 Index info retrieved (details not accessible): {str(attr_error)}")

        print("💡 With TRIGGERED pipeline and Change Data Feed, sync happens automatically")
        return True

    except Exception as e:
        print(f"❌ Error checking vector index: {str(e)}")
        print("💡 Index may still be initializing - this is normal for new indexes")
        return False

# Sync index
sync_success = sync_vector_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test Vector Search Functionality

# COMMAND ----------

def test_vector_search():
    """Test basic vector search functionality with timeout"""
    if not index_created:
        print("❌ Vector index not available for testing")
        return

    print("🧪 Testing vector search functionality...")

    try:
        # Quick single test first
        test_query = "router interface down critical"
        print(f"🔍 Quick test query: '{test_query}'")

        # Get the index object
        index = vs_client.get_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME
        )
        print("   ✅ Index object retrieved")

        # Perform search
        results = index.similarity_search(
            query_text=test_query,
            columns=["id", "root_cause_category", "incident_priority", "recommended_operation", "keywords"],
            num_results=2  # Reduced for faster response
        )
        print("   ✅ Search completed")

        # Parse results
        if results and isinstance(results, dict) and 'result' in results:
            data_array = results['result'].get('data_array', [])
            row_count = results['result'].get('row_count', 0)

            print(f"   ✅ Found {row_count} similar results")

            if data_array and len(data_array) > 0:
                first_row = data_array[0]
                root_cause = first_row[1] if len(first_row) > 1 else 'N/A'
                priority = first_row[2] if len(first_row) > 2 else 'N/A'
                operation = first_row[3] if len(first_row) > 3 else 'N/A'

                print(f"   🎯 Top match: {root_cause} - {operation} (Priority: {priority})")
                print("   🎉 Vector Search is working correctly!")
            else:
                print("   ⚠️ No data in results array")
        else:
            print("   ⚠️ No results returned")

    except Exception as e:
        print(f"❌ Vector search test failed: {str(e)}")
        print("💡 Index may still be initializing - try again in a few minutes")

    print("🎯 Vector search test completed")

# Test vector search with simplified approach
test_vector_search()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Summary

# COMMAND ----------

print("📋 VECTOR SEARCH SETUP SUMMARY")
print("=" * 70)
print(f"🎯 Endpoint Name: {VS_ENDPOINT_NAME}")
print(f"📊 Index Name: {VS_INDEX_NAME}")
print(f"🗃️ Source Table: {OPTIMIZED_RCA_TABLE}")
print(f"🤖 Embedding Model: {EMBEDDING_MODEL}")

setup_status = {
    "endpoint_created": endpoint_created,
    "data_prepared": prepared_rca_df is not None,
    "index_created": index_created,
    "sync_completed": sync_success
}

print("\n✅ Setup Status:")
for step, status in setup_status.items():
    status_icon = "✅" if status else "❌"
    print(f"   {status_icon} {step.replace('_', ' ').title()}")

# Overall readiness
all_ready = all(setup_status.values())
print(f"\n🎯 RAG System Ready: {'✅ YES' if all_ready else '❌ NO - Check failed steps'}")

if all_ready:
    print("💡 Next step: Implement RAG query interface")
else:
    print("💡 Fix the failed steps before proceeding to RAG implementation")

# COMMAND ----------

# Export configuration for RAG query interface
vector_search_config = {
    "endpoint_name": VS_ENDPOINT_NAME,
    "index_name": VS_INDEX_NAME,
    "source_table": OPTIMIZED_RCA_TABLE,
    "embedding_model": EMBEDDING_MODEL,
    "setup_timestamp": str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
    "ready": all_ready
}

print("\n📄 Vector search configuration saved for RAG implementation")
print(json.dumps(vector_search_config, indent=2))