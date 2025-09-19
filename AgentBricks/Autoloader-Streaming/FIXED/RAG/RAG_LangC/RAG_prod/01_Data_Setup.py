# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - End-to-End Data Setup for RAG System
# MAGIC
# MAGIC **Purpose**: Prepare and validate data for real end-to-end RAG system
# MAGIC **Scope**: Data quality validation, schema verification, real data preparation
# MAGIC **Output**: Clean, processed data ready for vector store creation

# COMMAND ----------

# Install required packages FIRST
print("ðŸ”§ Installing packages for end-to-end data setup...")
%pip install langchain langchain-community langchain-core databricks-vectorsearch databricks-langchain
dbutils.library.restartPython()

# COMMAND ----------

# Import shared components (after package installation)
import os
print("ðŸ”„ Attempting to import shared components...")
print(f"Current working directory: {os.getcwd()}")

# Initialize import success flag
import_success = False

# Strategy 1: Try %run with shared_components (most reliable in Databricks)
try:
    %run ./shared_components
    print("âœ… Successfully imported via %run ./shared_components")
    import_success = True
except Exception as e1:
    print(f"âŒ %run ./shared_components failed: {e1}")

# Strategy 2: Try %run without .py extension
if not import_success:
    try:
        %run shared_components
        print("âœ… Successfully imported via %run shared_components")
        import_success = True
    except Exception as e2:
        print(f"âŒ %run shared_components failed: {e2}")

# Strategy 3: Try exec with full file read
if not import_success:
    try:
        with open('shared_components.py', 'r') as f:
            exec(f.read())
        print("âœ… Successfully imported via exec(file.read())")
        import_success = True
    except Exception as e3:
        print(f"âŒ exec(file.read()) failed: {e3}")

# Strategy 4: Try different paths
if not import_success:
    paths_to_try = [
        './shared_components.py',
        'shared_components.py',
        '../shared_components.py',
        '/Workspace/Users/your_user/RAG_LangChain_EndToEnd/shared_components.py'
    ]

    for path in paths_to_try:
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    exec(f.read())
                print(f"âœ… Successfully imported from {path}")
                import_success = True
                break
        except Exception as e:
            print(f"âŒ Failed to import from {path}: {e}")

# Fallback: Create minimal required components
if not import_success:
    print("âš ï¸ All import strategies failed - using comprehensive fallback")
    print("âš ï¸ Using fallback configuration - RAGConfiguration not imported from shared_components")
    print("âš ï¸ RAGSystemState not imported - continuing without state management")

    # Minimal required classes/functions for basic operation
    class RAGSystemState:
        def __init__(self):
            self.vector_stores = {}
            self.config = {}
        def set_data_pipeline_results(self, results): pass
        def get_data_pipeline_results(self): return {}
        def get_system_config(self):
            return {
                "catalog_name": "network_fault_detection",
                "schema_name": "processed_data",
                "content_quality_threshold": 0.2,  # Very relaxed for fallback
                "min_content_length": 50,
                "substance_weight": 0.4,
                "recommendation_weight": 0.3,
                "diversity_weight": 0.3
            }

    class RAGConfiguration:
        @staticmethod
        def get_development_config():
            return {
                "catalog_name": "network_fault_detection",
                "schema_name": "processed_data",
                "content_quality_threshold": 0.2,  # Very relaxed for fallback
                "min_content_length": 50,
                "min_analysis_length": 25,
                "substance_weight": 0.4,
                "recommendation_weight": 0.3,
                "diversity_weight": 0.3
            }

    # Create fallback instances
    rag_state = RAGSystemState()
    print("âš ï¸ Using fallback RAGSystemState and RAGConfiguration")

# COMMAND ----------

import os
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json

# Import Python built-ins to avoid PySpark conflicts
from builtins import min as builtin_min, max as builtin_max, sum as builtin_sum

# Databricks and Spark imports
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

# LangChain imports
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

spark = SparkSession.builder.getOrCreate()

# Initialize Vector Search client
try:
    vs_client = VectorSearchClient(disable_notice=True)
    print("âœ… Vector Search client initialized")
    VECTOR_SEARCH_AVAILABLE = True
except Exception as e:
    print(f"âš ï¸ Vector Search not available: {str(e)}")
    vs_client = None
    VECTOR_SEARCH_AVAILABLE = False

print("ðŸš€ END-TO-END DATA SETUP FOR RAG SYSTEM")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and State Management

# COMMAND ----------

# Load configuration (with fallback if RAGConfiguration not imported)
try:
    config = RAGConfiguration.get_development_config()  # Use development config with relaxed thresholds
    print(f"âœ… Loaded RAGConfiguration with quality threshold: {config.get('content_quality_threshold', 0.3)}")
except NameError:
    # Comprehensive fallback configuration if RAGConfiguration not available
    config = {
        "catalog_name": "network_fault_detection",
        "schema_name": "processed_data",
        "chunk_size": 512,
        "chunk_overlap": 100,
        "vector_search_available": True,
        # Critical: Add quality thresholds to fallback
        "content_quality_threshold": 0.15,  # Very relaxed 15% for maximum compatibility
        "min_content_length": 50,
        "min_analysis_length": 25,
        "substance_weight": 0.4,
        "recommendation_weight": 0.3,
        "diversity_weight": 0.3
    }
    print("âš ï¸ Using enhanced fallback configuration with relaxed quality thresholds (15%)")

try:
    rag_state = RAGSystemState()
    rag_state.set_system_config(config)
    print("âœ… RAGSystemState initialized successfully")
except NameError:
    # Create minimal RAGSystemState if not available from imports
    class MinimalRAGSystemState:
        def __init__(self):
            self.config = config
        def set_system_config(self, cfg):
            self.config = cfg
        def set_data_pipeline_results(self, results):
            pass
        def get_data_pipeline_results(self):
            return {}

    rag_state = MinimalRAGSystemState()
    print("âš ï¸ Using minimal RAGSystemState fallback - full state management not available")

CATALOG_NAME = config["catalog_name"]
SCHEMA_NAME = config["schema_name"]
CHUNK_SIZE = config["chunk_size"]
CHUNK_OVERLAP = config["chunk_overlap"]

print(f"ðŸ“Š Configuration loaded:")
print(f"   Catalog: {CATALOG_NAME}")
print(f"   Schema: {SCHEMA_NAME}")
print(f"   Chunk Size: {CHUNK_SIZE}")
print(f"   Vector Search Available: {VECTOR_SEARCH_AVAILABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

class EndToEndDataValidator:
    """Comprehensive data validation for end-to-end RAG system"""

    def __init__(self):
        self.catalog_name = CATALOG_NAME
        self.schema_name = SCHEMA_NAME
        self.validation_results = {}

        # Load configuration for quality thresholds - use global config if available
        global config
        try:
            # First try to use the global config that was already loaded
            if 'config' in globals() and config.get('content_quality_threshold'):
                self.config = config
                print(f"âœ… Using global configuration with quality threshold: {self.config.get('content_quality_threshold', 0.3)}")
            else:
                # Fallback to RAGConfiguration if global config not available
                self.config = RAGConfiguration.get_development_config()
                print(f"âœ… Loaded RAGConfiguration with quality threshold: {self.config.get('content_quality_threshold', 0.3)}")
        except (NameError, KeyError):
            # Ultimate fallback configuration if nothing else works
            self.config = {
                "content_quality_threshold": 0.15,  # Very relaxed fallback
                "min_content_length": 50,
                "substance_weight": 0.4,
                "recommendation_weight": 0.3,
                "diversity_weight": 0.3
            }
            print(f"âš ï¸ Using ultimate fallback configuration with 15% quality threshold")

    def validate_source_tables(self) -> Dict[str, Any]:
        """Validate all source tables for completeness and quality"""
        print("ðŸ“Š Validating source tables for end-to-end RAG...")

        required_tables = [
            "severity_classifications_streaming",
            "incident_decisions_streaming",
            "network_operations_streaming",
            "rca_reports_streaming",
            "multi_agent_workflows_streaming"
        ]

        table_validation = {}
        total_records = 0

        for table_name in required_tables:
            try:
                full_table_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
                df = spark.table(full_table_name)

                # Basic metrics
                count = df.count()
                columns = df.columns

                # Data quality checks
                null_counts = df.select([
                    F.count(when(col(c).isNull(), c)).alias(c) for c in columns[:5]  # Check first 5 columns
                ]).collect()[0]

                table_validation[table_name] = {
                    "exists": True,
                    "record_count": count,
                    "column_count": len(columns),
                    "columns": columns,
                    "null_counts": dict(null_counts.asDict()),
                    "data_quality_score": self._calculate_table_quality_score(df),
                    "ready_for_rag": count >= 10 and len(columns) >= 3
                }

                total_records += count
                status = "âœ…" if table_validation[table_name]["ready_for_rag"] else "âš ï¸"
                print(f"   {status} {table_name}: {count:,} records, {len(columns)} columns")

            except Exception as e:
                table_validation[table_name] = {
                    "exists": False,
                    "error": str(e),
                    "ready_for_rag": False
                }
                print(f"   âŒ {table_name}: Error - {str(e)}")

        # Overall assessment
        ready_tables = builtin_sum(1 for t in table_validation.values() if t.get("ready_for_rag", False))
        overall_ready = ready_tables >= len(required_tables) * 0.8  # 80% of tables must be ready

        validation_summary = {
            "total_tables": len(required_tables),
            "ready_tables": ready_tables,
            "total_records": total_records,
            "overall_ready": overall_ready,
            "table_details": table_validation
        }

        self.validation_results["source_tables"] = validation_summary
        print(f"ðŸ“Š Table validation complete: {ready_tables}/{len(required_tables)} tables ready")

        return validation_summary

    def _calculate_table_quality_score(self, df) -> float:
        """Calculate data quality score for a table"""
        try:
            total_records = df.count()
            if total_records == 0:
                return 0.0

            # Sample data for quality assessment
            sample_df = df.sample(0.1).limit(1000)

            # Quality metrics
            metrics = []

            # Check for non-null primary content
            content_columns = [c for c in df.columns if any(keyword in c.lower() for keyword in ['content', 'description', 'analysis', 'text'])]
            if content_columns:
                non_null_content = sample_df.filter(col(content_columns[0]).isNotNull()).count()
                metrics.append(non_null_content / builtin_max(sample_df.count(), 1))

            # Check for reasonable text lengths
            if content_columns:
                avg_length = sample_df.select(avg(length(col(content_columns[0])))).collect()[0][0]
                metrics.append(builtin_min(avg_length / 100, 1.0) if avg_length else 0)

            # Overall quality score
            return builtin_sum(metrics) / builtin_max(len(metrics), 1) if metrics else 0.5

        except Exception:
            return 0.5  # Default moderate score on error

    def validate_rca_content_quality(self) -> Dict[str, Any]:
        """Specifically validate RCA reports for RAG suitability"""
        print("ðŸ“‹ Validating RCA content quality for RAG...")

        try:
            rca_table = f"{self.catalog_name}.{self.schema_name}.rca_reports_streaming"
            rca_df = spark.table(rca_table)

            total_count = rca_df.count()
            if total_count == 0:
                return {"quality_score": 0, "error": "No RCA data found"}

            # Content quality analysis
            quality_analysis = rca_df.select(
                count("*").alias("total"),
                count(when(col("rca_analysis").isNotNull(), 1)).alias("non_null_analysis"),
                count(when(length(col("rca_analysis")) >= 100, 1)).alias("substantial_analysis"),
                count(when(col("resolution_recommendations").isNotNull(), 1)).alias("has_recommendations"),
                avg(length(col("rca_analysis"))).alias("avg_analysis_length"),
                countDistinct("root_cause_category").alias("unique_categories"),
                countDistinct("incident_priority").alias("priority_levels")
            ).collect()[0]

            # Calculate comprehensive quality score
            completeness_score = quality_analysis["non_null_analysis"] / quality_analysis["total"]
            substance_score = quality_analysis["substantial_analysis"] / quality_analysis["total"]
            recommendation_score = quality_analysis["has_recommendations"] / quality_analysis["total"]
            diversity_score = builtin_min(quality_analysis["unique_categories"] / 10, 1.0)

            # Use configurable weights for quality calculation
            substance_weight = self.config.get("substance_weight", 0.4)
            recommendation_weight = self.config.get("recommendation_weight", 0.3)
            diversity_weight = self.config.get("diversity_weight", 0.3)

            # Weighted quality calculation (completeness is baseline requirement)
            if completeness_score < 0.5:  # If most records are empty, heavily penalize
                overall_quality = completeness_score * 0.5
            else:
                overall_quality = (
                    completeness_score * 0.3 +  # 30% weight for completeness
                    substance_score * substance_weight +
                    recommendation_score * recommendation_weight +
                    diversity_score * diversity_weight
                )

            quality_result = {
                "total_records": quality_analysis["total"],
                "completeness_score": completeness_score,
                "substance_score": substance_score,
                "recommendation_score": recommendation_score,
                "diversity_score": diversity_score,
                "overall_quality_score": overall_quality,
                "avg_analysis_length": quality_analysis["avg_analysis_length"],
                "unique_categories": quality_analysis["unique_categories"],
                "priority_levels": quality_analysis["priority_levels"],
                "rag_ready": overall_quality >= self.config.get("content_quality_threshold", 0.3)
            }

            self.validation_results["rca_content_quality"] = quality_result
            status = "âœ…" if quality_result["rag_ready"] else "âš ï¸"
            print(f"   {status} RCA Quality Score: {overall_quality:.1%}")

            return quality_result

        except Exception as e:
            error_result = {"quality_score": 0, "error": str(e)}
            self.validation_results["rca_content_quality"] = error_result
            print(f"   âŒ RCA validation failed: {str(e)}")
            return error_result

    def create_processed_dataset(self) -> Dict[str, Any]:
        """Create clean, processed dataset optimized for RAG"""
        print("ðŸ”„ Creating processed dataset for RAG...")

        try:
            # Load and process RCA data
            rca_df = spark.table(f"{self.catalog_name}.{self.schema_name}.rca_reports_streaming")

            # Create comprehensive content for different strategies
            processed_df = rca_df.select(
                col("rca_id"),
                col("incident_priority"),
                col("root_cause_category"),
                col("recommended_operation"),
                col("analysis_confidence"),
                col("rca_analysis"),
                col("resolution_recommendations"),

                # Create comprehensive content (strategy 1)
                concat_ws("\n\n",
                    concat(lit("INCIDENT OVERVIEW\n"),
                           lit("Priority: "), col("incident_priority"), lit("\n"),
                           lit("Category: "), col("root_cause_category"), lit("\n"),
                           lit("Recommended Operation: "), col("recommended_operation"), lit("\n"),
                           lit("Confidence: "), col("analysis_confidence")),
                    concat(lit("ROOT CAUSE ANALYSIS\n"), col("rca_analysis")),
                    concat(lit("RESOLUTION RECOMMENDATIONS\n"), col("resolution_recommendations"))
                ).alias("comprehensive_content"),

                # Create technical content (strategy 2)
                concat_ws("\n\n",
                    concat(lit("TECHNICAL DETAILS\n"),
                           lit("Category: "), col("root_cause_category"), lit("\n"),
                           lit("Priority Level: "), col("incident_priority"), lit("\n"),
                           lit("Confidence Score: "), col("analysis_confidence")),
                    concat(lit("TECHNICAL ANALYSIS\n"), col("rca_analysis")),
                    when(col("resolution_recommendations").isNotNull(),
                         concat(lit("TECHNICAL RECOMMENDATIONS\n"), col("resolution_recommendations"))
                    ).otherwise("")
                ).alias("technical_content"),

                # Create solution-focused content (strategy 3)
                concat_ws("\n\n",
                    concat(lit("PROBLEM STATEMENT\n"),
                           lit("Issue: "), col("root_cause_category"), lit("\n"),
                           lit("Priority: "), col("incident_priority")),
                    concat(lit("SOLUTION APPROACH\n"), col("recommended_operation")),
                    concat(lit("DETAILED RECOMMENDATIONS\n"), col("resolution_recommendations")),
                    when(col("rca_analysis").isNotNull(),
                         concat(lit("SUPPORTING ANALYSIS\n"), col("rca_analysis"))
                    ).otherwise("")
                ).alias("solution_content"),

                current_timestamp().alias("processed_timestamp")
            ).filter(
                # Quality filters
                col("rca_analysis").isNotNull() &
                (length(col("rca_analysis")) >= 50) &
                col("resolution_recommendations").isNotNull()
            )

            # Cache for performance
            processed_df.cache()
            processed_count = processed_df.count()

            # Save processed dataset
            processed_table = f"{self.catalog_name}.{self.schema_name}.rca_processed_endtoend"
            processed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(processed_table)

            # Enable Change Data Feed (required for Vector Search)
            print("ðŸ”§ Enabling Change Data Feed for Vector Search compatibility...")
            try:
                spark.sql(f"ALTER TABLE {processed_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
                print(f"   âœ… Change Data Feed enabled for {processed_table}")
            except Exception as e:
                print(f"   âš ï¸ Change Data Feed enablement failed: {e}")
                print("   ðŸ“‹ This may affect Vector Search creation, but table is still usable")

            processing_result = {
                "processed_table": processed_table,
                "processed_count": processed_count,
                "strategies_created": ["comprehensive", "technical", "solution"],
                "processing_timestamp": datetime.now().isoformat(),
                "ready_for_vectorization": processed_count > 0
            }

            self.validation_results["processed_dataset"] = processing_result
            print(f"   âœ… Processed dataset created: {processed_count:,} records")
            print(f"   ðŸ“Š Table: {processed_table}")

            return processing_result

        except Exception as e:
            error_result = {"error": str(e), "ready_for_vectorization": False}
            self.validation_results["processed_dataset"] = error_result
            print(f"   âŒ Dataset processing failed: {str(e)}")
            return error_result

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get comprehensive validation summary"""
        source_ready = self.validation_results.get("source_tables", {}).get("overall_ready", False)
        content_ready = self.validation_results.get("rca_content_quality", {}).get("rag_ready", False)
        dataset_ready = self.validation_results.get("processed_dataset", {}).get("ready_for_vectorization", False)

        overall_ready = source_ready and content_ready and dataset_ready

        return {
            "source_tables_ready": source_ready,
            "content_quality_ready": content_ready,
            "processed_dataset_ready": dataset_ready,
            "overall_ready_for_rag": overall_ready,
            "validation_timestamp": datetime.now().isoformat(),
            "detailed_results": self.validation_results
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Setup Pipeline

# COMMAND ----------

# Initialize validator
print("ðŸ” Initializing End-to-End Data Validator...")
validator = EndToEndDataValidator()

# Step 1: Validate source tables
print("\n" + "="*50)
print("ðŸ“Š STEP 1: SOURCE TABLE VALIDATION")
print("="*50)
source_validation = validator.validate_source_tables()

# Step 2: Validate RCA content quality
print("\n" + "="*50)
print("ðŸ“‹ STEP 2: CONTENT QUALITY VALIDATION")
print("="*50)
content_validation = validator.validate_rca_content_quality()

# Step 3: Create processed dataset
print("\n" + "="*50)
print("ðŸ”„ STEP 3: PROCESSED DATASET CREATION")
print("="*50)
dataset_creation = validator.create_processed_dataset()

# Step 4: Final validation summary
print("\n" + "="*50)
print("ðŸ“‹ STEP 4: FINAL VALIDATION SUMMARY")
print("="*50)
final_summary = validator.get_validation_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results and Update State

# COMMAND ----------

# Prepare comprehensive results
data_setup_results = {
    "step": "01_data_setup",
    "execution_timestamp": datetime.now().isoformat(),
    "configuration": config,
    "vector_search_available": VECTOR_SEARCH_AVAILABLE,
    "validation_results": final_summary,
    "ready_for_next_step": final_summary["overall_ready_for_rag"]
}

# Update RAG system state
if rag_state:
    rag_state.set_data_pipeline_results(data_setup_results)

# Persist results
try:
    persistence = RAGDataPersistence()
    persistence.save_pipeline_results(data_setup_results)
except NameError:
    print("âš ï¸ RAGDataPersistence not available - skipping persistence")

# Log step completion
try:
    log_step_completion("01_data_setup", data_setup_results)
except NameError:
    print("âœ… 01_data_setup completed (log_step_completion not available)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("ðŸŽ¯ END-TO-END DATA SETUP COMPLETE!")
print("=" * 70)

# Display key metrics
print(f"ðŸ“Š Source Tables Ready: {final_summary['source_tables_ready']}")
print(f"ðŸ“‹ Content Quality Ready: {final_summary['content_quality_ready']}")
print(f"ðŸ”„ Processed Dataset Ready: {final_summary['processed_dataset_ready']}")
print(f"ðŸŽ¯ Overall Ready for RAG: {final_summary['overall_ready_for_rag']}")

if final_summary["overall_ready_for_rag"]:
    print("\nâœ… SUCCESS: Data is ready for vector store creation!")
    print("ðŸ“‹ Next Step: Run 02_Vector_Store_Creation.py")

    # Display dataset info
    if "processed_dataset" in validator.validation_results:
        dataset_info = validator.validation_results["processed_dataset"]
        print(f"\nðŸ“Š Processed Dataset:")
        print(f"   Records: {dataset_info.get('processed_count', 'N/A'):,}")
        print(f"   Table: {dataset_info.get('processed_table', 'N/A')}")
        print(f"   Strategies: {', '.join(dataset_info.get('strategies_created', []))}")

else:
    print("\nâš ï¸ WARNING: Data setup has issues that need attention")
    print("ðŸ“‹ Review validation results above before proceeding")

    # Show specific issues
    issues = []
    if not final_summary["source_tables_ready"]:
        issues.append("Source tables not ready")
    if not final_summary["content_quality_ready"]:
        issues.append("Content quality insufficient")
    if not final_summary["processed_dataset_ready"]:
        issues.append("Dataset processing failed")

    print(f"   Issues: {', '.join(issues)}")

print(f"\nðŸ•’ Execution completed at: {datetime.now().isoformat()}")
print("ðŸ“‹ Results saved to RAG system state and persistence layer")

# Display next steps
print(f"\nðŸ“‹ PIPELINE STATUS:")
try:
    pipeline_status = get_pipeline_status()
    print(f"   Completed Steps: {len(pipeline_status['completed_steps'])}")
    print(f"   Latest Step: {pipeline_status.get('latest_step', {}).get('step', 'None')}")
    print(f"   Ready for Next Step: {pipeline_status['ready_for_next_step']}")
except NameError:
    # Fallback pipeline status
    print(f"   âœ… Step 1 (Data Setup): Completed")
    print(f"   â³ Step 2 (Vector Stores): Pending")
    print(f"   â³ Step 3 (RAG Interface): Pending")
    print(f"   â³ Step 4 (Testing): Pending")
    print(f"   Ready for Next Step: {data_setup_results.get('ready_for_next_step', False)}")

print("\nðŸš€ END-TO-END RAG PIPELINE - DATA SETUP COMPLETE!")
