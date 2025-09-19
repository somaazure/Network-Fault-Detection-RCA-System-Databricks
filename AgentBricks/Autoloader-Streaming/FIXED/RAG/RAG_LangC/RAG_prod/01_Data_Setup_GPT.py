# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - End-to-End Data Setup for RAG System
# MAGIC
# MAGIC **Purpose**: Prepare and validate data for real end-to-end RAG system
# MAGIC **Scope**: Data quality validation, schema verification, real data preparation
# MAGIC **Output**: Clean, processed data ready for vector store creation

# COMMAND ----------

# Install required packages
print("ðŸ”§ Installing packages for end-to-end data setup...")
%pip install langchain langchain-community langchain-core databricks-vectorsearch databricks-langchain
dbutils.library.restartPython()

# COMMAND ----------

import os

# Try importing shared components (Databricks first, then local Python fallback)
import_success = False

try:
    %run ./shared_components
    import_success = True
    print("âœ… Imported shared_components via %run (Databricks notebook)")
except Exception as e:
    print(f"âš ï¸ %run ./shared_components failed: {e}")

if not import_success:
    try:
        from shared_components import (
            RAGSystemState,
            RAGConfiguration,
            RAGDataPersistence,
            log_step_completion,
            get_pipeline_status,
        )
        import_success = True
        print("âœ… Imported shared_components as Python module (local mode)")
    except Exception as e:
        print(f"âŒ Python import of shared_components failed: {e}")

if not import_success:
    raise ImportError(
        "âŒ Could not import shared_components. Ensure this notebook is in the same folder as 'shared_components'."
    )

# COMMAND ----------

from typing import List, Dict, Any
from datetime import datetime
import json

# Python built-ins (avoid PySpark conflicts)
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

# Load configuration
config = RAGConfiguration.get_development_config()
print(f"âœ… Loaded RAGConfiguration with quality threshold: {config.get('content_quality_threshold', 0.3)}")

# Initialize state manager
rag_state = RAGSystemState()
rag_state.set_system_config(config)
print("âœ… RAGSystemState initialized successfully")

CATALOG_NAME = config["catalog_name"]
SCHEMA_NAME = config["schema_name"]
CHUNK_SIZE = config["chunk_size"]
CHUNK_OVERLAP = config["chunk_overlap"]

print("ðŸ“Š Configuration loaded:")
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

        # Load configuration
        global config
        self.config = config
        print(f"âœ… Using configuration with quality threshold: {self.config.get('content_quality_threshold', 0.3)}")

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

                count = df.count()
                columns = df.columns

                null_counts = df.select([
                    F.count(when(col(c).isNull(), c)).alias(c) for c in columns[:5]
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

        ready_tables = builtin_sum(1 for t in table_validation.values() if t.get("ready_for_rag", False))
        overall_ready = ready_tables >= len(required_tables) * 0.8

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
        try:
            total_records = df.count()
            if total_records == 0:
                return 0.0

            sample_df = df.sample(0.1).limit(1000)
            metrics = []

            content_columns = [c for c in df.columns if any(keyword in c.lower() for keyword in ['content', 'description', 'analysis', 'text'])]
            if content_columns:
                non_null_content = sample_df.filter(col(content_columns[0]).isNotNull()).count()
                metrics.append(non_null_content / builtin_max(sample_df.count(), 1))

                avg_length = sample_df.select(avg(length(col(content_columns[0])))).collect()[0][0]
                metrics.append(builtin_min(avg_length / 100, 1.0) if avg_length else 0)

            return builtin_sum(metrics) / builtin_max(len(metrics), 1) if metrics else 0.5
        except Exception:
            return 0.5

    def validate_rca_content_quality(self) -> Dict[str, Any]:
        print("ðŸ“‹ Validating RCA content quality for RAG...")

        try:
            rca_table = f"{self.catalog_name}.{self.schema_name}.rca_reports_streaming"
            rca_df = spark.table(rca_table)

            total_count = rca_df.count()
            if total_count == 0:
                return {"quality_score": 0, "error": "No RCA data found"}

            quality_analysis = rca_df.select(
                count("*").alias("total"),
                count(when(col("rca_analysis").isNotNull(), 1)).alias("non_null_analysis"),
                count(when(length(col("rca_analysis")) >= 100, 1)).alias("substantial_analysis"),
                count(when(col("resolution_recommendations").isNotNull(), 1)).alias("has_recommendations"),
                avg(length(col("rca_analysis"))).alias("avg_analysis_length"),
                countDistinct("root_cause_category").alias("unique_categories"),
                countDistinct("incident_priority").alias("priority_levels")
            ).collect()[0]

            completeness_score = quality_analysis["non_null_analysis"] / quality_analysis["total"]
            substance_score = quality_analysis["substantial_analysis"] / quality_analysis["total"]
            recommendation_score = quality_analysis["has_recommendations"] / quality_analysis["total"]
            diversity_score = builtin_min(quality_analysis["unique_categories"] / 10, 1.0)

            substance_weight = self.config.get("substance_weight", 0.4)
            recommendation_weight = self.config.get("recommendation_weight", 0.3)
            diversity_weight = self.config.get("diversity_weight", 0.3)

            if completeness_score < 0.5:
                overall_quality = completeness_score * 0.5
            else:
                overall_quality = (
                    completeness_score * 0.3 +
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
        print("ðŸ”„ Creating processed dataset for RAG...")

        try:
            rca_df = spark.table(f"{self.catalog_name}.{self.schema_name}.rca_reports_streaming")

            processed_df = rca_df.select(
                col("rca_id"),
                col("incident_priority"),
                col("root_cause_category"),
                col("recommended_operation"),
                col("analysis_confidence"),
                col("rca_analysis"),
                col("resolution_recommendations"),

                concat_ws("\n\n",
                    concat(lit("INCIDENT OVERVIEW\n"),
                           lit("Priority: "), col("incident_priority"), lit("\n"),
                           lit("Category: "), col("root_cause_category"), lit("\n"),
                           lit("Recommended Operation: "), col("recommended_operation"), lit("\n"),
                           lit("Confidence: "), col("analysis_confidence")),
                    concat(lit("ROOT CAUSE ANALYSIS\n"), col("rca_analysis")),
                    concat(lit("RESOLUTION RECOMMENDATIONS\n"), col("resolution_recommendations"))
                ).alias("comprehensive_content"),

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
                col("rca_analysis").isNotNull() &
                (length(col("rca_analysis")) >= 50) &
                col("resolution_recommendations").isNotNull()
            )

            processed_df.cache()
            processed_count = processed_df.count()

            processed_table = f"{self.catalog_name}.{self.schema_name}.rca_processed_endtoend"
            processed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(processed_table)

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

print("ðŸ” Initializing End-to-End Data Validator...")
validator = EndToEndDataValidator()

print("\n" + "="*50)
print("ðŸ“Š STEP 1: SOURCE TABLE VALIDATION")
print("="*50)
source_validation = validator.validate_source_tables()

print("\n" + "="*50)
print("ðŸ“‹ STEP 2: CONTENT QUALITY VALIDATION")
print("="*50)
content_validation = validator.validate_rca_content_quality()

print("\n" + "="*50)
print("ðŸ”„ STEP 3: PROCESSED DATASET CREATION")
print("="*50)
dataset_creation = validator.create_processed_dataset()

print("\n" + "="*50)
print("ðŸ“‹ STEP 4: FINAL VALIDATION SUMMARY")
print("="*50)
final_summary = validator.get_validation_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results and Update State

# COMMAND ----------

data_setup_results = {
    "step": "01_data_setup",
    "execution_timestamp": datetime.now().isoformat(),
    "configuration": config,
    "vector_search_available": VECTOR_SEARCH_AVAILABLE,
    "validation_results": final_summary,
    "ready_for_next_step": final_summary["overall_ready_for_rag"]
}

if rag_state:
    rag_state.set_data_pipeline_results(data_setup_results)

try:
    persistence = RAGDataPersistence()
    persistence.save_pipeline_results(data_setup_results)
except Exception:
    print("âš ï¸ RAGDataPersistence not available - skipping persistence")

try:
    log_step_completion("01_data_setup", data_setup_results)
except Exception:
    print("âœ… 01_data_setup completed (log_step_completion not available)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("ðŸŽ¯ END-TO-END DATA SETUP COMPLETE!")
print("=" * 70)

print(f"ðŸ“Š Source Tables Ready: {final_summary['source_tables_ready']}")
print(f"ðŸ“‹ Content Quality Ready: {final_summary['content_quality_ready']}")
print(f"ðŸ”„ Processed Dataset Ready: {final_summary['processed_dataset_ready']}")
print(f"ðŸŽ¯ Overall Ready for RAG: {final_summary['overall_ready_for_rag']}")

if final_summary["overall_ready_for_rag"]:
    print("\nâœ… SUCCESS: Data is ready for vector store creation!")
    print("ðŸ“‹ Next Step: Run 02_Vector_Store_Creation.py")

    dataset_info = validator.validation_results.get("processed_dataset", {})
    print(f"\nðŸ“Š Processed Dataset:")
    print(f"   Records: {dataset_info.get('processed_count', 'N/A'):,}")
    print(f"   Table: {dataset_info.get('processed_table', 'N/A')}")
    print(f"   Strategies: {', '.join(dataset_info.get('strategies_created', []))}")

else:
    print("\nâš ï¸ WARNING: Data setup has issues that need attention")
    print("ðŸ“‹ Review validation results above before proceeding")

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

print(f"\nðŸ“‹ PIPELINE STATUS:")
try:
    pipeline_status = get_pipeline_status()
    print(f"   Completed Steps: {len(pipeline_status['completed_steps'])}")
    print(f"   Latest Step: {pipeline_status.get('latest_step', {}).get('step', 'None')}")
    print(f"   Ready for Next Step: {pipeline
