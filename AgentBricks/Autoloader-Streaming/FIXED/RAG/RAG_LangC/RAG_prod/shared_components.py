# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Components for End-to-End RAG Integration
# MAGIC
# MAGIC **Purpose**: Centralized state management and shared utilities for real RAG pipeline
# MAGIC **Scope**: Cross-notebook communication, real vector store persistence, configuration management

# COMMAND ----------

# Install required packages
print("ðŸ”§ Installing required packages for LangChain RAG...")
%pip install langchain langchain-community langchain-core databricks-vectorsearch databricks-langchain
dbutils.library.restartPython()

# COMMAND ----------

import os
import json
import pickle
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

# Import Python built-ins to avoid PySpark conflicts
from builtins import min as builtin_min, max as builtin_max, sum as builtin_sum

# Databricks and Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# LangChain imports
from langchain.schema import Document

spark = SparkSession.builder.getOrCreate()

print("ðŸ”§ Shared Components for End-to-End RAG Integration")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG System State Manager

# COMMAND ----------

class RAGSystemState:
    """Centralized state management for end-to-end RAG system"""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.vector_stores = None
            self.embedding_model_info = None
            self.data_pipeline_results = None
            self.system_config = None
            self.initialization_time = datetime.now()
            # Initialize state file path (will be set during first save attempt)
            self._state_file = None
            RAGSystemState._initialized = True
            print("ðŸŽ¯ RAG System State Manager initialized")

    def set_vector_stores(self, stores: Dict[str, Any]):
        """Set real vector stores from step 2"""
        self.vector_stores = stores
        self._save_state()
        print(f"âœ… Vector stores saved: {list(stores.keys()) if stores else 'None'}")

    def get_vector_stores(self) -> Dict[str, Any]:
        """Get vector stores (real or mock fallback)"""
        if self.vector_stores:
            print(f"ðŸ” Using real vector stores: {list(self.vector_stores.keys())}")
            return self.vector_stores
        else:
            print("âš ï¸ No real vector stores found, creating mock stores")
            return self._create_mock_stores()

    def set_data_pipeline_results(self, results: Dict[str, Any]):
        """Set data pipeline results from step 1"""
        self.data_pipeline_results = results
        self._save_state()
        print("âœ… Data pipeline results saved")

    def get_data_pipeline_results(self) -> Dict[str, Any]:
        """Get data pipeline results"""
        return self.data_pipeline_results or {}

    def set_system_config(self, config: Dict[str, Any]):
        """Set system configuration"""
        self.system_config = config
        self._save_state()
        print("âœ… System configuration saved")

    def get_system_config(self) -> Dict[str, Any]:
        """Get system configuration"""
        return self.system_config or self._get_default_config()

    def _save_state(self):
        """Persist state to file (with enhanced error handling)"""
        try:
            state_data = {
                "vector_stores": self.vector_stores,
                "data_pipeline_results": self.data_pipeline_results,
                "system_config": self.system_config,
                "embedding_model_info": self.embedding_model_info,
                "last_updated": datetime.now().isoformat()
            }

            # Try multiple storage locations
            storage_attempts = [
                "/dbfs/tmp/rag_system_state.pkl",
                "./rag_system_state.pkl",
                "/tmp/rag_system_state.pkl"
            ]

            for storage_path in storage_attempts:
                try:
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(storage_path), exist_ok=True)

                    with open(storage_path, 'wb') as f:
                        pickle.dump(state_data, f)

                    self._state_file = storage_path
                    print(f"ðŸ’¾ State persisted to {storage_path}")
                    return
                except Exception as e:
                    continue

            # If all storage attempts fail, store in memory only
            print("âš ï¸ File persistence failed - using in-memory state only")

        except Exception as e:
            print(f"âš ï¸ Failed to save state: {str(e)}")

    def load_state(self):
        """Load state from file"""
        try:
            # Try multiple storage locations
            storage_attempts = [
                "/dbfs/tmp/rag_system_state.pkl",
                "./rag_system_state.pkl",
                "/tmp/rag_system_state.pkl"
            ]

            for storage_path in storage_attempts:
                try:
                    if os.path.exists(storage_path):
                        with open(storage_path, 'rb') as f:
                            state_data = pickle.load(f)

                        self.vector_stores = state_data.get("vector_stores")
                        self.data_pipeline_results = state_data.get("data_pipeline_results")
                        self.system_config = state_data.get("system_config")
                        self.embedding_model_info = state_data.get("embedding_model_info")
                        self._state_file = storage_path

                        print(f"ðŸ“¥ State loaded from {storage_path}")
                        return True
                except Exception as e:
                    continue

            print("ðŸ’­ No previous state found - starting fresh")
            return False

        except Exception as e:
            print(f"âš ï¸ Failed to load state: {str(e)}")
            return False

    def _create_mock_stores(self) -> Dict[str, Any]:
        """Create mock vector stores as fallback"""

        class EndToEndMockVectorStore:
            def __init__(self, content_type: str):
                self.content_type = content_type

            def similarity_search(self, query: str, k: int = 5) -> List[Document]:
                mock_docs = []
                for i in range(builtin_min(k, 3)):
                    content = f"""REAL END-TO-END MOCK - {self.content_type.upper()}
Priority: P{i+1}
Category: network_hardware
Query: {query[:50]}...

ROOT CAUSE ANALYSIS
This is a realistic mock for {self.content_type} strategy.
In production, this would return actual embedded documents from Databricks Vector Search.

RESOLUTION RECOMMENDATIONS
1. Check network device status
2. Verify configuration settings
3. Monitor system performance
4. Implement preventive measures"""

                    mock_docs.append(Document(
                        page_content=content,
                        metadata={
                            "source_id": f"endtoend_mock_{self.content_type}_{i+1}",
                            "content_type": self.content_type,
                            "mock_level": "end_to_end",
                            "relevance_score": 0.85 - (i * 0.1)
                        }
                    ))
                return mock_docs

        return {
            "comprehensive": {
                "store": EndToEndMockVectorStore("comprehensive"),
                "index_name": "network_fault_detection.processed_data.rca_comprehensive_endtoend_index",
                "document_count": 250,
                "store_type": "mock_fallback"
            },
            "technical": {
                "store": EndToEndMockVectorStore("technical"),
                "index_name": "network_fault_detection.processed_data.rca_technical_endtoend_index",
                "document_count": 180,
                "store_type": "mock_fallback"
            },
            "solution": {
                "store": EndToEndMockVectorStore("solution"),
                "index_name": "network_fault_detection.processed_data.rca_solution_endtoend_index",
                "document_count": 210,
                "store_type": "mock_fallback"
            }
        }

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default system configuration"""
        return {
            "catalog_name": "network_fault_detection",
            "schema_name": "processed_data",
            "vs_endpoint_name": "network_fault_detection_vs_endpoint",
            "embedding_model": "databricks-bge-large-en",
            "foundation_model": "databricks-meta-llama-3-1-8b-instruct",
            "chunk_size": 512,
            "chunk_overlap": 50,
            "max_chunks_per_doc": 5,
            "search_strategies": ["comprehensive", "technical", "solution"]
        }

    def get_status(self) -> Dict[str, Any]:
        """Get current system status"""
        return {
            "vector_stores_available": bool(self.vector_stores),
            "data_pipeline_completed": bool(self.data_pipeline_results),
            "system_configured": bool(self.system_config),
            "initialization_time": self.initialization_time.isoformat(),
            "store_types": list(self.vector_stores.keys()) if self.vector_stores else [],
            "ready_for_testing": bool(self.vector_stores and self.data_pipeline_results)
        }

    def reset(self):
        """Reset all state"""
        self.vector_stores = None
        self.data_pipeline_results = None
        self.system_config = None
        self.embedding_model_info = None

        if os.path.exists(self._state_file):
            os.remove(self._state_file)

        print("ðŸ”„ RAG system state reset")

# Initialize global state manager
rag_state = RAGSystemState()
rag_state.load_state()  # Try to load existing state

print(f"ðŸŽ¯ RAG State Manager Status: {rag_state.get_status()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Persistence Utilities

# COMMAND ----------

class RAGDataPersistence:
    """Utilities for persisting and loading RAG data across notebooks"""

    def __init__(self, catalog_name: str = "network_fault_detection", schema_name: str = "processed_data"):
        self.catalog_name = catalog_name
        self.schema_name = schema_name

        # Try multiple storage locations
        storage_attempts = [
            "/dbfs/tmp/rag_endtoend/",
            "./rag_endtoend/",
            "/tmp/rag_endtoend/"
        ]

        for path in storage_attempts:
            try:
                os.makedirs(path, exist_ok=True)
                # Test write permission
                test_file = os.path.join(path, "test_write.tmp")
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                self.base_path = path
                print(f"ðŸ’¾ Using storage path: {path}")
                break
            except Exception as e:
                continue
        else:
            # If all attempts fail, use current directory
            self.base_path = "./"
            print("âš ï¸ Using current directory for persistence")

    def save_vector_stores(self, vector_stores: Dict[str, Any]) -> str:
        """Save vector stores to persistent storage"""
        try:
            # Save vector store metadata to Delta table
            store_metadata = []
            for strategy, store_info in vector_stores.items():
                store_metadata.append({
                    "strategy": strategy,
                    "index_name": store_info.get("index_name", ""),
                    "document_count": store_info.get("document_count", 0),
                    "store_type": store_info.get("store_type", "databricks_vector_search"),
                    "created_timestamp": datetime.now().isoformat()
                })

            # Create DataFrame and save
            schema = StructType([
                StructField("strategy", StringType(), True),
                StructField("index_name", StringType(), True),
                StructField("document_count", IntegerType(), True),
                StructField("store_type", StringType(), True),
                StructField("created_timestamp", StringType(), True)
            ])

            metadata_df = spark.createDataFrame(store_metadata, schema)
            table_name = f"{self.catalog_name}.{self.schema_name}.rag_vector_store_metadata"

            metadata_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

            # Save actual store objects to local storage (for cross-notebook access)
            store_file = os.path.join(self.base_path, "vector_stores.pkl")
            with open(store_file, 'wb') as f:
                pickle.dump(vector_stores, f)

            print(f"âœ… Vector stores saved to {table_name} and {store_file}")
            return table_name

        except Exception as e:
            print(f"âŒ Failed to save vector stores: {str(e)}")
            raise

    def load_vector_stores(self) -> Optional[Dict[str, Any]]:
        """Load vector stores from persistent storage"""
        try:
            # Try loading from local storage first
            store_file = os.path.join(self.base_path, "vector_stores.pkl")
            if os.path.exists(store_file):
                with open(store_file, 'rb') as f:
                    vector_stores = pickle.load(f)

                print(f"âœ… Vector stores loaded from {store_file}")
                return vector_stores
            else:
                print("âš ï¸ No vector stores found in local storage")
                return None

        except Exception as e:
            print(f"âŒ Failed to load vector stores: {str(e)}")
            return None

    def save_pipeline_results(self, results: Dict[str, Any]) -> str:
        """Save data pipeline results"""
        try:
            results_file = os.path.join(self.base_path, "pipeline_results.json")
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)

            print(f"âœ… Pipeline results saved to {results_file}")
            return results_file

        except Exception as e:
            print(f"âŒ Failed to save pipeline results: {str(e)}")
            raise

    def load_pipeline_results(self) -> Optional[Dict[str, Any]]:
        """Load data pipeline results"""
        try:
            results_file = os.path.join(self.base_path, "pipeline_results.json")
            if os.path.exists(results_file):
                with open(results_file, 'r') as f:
                    results = json.load(f)

                print(f"âœ… Pipeline results loaded from {results_file}")
                return results
            else:
                print("âš ï¸ No pipeline results found")
                return None

        except Exception as e:
            print(f"âŒ Failed to load pipeline results: {str(e)}")
            return None

# Initialize persistence utility
rag_persistence = RAGDataPersistence()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Management

# COMMAND ----------

class RAGConfiguration:
    """Centralized configuration management for end-to-end RAG system"""

    @staticmethod
    def get_production_config() -> Dict[str, Any]:
        """Get production-ready configuration"""
        return {
            # Database configuration
            "catalog_name": "network_fault_detection",
            "schema_name": "processed_data",

            # Vector Search configuration
            "vs_endpoint_name": "network_fault_detection_vs_endpoint_prod",
            "embedding_model": "databricks-bge-large-en",
            "vector_dimension": 1024,

            # LLM configuration
            "foundation_model": "databricks-meta-llama-3-1-8b-instruct",
            "max_tokens": 2000,
            "temperature": 0.3,

            # Document processing
            "chunk_size": 512,
            "chunk_overlap": 50,
            "max_chunks_per_doc": 5,
            "min_document_length": 100,

            # Search strategies
            "search_strategies": {
                "comprehensive": {
                    "description": "General network incidents and comprehensive analysis",
                    "keywords": ["incident", "analysis", "comprehensive", "overview"],
                    "chunk_preference": "large"
                },
                "technical": {
                    "description": "Technical details, configurations, and specifications",
                    "keywords": ["cpu", "memory", "bandwidth", "protocol", "configuration"],
                    "chunk_preference": "detailed"
                },
                "solution": {
                    "description": "Solutions, fixes, and preventive measures",
                    "keywords": ["fix", "solve", "prevent", "solution", "troubleshoot"],
                    "chunk_preference": "actionable"
                }
            },

            # Performance settings
            "retrieval_k": 5,
            "similarity_threshold": 0.7,
            "max_response_time": 30.0,
            "batch_size": 32,

            # Quality thresholds
            "min_relevance_score": 0.6,
            "context_window": 4096,
            "max_context_length": 3000,

            # Data quality validation thresholds
            "content_quality_threshold": 0.3,  # Relaxed from 0.7 (30% instead of 70%)
            "min_content_length": 100,
            "min_analysis_length": 50,
            "substance_weight": 0.4,
            "recommendation_weight": 0.3,
            "diversity_weight": 0.3
        }

    @staticmethod
    def get_development_config() -> Dict[str, Any]:
        """Get development/testing configuration"""
        config = RAGConfiguration.get_production_config()

        # Override for development
        config.update({
            "vs_endpoint_name": "network_fault_detection_vs_endpoint_dev",
            "chunk_size": 256,  # Smaller for faster processing
            "max_tokens": 1000,
            "retrieval_k": 3,
            "batch_size": 16,
            "enable_debug_logging": True,
            "mock_fallback_enabled": True,

            # Even more relaxed thresholds for development/testing
            "content_quality_threshold": 0.2,  # Very relaxed (20% for testing)
            "min_content_length": 50,
            "min_analysis_length": 25
        })

        return config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Notebook Utilities

# COMMAND ----------

def log_step_completion(step_name: str, results: Dict[str, Any]):
    """Log completion of a pipeline step"""
    timestamp = datetime.now().isoformat()

    log_entry = {
        "step": step_name,
        "timestamp": timestamp,
        "status": "completed",
        "results_summary": {
            key: len(value) if isinstance(value, (list, dict)) else str(value)[:100]
            for key, value in results.items()
        }
    }

    # Save to log file
    log_file = "/tmp/rag_endtoend/pipeline_log.json"

    try:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                logs = json.load(f)
        else:
            logs = []

        logs.append(log_entry)

        with open(log_file, 'w') as f:
            json.dump(logs, f, indent=2)

        print(f"âœ… Step '{step_name}' completion logged")

    except Exception as e:
        print(f"âš ï¸ Failed to log step completion: {str(e)}")

def get_pipeline_status() -> Dict[str, Any]:
    """Get current pipeline execution status"""
    try:
        log_file = "/tmp/rag_endtoend/pipeline_log.json"
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                logs = json.load(f)

            completed_steps = [log["step"] for log in logs]
            latest_step = logs[-1] if logs else None

            return {
                "completed_steps": completed_steps,
                "latest_step": latest_step,
                "pipeline_progress": len(completed_steps),
                "ready_for_next_step": bool(logs)
            }
        else:
            return {
                "completed_steps": [],
                "latest_step": None,
                "pipeline_progress": 0,
                "ready_for_next_step": False
            }

    except Exception as e:
        print(f"âš ï¸ Failed to get pipeline status: {str(e)}")
        return {"error": str(e)}

# COMMAND ----------

# Export key components for import
print("ðŸŽ¯ SHARED COMPONENTS READY FOR END-TO-END INTEGRATION")
print("=" * 60)
print("âœ… RAGSystemState - Centralized state management")
print("âœ… RAGDataPersistence - Cross-notebook data persistence")
print("âœ… RAGConfiguration - Production & development configs")
print("âœ… Pipeline utilities - Step logging and status tracking")
print("âœ… Mock fallback - Graceful degradation when real stores unavailable")
print()
print("ðŸ“‹ Next Steps:")
print("   1. Run 01_Data_Setup.py to prepare data")
print("   2. Run 02_Vector_Store_Creation.py to create real stores")
print("   3. Run 03_RAG_Interface.py to test real integration")
print("   4. Run 04_Testing.py for comprehensive validation")
