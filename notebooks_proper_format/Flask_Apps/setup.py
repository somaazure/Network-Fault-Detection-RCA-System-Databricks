# Databricks notebook source
# MAGIC %md
# MAGIC # Setup
# MAGIC
# MAGIC **Network Fault Detection RCA System**
# MAGIC
# MAGIC This notebook is part of the production-ready Network Fault Detection and Root Cause Analysis system.
# MAGIC
# MAGIC ## 🔧 Configuration
# MAGIC
# MAGIC ```python
# MAGIC # Secure configuration pattern
# MAGIC DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', dbutils.secrets.get('default', 'databricks-host'))
# MAGIC DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', dbutils.secrets.get('default', 'databricks-token'))
# MAGIC ```

# COMMAND ----------

import os
from setuptools import setup, find_packages

setup(
    name="flask-nwk-rca-real-rag",
    version="1.0.0",
    description="Network RCA Assistant with Real RAG System",
    py_modules=["app_flask_real_rag"],
    install_requires=[
        "Flask==2.3.3",
        "pandas==1.5.3",
        "databricks-vectorsearch>=0.22",
        "mlflow>=2.8.0",
        "requests>=2.31.0",
        "numpy>=1.24.0"
    ],
    python_requires=">=3.8",
)