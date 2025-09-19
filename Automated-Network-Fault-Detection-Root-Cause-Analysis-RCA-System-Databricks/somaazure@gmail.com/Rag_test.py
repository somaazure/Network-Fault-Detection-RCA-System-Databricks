# Databricks notebook source
# Set environment variables (Free Trial Compatible)
import os
os.environ["OPENAI_API_KEY"] = "YOUR_OPENAI_API_KEY"

# Optional - Only needed for advanced features
# os.environ["DATABRICKS_TOKEN"] = "your_databricks_token_here"
# os.environ["DATABRICKS_HOST"] = "https://adb-xxxxx.azuredatabricks.net"

# COMMAND ----------

  # Run the full Gradio chatbot
from databricks_rag_chatbot import main
main()

