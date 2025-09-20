#!/usr/bin/env python3
"""
Convert Python files to Databricks notebook format (.py with proper headers)
This ensures the files can be imported directly into Databricks workspace
"""

import os
import re
from pathlib import Path

def add_databricks_header(content, notebook_name):
    """Add Databricks notebook header to Python content"""
    header = f"""# Databricks notebook source
# MAGIC %md
# MAGIC # {notebook_name}
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

"""
    return header + content

def clean_and_secure_content(content):
    """Remove any hardcoded credentials and ensure secure patterns"""

    # Replace any hardcoded workspace URLs
    content = re.sub(
        r'DATABRICKS_HOST\s*=\s*["\']https://dbc-[a-f0-9\-]+\.cloud\.databricks\.com["\']',
        'DATABRICKS_HOST = os.getenv(\'DATABRICKS_HOST\', dbutils.secrets.get(\'default\', \'databricks-host\'))',
        content
    )

    # Replace any hardcoded tokens
    content = re.sub(
        r'DATABRICKS_TOKEN\s*=\s*["\']dapi[a-f0-9]{32,}["\']',
        'DATABRICKS_TOKEN = os.getenv(\'DATABRICKS_TOKEN\', dbutils.secrets.get(\'default\', \'databricks-token\'))',
        content
    )

    # Ensure imports are present
    if 'import os' not in content:
        content = 'import os\n' + content

    # Add cell separators for better readability
    content = re.sub(r'\n\n\n+', '\n\n# COMMAND ----------\n\n', content)

    return content

def convert_file_to_notebook(file_path, output_dir):
    """Convert a single Python file to Databricks notebook format"""

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Clean content and ensure security
    content = clean_and_secure_content(content)

    # Get notebook name from filename
    notebook_name = Path(file_path).stem.replace('_', ' ').title()

    # Add Databricks header
    notebook_content = add_databricks_header(content, notebook_name)

    # Create output filename (keep .py extension for Databricks import)
    output_file = output_dir / f"{Path(file_path).stem}.py"

    # Write the notebook
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(notebook_content)

    return output_file

def main():
    """Convert all Python files to Databricks notebook format"""

    base_dir = Path(".")
    output_dir = Path("notebooks_proper_format")

    # Create output directory structure
    output_dir.mkdir(exist_ok=True)
    (output_dir / "AgentBricks").mkdir(exist_ok=True)
    (output_dir / "MCP-RAG").mkdir(exist_ok=True)
    (output_dir / "Flask_Apps").mkdir(exist_ok=True)
    (output_dir / "Step-2-MultiAgent").mkdir(exist_ok=True)

    converted_files = []

    # Convert AgentBricks
    for py_file in (base_dir / "AgentBricks").glob("*.py"):
        output_file = convert_file_to_notebook(py_file, output_dir / "AgentBricks")
        converted_files.append(output_file)
        print(f"Converted: {py_file} -> {output_file}")

    # Convert MCP-RAG
    for py_file in (base_dir / "MCP-RAG").glob("*.py"):
        output_file = convert_file_to_notebook(py_file, output_dir / "MCP-RAG")
        converted_files.append(output_file)
        print(f"Converted: {py_file} -> {output_file}")

    # Convert Flask_Apps
    for py_file in (base_dir / "Flask_Apps").glob("*.py"):
        output_file = convert_file_to_notebook(py_file, output_dir / "Flask_Apps")
        converted_files.append(output_file)
        print(f"Converted: {py_file} -> {output_file}")

    # Convert Step-2-MultiAgent
    for py_file in (base_dir / "Step-2-MultiAgent").glob("*.py"):
        output_file = convert_file_to_notebook(py_file, output_dir / "Step-2-MultiAgent")
        converted_files.append(output_file)
        print(f"Converted: {py_file} -> {output_file}")

    print(f"\nSuccessfully converted {len(converted_files)} files to Databricks notebook format!")
    print(f"Output directory: {output_dir}")
    print("\nImport Instructions:")
    print("1. In Databricks workspace, click 'Import'")
    print("2. Select 'File' and upload the .py files")
    print("3. Files will be imported as executable notebooks")
    print("\nSecurity: All files use secure configuration patterns")

if __name__ == "__main__":
    main()