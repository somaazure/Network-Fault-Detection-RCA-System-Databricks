# 🚀 Databricks Export & Git Push Execution Guide

## 📋 Prerequisites

### ✅ Required Software
1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

2. **Git** installed and configured
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

3. **Access Verification**
   ```bash
   databricks workspace list /Workspace/Users/somaazure@gmail.com
   ```

## 🎯 Step-by-Step Execution

### **Step 1: Export Databricks Notebooks**

#### Option A: PowerShell (Windows - Recommended)
```powershell
cd C:\databricks-export\GitUpload\DBKformat_OP
.\export_commands.ps1
```

#### Option B: Bash (Linux/Mac/WSL)
```bash
cd /c/databricks-export/GitUpload/DBKformat_OP
bash export_commands.sh
```

**Expected Output:**
- 25+ Python files exported from Databricks workspace
- Clean SOURCE format (no HTML)
- Organized directory structure

### **Step 2: Verify Exports**
```powershell
# Check exported files
Get-ChildItem -Recurse *.py | Measure-Object
```

### **Step 3: Git Setup and Push**
```powershell
.\git_setup.ps1
```

**This script will:**
- ✅ Initialize Git repository
- ✅ Create .gitignore for Databricks projects
- ✅ Add remote repository
- ✅ Create comprehensive README.md
- ✅ Commit all files
- ✅ Push to GitHub

## 🔍 What Gets Exported

### **🤖 Core Agent System**
```
AgentBricks/
├── 01_Incident_Manager_AgentBricks.py
├── 02_Severity_Classification_AgentBricks.py
├── 03_Network_Ops_Agent_AgentBricks.py
├── 04_RCA_Agent_AgentBricks_Initial.py
├── 05_Multi_Agent_Orchestrator_AgentBricks_Initial.py
└── Autoloader-Streaming/
    └── 02_Severity_Classification_Agent_PRODUCTION_READY.py
```

### **📊 Multi-Agent Pipeline**
```
Step-2-MultiAgent/
├── 01_Incident_Manager_Agent.py
├── 01_Incident_Manager_Agent_FALLBACK.py
├── 02_Network_Ops_Agent_Fixed.py
├── 03_RCA_Agent_Fixed.py
├── 03_RCA_Agent_Fixed_FALLBACK.py
└── 04_Multi_Agent_Orchestrator_Fixed.py
```

### **🔍 RAG System**
```
MCP-RAG/
├── MCP_Test.py
└── MCP_RAG_Production_Fixed.py
```

### **🌐 Flask Applications**
```
Flask_Apps/
└── deploy_workspace_flask_fixed.py
```

### **🛠️ Infrastructure & Testing**
```
├── Create_Serving_Endpoint.py
├── Create_Serving_Endpoint_Enhanced.py
├── Create_Serving_Endpoint_Original.py
├── Demo_UC_Enhanced.py
├── RAG_Test.py
└── Development_Notebook_*.py (5 files)
```

## 🛡️ Security Features

### **✅ Secure Configuration Pattern**
All exported files will use secure patterns:
```python
# Secure - No hardcoded credentials
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST', dbutils.secrets.get('default', 'databricks-host'))
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', dbutils.secrets.get('default', 'databricks-token'))
```

### **✅ .gitignore Protection**
Automatically created to exclude:
- Credentials and keys
- IDE files
- Temporary files
- Environment files

## ⚠️ Troubleshooting

### **Issue: Export Command Fails**
```bash
# Check authentication
databricks auth list

# Test workspace access
databricks workspace list /Workspace/Users/somaazure@gmail.com
```

### **Issue: Git Push Fails**
```bash
# Check remote URL
git remote -v

# Force push if needed (use carefully)
git push --force-with-lease origin main
```

### **Issue: Permission Denied**
```powershell
# Set execution policy for PowerShell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 📊 Success Verification

### **✅ Export Success Indicators**
- [ ] 25+ .py files created
- [ ] No HTML files in output
- [ ] Directory structure matches plan
- [ ] Files contain SOURCE format code

### **✅ Git Push Success Indicators**
- [ ] Repository visible at: https://github.com/somaazure/Network-Fault-Detection-RCA-System-Databricks
- [ ] README.md displays correctly
- [ ] All Python files uploaded
- [ ] No credential exposure warnings

## 🚀 Next Steps After Success

1. **Verify Repository**: Visit GitHub repository and check files
2. **Clone Test**: `git clone` the repository to verify integrity
3. **Branch Protection**: Set up main branch protection rules
4. **Collaborators**: Add team members with appropriate permissions
5. **Documentation**: Update any additional documentation needed

## 📞 Support

If you encounter issues:
1. Check Databricks CLI configuration
2. Verify workspace access permissions
3. Ensure Git authentication is working
4. Review error messages in PowerShell/Bash output

---

**🎯 Goal**: Clean, secure, production-ready Databricks code in Git
**⏱️ Est. Time**: 10-15 minutes for complete export and push
**🔐 Security**: No hardcoded credentials, proper .gitignore protection