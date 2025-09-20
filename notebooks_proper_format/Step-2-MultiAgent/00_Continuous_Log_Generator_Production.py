# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Continuous Log Generator Production
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
# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Continuous Network Log Generator - Production Simulation
# MAGIC
# MAGIC **Purpose**: Generate realistic network logs continuously to simulate production environment
# MAGIC **Frequency**: Every 5-10 minutes with batch sizes of 10-50 logs
# MAGIC **Variety**: Multiple severity levels, network components, and failure patterns

# COMMAND ----------

import time
import random
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import uuid

spark = SparkSession.builder.getOrCreate()

print("🔄 Continuous Network Log Generator - Production Simulation")
print("=" * 70)

# COMMAND ----------

# Configuration
LOG_OUTPUT_PATH = "/FileStore/logs/network_logs_streaming/"
BATCH_SIZE_MIN = 10
BATCH_SIZE_MAX = 50
GENERATION_INTERVAL = 300  # 5 minutes in seconds

# Network components and failure scenarios for realistic simulation
NETWORK_COMPONENTS = [
    "router-core-01", "router-core-02", "switch-access-01", "switch-access-02",
    "firewall-edge-01", "firewall-edge-02", "load-balancer-01", "load-balancer-02",
    "dns-server-01", "dns-server-02", "dhcp-server-01", "wan-link-primary",
    "wan-link-backup", "vpn-gateway-01", "proxy-server-01", "ntp-server-01"
]

FAILURE_SCENARIOS = {
    "P1": [  # Critical failures
        "INTERFACE_DOWN: Critical interface failure on {component}",
        "POWER_FAILURE: Primary power lost on {component}",
        "MEMORY_CRITICAL: Memory utilization exceeded 95% on {component}",
        "CPU_CRITICAL: CPU utilization exceeded 98% on {component}",
        "ROUTING_LOOP: Critical routing loop detected involving {component}",
        "BGP_DOWN: BGP neighbor down on {component}",
        "OSPF_FAILURE: OSPF area failure on {component}",
        "SPANNING_TREE_LOOP: STP loop detected on {component}"
    ],
    "P2": [  # High priority issues
        "HIGH_LATENCY: Response time degraded on {component}",
        "BANDWIDTH_UTILIZATION: Link utilization >85% on {component}",
        "PACKET_LOSS: Packet loss detected 5-15% on {component}",
        "AUTHENTICATION_ISSUES: Auth failures increasing on {component}",
        "CONFIG_DRIFT: Configuration drift detected on {component}",
        "REDUNDANCY_LOST: Backup path unavailable for {component}",
        "VLAN_MISCONFIGURATION: VLAN mismatch detected on {component}",
        "QOS_VIOLATION: Quality of Service thresholds exceeded on {component}"
    ],
    "P3": [  # Medium priority issues
        "PERFORMANCE_DEGRADATION: Minor performance issue on {component}",
        "LOG_ROTATION_ISSUE: Log files growing large on {component}",
        "SNMP_TIMEOUT: SNMP monitoring timeout on {component}",
        "LICENSE_WARNING: Software license expiring on {component}",
        "MAINTENANCE_DUE: Scheduled maintenance overdue on {component}",
        "CERT_EXPIRATION: SSL certificate expiring in 30 days on {component}",
        "BACKUP_DELAYED: Configuration backup delayed on {component}",
        "TIME_SYNC_DRIFT: NTP sync drift detected on {component}"
    ],
    "INFO": [  # Informational events
        "INTERFACE_UP: Interface restored on {component}",
        "MAINTENANCE_COMPLETED: Scheduled maintenance completed on {component}",
        "CONFIG_UPDATED: Configuration successfully updated on {component}",
        "BACKUP_COMPLETED: Configuration backup completed on {component}",
        "USER_LOGIN: Administrative login to {component}",
        "SOFTWARE_UPDATED: Firmware update completed on {component}",
        "MONITORING_RESTORED: Monitoring connection restored to {component}",
        "REDUNDANCY_RESTORED: Backup path restored for {component}"
    ]
}

# Weight distribution for realistic production patterns
SEVERITY_WEIGHTS = {
    "P1": 5,    # 5% critical
    "P2": 15,   # 15% high
    "P3": 30,   # 30% medium
    "INFO": 50  # 50% informational
}

# COMMAND ----------

def generate_realistic_log_entry():
    """Generate a single realistic network log entry"""
    # Select severity based on weighted distribution
    severity_choices = []
    for severity, weight in SEVERITY_WEIGHTS.items():
        severity_choices.extend([severity] * weight)

    severity = random.choice(severity_choices)
    component = random.choice(NETWORK_COMPONENTS)
    message_template = random.choice(FAILURE_SCENARIOS[severity])
    message = message_template.format(component=component)

    # Generate realistic timestamp with some jitter
    timestamp = datetime.now() - timedelta(seconds=random.randint(0, 300))

    # Create log entry in standard network log format
    log_entry = {
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "severity": severity,
        "component": component,
        "message": message,
        "source_ip": f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
        "event_id": str(uuid.uuid4())[:8],
        "facility": "network.infrastructure"
    }

    # Format as traditional syslog-style entry
    formatted_log = f"{log_entry['timestamp']} {log_entry['component']} [{log_entry['severity']}] {log_entry['message']} | Source: {log_entry['source_ip']} | EventID: {log_entry['event_id']}"

    return formatted_log

def generate_log_batch():
    """Generate a batch of log entries"""
    batch_size = random.randint(BATCH_SIZE_MIN, BATCH_SIZE_MAX)
    logs = []

    print(f"📝 Generating batch of {batch_size} log entries...")

    for i in range(batch_size):
        log_entry = generate_realistic_log_entry()
        logs.append(log_entry)

    return logs

def save_log_batch(logs, batch_number):
    """Save log batch to file for Auto Loader pickup"""
    filename = f"network_logs_batch_{batch_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_path = f"{LOG_OUTPUT_PATH}{filename}"

    # Convert to DataFrame and save
    log_df = spark.createDataFrame([(log,) for log in logs], ["log_entry"])

    # Write as text file for Auto Loader compatibility
    log_df.select("log_entry").write.mode("overwrite").text(file_path.replace('.log', ''))

    print(f"💾 Saved {len(logs)} logs to: {file_path}")
    return file_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Simulation Execution

# COMMAND ----------

def run_continuous_generation(duration_hours=24, test_mode=False):
    """
    Run continuous log generation

    Args:
        duration_hours: How long to run (default 24 hours)
        test_mode: If True, run only 3 iterations for testing
    """
    print(f"🚀 Starting continuous log generation")
    print(f"📊 Target duration: {duration_hours} hours")
    print(f"⏱️ Batch interval: {GENERATION_INTERVAL} seconds ({GENERATION_INTERVAL/60:.1f} minutes)")
    print(f"📦 Batch size: {BATCH_SIZE_MIN}-{BATCH_SIZE_MAX} logs per batch")
    print("=" * 70)

    start_time = datetime.now()
    end_time = start_time + timedelta(hours=duration_hours)
    batch_number = 1
    total_logs_generated = 0

    # Test mode: only run 3 iterations
    if test_mode:
        max_iterations = 3
        iteration_count = 0

    while datetime.now() < end_time:
        if test_mode and iteration_count >= max_iterations:
            print(f"🧪 Test mode: completed {max_iterations} iterations")
            break

        try:
            # Generate and save log batch
            logs = generate_log_batch()
            file_path = save_log_batch(logs, batch_number)

            total_logs_generated += len(logs)

            print(f"✅ Batch {batch_number} completed: {len(logs)} logs")
            print(f"📈 Total logs generated so far: {total_logs_generated}")
            print(f"⏰ Next batch at: {(datetime.now() + timedelta(seconds=GENERATION_INTERVAL)).strftime('%H:%M:%S')}")
            print("-" * 50)

            batch_number += 1

            if test_mode:
                iteration_count += 1
                time.sleep(10)  # Short delay for test mode
            else:
                # Wait for next generation cycle
                time.sleep(GENERATION_INTERVAL)

        except Exception as e:
            print(f"❌ Error generating batch {batch_number}: {str(e)}")
            time.sleep(60)  # Wait 1 minute before retry
            continue

    # Final statistics
    elapsed_time = datetime.now() - start_time
    print("=" * 70)
    print("🎯 CONTINUOUS LOG GENERATION COMPLETED")
    print(f"⏰ Total runtime: {elapsed_time}")
    print(f"📊 Total batches: {batch_number - 1}")
    print(f"📝 Total logs generated: {total_logs_generated}")
    print(f"📈 Average logs per batch: {total_logs_generated / (batch_number - 1):.1f}")
    print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Options

# COMMAND ----------

# Option 1: Test Mode - Generate 3 batches for immediate testing
print("🧪 OPTION 1: Test Mode (3 batches)")
run_continuous_generation(duration_hours=1, test_mode=True)

# COMMAND ----------

# Option 2: Production Mode - Run continuously
# Uncomment the line below to run in production mode for 24 hours
# print("🚀 OPTION 2: Production Mode (24 hours)")
# run_continuous_generation(duration_hours=24, test_mode=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Verification

# COMMAND ----------

# Check generated files
files = dbutils.fs.ls(LOG_OUTPUT_PATH)
print(f"📁 Generated log files in {LOG_OUTPUT_PATH}:")
for file in files[-10:]:  # Show last 10 files
    print(f"   📄 {file.name} ({file.size} bytes)")

# COMMAND ----------

# Preview latest log entries
if files:
    latest_file = sorted(files, key=lambda x: x.name, reverse=True)[0]
    print(f"📖 Preview of latest file: {latest_file.name}")
    sample_logs = spark.read.text(latest_file.path)
    sample_logs.show(10, truncate=False)