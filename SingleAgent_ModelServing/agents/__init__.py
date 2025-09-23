"""
Databricks Agent Framework - Single Agent Components
====================================================

This module contains the Severity Classification Agent implementation
for the Network Fault Detection and RCA System.

Components:
- SeverityClassifierAgent: Enhanced ML-powered severity classification
- SeverityResult: Structured result object with detailed analysis
- SAMPLE_NETWORK_LOGS: Test data for validation

Usage:
    from agents.severity_agent import SeverityClassifierAgent

    agent = SeverityClassifierAgent()
    result = agent.classify_severity_sync(log_content)
    print(f"Severity: {result.severity}, Confidence: {result.confidence}")
"""

from .severity_agent import (
    SeverityClassifierAgent,
    SeverityResult,
    AgentSeverityClassifier,
    SAMPLE_NETWORK_LOGS,
    test_severity_agent
)

__version__ = "1.0.0"
__author__ = "Network Fault Detection Team"
__status__ = "Production Ready"

__all__ = [
    "SeverityClassifierAgent",
    "SeverityResult",
    "AgentSeverityClassifier",
    "SAMPLE_NETWORK_LOGS",
    "test_severity_agent"
]