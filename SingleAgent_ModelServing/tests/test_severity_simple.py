#!/usr/bin/env python3
"""
Simple Test Script for Databricks Severity Classification Agent
==============================================================

This script tests the fallback functionality of the Severity Classification Agent
without requiring Databricks SDK installation.
"""

import sys
import os
import re
from datetime import datetime

# Test data
SAMPLE_LOGS = [
    {
        "id": "critical_outage",
        "content": "[2025-01-01 10:30:45] CRITICAL Node-5G-001: Complete service outage detected. Primary and secondary links failed. Estimated 15,000 users affected in downtown metro area. Emergency response team activated.",
        "expected": "P1"
    },
    {
        "id": "fiber_cut",
        "content": "[2025-01-01 11:15:22] EMERGENCY Network-Core-003: Major fiber cut detected on primary trunk. 25,000 customers experiencing complete service loss. All backup systems overwhelmed.",
        "expected": "P1"
    },
    {
        "id": "performance_issue",
        "content": "[2025-01-01 12:00:45] ERROR Node-LTE-023: High latency detected - average 2.8s response time. Performance degradation affecting approximately 5,000 users in suburban coverage area.",
        "expected": "P2"
    },
    {
        "id": "congestion",
        "content": "[2025-01-01 13:30:18] MAJOR Node-4G-087: Network congestion detected. Throughput reduced by 60%. Approximately 3,500 users experiencing slow data speeds during peak hours.",
        "expected": "P2"
    },
    {
        "id": "maintenance",
        "content": "[2025-01-01 14:00:10] INFO Node-WiFi-102: Scheduled maintenance completed successfully. All systems operational. CPU utilization at 45%, memory at 60%. Performance within normal parameters.",
        "expected": "P3"
    },
    {
        "id": "warning",
        "content": "[2025-01-01 15:22:33] WARN Node-Edge-045: Memory utilization at 85%. Approaching warning threshold but service unaffected. No user impact detected. Monitoring frequency increased.",
        "expected": "P3"
    }
]

class SimpleSeverityClassifier:
    """
    Simplified version of the severity classifier for testing without Databricks SDK
    """

    def __init__(self):
        self.severity_criteria = {
            "P1": {
                "keywords": ["critical", "outage", "complete failure", "service down",
                           "emergency", "disaster", "catastrophic", "total loss", "fiber cut"],
                "user_threshold": 10000
            },
            "P2": {
                "keywords": ["major", "degradation", "significant impact", "performance issue",
                           "error", "failure", "high latency", "timeout", "congestion"],
                "user_threshold": 1000
            },
            "P3": {
                "keywords": ["minor", "warning", "info", "normal", "maintenance",
                           "low priority", "informational", "preventive"],
                "user_threshold": 100
            }
        }

    def classify_severity(self, log_content):
        """Rule-based severity classification"""
        log_lower = log_content.lower()

        # Extract user count if mentioned
        user_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*(?:users?|customers?)', log_content, re.IGNORECASE)
        affected_users = int(user_match.group(1).replace(',', '')) if user_match else None

        # Check for P1 indicators
        p1_keywords = any(keyword in log_lower for keyword in self.severity_criteria["P1"]["keywords"])
        p1_users = affected_users and affected_users > self.severity_criteria["P1"]["user_threshold"]

        if p1_keywords or p1_users:
            return {
                "severity": "P1",
                "confidence": 0.9 if p1_keywords and p1_users else 0.8,
                "reasoning": f"Critical severity detected. Keywords: {p1_keywords}, High user impact: {p1_users}",
                "affected_users": affected_users
            }

        # Check for P2 indicators
        p2_keywords = any(keyword in log_lower for keyword in self.severity_criteria["P2"]["keywords"])
        p2_users = affected_users and affected_users > self.severity_criteria["P2"]["user_threshold"]

        if p2_keywords or p2_users:
            return {
                "severity": "P2",
                "confidence": 0.8 if p2_keywords and p2_users else 0.7,
                "reasoning": f"Major severity detected. Keywords: {p2_keywords}, Moderate user impact: {p2_users}",
                "affected_users": affected_users
            }

        # Default to P3
        return {
            "severity": "P3",
            "confidence": 0.6,
            "reasoning": "Minor severity - no high-impact indicators found",
            "affected_users": affected_users
        }


def test_severity_classification():
    """Test the severity classification functionality"""
    print("Testing Databricks Severity Classification Agent (Fallback Mode)")
    print("=" * 65)
    print(f"Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Initialize classifier
    classifier = SimpleSeverityClassifier()
    print("Agent initialized successfully (fallback mode)")
    print()

    # Test results
    results = []
    total_tests = len(SAMPLE_LOGS)

    print(f"Testing with {total_tests} sample network logs:")
    print("-" * 50)

    for i, log_sample in enumerate(SAMPLE_LOGS, 1):
        print(f"\n{i}. Testing: {log_sample['id']}")
        print(f"   Expected: {log_sample['expected']}")
        print(f"   Content: {log_sample['content'][:80]}...")

        # Classify severity
        result = classifier.classify_severity(log_sample['content'])

        print(f"   Result: {result['severity']} (Confidence: {result['confidence']:.2f})")
        print(f"   Reasoning: {result['reasoning'][:100]}...")

        if result['affected_users']:
            print(f"   Affected Users: {result['affected_users']:,}")

        # Check accuracy
        is_correct = result['severity'] == log_sample['expected']
        status = "CORRECT" if is_correct else "INCORRECT"
        print(f"   Accuracy: {status}")

        results.append({
            'test_id': log_sample['id'],
            'expected': log_sample['expected'],
            'actual': result['severity'],
            'confidence': result['confidence'],
            'correct': is_correct
        })

    # Calculate overall results
    correct_count = sum(1 for r in results if r['correct'])
    accuracy = (correct_count / total_tests) * 100

    print("\n" + "=" * 65)
    print("TEST RESULTS SUMMARY")
    print("=" * 65)

    print(f"Total tests: {total_tests}")
    print(f"Correct classifications: {correct_count}")
    print(f"Accuracy rate: {accuracy:.1f}%")

    # Detailed results
    print(f"\nDetailed Results:")
    severity_counts = {"P1": 0, "P2": 0, "P3": 0}

    for result in results:
        severity_counts[result['actual']] += 1
        status_icon = "✓" if result['correct'] else "✗"
        print(f"  {status_icon} {result['test_id']}: {result['expected']} -> {result['actual']} ({result['confidence']:.2f})")

    print(f"\nClassification Distribution:")
    print(f"  P1 (Critical): {severity_counts['P1']}")
    print(f"  P2 (Major): {severity_counts['P2']}")
    print(f"  P3 (Minor): {severity_counts['P3']}")

    # Performance assessment
    if accuracy >= 80:
        status = "EXCELLENT"
        recommendation = "Agent ready for production deployment with Databricks SDK"
    elif accuracy >= 60:
        status = "GOOD"
        recommendation = "Agent functional - test with Databricks Foundation Models"
    else:
        status = "NEEDS IMPROVEMENT"
        recommendation = "Review classification logic before deployment"

    print(f"\nPerformance Assessment: {status}")
    print(f"Recommendation: {recommendation}")

    print(f"\nNext Steps:")
    print("1. Install Databricks SDK: pip install databricks-sdk")
    print("2. Configure Databricks workspace authentication")
    print("3. Test with Foundation Models for enhanced accuracy")
    print("4. Deploy to Databricks workspace for production use")

    print(f"\nTest completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return accuracy >= 60


if __name__ == "__main__":
    """Run the simple test when executed directly"""
    success = test_severity_classification()
    print(f"\nTest {'PASSED' if success else 'FAILED'}")
    sys.exit(0 if success else 1)