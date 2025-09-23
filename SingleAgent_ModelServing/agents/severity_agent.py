"""
Databricks Agent Framework - Severity Classification Agent
Enhanced version with comprehensive testing and validation capabilities
Migrated from legacy agents/agent_orchestrator.py for Databricks Agent Bricks
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
import os
import json
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class SeverityResult:
    """Enhanced result object for severity classification"""
    severity: str  # P1, P2, or P3
    confidence: float  # 0.0 to 1.0
    reasoning: str
    affected_users: Optional[int] = None
    estimated_downtime: Optional[str] = None
    business_impact: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class SeverityClassifierAgent:
    """
    Enhanced Databricks Agent for classifying network incident severity
    Uses Databricks Foundation Models with comprehensive analysis capabilities
    """

    def __init__(self):
        load_dotenv()
        self.client = WorkspaceClient()
        self.endpoint_name = os.getenv("DATABRICKS_SERVING_ENDPOINT", "databricks-meta-llama-3-1-405b-instruct")

        # Enhanced severity criteria with business impact assessment
        self.severity_criteria = {
            "P1": {
                "keywords": ["critical", "outage", "complete failure", "service down",
                           "emergency", "disaster", "catastrophic", "total loss", "fiber cut"],
                "user_impact_threshold": 10000,
                "description": "Critical - Complete service outage affecting large user base"
            },
            "P2": {
                "keywords": ["major", "degradation", "significant impact", "performance issue",
                           "error", "failure", "high latency", "timeout", "congestion"],
                "user_impact_threshold": 1000,
                "description": "Major - Service degradation with noticeable impact"
            },
            "P3": {
                "keywords": ["minor", "warning", "info", "normal", "maintenance",
                           "low priority", "informational", "preventive"],
                "user_impact_threshold": 100,
                "description": "Minor - Low impact or informational event"
            }
        }

    def get_enhanced_severity_prompt(self, log_content: str) -> str:
        """Generate enhanced severity classification prompt with detailed analysis"""
        return f"""You are a network operations expert specializing in telecom incident severity classification.
Analyze the following network log and provide a comprehensive severity assessment.

**SEVERITY LEVELS:**
- **P1 (Critical)**: Complete service outage, major infrastructure failure, >10,000 users affected
  Examples: Core network failure, multiple eNB/gNB down, fiber cut affecting major area, data center outage
- **P2 (Major)**: Service degradation, performance issues, 1,000-10,000 users affected
  Examples: Single node failure, congestion affecting >50% capacity, high latency issues, backup systems activated
- **P3 (Minor)**: Low impact events, warnings, <1,000 users affected
  Examples: Single cell congestion, minor KPI degradation, preventive maintenance, threshold warnings

**LOG CONTENT TO ANALYZE:**
```
{log_content}
```

**ANALYSIS REQUIREMENTS:**
1. Determine severity level (P1, P2, or P3) based on impact scope and service availability
2. Estimate number of affected users (if mentioned or can be inferred from context)
3. Assess potential business impact and revenue implications
4. Provide clear technical reasoning for your classification
5. Estimate potential downtime or service restoration time

**RESPONSE FORMAT (JSON):**
```json
{{
    "severity": "P1|P2|P3",
    "confidence": 0.95,
    "reasoning": "Detailed technical explanation of why this severity was chosen based on network impact analysis",
    "affected_users": 15000,
    "estimated_downtime": "30-45 minutes",
    "business_impact": "Complete service disruption for major customer segment with high revenue impact"
}}
```

**RULES:**
- Focus on network impact assessment, not corrective actions
- Base classification on technical severity and user impact
- Consider both immediate and potential cascading effects
- Respond only with valid JSON format
- Be precise and concise in your technical analysis
"""

    def get_severity_prompt(self, log_content: str) -> str:
        """Backward compatibility - simple severity classification prompt"""
        return f"""You are a Severity Classifier for Telecom incidents.
Analyze the following log content and classify the incident severity:

- P1 (Critical): Complete outage, major infrastructure failure
- P2 (Major): Service degradation, significant performance impact
- P3 (Minor): Isolated issue, early warning, minimal impact

LOG CONTENT: {log_content}

Respond ONLY with: "SEVERITY_CLASSIFIER > P1|P2|P3"
"""

    async def classify_severity(self, log_content: str) -> str:
        """
        Classify incident severity using Databricks Foundation Model (backward compatibility)
        Returns simple severity classification string for existing integrations
        """
        try:
            result = await self.classify_severity_enhanced(log_content)
            return f"SEVERITY_CLASSIFIER > {result.severity}"
        except Exception as e:
            print(f"Error in severity classification: {str(e)}")
            return "SEVERITY_CLASSIFIER > P2"  # Default fallback

    async def classify_severity_enhanced(self, log_content: str) -> SeverityResult:
        """
        Enhanced severity classification with detailed analysis using Databricks Foundation Model
        """
        try:
            prompt = self.get_enhanced_severity_prompt(log_content)

            # Use Databricks Model Serving API
            response = self.client.serving_endpoints.query(
                name=self.endpoint_name,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.USER,
                        content=prompt
                    )
                ],
                max_tokens=500,
                temperature=0.1
            )

            # Parse model response
            model_output = response.choices[0].message.content
            return self._parse_model_response(model_output)

        except Exception as e:
            # Return fallback classification on any error
            return self._fallback_classification(log_content, str(e))

    def _parse_model_response(self, response: str) -> SeverityResult:
        """Parse the Foundation Model response into structured data"""
        try:
            # Extract JSON from response
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON without code blocks
                json_str = response.strip()
                if not json_str.startswith('{'):
                    raise ValueError("No JSON found in response")

            data = json.loads(json_str)

            return SeverityResult(
                severity=data.get("severity", "P3"),
                confidence=float(data.get("confidence", 0.5)),
                reasoning=data.get("reasoning", "No reasoning provided"),
                affected_users=data.get("affected_users"),
                estimated_downtime=data.get("estimated_downtime"),
                business_impact=data.get("business_impact")
            )

        except Exception as e:
            # Fallback to rule-based classification
            return self._fallback_classification(response, str(e))

    def _fallback_classification(self, log_content: str, error: str) -> SeverityResult:
        """Fallback rule-based classification if model parsing fails"""
        log_lower = log_content.lower()

        # Extract user count if mentioned
        user_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*users?', log_content, re.IGNORECASE)
        affected_users = int(user_match.group(1).replace(',', '')) if user_match else None

        # Check for P1 indicators
        if any(keyword in log_lower for keyword in self.severity_criteria["P1"]["keywords"]) or \
           (affected_users and affected_users > self.severity_criteria["P1"]["user_impact_threshold"]):
            return SeverityResult(
                severity="P1",
                confidence=0.8,
                reasoning=f"Fallback rule-based classification due to parsing error: {error}. "
                          f"Detected critical keywords or high user impact in log content.",
                affected_users=affected_users,
                business_impact="High - Critical system impact detected"
            )

        # Check for P2 indicators
        elif any(keyword in log_lower for keyword in self.severity_criteria["P2"]["keywords"]) or \
             (affected_users and affected_users > self.severity_criteria["P2"]["user_impact_threshold"]):
            return SeverityResult(
                severity="P2",
                confidence=0.7,
                reasoning=f"Fallback rule-based classification due to parsing error: {error}. "
                          f"Detected major impact keywords or moderate user impact in log content.",
                affected_users=affected_users,
                business_impact="Medium - Service degradation detected"
            )

        # Default to P3
        else:
            return SeverityResult(
                severity="P3",
                confidence=0.6,
                reasoning=f"Fallback rule-based classification due to parsing error: {error}. "
                          f"No high-severity indicators found.",
                affected_users=affected_users,
                business_impact="Low - Minor or informational event"
            )

    def classify_severity_sync(self, log_content: str) -> SeverityResult:
        """Synchronous version of enhanced severity classification (for testing)"""
        import asyncio
        return asyncio.run(self.classify_severity_enhanced(log_content))

    def batch_classify(self, log_entries: List[str]) -> List[SeverityResult]:
        """Classify multiple log entries in batch"""
        results = []
        for log_entry in log_entries:
            try:
                result = self.classify_severity_sync(log_entry)
                results.append(result)
            except Exception as e:
                # Add error result
                error_result = SeverityResult(
                    severity="P3",
                    confidence=0.3,
                    reasoning=f"Batch classification error: {str(e)}",
                    business_impact="Unknown - Classification error occurred"
                )
                results.append(error_result)

        return results

    def get_classification_stats(self, classifications: List[SeverityResult]) -> Dict:
        """Generate statistics from a batch of classifications"""
        if not classifications:
            return {}

        severity_counts = {"P1": 0, "P2": 0, "P3": 0}
        total_confidence = 0
        total_affected_users = 0

        for classification in classifications:
            severity_counts[classification.severity] += 1
            total_confidence += classification.confidence
            if classification.affected_users:
                total_affected_users += classification.affected_users

        return {
            "total_incidents": len(classifications),
            "severity_breakdown": severity_counts,
            "average_confidence": total_confidence / len(classifications),
            "total_affected_users": total_affected_users,
            "high_priority_incidents": severity_counts["P1"] + severity_counts["P2"],
            "classification_timestamp": datetime.now().isoformat()
        }

# Legacy compatibility wrapper
class AgentSeverityClassifier(SeverityClassifierAgent):
    """Backwards compatibility with existing codebase"""
    pass


# Test data for agent validation
SAMPLE_NETWORK_LOGS = [
    {
        "id": "critical_outage_p1",
        "content": "[2025-01-01 10:30:45] CRITICAL Node-5G-001: Complete service outage detected. Primary and secondary links failed. Estimated 15,000 users affected in downtown metro area. Emergency response team activated.",
        "expected_severity": "P1"
    },
    {
        "id": "fiber_cut_p1",
        "content": "[2025-01-01 11:15:22] EMERGENCY Network-Core-003: Major fiber cut detected on primary trunk. 25,000 customers experiencing complete service loss. All backup systems overwhelmed. ETA repair: 2-4 hours.",
        "expected_severity": "P1"
    },
    {
        "id": "performance_degradation_p2",
        "content": "[2025-01-01 12:00:45] ERROR Node-LTE-023: High latency detected - average 2.8s response time. Performance degradation affecting approximately 5,000 users in suburban coverage area. Backup link activated.",
        "expected_severity": "P2"
    },
    {
        "id": "congestion_p2",
        "content": "[2025-01-01 13:30:18] MAJOR Node-4G-087: Network congestion detected. Throughput reduced by 60%. Approximately 3,500 users experiencing slow data speeds during peak hours. Load balancing in progress.",
        "expected_severity": "P2"
    },
    {
        "id": "routine_maintenance_p3",
        "content": "[2025-01-01 14:00:10] INFO Node-WiFi-102: Scheduled maintenance completed successfully. All systems operational. CPU utilization at 45%, memory at 60%. Performance within normal parameters.",
        "expected_severity": "P3"
    },
    {
        "id": "threshold_warning_p3",
        "content": "[2025-01-01 15:22:33] WARN Node-Edge-045: Memory utilization at 85%. Approaching warning threshold but service unaffected. No user impact detected. Monitoring frequency increased.",
        "expected_severity": "P3"
    },
    {
        "id": "preventive_scaling_p3",
        "content": "[2025-01-01 16:45:12] INFO Auto-Scaler: Preventive capacity scaling initiated. Adding 2 additional instances to handle projected evening traffic. Estimated completion: 10 minutes.",
        "expected_severity": "P3"
    }
]


def test_severity_agent():
    """
    Comprehensive test function for the Severity Classification Agent
    Tests both Foundation Model integration and fallback capabilities
    """
    print("🤖 Testing Databricks Severity Classification Agent")
    print("=" * 70)

    # Initialize agent
    try:
        agent = SeverityClassifierAgent()
        print("✅ Agent initialized successfully")
        print(f"📡 Model endpoint: {agent.endpoint_name}")
        print()
    except Exception as e:
        print(f"❌ Agent initialization failed: {e}")
        print("💡 Make sure Databricks SDK is configured and workspace is accessible")
        return False

    # Test results tracking
    test_results = []
    total_tests = len(SAMPLE_NETWORK_LOGS)
    passed_tests = 0

    # Test with sample logs
    print("📋 Testing with sample network logs:")
    print("-" * 50)

    for i, log_sample in enumerate(SAMPLE_NETWORK_LOGS, 1):
        print(f"\n{i}/{total_tests}. Testing: {log_sample['id']}")
        print(f"Expected: {log_sample['expected_severity']}")
        print(f"Log: {log_sample['content'][:100]}...")

        try:
            # Test enhanced classification
            result = agent.classify_severity_sync(log_sample['content'])

            # Display results
            print(f"🤖 Classified: {result.severity} (Confidence: {result.confidence:.2f})")
            print(f"📝 Reasoning: {result.reasoning[:150]}...")

            if result.affected_users:
                print(f"👥 Affected Users: {result.affected_users:,}")
            if result.estimated_downtime:
                print(f"⏱️ Estimated Downtime: {result.estimated_downtime}")
            if result.business_impact:
                print(f"💼 Business Impact: {result.business_impact[:100]}...")

            # Check accuracy
            is_correct = result.severity == log_sample['expected_severity']
            if is_correct:
                passed_tests += 1
                print("🎯 Accuracy: ✅ CORRECT")
            else:
                print("🎯 Accuracy: ❌ INCORRECT")

            test_results.append({
                "test_id": log_sample['id'],
                "expected": log_sample['expected_severity'],
                "actual": result.severity,
                "confidence": result.confidence,
                "correct": is_correct
            })

        except Exception as e:
            print(f"❌ Classification failed: {e}")
            test_results.append({
                "test_id": log_sample['id'],
                "expected": log_sample['expected_severity'],
                "actual": "ERROR",
                "confidence": 0.0,
                "correct": False,
                "error": str(e)
            })

    # Test batch classification
    print(f"\n🔄 Testing batch classification with {total_tests} logs:")
    print("-" * 50)

    try:
        log_contents = [sample['content'] for sample in SAMPLE_NETWORK_LOGS]
        batch_results = agent.batch_classify(log_contents)

        print(f"✅ Batch classification completed: {len(batch_results)} results")

        # Generate statistics
        stats = agent.get_classification_stats(batch_results)
        print(f"📊 Classification Statistics:")
        print(f"   • Total incidents: {stats['total_incidents']}")
        print(f"   • P1 (Critical): {stats['severity_breakdown']['P1']}")
        print(f"   • P2 (Major): {stats['severity_breakdown']['P2']}")
        print(f"   • P3 (Minor): {stats['severity_breakdown']['P3']}")
        print(f"   • Average confidence: {stats['average_confidence']:.2f}")
        print(f"   • High priority incidents: {stats['high_priority_incidents']}")
        if stats['total_affected_users'] > 0:
            print(f"   • Total affected users: {stats['total_affected_users']:,}")

    except Exception as e:
        print(f"❌ Batch classification failed: {e}")

    # Final test summary
    print("\n" + "=" * 70)
    print("🎉 SEVERITY CLASSIFICATION AGENT TEST COMPLETE!")
    print("=" * 70)

    accuracy = (passed_tests / total_tests) * 100
    print(f"📊 Overall Test Results:")
    print(f"   • Tests passed: {passed_tests}/{total_tests}")
    print(f"   • Accuracy rate: {accuracy:.1f}%")

    if accuracy >= 80:
        print("✅ Agent performance: EXCELLENT (≥80% accuracy)")
        status = "PASSED"
    elif accuracy >= 60:
        print("⚠️ Agent performance: GOOD (≥60% accuracy)")
        status = "PASSED"
    else:
        print("❌ Agent performance: NEEDS IMPROVEMENT (<60% accuracy)")
        status = "FAILED"

    print(f"🏆 Test Suite Status: {status}")

    # Recommendations
    print(f"\n💡 Recommendations:")
    if accuracy >= 80:
        print("   • Agent ready for production deployment")
        print("   • Consider testing with larger dataset")
        print("   • Monitor performance with real network logs")
    elif accuracy >= 60:
        print("   • Agent functional but may need tuning")
        print("   • Review misclassified cases for improvement")
        print("   • Consider adjusting severity criteria or prompt engineering")
    else:
        print("   • Agent needs significant improvement before deployment")
        print("   • Check Foundation Model access and configuration")
        print("   • Review fallback classification logic")

    print("\n🚀 Next Steps:")
    print("   1. Test with real network log files")
    print("   2. Integrate with RCA generation pipeline")
    print("   3. Set up real-time monitoring dashboard")
    print("   4. Deploy to production Databricks workspace")

    return status == "PASSED"


if __name__ == "__main__":
    """Run comprehensive tests when executed directly"""
    success = test_severity_agent()
    exit(0 if success else 1)