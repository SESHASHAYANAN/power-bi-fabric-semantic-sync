#!/usr/bin/env python3
"""
Bidirectional Sync Test Script for Fabric-Snowflake Semantic Sync.

This script tests the synchronization of measure/calculation changes
in both directions:
1. Power BI Fabric → Snowflake
2. Snowflake → Power BI Fabric

Usage:
    # Run all tests with mock data (no live connections required)
    python test_bidirectional_sync.py

    # Run integration tests with live connections
    python test_bidirectional_sync.py --integration

    # Run specific test
    python test_bidirectional_sync.py --test fabric_to_snowflake

    # Verbose output
    python test_bidirectional_sync.py -v
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# TEST UTILITIES
# =============================================================================


class TestResult:
    """Container for test results."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.passed = False
        self.message = ""
        self.details: Dict[str, Any] = {}
        self.duration_seconds = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "details": self.details,
            "duration_seconds": self.duration_seconds,
        }


class TestRunner:
    """Runs and reports on sync tests."""

    def __init__(self, verbose: bool = False) -> None:
        self.verbose = verbose
        self.results: List[TestResult] = []

    def run_test(self, name: str, test_func) -> TestResult:
        """Run a single test and capture result."""
        result = TestResult(name)
        start_time = datetime.now()

        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🧪 Running: {name}")
            logger.info("=" * 60)

            passed, message, details = test_func()
            result.passed = passed
            result.message = message
            result.details = details

        except Exception as e:
            result.passed = False
            result.message = f"Test failed with exception: {str(e)}"
            result.details = {"exception": str(e)}
            logger.error(f"❌ Test exception: {e}")

        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        self.results.append(result)

        status = "✅ PASSED" if result.passed else "❌ FAILED"
        logger.info(f"{status}: {name}")
        logger.info(f"   Duration: {result.duration_seconds:.2f}s")
        logger.info(f"   Message: {result.message}")

        return result

    def print_summary(self) -> None:
        """Print test summary."""
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total = len(self.results)

        logger.info("\n" + "=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total:  {total}")
        logger.info(f"Passed: {passed} ✅")
        logger.info(f"Failed: {failed} ❌")
        logger.info("=" * 60)

        if failed > 0:
            logger.info("\nFailed Tests:")
            for r in self.results:
                if not r.passed:
                    logger.info(f"  - {r.name}: {r.message}")

    def save_results(self, filepath: str) -> None:
        """Save results to JSON file."""
        output = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.passed),
                "failed": sum(1 for r in self.results if not r.passed),
            },
            "results": [r.to_dict() for r in self.results],
        }

        with open(filepath, "w") as f:
            json.dump(output, f, indent=2)

        logger.info(f"\n📄 Results saved to: {filepath}")


# =============================================================================
# MOCK-BASED TESTS (No live connections required)
# =============================================================================


def test_change_detection_snapshot_capture() -> Tuple[bool, str, Dict]:
    """
    Test: Verify snapshot capture works correctly.
    
    Steps:
    1. Create mock Fabric model data
    2. Capture snapshot using ChangeDetector
    3. Verify snapshot contains all expected tables/measures
    """
    from change_detector import ChangeDetector, SourceSystem
    from mock_data import MockFabricClient, get_sample_fabric_model

    # Setup
    mock_client = MockFabricClient()
    detector = ChangeDetector(fabric_client=mock_client)

    # Execute
    model_data = get_sample_fabric_model()
    snapshot = detector.capture_fabric_snapshot("model-001", model_data)

    # Verify
    if snapshot is None:
        return False, "Failed to capture snapshot", {}

    if snapshot.source != SourceSystem.FABRIC:
        return False, f"Wrong source: {snapshot.source}", {}

    if len(snapshot.tables) != 2:
        return False, f"Expected 2 tables, got {len(snapshot.tables)}", {}

    # Check measures were captured
    all_measures = snapshot.get_all_measures()
    if len(all_measures) != 5:  # 4 in Sales + 1 in Products
        return False, f"Expected 5 measures, got {len(all_measures)}", {}

    return True, "Snapshot captured successfully with all measures", {
        "tables": len(snapshot.tables),
        "measures": len(all_measures),
        "model_name": snapshot.model_name,
    }


def test_change_detection_measure_modification() -> Tuple[bool, str, Dict]:
    """
    Test: Verify change detection identifies modified measures.
    
    Steps:
    1. Capture snapshot of original model
    2. Capture snapshot of modified model (TotalRevenue changed)
    3. Compare snapshots
    4. Verify modification is detected
    """
    from change_detector import ChangeDetector, ChangeType
    from mock_data import (
        get_sample_fabric_model,
        get_sample_fabric_model_modified,
    )

    detector = ChangeDetector()

    # Capture before snapshot
    original = get_sample_fabric_model()
    snapshot_before = detector.capture_fabric_snapshot("model-001", original)

    # Capture after snapshot (with modification)
    modified = get_sample_fabric_model_modified()
    snapshot_after = detector.capture_fabric_snapshot("model-001-modified", modified)

    if not snapshot_before or not snapshot_after:
        return False, "Failed to capture snapshots", {}

    # Compare
    report = detector.compare_snapshots(snapshot_before, snapshot_after)

    # Verify changes detected
    measure_changes = report.get_measure_changes()

    # Should detect: TotalRevenue modified + RevenueWithTax added
    modified_measures = [
        c for c in measure_changes if c.change_type == ChangeType.MODIFIED
    ]
    added_measures = [
        c for c in measure_changes if c.change_type == ChangeType.ADDED
    ]

    if len(modified_measures) != 1:
        return False, f"Expected 1 modified measure, got {len(modified_measures)}", {}

    if modified_measures[0].item_name != "TotalRevenue":
        return False, f"Expected TotalRevenue to be modified", {}

    if len(added_measures) != 1:
        return False, f"Expected 1 added measure, got {len(added_measures)}", {}

    return True, "Correctly detected measure modification and addition", {
        "modified": [c.item_name for c in modified_measures],
        "added": [c.item_name for c in added_measures],
        "total_changes": len(measure_changes),
    }


def test_change_report_generation() -> Tuple[bool, str, Dict]:
    """
    Test: Verify change report is generated correctly.
    
    Steps:
    1. Detect changes between original and modified models
    2. Generate formatted report
    3. Verify report contains expected information
    """
    from change_detector import ChangeDetector
    from mock_data import (
        get_sample_fabric_model,
        get_sample_fabric_model_modified,
    )

    detector = ChangeDetector()

    # Capture snapshots
    original = get_sample_fabric_model()
    modified = get_sample_fabric_model_modified()
    
    snapshot_before = detector.capture_fabric_snapshot("model-001", original)
    snapshot_after = detector.capture_fabric_snapshot("model-001", modified)

    if not snapshot_before or not snapshot_after:
        return False, "Failed to capture snapshots", {}

    # Generate report
    report = detector.compare_snapshots(snapshot_before, snapshot_after)
    formatted = report.format_report()

    # Verify report content
    checks = [
        ("CHANGE DETECTION REPORT" in formatted, "Missing report header"),
        ("MODIFIED:" in formatted, "Missing MODIFIED section"),
        ("TotalRevenue" in formatted, "Missing TotalRevenue change"),
        ("SUMMARY:" in formatted, "Missing summary section"),
    ]

    for check, error_msg in checks:
        if not check:
            return False, error_msg, {"report": formatted}

    return True, "Change report generated successfully", {
        "summary": report.summary,
        "report_length": len(formatted),
    }


def test_fabric_to_snowflake_mock_sync() -> Tuple[bool, str, Dict]:
    """
    Test: Simulate Fabric → Snowflake sync with mock data.
    
    Steps:
    1. Setup mock Fabric client and Snowflake connector
    2. Detect changes (Fabric has new/modified measure)
    3. Simulate sync to Snowflake
    4. Verify sync would apply correct changes
    """
    from change_detector import ChangeDetector, ChangeType
    from mock_data import (
        MockFabricClient,
        MockSnowflakeConnector,
        get_sample_fabric_model_modified,
        get_sample_snowflake_view_data,
    )

    # Setup mocks
    fabric_client = MockFabricClient(use_modified=True)
    snowflake_connector = MockSnowflakeConnector()

    detector = ChangeDetector(
        fabric_client=fabric_client,
        snowflake_connector=snowflake_connector,
    )

    # Capture Fabric state (modified)
    fabric_data = get_sample_fabric_model_modified()
    fabric_snapshot = detector.capture_fabric_snapshot("model-001", fabric_data)

    # Capture Snowflake state (original)
    snowflake_data = get_sample_snowflake_view_data()
    snowflake_snapshot = detector.capture_snowflake_snapshot(
        "SV_SALESMODEL_SALES",
        snowflake_data,
    )

    if not fabric_snapshot or not snowflake_snapshot:
        return False, "Failed to capture snapshots", {}

    # Detect changes (what differs between Fabric and Snowflake)
    report = detector.compare_snapshots(snowflake_snapshot, fabric_snapshot)

    # Verify changes would sync
    if not report.has_changes():
        return False, "No changes detected, but expected differences", {}

    measure_changes = report.get_measure_changes()
    if len(measure_changes) == 0:
        return False, "Expected measure changes to be detected", {}

    return True, "Fabric to Snowflake sync simulation successful", {
        "changes_detected": len(measure_changes),
        "summary": report.summary,
    }


def test_snowflake_to_fabric_mock_sync() -> Tuple[bool, str, Dict]:
    """
    Test: Simulate Snowflake → Fabric sync with mock data.
    
    Steps:
    1. Setup mock connectors
    2. Detect changes (Snowflake has modified measure)
    3. Simulate sync to Fabric
    4. Verify sync would apply correct changes
    """
    from change_detector import ChangeDetector
    from mock_data import (
        MockFabricClient,
        MockSnowflakeConnector,
        get_sample_fabric_model,
        get_sample_snowflake_view_modified,
    )

    # Setup mocks
    fabric_client = MockFabricClient(use_modified=False)  # Original Fabric
    snowflake_connector = MockSnowflakeConnector(use_modified=True)  # Modified SF

    detector = ChangeDetector(
        fabric_client=fabric_client,
        snowflake_connector=snowflake_connector,
    )

    # Capture Fabric state (original)
    fabric_data = get_sample_fabric_model()
    fabric_snapshot = detector.capture_fabric_snapshot("model-001", fabric_data)

    # Capture Snowflake state (modified)
    snowflake_data = get_sample_snowflake_view_modified()
    snowflake_snapshot = detector.capture_snowflake_snapshot(
        "SV_SALESMODEL_SALES",
        snowflake_data,
    )

    if not fabric_snapshot or not snowflake_snapshot:
        return False, "Failed to capture snapshots", {}

    # Detect changes (what differs between Snowflake and Fabric)
    report = detector.compare_snapshots(fabric_snapshot, snowflake_snapshot)

    # Verify changes would sync
    if not report.has_changes():
        return False, "No changes detected, but expected differences", {}

    return True, "Snowflake to Fabric sync simulation successful", {
        "changes_detected": report.summary["total"],
        "summary": report.summary,
    }


def test_bidirectional_change_detection() -> Tuple[bool, str, Dict]:
    """
    Test: Verify bidirectional change detection works.
    
    Steps:
    1. Setup both systems with different changes
    2. Run bidirectional detection
    3. Verify both directions report correct changes
    """
    from change_detector import ChangeDetector
    from mock_data import (
        get_sample_fabric_model_modified,
        get_sample_snowflake_view_modified,
    )

    detector = ChangeDetector()

    # Fabric has its modifications
    fabric_data = get_sample_fabric_model_modified()
    fabric_snapshot = detector.capture_fabric_snapshot("model-001", fabric_data)

    # Snowflake has different modifications
    snowflake_data = get_sample_snowflake_view_modified()
    snowflake_snapshot = detector.capture_snowflake_snapshot(
        "SV_SALESMODEL_SALES",
        snowflake_data,
    )

    if not fabric_snapshot or not snowflake_snapshot:
        return False, "Failed to capture snapshots", {}

    # Run bidirectional detection
    fabric_to_sf, sf_to_fabric = detector.detect_changes_bidirectional(
        fabric_snapshot,
        snowflake_snapshot,
    )

    # Verify both directions have changes
    details = {
        "fabric_to_snowflake_changes": fabric_to_sf.summary["total"],
        "snowflake_to_fabric_changes": sf_to_fabric.summary["total"],
    }

    # Both should detect differences
    if fabric_to_sf.summary["total"] == 0 and sf_to_fabric.summary["total"] == 0:
        return False, "Expected changes in at least one direction", details

    return True, "Bidirectional change detection successful", details


def test_expression_conversion() -> Tuple[bool, str, Dict]:
    """
    Test: Verify DAX ↔ SQL expression conversion.
    
    Steps:
    1. Test DAX to SQL conversion
    2. Test SQL to DAX conversion
    3. Verify common patterns are converted correctly
    """
    from fabric_snowflake_sync import SemanticSyncEngine, SyncDirection

    engine = SemanticSyncEngine(SyncDirection.BIDIRECTIONAL)

    # Test cases: (dax, expected_sql)
    dax_to_sql_cases = [
        ("SUM([Amount])", "SUM([Amount])"),
        ("AVERAGE([Amount])", "AVG([Amount])"),
        ("COUNTROWS([Sales])", "COUNT(*[Sales])"),
        ("DISTINCTCOUNT([CustomerID])", "COUNT(DISTINCT [CustomerID])"),
    ]

    # Test cases: (sql, expected_dax)
    sql_to_dax_cases = [
        ("AVG(AMOUNT)", "AVERAGE(AMOUNT)"),
        ("COUNT(*)", "COUNTROWS()"),
        ("COUNT(DISTINCT CUSTOMER_ID)", "DISTINCTCOUNT(CUSTOMER_ID)"),
    ]

    results = {"dax_to_sql": [], "sql_to_dax": []}
    all_passed = True

    # Test DAX to SQL
    for dax, expected in dax_to_sql_cases:
        result = engine._convert_dax_to_sql(dax)
        passed = result == expected
        results["dax_to_sql"].append({
            "input": dax,
            "expected": expected,
            "got": result,
            "passed": passed,
        })
        if not passed:
            all_passed = False

    # Test SQL to DAX
    for sql, expected in sql_to_dax_cases:
        result = engine._convert_sql_to_dax(sql)
        passed = result == expected
        results["sql_to_dax"].append({
            "input": sql,
            "expected": expected,
            "got": result,
            "passed": passed,
        })
        if not passed:
            all_passed = False

    if not all_passed:
        failed = [
            r for r in results["dax_to_sql"] + results["sql_to_dax"]
            if not r["passed"]
        ]
        return False, f"Some conversions failed: {len(failed)}", results

    return True, "All expression conversions passed", results


def test_snapshot_serialization() -> Tuple[bool, str, Dict]:
    """
    Test: Verify snapshots can be serialized and deserialized.
    
    Steps:
    1. Create snapshot
    2. Serialize to JSON
    3. Deserialize from JSON
    4. Verify equality
    """
    from change_detector import ChangeDetector, SchemaSnapshot
    from mock_data import get_sample_fabric_model
    import tempfile
    import os

    detector = ChangeDetector()

    # Create snapshot
    model_data = get_sample_fabric_model()
    original_snapshot = detector.capture_fabric_snapshot("model-001", model_data)

    if not original_snapshot:
        return False, "Failed to create snapshot", {}

    # Serialize to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_path = f.name
        f.write(original_snapshot.to_json())

    try:
        # Deserialize
        loaded = detector.load_snapshot(temp_path)

        if loaded is None:
            return False, "Failed to load snapshot", {}

        # Verify key attributes match
        checks = [
            (loaded.model_name == original_snapshot.model_name, "model_name mismatch"),
            (loaded.model_id == original_snapshot.model_id, "model_id mismatch"),
            (len(loaded.tables) == len(original_snapshot.tables), "tables count mismatch"),
        ]

        for check, error in checks:
            if not check:
                return False, error, {}

        return True, "Snapshot serialization/deserialization successful", {
            "tables": len(loaded.tables),
            "measures": len(loaded.get_all_measures()),
        }

    finally:
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)


# =============================================================================
# INTEGRATION TESTS (Require live connections)
# =============================================================================


def test_fabric_connection_integration() -> Tuple[bool, str, Dict]:
    """
    Integration Test: Verify connection to Fabric API.
    
    Requires valid .env configuration.
    """
    from fabric_snowflake_sync import FabricApiClient

    client = FabricApiClient()

    if not client.tenant_id or not client.client_id:
        return False, "Missing Fabric credentials in .env", {}

    if client.authenticate():
        models = client.get_semantic_models()
        return True, f"Connected to Fabric, found {len(models)} models", {
            "models_count": len(models),
        }

    return False, "Failed to authenticate with Fabric", {}


def test_snowflake_connection_integration() -> Tuple[bool, str, Dict]:
    """
    Integration Test: Verify connection to Snowflake.
    
    Requires valid .env configuration.
    """
    from fabric_snowflake_sync import SnowflakeConnector

    connector = SnowflakeConnector()

    if not connector.account or not connector.user:
        return False, "Missing Snowflake credentials in .env", {}

    if connector.connect():
        views = connector.get_semantic_views()
        connector.disconnect()
        return True, f"Connected to Snowflake, found {len(views)} views", {
            "views_count": len(views),
        }

    return False, "Failed to connect to Snowflake", {}


def test_full_sync_integration() -> Tuple[bool, str, Dict]:
    """
    Integration Test: Run full bidirectional sync.
    
    Requires live connections to both Fabric and Snowflake.
    """
    from fabric_snowflake_sync import SemanticSyncEngine, SyncDirection

    engine = SemanticSyncEngine(SyncDirection.BIDIRECTIONAL)
    results = engine.run_sync()

    if results["status"] == "COMPLETED_SUCCESSFULLY":
        return True, "Full sync completed successfully", results

    if results["status"] == "COMPLETED_WITH_WARNINGS":
        return True, "Sync completed with warnings", results

    return False, f"Sync failed: {results.get('error', 'Unknown error')}", results


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================


def run_mock_tests(runner: TestRunner) -> None:
    """Run all mock-based tests."""
    logger.info("\n" + "=" * 60)
    logger.info("RUNNING MOCK TESTS (No live connections required)")
    logger.info("=" * 60 + "\n")

    mock_tests = [
        ("Change Detection: Snapshot Capture", test_change_detection_snapshot_capture),
        ("Change Detection: Measure Modification", test_change_detection_measure_modification),
        ("Change Report Generation", test_change_report_generation),
        ("Fabric → Snowflake Mock Sync", test_fabric_to_snowflake_mock_sync),
        ("Snowflake → Fabric Mock Sync", test_snowflake_to_fabric_mock_sync),
        ("Bidirectional Change Detection", test_bidirectional_change_detection),
        ("Expression Conversion (DAX ↔ SQL)", test_expression_conversion),
        ("Snapshot Serialization", test_snapshot_serialization),
    ]

    for name, test_func in mock_tests:
        runner.run_test(name, test_func)


def run_integration_tests(runner: TestRunner) -> None:
    """Run integration tests with live connections."""
    logger.info("\n" + "=" * 60)
    logger.info("RUNNING INTEGRATION TESTS (Require live connections)")
    logger.info("=" * 60 + "\n")

    integration_tests = [
        ("Fabric Connection", test_fabric_connection_integration),
        ("Snowflake Connection", test_snowflake_connection_integration),
        ("Full Bidirectional Sync", test_full_sync_integration),
    ]

    for name, test_func in integration_tests:
        runner.run_test(name, test_func)


def main() -> int:
    """Main entry point for test script."""
    parser = argparse.ArgumentParser(
        description="Bidirectional Sync Test Script"
    )
    parser.add_argument(
        "--integration",
        action="store_true",
        help="Run integration tests with live connections",
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=["fabric_to_snowflake", "snowflake_to_fabric", "all"],
        default="all",
        help="Specific test to run",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="test_results.json",
        help="Output file for test results",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = TestRunner(verbose=args.verbose)

    # Always run mock tests
    run_mock_tests(runner)

    # Run integration tests if requested
    if args.integration:
        run_integration_tests(runner)

    # Print summary and save results
    runner.print_summary()
    runner.save_results(args.output)

    # Return exit code based on results
    failed = sum(1 for r in runner.results if not r.passed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
