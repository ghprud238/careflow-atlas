"""Unit tests for Schema Drift and Structural Evolution Detector."""

import pytest
from services.drift.drift_detector import DriftDetector


class TestDriftDetector:
    def setup_method(self):
        self.detector = DriftDetector()

    def test_schema_exact_match_proceeds(self):
        headers = [
            "OBS_ID", "PATIENT_ID", "ENCOUNTER_ID", "LOCAL_CODE", "DISPLAY_NAME",
            "VALUE", "UNIT", "EFFECTIVE_TIME", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION"
        ]
        report = self.detector.evaluate_headers("provider-b", "observations.csv", headers)
        assert report.severity == "none"
        assert report.recommended_action == "proceed"
        assert len(report.missing_required_columns) == 0
        assert len(report.unexpected_added_columns) == 0

    def test_unexpected_added_column_triggers_agent_investigation(self):
        headers = [
            "OBS_ID", "PATIENT_ID", "ENCOUNTER_ID", "LOCAL_CODE", "DISPLAY_NAME",
            "VALUE", "UNIT", "EFFECTIVE_TIME", "STATUS", "_CHANGE_SEQUENCE", "_OPERATION",
            "UNEXPECTED_TECHNICIAN_MEMO"
        ]
        report = self.detector.evaluate_headers("provider-b", "observations.csv", headers)
        assert report.severity == "warning"
        assert report.recommended_action == "investigate_agent"
        assert "UNEXPECTED_TECHNICIAN_MEMO" in report.unexpected_added_columns

    def test_missing_required_column_triggers_quarantine(self):
        # Missing VALUE and EFFECTIVE_TIME
        headers = [
            "OBS_ID", "PATIENT_ID", "LOCAL_CODE", "DISPLAY_NAME", "STATUS"
        ]
        report = self.detector.evaluate_headers("provider-b", "observations.csv", headers)
        assert report.severity == "critical"
        assert report.recommended_action == "quarantine_batch"
        assert "VALUE" in report.missing_required_columns
        assert "EFFECTIVE_TIME" in report.missing_required_columns
