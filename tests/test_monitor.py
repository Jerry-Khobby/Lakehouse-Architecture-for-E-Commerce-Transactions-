"""
Unit tests for PipelineMonitor (glue_jobs.utils.monitor).

The monitor wraps each stage in a context manager that times the stage, logs
the outcome, and forwards a live START + SUCCESS/FAILURE feed to an optional
notifier. The stage() context manager yields a StageReport the caller fills
with metrics that are rendered into the SUCCESS notification.
"""

from unittest.mock import MagicMock

import pytest

from glue_jobs.utils.monitor import PipelineMonitor, StageReport


def test_stage_notifies_start_and_success():
    # Live-feed policy: every stage publishes START on entry and SUCCESS on a
    # clean exit, so operators watch the run unfold instead of waiting for the
    # whole job to finish or break.
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Read") as report:
        assert isinstance(report, StageReport)

    notifier.send_job_started.assert_called_once_with("test-job", "Read")
    assert notifier.send_job_succeeded.call_count == 1
    notifier.send_job_failed.assert_not_called()
    assert "Read" in monitor._stage_timings


def test_recorded_metrics_are_passed_to_success_notification():
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Validate") as report:
        report.record(read=1000, valid=998, rejected=2)

    job_name, stage_name, elapsed, detail = notifier.send_job_succeeded.call_args[0]
    assert job_name == "test-job"
    assert stage_name == "Validate"
    assert elapsed >= 0
    assert detail == "read=1000 | valid=998 | rejected=2"


def test_success_detail_is_empty_when_no_metrics_recorded():
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Archive") as report:
        assert report.summary() == ""

    _, _, _, detail = notifier.send_job_succeeded.call_args[0]
    assert detail == ""


def test_stage_reraises_and_notifies_on_failure():
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with pytest.raises(ValueError):
        with monitor.stage("Validate"):
            raise ValueError("boom")

    notifier.send_job_started.assert_called_once_with("test-job", "Validate")
    job_name, stage_name, error = notifier.send_job_failed.call_args[0]
    assert job_name == "test-job"
    assert stage_name == "Validate"
    assert isinstance(error, ValueError)
    notifier.send_job_succeeded.assert_not_called()


def test_stage_works_without_a_notifier():
    monitor = PipelineMonitor("test-job")
    with monitor.stage("Read") as report:
        report.record(rows=5)
    assert monitor._stage_timings["Read"] >= 0


def test_log_summary_records_total_without_notifying():
    # log_summary writes the per-stage timing table to CloudWatch but does not
    # publish — pipeline-level success is announced once by the Step Functions
    # NotifySuccess state, not duplicated per job by log_summary.
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Read") as report:
        assert report.metrics == {}
    notifier.reset_mock()  # discard the stage's own START/SUCCESS calls

    monitor.log_summary()

    notifier.send_job_started.assert_not_called()
    notifier.send_job_succeeded.assert_not_called()
    notifier.send_job_failed.assert_not_called()
    assert "Read" in monitor._stage_timings


def test_stage_report_summary_renders_key_value_pairs():
    report = StageReport()
    report.record(rows=10)
    report.record(valid=9, rejected=1)
    assert report.summary() == "rows=10 | valid=9 | rejected=1"


def test_stage_report_summary_is_empty_without_metrics():
    assert StageReport().summary() == ""
