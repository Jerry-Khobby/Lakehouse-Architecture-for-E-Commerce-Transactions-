"""
Unit tests for PipelineMonitor (glue_jobs.utils.monitor).

The monitor wraps each stage in a context manager that times the stage, logs
the outcome, and forwards START/SUCCESS/FAILURE events to an optional notifier.
"""

from unittest.mock import MagicMock

import pytest

from glue_jobs.utils.monitor import PipelineMonitor


def test_stage_records_timing_and_stays_silent_on_success():
    # Notification policy is failure-only: a successful stage records its
    # timing but must NOT publish start/success notifications (avoids ~30
    # SNS messages per healthy pipeline run — see monitor.py module docstring).
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Read"):
        pass

    notifier.send_job_started.assert_not_called()
    notifier.send_job_succeeded.assert_not_called()
    notifier.send_job_failed.assert_not_called()
    assert "Read" in monitor._stage_timings


def test_stage_reraises_and_notifies_on_failure():
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with pytest.raises(ValueError):
        with monitor.stage("Validate"):
            raise ValueError("boom")

    job_name, stage_name, error = notifier.send_job_failed.call_args[0]
    assert job_name == "test-job"
    assert stage_name == "Validate"
    assert isinstance(error, ValueError)
    notifier.send_job_succeeded.assert_not_called()


def test_stage_works_without_a_notifier():
    monitor = PipelineMonitor("test-job")
    with monitor.stage("Read"):
        pass
    assert monitor._stage_timings["Read"] >= 0


def test_log_summary_records_total_without_notifying():
    # log_summary writes the per-stage timing table to CloudWatch but does not
    # publish a success notification — pipeline-level success is announced once
    # by the Step Functions NotifySuccess state, not per job.
    notifier = MagicMock()
    monitor = PipelineMonitor("test-job", notifier)

    with monitor.stage("Read"):
        pass
    monitor.log_summary()

    notifier.send_job_succeeded.assert_not_called()
    assert "Read" in monitor._stage_timings
