"""
Unit tests for SnsNotifier (glue_jobs.utils.notifier).

boto3.client is patched at construction time so no real SNS client is created;
tests assert on the publish payload and that publish failures are swallowed
(notification delivery is non-fatal to the pipeline).
"""

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from glue_jobs.utils.notifier import SnsNotifier


def _make_notifier():
    mock_sns = MagicMock()
    with patch("boto3.client", return_value=mock_sns):
        notifier = SnsNotifier("arn:aws:sns:us-east-1:000000000000:test", "test")
    return notifier, mock_sns


def test_send_job_started_publishes_subject_and_message():
    notifier, mock_sns = _make_notifier()
    notifier.send_job_started("job", "Read")
    kwargs = mock_sns.publish.call_args[1]
    assert "STARTED: Read" in kwargs["Subject"]
    assert "Read" in kwargs["Message"]


def test_send_job_succeeded_includes_elapsed_seconds():
    notifier, mock_sns = _make_notifier()
    notifier.send_job_succeeded("job", "Validate", 12.3)
    kwargs = mock_sns.publish.call_args[1]
    assert "SUCCESS: Validate" in kwargs["Subject"]
    assert "12.3s" in kwargs["Message"]


def test_send_job_failed_includes_error_text():
    notifier, mock_sns = _make_notifier()
    notifier.send_job_failed("job", "Merge", RuntimeError("kaboom"))
    kwargs = mock_sns.publish.call_args[1]
    assert "FAILED: Merge" in kwargs["Subject"]
    assert "kaboom" in kwargs["Message"]


def test_subject_is_truncated_to_sns_limit():
    notifier, mock_sns = _make_notifier()
    notifier.send_job_started("job", "x" * 200)
    kwargs = mock_sns.publish.call_args[1]
    assert len(kwargs["Subject"]) <= 100


def test_publish_swallows_client_error():
    notifier, mock_sns = _make_notifier()
    mock_sns.publish.side_effect = ClientError(
        {"Error": {"Code": "AuthorizationError", "Message": "denied"}}, "Publish"
    )
    # Must not raise — a failed alert cannot fail the pipeline.
    notifier.send_job_started("job", "Read")
