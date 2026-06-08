"""
notifier.py — SNS notifier used by PipelineMonitor to publish stage-level
START / SUCCESS / FAILURE events to the pipeline alerts topic. Naming follows
PEP 8 snake_case to stay consistent with the rest of glue_jobs/.
"""

import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SNS_SUBJECT_MAX_LENGTH = 100


class SnsNotifier:

    def __init__(self, topic_arn: str, environment: str):
        self._topic_arn = topic_arn
        self._environment = environment
        self._client = boto3.client("sns")

    def send_job_started(self, job_name: str, stage_name: str) -> None:
        self._publish(
            subject=f"[{self._environment}] {job_name} — STARTED: {stage_name}",
            message=f"Stage '{stage_name}' started in job '{job_name}'.",
        )

    def send_job_succeeded(self, job_name: str, stage_name: str, elapsed: float) -> None:
        self._publish(
            subject=f"[{self._environment}] {job_name} — SUCCESS: {stage_name}",
            message=f"Stage '{stage_name}' completed in {elapsed:.1f}s.",
        )

    def send_job_failed(self, job_name: str, stage_name: str, error: Exception) -> None:
        self._publish(
            subject=f"[{self._environment}] {job_name} — FAILED: {stage_name}",
            message=f"Stage '{stage_name}' FAILED.\nError: {error}",
        )

    def _publish(self, subject: str, message: str) -> None:
        try:
            self._client.publish(
                TopicArn=self._topic_arn,
                Subject=subject[:SNS_SUBJECT_MAX_LENGTH],
                Message=message,
            )
        except ClientError as exc:
            logger.error("SNS publish failed — %s", exc)
