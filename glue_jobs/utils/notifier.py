import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SNS_SUBJECT_MAX_LENGTH = 100


class SnsNotifier:

    def __init__(self, topicArn: str, environment: str):
        self._topicArn    = topicArn
        self._environment = environment
        self._client      = boto3.client("sns")

    def sendJobStarted(self, jobName: str, stageName: str) -> None:
        self._publish(
            subject=f"[{self._environment}] {jobName} — STARTED: {stageName}",
            message=f"Stage '{stageName}' started in job '{jobName}'.",
        )

    def sendJobSucceeded(self, jobName: str, stageName: str, elapsed: float) -> None:
        self._publish(
            subject=f"[{self._environment}] {jobName} — SUCCESS: {stageName}",
            message=f"Stage '{stageName}' completed in {elapsed:.1f}s.",
        )

    def sendJobFailed(self, jobName: str, stageName: str, error: Exception) -> None:
        self._publish(
            subject=f"[{self._environment}] {jobName} — FAILED: {stageName}",
            message=f"Stage '{stageName}' FAILED.\nError: {error}",
        )

    def _publish(self, subject: str, message: str) -> None:
        try:
            self._client.publish(
                TopicArn=self._topicArn,
                Subject=subject[:SNS_SUBJECT_MAX_LENGTH],
                Message=message,
            )
        except ClientError as exc:
            logger.error("SNS publish failed — %s", exc)
