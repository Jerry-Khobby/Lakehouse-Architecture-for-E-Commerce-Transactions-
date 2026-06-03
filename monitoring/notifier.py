import json
import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECS = 10
MAX_ERROR_CHARS      = 500


def resolveWebhookUrl(argv):
    for i, arg in enumerate(argv):
        if arg == "--slack_webhook_url" and i + 1 < len(argv):
            return argv[i + 1]
    return os.environ.get("SLACK_APP_WEBHOOK_URL")


class SlackNotifier:

    def __init__(self, webhookUrl):
        self._webhookUrl = webhookUrl

    # ── PUBLIC METHODS ────────────────────────────────────────────────────────

    def sendJobStarted(self, jobName, stage):
        self._post(self._buildStatusMessage(
            emoji   = ":hourglass_flowing_sand:",
            color   = "#A0A0A0",
            status  = "In Progress",
            jobName = jobName,
            stage   = stage,
            message = "Job has started running."
        ))

    def sendJobSucceeded(self, jobName, stage, durationSecs=None):
        extra = f"Completed in `{durationSecs:.1f}s`." if durationSecs else "Completed successfully."
        self._post(self._buildStatusMessage(
            emoji   = ":white_check_mark:",
            color   = "#36A64F",
            status  = "Succeeded",
            jobName = jobName,
            stage   = stage,
            message = extra
        ))

    def sendJobFailed(self, jobName, stage, error):
        self._post(self._buildFailureMessage(jobName, stage, error))

    def sendJobCancelled(self, jobName, stage):
        self._post(self._buildStatusMessage(
            emoji   = ":no_entry_sign:",
            color   = "#E8A838",
            status  = "Cancelled",
            jobName = jobName,
            stage   = stage,
            message = "Job was cancelled before completion."
        ))

    def sendJobCaughtError(self, jobName, stage, error):
        self._post(self._buildStatusMessage(
            emoji   = ":warning:",
            color   = "#E8A838",
            status  = "Caught Error — Retrying",
            jobName = jobName,
            stage   = stage,
            message = f"Non-fatal error caught: `{str(error)[:200]}`"
        ))

    def sendPipelineStarted(self, executionId):
        timestamp = self._now()
        self._post({
            "text": "Music Streaming Pipeline — Started",
            "attachments": [{
                "color": "#A0A0A0",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":rocket: *Music Streaming Pipeline — Started*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
                            {"type": "mrkdwn", "text": f"*Time:*\n{timestamp}"},
                        ]
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": "Crawler → Validate → Transform → KPIs → DynamoDB → Archive"}
                        ]
                    }
                ]
            }]
        })

    def sendPipelineSucceeded(self, executionId, filesProcessed=None, durationSecs=None):
        timestamp = self._now()
        fields = [
            {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
            {"type": "mrkdwn", "text": f"*Completed:*\n{timestamp}"},
        ]
        if filesProcessed is not None:
            fields.append({"type": "mrkdwn", "text": f"*Files Processed:*\n`{filesProcessed}`"})
        if durationSecs is not None:
            fields.append({"type": "mrkdwn", "text": f"*Duration:*\n`{durationSecs:.0f}s`"})

        self._post({
            "text": "Music Streaming Pipeline — Succeeded",
            "attachments": [{
                "color": "#36A64F",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":large_green_circle: *Music Streaming Pipeline — Succeeded*\nAll KPIs computed and loaded to DynamoDB."
                        }
                    },
                    {"type": "section", "fields": fields}
                ]
            }]
        })

    def sendPipelineFailed(self, executionId, failedStep, error):
        timestamp = self._now()
        self._post({
            "text": "Music Streaming Pipeline — FAILED",
            "attachments": [{
                "color": "#E01E5A",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":red_circle: *Music Streaming Pipeline — FAILED*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Failed Step:*\n`{failedStep}`"},
                            {"type": "mrkdwn", "text": f"*Execution:*\n`{executionId[-12:]}`"},
                            {"type": "mrkdwn", "text": f"*Time:*\n{timestamp}"},
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Error:*\n```{str(error)[:MAX_ERROR_CHARS]}```"
                        }
                    }
                ]
            }]
        })

    # ── PRIVATE HELPERS ───────────────────────────────────────────────────────

    def _buildStatusMessage(self, emoji, color, status, jobName, stage, message):
        timestamp = self._now()
        return {
            "text": f"{jobName} — {status}",
            "attachments": [{
                "color": color,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"{emoji} *{jobName}* — *{status}*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Stage:*\n`{stage}`"},
                            {"type": "mrkdwn", "text": f"*Time:*\n{timestamp}"},
                        ]
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": message}
                        ]
                    }
                ]
            }]
        }

    def _buildFailureMessage(self, jobName, stage, error):
        timestamp = self._now()
        return {
            "text": f"{jobName} — Failed",
            "attachments": [{
                "color": "#E01E5A",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f":red_circle: *{jobName}* — *Failed*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Stage:*\n`{stage}`"},
                            {"type": "mrkdwn", "text": f"*Time:*\n{timestamp}"},
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Error:*\n```{str(error)[:MAX_ERROR_CHARS]}```"
                        }
                    }
                ]
            }]
        }

    def _now(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _post(self, message):
        if not self._webhookUrl:
            logger.warning("Slack webhook URL not set — skipping notification")
            return
        try:
            response = requests.post(
                self._webhookUrl,
                data=json.dumps(message),
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT_SECS,
            )
            response.raise_for_status()
        except requests.RequestException as error:
            logger.error(f"Slack alert could not be delivered: {error}")