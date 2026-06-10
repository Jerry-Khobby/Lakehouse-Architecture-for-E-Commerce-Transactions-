"""
monitor.py — Stage timing + live progress alerting for the Glue ETL jobs.

PipelineMonitor wraps each pipeline stage in a context manager that logs
start/success/failure to CloudWatch, records elapsed time, and forwards a
progress event to the optional notifier at *every* stage boundary.

Notification policy — LIVE PER-STAGE FEED:
    Each stage publishes a START event on entry and a SUCCESS or FAILURE event
    on exit, so operators watch the run unfold in Slack instead of waiting for
    the whole job to finish or break. The stage() context manager yields a
    StageReport; the job records metrics on it (rows read, valid/rejected,
    rows merged, registered table) and those are rendered into the SUCCESS
    notification and the CloudWatch success line.

    Pipeline-level success is still announced once by the Step Functions
    NotifySuccess state — log_summary() writes the per-stage timing table to
    CloudWatch only and never publishes, so the per-job feed and the
    pipeline-level summary do not duplicate each other.
"""

import time
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SECTION_LINE = "─" * 56
SUMMARY_LINE = "═" * 56
STAGE_WIDTH = 44


class StageReport:
    """
    Mutable handle yielded by PipelineMonitor.stage().

    The stage body calls record() to attach metrics that are rendered into the
    SUCCESS notification and the CloudWatch success line. Keeping the metrics on
    a yielded handle lets the job decide what is worth reporting without the
    monitor having to know anything about the dataset.
    """

    def __init__(self):
        self.metrics = {}

    def record(self, **metrics) -> None:
        """Attach one or more key=value metrics to this stage."""
        self.metrics.update(metrics)

    def summary(self) -> str:
        """Render attached metrics as 'key=value | key=value' (empty if none)."""
        return " | ".join(f"{key}={value}" for key, value in self.metrics.items())


class PipelineMonitor:

    def __init__(self, job_name, notifier=None):
        self._job_name = job_name
        self._notifier = notifier
        self._stage_timings = {}

    @contextmanager
    def stage(self, stage_name):
        report = StageReport()

        logger.info("\n%s", SECTION_LINE)
        logger.info("  [START] %s | job=%s", stage_name, self._job_name)
        logger.info("%s", SECTION_LINE)
        self._notify_started(stage_name)

        start_time = time.time()
        try:
            yield report
            elapsed = time.time() - start_time
            self._stage_timings[stage_name] = elapsed

            detail = report.summary()
            suffix = f" | {detail}" if detail else ""
            logger.info(
                "  [SUCCESS] %s — %.1fs%s | job=%s",
                stage_name,
                elapsed,
                suffix,
                self._job_name,
            )
            self._notify_succeeded(stage_name, elapsed, detail)

        except Exception as error:
            elapsed = time.time() - start_time
            # logger.exception captures the full traceback in CloudWatch, which
            # the SNS alert (subject + message only) cannot carry.
            logger.exception("  [FAILED] %s — %.1fs | job=%s", stage_name, elapsed, self._job_name)
            self._notify_failed(stage_name, error)
            raise

    def _notify_started(self, stage_name):
        if self._notifier:
            self._notifier.send_job_started(self._job_name, stage_name)

    def _notify_succeeded(self, stage_name, elapsed, detail):
        if self._notifier:
            self._notifier.send_job_succeeded(self._job_name, stage_name, elapsed, detail)

    def _notify_failed(self, stage_name, error):
        if self._notifier:
            self._notifier.send_job_failed(self._job_name, stage_name, error)

    def log_summary(self):
        logger.info("\n%s", SUMMARY_LINE)
        logger.info("  %s — all stages complete", self._job_name)
        logger.info("%s", SECTION_LINE)
        for stage_name, elapsed in self._stage_timings.items():
            logger.info("    %-*s %6.1fs", STAGE_WIDTH, stage_name, elapsed)
        total = sum(self._stage_timings.values())
        logger.info("%s", SECTION_LINE)
        logger.info("    %-*s %6.1fs", STAGE_WIDTH, "Total", total)
        logger.info("%s", SUMMARY_LINE)
