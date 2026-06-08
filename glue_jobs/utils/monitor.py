"""
monitor.py — Stage timing + alerting helper for the Glue ETL jobs.

PipelineMonitor wraps each pipeline stage in a context manager that logs
start/success/failure to CloudWatch and records elapsed time.

Notification policy — FAILURE ONLY:
    The monitor forwards an event to the optional notifier *only when a stage
    fails*. Per-stage start/success notifications are deliberately NOT sent:
    a run touches five stages across three jobs, so notifying on every
    start+success would publish ~30 SNS messages per healthy run and bury the
    one message that matters (the failure) under alert fatigue. Pipeline-level
    success is announced once by the Step Functions NotifySuccess state, so the
    job-level channel is reserved for the precise failing stage + error.
"""

import time
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SECTION_LINE = "─" * 56
SUMMARY_LINE = "═" * 56
STAGE_WIDTH = 44


class PipelineMonitor:

    def __init__(self, job_name, notifier=None):
        self._job_name = job_name
        self._notifier = notifier
        self._stage_timings = {}

    @contextmanager
    def stage(self, stage_name):
        logger.info("\n%s", SECTION_LINE)
        logger.info("  [START] %s | job=%s", stage_name, self._job_name)
        logger.info("%s", SECTION_LINE)

        start_time = time.time()
        try:
            yield
            elapsed = time.time() - start_time
            self._stage_timings[stage_name] = elapsed
            logger.info("  [SUCCESS] %s — %.1fs | job=%s", stage_name, elapsed, self._job_name)

        except Exception as error:
            elapsed = time.time() - start_time
            # logger.exception captures the full traceback in CloudWatch, which
            # the SNS alert (subject + message only) cannot carry.
            logger.exception("  [FAILED] %s — %.1fs | job=%s", stage_name, elapsed, self._job_name)

            if self._notifier:
                self._notifier.send_job_failed(self._job_name, stage_name, error)
            raise

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
