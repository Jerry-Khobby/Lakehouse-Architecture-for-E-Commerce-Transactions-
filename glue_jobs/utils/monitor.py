import time
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

SECTION_LINE = "─" * 56
SUMMARY_LINE = "═" * 56
STAGE_WIDTH = 44


class PipelineMonitor:

    def __init__(self, jobName, notifier=None):
        self._jobName = jobName
        self._notifier = notifier
        self._stageTimings = {}

    @contextmanager
    def stage(self, stageName):
        logger.info("\n%s", SECTION_LINE)
        logger.info("  [START] %s | job=%s", stageName, self._jobName)
        logger.info("%s", SECTION_LINE)

        if self._notifier:
            self._notifier.sendJobStarted(self._jobName, stageName)

        startTime = time.time()
        try:
            yield
            elapsed = time.time() - startTime
            self._stageTimings[stageName] = elapsed
            logger.info("  [SUCCESS] %s — %.1fs | job=%s", stageName, elapsed, self._jobName)

            if self._notifier:
                self._notifier.sendJobSucceeded(self._jobName, stageName, elapsed)

        except Exception as error:
            elapsed = time.time() - startTime
            logger.error("  [FAILED] %s — %.1fs | job=%s | reason=%s", stageName, elapsed, self._jobName, error)

            if self._notifier:
                self._notifier.sendJobFailed(self._jobName, stageName, error)
            raise

    def logSummary(self):
        logger.info("\n%s", SUMMARY_LINE)
        logger.info("  %s — all stages complete", self._jobName)
        logger.info("%s", SECTION_LINE)
        for stageName, elapsed in self._stageTimings.items():
            logger.info("    %-*s %6.1fs", STAGE_WIDTH, stageName, elapsed)
        total = sum(self._stageTimings.values())
        logger.info("%s", SECTION_LINE)
        logger.info("    %-*s %6.1fs", STAGE_WIDTH, "Total", total)
        logger.info("%s", SUMMARY_LINE)

        if self._notifier:
            self._notifier.sendJobSucceeded(self._jobName, "All stages complete", total)
