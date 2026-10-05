"""Manual retry support for failed / stale import runs.

Strict runs restart both phases from scratch (rows are rebuilt from the
staged source file); fault tolerant runs keep their terminal outcomes and
only re-queue retryable rows.
"""

import logging
import os

from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone

from apps.import_app.models import ImportRow, ImportRun

logger = logging.getLogger(__name__)


class RetryNotAvailable(Exception):
    """Raised when a run cannot be retried in its current state."""

    def __init__(self, code: str, message: str, http_status: int = 409):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(message)


def _source_path(run: ImportRun) -> str:
    return run.stored_file_path or ""


def is_retriable(run: ImportRun, *, now=None) -> bool:
    return run.is_retriable if now is None else _is_retriable_at(run, now)


def _is_retriable_at(run: ImportRun, now) -> bool:
    if run.status == ImportRun.Status.FAILED:
        return True
    # A PROCESSING run whose lease expired is a crashed worker.
    return (
        run.status == ImportRun.Status.PROCESSING
        and run.lease_expires_at is not None
        and run.lease_expires_at < now
    )


def retry_import_run(run: ImportRun, *, user_id: int | None) -> ImportRun:
    """Reset a run for retry and re-defer its processing task.

    Raises :class:`RetryNotAvailable` with an HTTP status when the run is
    not retriable or the staged source file is gone.
    """
    from apps.import_app.tasks import process_import

    if not is_retriable(run):
        raise RetryNotAvailable(
            "run_not_retriable",
            f"Run in status {run.status} cannot be retried.",
            http_status=409,
        )

    file_path = _source_path(run)
    if not file_path or not os.path.exists(file_path):
        raise RetryNotAvailable(
            "source_file_missing",
            "The staged source file no longer exists; the run cannot be retried.",
            http_status=409,
        )

    now = timezone.now()
    try:
        with transaction.atomic():
            # Conditional state transition: only a retriable run can flip
            # to QUEUED. The flip can also collide with the partial unique
            # constraint (a newer active run for the same profile + file
            # content already exists); IntegrityError rolls this whole
            # block back — including a strict run's row deletion — so no
            # inconsistent half-reset can be left behind.
            claimed = (
                ImportRun.objects.filter(pk=run.pk)
                .filter(
                    Q(status=ImportRun.Status.FAILED)
                    | Q(
                        status=ImportRun.Status.PROCESSING,
                        lease_expires_at__lt=now,
                    )
                )
                .update(status=ImportRun.Status.QUEUED)
            )
            if not claimed:
                raise RetryNotAvailable(
                    "run_not_retriable",
                    f"Run in status {run.status} cannot be retried.",
                    http_status=409,
                )

            update_fields = [
                "phase",
                "lease_owner",
                "lease_expires_at",
                "started_at",
                "finished_at",
                "cursor",
                "logs",
            ]
            run.status = ImportRun.Status.QUEUED
            run.phase = ImportRun.Phase.ENQUEUED
            run.lease_owner = None
            run.lease_expires_at = None
            run.started_at = None
            run.finished_at = None
            run.cursor = {}

            if run.mode == ImportRun.Mode.STRICT:
                # Deterministic two-phase restart: rebuild every row.
                run.rows.all().delete()
                run.total_rows = 0
                run.processed_rows = 0
                run.successful_rows = 0
                run.skipped_rows = 0
                run.failed_rows = 0
                run.retryable_rows = 0
                update_fields += [
                    "total_rows",
                    "processed_rows",
                    "successful_rows",
                    "skipped_rows",
                    "failed_rows",
                    "retryable_rows",
                ]
            else:
                # Keep COMMITTED / SKIPPED / FAILED_PERMANENT outcomes;
                # give transient failures another attempt. Attempt
                # counters are kept; fix counters for the moved rows
                # immediately (the worker reconciles the rest on commit).
                run.rows.filter(
                    status=ImportRow.Status.FAILED_RETRYABLE
                ).update(status=ImportRow.Status.STAGED, failure_reason=None)
                permanent = run.rows.filter(
                    status=ImportRow.Status.FAILED_PERMANENT
                ).count()
                run.failed_rows = permanent
                run.retryable_rows = 0
                update_fields += ["failed_rows", "retryable_rows"]

            if user_id is not None:
                run.requested_by_id = user_id
                update_fields.append("requested_by")
            run.logs = (run.logs or "") + (
                f"[{timezone.now():%Y-%m-%d %H:%M:%S}] INFO: Manual retry "
                "requested\n"
            )
            run.save(update_fields=update_fields)
    except IntegrityError as exc:
        raise RetryNotAvailable(
            "active_run_exists",
            "An active run for the same file already exists.",
            http_status=409,
        ) from exc

    process_import.defer(
        import_run_id=run.id,
        file_path=file_path,
        user_id=run.requested_by_id,
    )
    return run
