import logging

from django.contrib.auth import get_user_model
from django.utils import timezone
from procrastinate.contrib.django import app

from apps.common.middleware.thread_local import write_current_user, delete_current_user
from apps.import_app.models import ImportRun
from apps.import_app.services import ImportServiceV1

logger = logging.getLogger(__name__)


@app.task(name="process_import")
def process_import(import_run_id: int, file_path: str, user_id: int):
    if user_id:
        user = get_user_model().objects.filter(id=user_id).first()
        if user is not None:
            write_current_user(user)

    try:
        import_run = ImportRun.objects.get(id=import_run_id)
        import_service = ImportServiceV1(import_run)
        # The run's stored path wins when the task is re-deferred by the
        # recovery periodic job (which does not know the original argument).
        path = file_path or import_run.stored_file_path
        # When another worker owns a fresh lease, process_file exits as a
        # no-op without touching rows or files.
        import_service.process_file(path)
    except ImportRun.DoesNotExist:
        raise ValueError(f"ImportRun with id {import_run_id} not found")
    finally:
        delete_current_user()


@app.periodic(cron="*/5 * * * *")
@app.task(
    name="recover_stale_import_runs",
    queueing_lock="recover_stale_import_runs",
)
def recover_stale_import_runs(timestamp=None) -> dict:
    """Re-defer processing for runs stuck with an expired lease."""
    now = timezone.now()
    stale_runs = list(
        ImportRun.objects.filter(
            status=ImportRun.Status.PROCESSING,
            lease_expires_at__isnull=False,
            lease_expires_at__lt=now,
        )
    )

    recovered = 0
    for run in stale_runs:
        try:
            process_import.defer(
                import_run_id=run.id,
                file_path=run.stored_file_path,
                user_id=run.requested_by_id,
                queueing_lock=f"import-recover-run-{run.id}",
            )
            recovered += 1
        except Exception:
            # AlreadyEnqueued etc.: a recovery job is pending already.
            logger.debug(
                "Could not re-defer recovery for ImportRun %s",
                run.id,
                exc_info=True,
            )

    logger.info("recover_stale_import_runs recovered %s run(s)", recovered)
    return {"candidates": len(stale_runs), "recovered": recovered}


@app.periodic(cron="30 3 * * *")
@app.task(
    name="cleanup_orphan_temp_files",
    queueing_lock="cleanup_orphan_temp_files",
)
def cleanup_orphan_temp_files(timestamp=None) -> dict:
    """Delete unreferenced files left behind in the import temp directory."""
    import os
    import time

    from apps.import_app.services.enqueue import (
        DEFAULT_TEMP_DIR,
        ORPHAN_GRACE_PERIOD_SECONDS,
    )

    temp_dir = DEFAULT_TEMP_DIR
    if not os.path.isdir(temp_dir):
        return {"scanned": 0, "removed": 0}

    referenced: set[str] = set()
    active_statuses = (
        ImportRun.Status.QUEUED,
        ImportRun.Status.PROCESSING,
        ImportRun.Status.FAILED,
    )
    for path in (
        ImportRun.objects.filter(status__in=active_statuses)
        .exclude(stored_file_path="")
        .values_list("stored_file_path", flat=True)
    ):
        if path:
            referenced.add(os.path.abspath(path))

    cutoff = time.time() - ORPHAN_GRACE_PERIOD_SECONDS
    scanned = removed = 0
    for entry in os.listdir(temp_dir):
        file_path = os.path.abspath(os.path.join(temp_dir, entry))
        if not os.path.isfile(file_path):
            continue
        scanned += 1
        if file_path in referenced:
            continue
        try:
            if os.path.getmtime(file_path) >= cutoff:
                continue
            os.remove(file_path)
            removed += 1
        except OSError:
            logger.debug(
                "Failed to remove orphan temp file %s", file_path, exc_info=True
            )

    logger.info(
        "cleanup_orphan_temp_files scanned %s removed %s", scanned, removed
    )
    return {"scanned": scanned, "removed": removed}
