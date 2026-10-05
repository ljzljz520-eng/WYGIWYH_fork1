"""Enqueue-side helpers for import runs.

Responsible for persisting source file summaries, config snapshots and the
active-run deduplication guard before the processing task is deferred.
"""

import hashlib
import logging
import os

import yaml
from django.core.files.storage import FileSystemStorage
from django.db import IntegrityError

from apps.import_app.models import ImportProfile, ImportRun
from apps.import_app.schemas import version_1

logger = logging.getLogger(__name__)

DEFAULT_TEMP_DIR = "/usr/src/app/temp"

# Files younger than this are never removed by the orphan cleanup even if
# no run references them yet (a run may be about to be created).
ORPHAN_GRACE_PERIOD_SECONDS = 24 * 60 * 60


def derive_mode(yaml_config: str) -> str:
    """Return the run mode derived from the profile settings.

    Invalid configs fall back to strict mode; the processing task is
    responsible for surfacing configuration errors (as it already does).
    """
    try:
        config = version_1.ImportProfileSchema(**yaml.safe_load(yaml_config))
    except Exception:
        logger.warning("Could not derive import mode from profile config", exc_info=True)
        return ImportRun.Mode.STRICT
    if config.settings.skip_errors:
        return ImportRun.Mode.FAULT_TOLERANT
    return ImportRun.Mode.STRICT


def hash_uploaded_file(uploaded_file) -> tuple[str, int]:
    """Compute (sha256 hex, size in bytes) for an uploaded file object."""
    digest = hashlib.sha256()
    size = 0
    uploaded_file.seek(0)
    for chunk in uploaded_file.chunks():
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        digest.update(chunk)
        size += len(chunk)
    uploaded_file.seek(0)
    return digest.hexdigest(), size


def get_active_run(profile: ImportProfile, file_hash: str) -> ImportRun | None:
    return (
        ImportRun.objects.filter(
            profile=profile,
            file_hash=file_hash,
            status__in=ImportRun.ACTIVE_STATUSES,
        )
        .order_by("-id")
        .first()
    )


def enqueue_import_run(
    *,
    profile: ImportProfile,
    uploaded_file,
    user_id: int,
    temp_dir: str | None = None,
) -> tuple[ImportRun, bool]:
    """Persist the uploaded file, create the ImportRun and defer the task.

    Returns ``(import_run, created)``. When an active run for the same
    profile + file content already exists, no new run is created, the staged
    file is removed and ``created=False`` is returned with the existing run.
    """
    # Imported here to avoid importing the task module at package import time.
    from apps.import_app.tasks import process_import

    # FileSystemStorage creates the directory on save; do not create it
    # eagerly here (tests may mock the storage layer entirely).
    temp_dir = temp_dir or DEFAULT_TEMP_DIR

    file_hash, file_size = hash_uploaded_file(uploaded_file)

    existing = get_active_run(profile, file_hash)
    if existing is not None:
        # Nothing was staged yet, nothing to clean up.
        return existing, False

    fs = FileSystemStorage(location=temp_dir)
    stored_name = fs.save(uploaded_file.name, uploaded_file)
    file_path = fs.path(stored_name)

    try:
        import_run = ImportRun.objects.create(
            profile=profile,
            requested_by_id=user_id,
            file_name=stored_name,
            file_hash=file_hash,
            file_size=file_size,
            stored_file_path=file_path,
            config_snapshot=profile.yaml_config,
            mode=derive_mode(profile.yaml_config),
            phase=ImportRun.Phase.ENQUEUED,
        )
    except IntegrityError:
        # Concurrent submission raced us to the partial unique constraint.
        logger.info(
            "Active ImportRun already exists for profile %s and file hash %s",
            profile.id,
            file_hash,
        )
        _safe_remove(file_path)
        existing = get_active_run(profile, file_hash)
        if existing is None:
            raise
        return existing, False

    process_import.defer(
        import_run_id=import_run.id,
        file_path=file_path,
        user_id=user_id,
    )

    return import_run, True


def _safe_remove(file_path: str) -> None:
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except OSError:
        logger.warning("Failed to remove staged file %s", file_path, exc_info=True)
