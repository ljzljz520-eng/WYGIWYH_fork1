import yaml

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from apps.import_app.schemas import version_1


class ImportProfile(models.Model):
    class Versions(models.IntegerChoices):
        VERSION_1 = 1, "Version 1"

    name = models.CharField(max_length=100, verbose_name=_("Name"), unique=True)
    yaml_config = models.TextField(verbose_name=_("YAML Configuration"))
    version = models.IntegerField(
        choices=Versions,
        default=Versions.VERSION_1,
        verbose_name=_("Version"),
    )

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["name"]

    def get_version_display(self):
        version_number = self.Versions(self.version).name.split("_")[1]
        return _("Version {number}").format(number=version_number)

    def clean(self):
        if self.version and self.version == self.Versions.VERSION_1:
            try:
                yaml_data = yaml.safe_load(self.yaml_config)
                version_1.ImportProfileSchema(**yaml_data)
            except Exception as e:
                raise ValidationError(
                    {"yaml_config": _("Invalid YAML Configuration: ") + str(e)}
                )


class ImportRun(models.Model):
    class Status(models.TextChoices):
        QUEUED = "QUEUED", _("Queued")
        PROCESSING = "PROCESSING", _("Processing")
        FAILED = "FAILED", _("Failed")
        FINISHED = "FINISHED", _("Finished")

    class Mode(models.TextChoices):
        STRICT = "strict", _("Strict")
        FAULT_TOLERANT = "fault_tolerant", _("Fault tolerant")

    class Phase(models.TextChoices):
        ENQUEUED = "enqueued", _("Enqueued")
        PARSING = "parsing", _("Parsing")
        COMMITTING = "committing", _("Committing")
        FINISHED = "finished", _("Finished")
        FAILED = "failed", _("Failed")

    ACTIVE_STATUSES = (Status.QUEUED, Status.PROCESSING)

    status = models.CharField(
        max_length=10,
        choices=Status,
        default=Status.QUEUED,
        verbose_name=_("Status"),
    )
    mode = models.CharField(
        max_length=20,
        choices=Mode,
        default=Mode.STRICT,
        verbose_name=_("Mode"),
    )
    phase = models.CharField(
        max_length=20,
        choices=Phase,
        default=Phase.ENQUEUED,
        verbose_name=_("Phase"),
    )
    profile = models.ForeignKey(
        ImportProfile,
        on_delete=models.CASCADE,
    )
    requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="import_runs",
        help_text=_("User who enqueued the run; used for recovered retries"),
    )
    file_name = models.CharField(
        max_length=10000,
        help_text=_("File name"),
    )

    # --- Source file summary -------------------------------------------------
    file_hash = models.CharField(
        max_length=64,
        null=True,
        blank=True,
        db_index=True,
        help_text=_("SHA-256 hash of the source file content"),
    )
    file_size = models.BigIntegerField(
        null=True,
        blank=True,
        help_text=_("Source file size in bytes"),
    )
    stored_file_path = models.CharField(
        max_length=10000,
        blank=True,
        default="",
        help_text=_("Absolute path of the staged source file (server side)"),
    )
    source_summary = models.JSONField(
        default=dict,
        blank=True,
        help_text=_("Summary of the parsed source (sections, row counts, ...)"),
    )

    # --- Config snapshot & resume cursor ------------------------------------
    config_snapshot = models.TextField(
        blank=True,
        default="",
        help_text=_("YAML configuration snapshot taken at enqueue time"),
    )
    cursor = models.JSONField(
        default=dict,
        blank=True,
        help_text=_("Persistent resume cursor"),
    )

    # --- Task lease ----------------------------------------------------------
    lease_owner = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text=_("Identifier of the worker owning the current lease"),
    )
    lease_expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Time at which the current lease expires"),
    )
    run_attempts = models.IntegerField(
        default=0,
        help_text=_("How many times the processing task was (re)started"),
    )

    transactions = models.ManyToManyField(
        "transactions.Transaction", related_name="import_runs"
    )
    tags = models.ManyToManyField(
        "transactions.TransactionTag", related_name="import_runs"
    )
    categories = models.ManyToManyField(
        "transactions.TransactionCategory", related_name="import_runs"
    )
    entities = models.ManyToManyField(
        "transactions.TransactionEntity", related_name="import_runs"
    )
    currencies = models.ManyToManyField(
        "currencies.Currency", related_name="import_runs"
    )

    logs = models.TextField(blank=True)
    processed_rows = models.IntegerField(default=0)
    total_rows = models.IntegerField(default=0)
    successful_rows = models.IntegerField(default=0)
    skipped_rows = models.IntegerField(default=0)
    failed_rows = models.IntegerField(default=0)
    retryable_rows = models.IntegerField(default=0)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)

    class Meta:
        constraints = [
            # Only one active (queued/processing) run per profile + source file.
            models.UniqueConstraint(
                fields=["profile", "file_hash"],
                condition=models.Q(status__in=["QUEUED", "PROCESSING"]),
                name="uniq_active_run_profile_filehash",
            ),
        ]

    def lease_is_active(self, now=None) -> bool:
        """Whether the run currently holds a non-expired lease."""
        from django.utils import timezone

        if not self.lease_owner or not self.lease_expires_at:
            return False
        now = now or timezone.now()
        return self.lease_expires_at > now

    @property
    def is_retriable(self, now=None) -> bool:
        """FAILED runs, or PROCESSING runs whose lease has expired (crash)."""
        from django.utils import timezone

        now = now or timezone.now()
        if self.status == self.Status.FAILED:
            return True
        return (
            self.status == self.Status.PROCESSING
            and self.lease_expires_at is not None
            and self.lease_expires_at < now
        )


class ImportRow(models.Model):
    """Persistent staging record for a single source row of an ImportRun."""

    class Status(models.TextChoices):
        PENDING = "PENDING", _("Pending")
        STAGED = "STAGED", _("Staged")
        COMMITTED = "COMMITTED", _("Committed")
        SKIPPED = "SKIPPED", _("Skipped")
        FAILED_RETRYABLE = "FAILED_RETRYABLE", _("Failed (retryable)")
        FAILED_PERMANENT = "FAILED_PERMANENT", _("Failed (permanent)")

    TERMINAL_STATUSES = (
        Status.COMMITTED,
        Status.SKIPPED,
        Status.FAILED_PERMANENT,
    )

    run = models.ForeignKey(
        ImportRun,
        on_delete=models.CASCADE,
        related_name="rows",
    )
    sequence = models.IntegerField(
        help_text=_("Global ordering of the row within the run"),
    )
    section = models.CharField(
        max_length=10000,
        blank=True,
        default="",
        help_text=_("Source section (file name, sheet name, zip member)"),
    )
    row_number = models.IntegerField(
        help_text=_("Row number inside the source section"),
    )
    idempotency_key = models.CharField(
        max_length=64,
        db_index=True,
        help_text=_("Stable key derived from the source file and row"),
    )
    raw_payload = models.JSONField(
        default=dict,
        help_text=_("Raw values extracted from the source file"),
    )
    mapped_payload = models.JSONField(
        null=True,
        blank=True,
        help_text=_("Mapped, JSON-serializable values ready to be committed"),
    )
    status = models.CharField(
        max_length=20,
        choices=Status,
        default=Status.PENDING,
        verbose_name=_("Status"),
    )
    failure_reason = models.JSONField(
        null=True,
        blank=True,
        help_text=_(
            "Structured failure reason: {stage, code, message, line, exception}"
        ),
    )
    attempts = models.IntegerField(default=0)
    transaction = models.ForeignKey(
        "transactions.Transaction",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="import_rows",
        help_text=_("Transaction produced when this row was committed"),
    )
    committed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["sequence"]
        constraints = [
            models.UniqueConstraint(
                fields=["run", "sequence"],
                name="uniq_importrow_run_sequence",
            ),
            models.UniqueConstraint(
                fields=["run", "idempotency_key"],
                name="uniq_importrow_run_idempotency_key",
            ),
        ]
        indexes = [
            models.Index(fields=["run", "status"]),
        ]

    def __str__(self):
        return f"ImportRow(run={self.run_id}, seq={self.sequence}, status={self.status})"
