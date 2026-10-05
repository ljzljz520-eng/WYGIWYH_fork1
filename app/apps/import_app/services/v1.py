import hashlib
import logging
import os
import re
import socket
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Literal, Union

import yaml
from cachalot.api import cachalot_disabled
from django.core.exceptions import FieldDoesNotExist
from django.db import OperationalError, transaction
from django.db.models import Count, F, Q
from django.utils import timezone

from apps.accounts.models import Account, AccountGroup
from apps.currencies.models import Currency
from apps.import_app.models import ImportProfile, ImportRow, ImportRun
from apps.import_app.schemas import version_1
from apps.import_app.services import source_rows
from apps.transactions.models import (
    Transaction,
    TransactionCategory,
    TransactionTag,
    TransactionEntity,
)
from apps.import_app.schemas.v1 import (
    TransactionCategoryMapping,
    TransactionAccountMapping,
    TransactionTagsMapping,
    TransactionEntitiesMapping,
    AccountGroupMapping,
    AccountCurrencyMapping,
    AccountExchangeCurrencyMapping,
    CurrencyExchangeMapping,
)

logger = logging.getLogger(__name__)


class FatalImportError(Exception):
    """Raised when the run must stop and be marked FAILED."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class LeaseLostError(FatalImportError):
    """Raised when this worker no longer owns the run lease.

    The worker must exit without touching terminal run state or the staged
    source file: the lease owner (or the stale-run recovery task) is
    responsible for finalization. Writing FAILED / deleting the file here
    could clobber a run another worker is actively finishing.
    """

    def __init__(self, message: str = "Lease lost; aborting worker"):
        super().__init__("lease_lost", message)


# Exception classes considered transient at commit time (psycopg
# OperationalError, including deadlocks reported as such, is a subclass).
_TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (OperationalError,)


class ImportService:
    TEMP_DIR = "/usr/src/app/temp"

    STAGING_BATCH_SIZE = 200
    LEASE_TTL = timedelta(minutes=10)

    def __init__(self, import_run: ImportRun):
        self.import_run: ImportRun = import_run
        self.profile: ImportProfile = import_run.profile
        # The config snapshot taken at enqueue time wins over live profile
        # content, so executing a run is not affected by profile edits.
        snapshot = import_run.config_snapshot or self.profile.yaml_config
        self.config: version_1.ImportProfileSchema = self._load_config(snapshot)
        self.settings: (
            version_1.CSVImportSettings
            | version_1.ExcelImportSettings
            | version_1.QIFImportSettings
        ) = self.config.settings
        self.deduplication: list[version_1.CompareDeduplicationRule] = (
            self.config.deduplication
        )
        self.mapping: Dict[str, version_1.ColumnMapping] = self.config.mapping
        self._lease_owner: str | None = None

        # Runs created outside the enqueue pipeline carry no config
        # snapshot: keep historical behavior by deriving the execution mode
        # from the parsed settings. Enqueued runs keep the frozen mode.
        if not import_run.config_snapshot and import_run.pk:
            derived_mode = (
                ImportRun.Mode.FAULT_TOLERANT
                if getattr(self.settings, "skip_errors", False)
                else ImportRun.Mode.STRICT
            )
            if import_run.mode != derived_mode:
                ImportRun.objects.filter(pk=import_run.pk).update(mode=derived_mode)
                import_run.mode = derived_mode

    # ------------------------------------------------------------------
    # Config / logging / status helpers
    # ------------------------------------------------------------------

    def _load_config(self, yaml_config: str) -> version_1.ImportProfileSchema:
        yaml_data = yaml.safe_load(yaml_config)
        try:
            return version_1.ImportProfileSchema(**yaml_data)
        except Exception as e:
            self._log("error", f"Fatal error processing YAML config: {str(e)}")
            self._update_status("FAILED")
            raise e

    def _log(self, level: str, message: str, **kwargs) -> None:
        """Add a log entry to the import run logs.

        Callers must invoke this outside of a domain atomic block so the
        diagnostic survives domain rollbacks (autocommit persists it
        immediately).
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Format additional context if present
        context = ""
        if kwargs:
            context = " - " + ", ".join(f"{k}={v}" for k, v in kwargs.items())

        log_line = f"[{timestamp}] {level.upper()}: {message}{context}\n"

        self.import_run.logs = (self.import_run.logs or "") + log_line
        self.import_run.save(update_fields=["logs"])

        if level == "info":
            logger.info(log_line)
        elif level == "warning":
            logger.warning(log_line)
        elif level == "error":
            logger.error(log_line, exc_info=True)

    def _save_fields(self, **fields) -> None:
        """Persist run fields outside of any open domain transaction.

        Must only be called while no domain atomic block is open.
        """
        ImportRun.objects.filter(id=self.import_run.id).update(**fields)
        for key, value in fields.items():
            setattr(self.import_run, key, value)

    def _update_status(self, new_status: Literal["PROCESSING", "FAILED", "FINISHED"]):
        if new_status == "PROCESSING":
            self.import_run.status = ImportRun.Status.PROCESSING
        elif new_status == "FAILED":
            self.import_run.status = ImportRun.Status.FAILED
        elif new_status == "FINISHED":
            self.import_run.status = ImportRun.Status.FINISHED

        self.import_run.save(update_fields=["status"])

    def _set_phase(self, phase: str) -> None:
        self._save_fields(phase=phase)

    # ------------------------------------------------------------------
    # Mapping / transformation / coercion (shared stage-1 machinery)
    # ------------------------------------------------------------------

    def _transform_value(
        self,
        value: str,
        mapping: version_1.ColumnMapping,
        row: Dict[str, str] = None,
        mapped_data: Dict[str, Any] = None,
    ) -> Any:
        transformed = value

        for transform in mapping.transformations:
            if transform.type == "hash":
                values_to_hash = []
                for field in transform.fields:
                    if field in row:
                        values_to_hash.append(str(row[field]))
                    elif (
                        field.startswith("__")
                        and mapped_data
                        and field[2:] in mapped_data
                    ):
                        values_to_hash.append(str(mapped_data[field[2:]]))
                if values_to_hash:
                    concatenated = "|".join(values_to_hash)
                    transformed = hashlib.sha256(
                        concatenated.encode()
                    ).hexdigest()

            elif transform.type == "replace":
                if transform.exclusive:
                    transformed = value.replace(
                        transform.pattern, transform.replacement
                    )
                else:
                    transformed = transformed.replace(
                        transform.pattern, transform.replacement
                    )

            elif transform.type == "regex":
                if transform.exclusive:
                    transformed = re.sub(
                        transform.pattern, transform.replacement, value
                    )
                else:
                    transformed = re.sub(
                        transform.pattern, transform.replacement, transformed
                    )

            elif transform.type == "date_format":
                transformed = datetime.strptime(
                    transformed, transform.original_format
                ).strftime(transform.new_format)

            elif transform.type == "merge":
                values_to_merge = []
                for field in transform.fields:
                    if field in row:
                        values_to_merge.append(str(row[field]))
                    elif (
                        field.startswith("__")
                        and mapped_data
                        and field[2:] in mapped_data
                    ):
                        values_to_merge.append(str(mapped_data[field[2:]]))
                transformed = transform.separator.join(values_to_merge)

            elif transform.type == "split":
                parts = transformed.split(transform.separator)
                if transform.index is not None:
                    transformed = parts[transform.index] if parts else ""
                else:
                    transformed = parts

            elif transform.type in ["add", "subtract"]:
                try:
                    source_value = Decimal(transformed)

                    field_value = row.get(transform.field)
                    if field_value is None and transform.field.startswith("__"):
                        field_value = mapped_data.get(transform.field[2:])

                    if field_value is None:
                        raise KeyError(
                            f"Field '{transform.field}' not found in row or mapped data"
                        )

                    field_value = self._prepare_numeric_value(
                        str(field_value),
                        transform.thousand_separator,
                        transform.decimal_separator,
                    )

                    if transform.absolute_values:
                        source_value = abs(source_value)
                        field_value = abs(field_value)

                    if transform.type == "add":
                        transformed = str(source_value + field_value)
                    else:  # subtract
                        transformed = str(source_value - field_value)
                except (InvalidOperation, KeyError, AttributeError) as e:
                    logger.warning(
                        f"Error in {transform.type} transformation: {e}. Values: {transformed}, {transform.field}"
                    )
        return transformed

    def _coerce_type(
        self, value: str, mapping: version_1.ColumnMapping
    ) -> Union[str, int, bool, Decimal, datetime, list, None]:
        coerce_to = mapping.coerce_to

        if coerce_to == "transaction_type" and isinstance(
            mapping, version_1.TransactionTypeMapping
        ):
            if mapping.detection_method == "always_income":
                return Transaction.Type.INCOME
            elif mapping.detection_method == "always_expense":
                return Transaction.Type.EXPENSE
        elif coerce_to == "is_paid" and isinstance(
            mapping, version_1.TransactionIsPaidMapping
        ):
            if mapping.detection_method == "always_paid":
                return True
            elif mapping.detection_method == "always_unpaid":
                return False

        if not value:
            return None

        return self._coerce_single_type(value, coerce_to, mapping)

    @staticmethod
    def _coerce_single_type(
        value: str, coerce_to: str, mapping: version_1.ColumnMapping
    ) -> Union[str, int, bool, Decimal, datetime.date, list]:
        if coerce_to == "str":
            return str(value)
        elif coerce_to == "int":
            return int(value)
        elif coerce_to == "str|int":
            if hasattr(mapping, "type") and mapping.type == "id":
                return int(value)
            elif hasattr(mapping, "type") and mapping.type in ["name", "code"]:
                return str(value)
            else:
                return str(value)
        elif coerce_to == "bool":
            return value.lower() in ["true", "1", "yes", "y", "on"]
        elif coerce_to == "positive_decimal":
            return abs(Decimal(value))
        elif coerce_to == "date":
            if isinstance(
                mapping,
                (
                    version_1.TransactionDateMapping,
                    version_1.TransactionReferenceDateMapping,
                ),
            ):
                if isinstance(value, datetime):
                    return value.date()
                elif isinstance(value, date):
                    return value

                formats = (
                    mapping.format
                    if isinstance(mapping.format, list)
                    else [mapping.format]
                )
                for fmt in formats:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                raise ValueError(
                    f"Could not parse date '{value}' with any of the provided formats"
                )
            else:
                raise ValueError(
                    "Date coercion is only supported for TransactionDateMapping and TransactionReferenceDateMapping"
                )
        elif coerce_to == "list":
            return (
                value
                if isinstance(value, list)
                else [item.strip() for item in value.split(",") if item.strip()]
            )
        elif coerce_to == "transaction_type":
            if isinstance(mapping, version_1.TransactionTypeMapping):
                if mapping.detection_method == "sign":
                    return (
                        Transaction.Type.EXPENSE
                        if str(value).startswith("-")
                        else Transaction.Type.INCOME
                    )
                elif mapping.detection_method == "always_income":
                    return Transaction.Type.INCOME
                elif mapping.detection_method == "always_expense":
                    return Transaction.Type.EXPENSE
            raise ValueError("Invalid transaction type detection method")
        elif coerce_to == "is_paid":
            if isinstance(mapping, version_1.TransactionIsPaidMapping):
                if mapping.detection_method == "boolean":
                    return value.lower() in ["true", "1", "yes", "y", "on"]
                elif mapping.detection_method == "always_paid":
                    return True
                elif mapping.detection_method == "always_unpaid":
                    return False
            raise ValueError("Invalid is_paid detection method")
        else:
            raise ValueError(f"Unsupported coercion type: {coerce_to}")

    def _map_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        mapped_data = {}
        for field, mapping in self.mapping.items():
            value = None
            if isinstance(mapping.source, str):
                if mapping.source in row:
                    value = row[mapping.source]
                elif (
                    mapping.source.startswith("__")
                    and mapping.source[2:] in mapped_data
                ):
                    value = mapped_data[mapping.source[2:]]
            elif isinstance(mapping.source, list):
                for source in mapping.source:
                    if source in row:
                        value = row[source]
                        break
                    elif source.startswith("__") and source[2:] in mapped_data:
                        value = mapped_data[source[2:]]
                        break

            if value is None:
                value = mapping.default

            if mapping.transformations:
                value = self._transform_value(value, mapping, row, mapped_data)

            value = self._coerce_type(value, mapping)

            if mapping.required and value is None:
                raise ValueError(f"Required field {field} is missing")

            if value is not None:
                target = mapping.target
                if self.settings.importing == "transactions":
                    mapped_data[target] = value
                else:
                    field_name = target.split("_", 1)[1]
                    mapped_data[field_name] = value

        return mapped_data

    @staticmethod
    def _prepare_numeric_value(
        value: str, thousand_separator: str, decimal_separator: str
    ) -> Decimal:
        if thousand_separator:
            value = value.replace(thousand_separator, "")

        if decimal_separator != ".":
            value = value.replace(decimal_separator, ".")

        return Decimal(value)

    # ------------------------------------------------------------------
    # Strict-mode reference pre-validation
    # ------------------------------------------------------------------

    def _validate_references(self, mapped_data: Dict[str, Any]) -> None:
        """Validate that referenced domain objects exist (strict mode only).

        Fault-tolerant mode keeps historical lenient semantics (unresolved
        references are dropped/nulled at commit time).
        """
        importing = self.settings.importing

        if importing == "transactions":
            account_mapping = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, TransactionAccountMapping)
                ),
                None,
            )
            if account_mapping and "account" in mapped_data:
                value = mapped_data["account"]
                exists = (
                    Account.objects.filter(id=value).exists()
                    if account_mapping.type == "id"
                    else Account.objects.filter(name=value).exists()
                )
                if not exists:
                    raise ValueError(f"Account '{value}' does not exist")

            for mapping_class, key, model in (
                (TransactionCategoryMapping, "category", TransactionCategory),
                (TransactionTagsMapping, "tags", TransactionTag),
                (TransactionEntitiesMapping, "entities", TransactionEntity),
            ):
                mapping_obj = next(
                    (
                        m
                        for m in self.mapping.values()
                        if isinstance(m, mapping_class)
                    ),
                    None,
                )
                if not mapping_obj or key not in mapped_data:
                    continue
                if getattr(mapping_obj, "create", True):
                    continue
                values = mapped_data[key]
                values = values if isinstance(values, list) else [values]
                for value in values:
                    exists = (
                        model.all_objects.filter(id=value).exists()
                        if mapping_obj.type == "id"
                        else model.all_objects.filter(name=value).exists()
                    )
                    if not exists:
                        raise ValueError(
                            f"{model.__name__} '{value}' does not exist"
                        )

        elif importing == "accounts":
            group_mapping = next(
                (m for m in self.mapping.values() if isinstance(m, AccountGroupMapping)),
                None,
            )
            if group_mapping and "group" in mapped_data:
                value = mapped_data["group"]
                exists = (
                    AccountGroup.objects.filter(id=value).exists()
                    if group_mapping.type == "id"
                    else AccountGroup.objects.filter(name=value).exists()
                )
                if not exists:
                    raise ValueError(f"Account group '{value}' does not exist")

            for mapping_class, key in (
                (AccountCurrencyMapping, "currency"),
                (AccountExchangeCurrencyMapping, "exchange_currency"),
            ):
                mapping_obj = next(
                    (
                        m
                        for m in self.mapping.values()
                        if isinstance(m, mapping_class)
                    ),
                    None,
                )
                if mapping_obj and key in mapped_data:
                    self._check_currency_exists(mapping_obj.type, mapped_data[key])

        elif importing == "currencies":
            mapping_obj = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, CurrencyExchangeMapping)
                ),
                None,
            )
            if mapping_obj and "exchange_currency" in mapped_data:
                self._check_currency_exists(
                    mapping_obj.type, mapped_data["exchange_currency"]
                )

    @staticmethod
    def _check_currency_exists(mapping_type: str, value) -> None:
        if mapping_type == "id":
            exists = Currency.objects.filter(id=value).exists()
        elif mapping_type == "code":
            exists = Currency.objects.filter(code=value).exists()
        else:
            exists = Currency.objects.filter(name=value).exists()
        if not exists:
            raise ValueError(f"Currency '{value}' does not exist")

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _check_duplicate_transaction(self, transaction_data: Dict[str, Any]) -> bool:
        for rule in self.deduplication:
            if rule.type == "compare":
                query = Transaction.all_objects.all().values("id")

                for field in rule.fields:
                    if field in transaction_data:
                        value = transaction_data[field]
                        query = self._apply_deduplication_filter(
                            query=query,
                            field=field,
                            value=value,
                            match_type=rule.match_type,
                        )

                if query.exists():
                    return True

        return False

    @staticmethod
    def _is_int_like(value: Any) -> bool:
        try:
            int(value)
        except (TypeError, ValueError):
            return False
        return True

    def _apply_deduplication_filter(
        self,
        query,
        field: str,
        value: Any,
        match_type: Literal["lax", "strict"],
    ):
        if isinstance(value, list):
            return self._apply_list_deduplication_filter(
                query=query,
                field=field,
                values=value,
                match_type=match_type,
            )

        if match_type == "strict" or not isinstance(value, str):
            return query.filter(**{field: value})

        return query.filter(**{f"{field}__iexact": value})

    def _apply_list_deduplication_filter(
        self,
        query,
        field: str,
        values: list[Any],
        match_type: Literal["lax", "strict"],
    ):
        clean_values = [v for v in values if v not in (None, "")]
        if not clean_values:
            return query

        try:
            model_field = Transaction._meta.get_field(field)
        except FieldDoesNotExist:
            return query.filter(**{f"{field}__in": clean_values})

        if getattr(model_field, "many_to_many", False):
            if all(self._is_int_like(v) for v in clean_values):
                for value in clean_values:
                    query = query.filter(**{f"{field}__id": int(value)})
            else:
                for value in clean_values:
                    lookup = (
                        f"{field}__name"
                        if match_type == "strict"
                        else f"{field}__name__iexact"
                    )
                    query = query.filter(**{lookup: str(value).strip()})

            return query.distinct()

        return query.filter(**{f"{field}__in": clean_values})

    # ------------------------------------------------------------------
    # Lease
    # ------------------------------------------------------------------

    def _acquire_lease(self) -> bool:
        now = timezone.now()
        run = (
            ImportRun.objects.filter(id=self.import_run.id)
            .filter(
                Q(lease_expires_at__isnull=True)
                | Q(lease_expires_at__lte=now)
            )
            .filter(
                Q(status=ImportRun.Status.QUEUED)
                | Q(status=ImportRun.Status.PROCESSING)
                | Q(status=ImportRun.Status.FAILED)
            )
        )
        owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
        updated = run.update(
            lease_owner=owner,
            lease_expires_at=now + self.LEASE_TTL,
            status=ImportRun.Status.PROCESSING,
            run_attempts=F("run_attempts") + 1,
        )
        if not updated:
            self.import_run.refresh_from_db()
            return False

        self._lease_owner = owner
        self.import_run.refresh_from_db()
        return True

    def _heartbeat(self) -> bool:
        """Renew the lease. Returns False if ownership was lost.

        A 0-row UPDATE means another worker took over (e.g. this worker ran
        longer than the TTL inside a slow batch): this instance must stop
        immediately instead of clobbering the new owner's progress.
        """
        if not self._lease_owner:
            return False
        updated = ImportRun.objects.filter(
            id=self.import_run.id, lease_owner=self._lease_owner
        ).update(lease_expires_at=timezone.now() + self.LEASE_TTL)
        return bool(updated)

    def _check_lease(self) -> None:
        """Abort this worker when its lease has been taken over.

        A service instance used directly (without acquiring a lease, e.g.
        in tests or legacy direct construction) is unfenced: only worker
        instances that passed ``_acquire_lease`` enforce ownership.
        """
        if self._lease_owner is None:
            return
        if not self._heartbeat():
            logger.warning(
                "Run %s lease lost for worker %s; aborting",
                self.import_run.id,
                self._lease_owner,
            )
            raise LeaseLostError()

    def _release_lease(self) -> None:
        if not self._lease_owner:
            return
        ImportRun.objects.filter(
            id=self.import_run.id, lease_owner=self._lease_owner
        ).update(lease_owner=None, lease_expires_at=None)
        self._lease_owner = None

    def _finalize(self, **fields) -> bool:
        """Persist terminal fields only while this worker owns the lease.

        Returns True when this worker was still the owner. A False result
        means another worker took over: callers must leave status/files
        alone and exit quietly.
        """
        if not self._lease_owner:
            return False
        updated = ImportRun.objects.filter(
            id=self.import_run.id, lease_owner=self._lease_owner
        ).update(**fields)
        return bool(updated)

    # ------------------------------------------------------------------
    # Stage 1: parse + validate + persist staging rows
    # ------------------------------------------------------------------

    def _ensure_file_metadata(self, file_path: str) -> None:
        updates = {}
        if not self.import_run.stored_file_path:
            updates["stored_file_path"] = file_path
        if not self.import_run.file_hash:
            digest = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    digest.update(chunk)
            updates["file_hash"] = digest.hexdigest()
        if not self.import_run.file_size and os.path.exists(file_path):
            updates["file_size"] = os.path.getsize(file_path)
        if updates:
            self._save_fields(**updates)

    def _read_source(self, file_path: str):
        """Return (kind, items, warnings). kind is csv/excel/qif."""
        if isinstance(self.settings, version_1.QIFImportSettings):
            records, warnings = source_rows.iter_qif_records(
                file_path, self.settings
            )
            return "qif", records, warnings

        if isinstance(self.settings, version_1.CSVImportSettings):
            if self.settings.skip_lines:
                self._log(
                    "info",
                    f"Skipped {self.settings.skip_lines} initial lines",
                )
            return "csv", source_rows.iter_csv_rows(file_path, self.settings), []

        rows, warnings = source_rows.iter_excel_rows(file_path, self.settings)
        for warning in warnings:
            self._log("warning", warning)
        return "excel", rows, warnings

    @staticmethod
    def _json_default(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return str(value)

    def _json_safe_mapped(self, mapped: Dict[str, Any]) -> Dict[str, Any]:
        import json

        return json.loads(
            json.dumps(mapped, default=self._json_default, ensure_ascii=False)
        )

    def _failure(
        self,
        stage: str,
        code: str,
        message: str,
        row: source_rows.RawRow | source_rows.QifRecord,
    ) -> dict:
        return {
            "stage": stage,
            "code": code,
            "message": message,
            "section": row.section,
            "line": row.row_number,
        }

    def _stage_tabular_row(self, raw: source_rows.RawRow):
        """Map + validate a CSV/Excel row. Returns (mapped, failure)."""
        try:
            mapped = self._map_row(raw.native_payload)
        except Exception as e:
            return None, self._failure(
                "validate", type(e).__name__, str(e), raw
            )

        strict = self.import_run.mode == ImportRun.Mode.STRICT
        if strict:
            try:
                self._validate_references(mapped)
            except Exception as e:
                return None, self._failure(
                    "validate", type(e).__name__, str(e), raw
                )
        return self._json_safe_mapped(mapped), None

    def _stage_qif_row(self, record: source_rows.QifRecord):
        """Validate a QIF record and build its serializable mapped payload."""
        fields = record.fields

        account = Account.objects.filter(name=record.account_name).first()
        if not account:
            return None, self._failure(
                "validate",
                "account_missing",
                f"Account '{record.account_name}' not found.",
                record,
            )

        if "D" not in fields:
            return None, self._failure(
                "validate", "date_missing", "Transaction date is required", record
            )
        try:
            parsed_date = datetime.strptime(
                fields["D"], self.settings.date_format
            ).date()
        except ValueError:
            return None, self._failure(
                "parse",
                "invalid_date",
                (
                    f"Could not parse date '{fields['D']}' using format "
                    f"'{self.settings.date_format}'"
                ),
                record,
            )

        if "T" not in fields:
            return None, self._failure(
                "validate", "amount_missing", "Transaction amount is required",
                record,
            )
        try:
            amount = Decimal(fields["T"].replace(",", ""))
        except InvalidOperation:
            return None, self._failure(
                "parse",
                "invalid_amount",
                f"Could not parse amount '{fields['T']}'",
                record,
            )

        internal_id = hashlib.sha256(
            "".join(record.lines).encode("utf-8")
        ).hexdigest()

        mapped = {
            "kind": "qif",
            "account_name": record.account_name,
            "account_id": account.id,
            "date": parsed_date.isoformat(),
            "type": (
                Transaction.Type.EXPENSE if amount < 0 else Transaction.Type.INCOME
            ),
            "amount": str(abs(amount)),
            "internal_id": internal_id,
            "description": fields.get("M", ""),
            "payee": fields.get("P"),
            "label": fields.get("L"),
        }
        return mapped, None

    def parse_and_stage(self, file_path: str) -> str:
        """Phase 1: read every source row and persist staging results.

        Returns the source kind ("csv" / "excel" / "qif"). Raises
        FatalImportError in strict mode when any row fails validation.
        """
        self._set_phase(ImportRun.Phase.PARSING)
        self._ensure_file_metadata(file_path)

        kind, items, warnings = self._read_source(file_path)

        existing_keys = set(
            self.import_run.rows.values_list("idempotency_key", flat=True)
        )
        section_counts: dict[str, int] = {}
        staged_batch: list[ImportRow] = []
        permanent_failures = 0
        sequence = 0

        def flush() -> None:
            if not staged_batch:
                return
            ImportRow.objects.bulk_create(
                staged_batch,
                batch_size=self.STAGING_BATCH_SIZE,
                ignore_conflicts=True,
            )
            staged_batch.clear()
            self._save_fields(
                total_rows=sequence,
                cursor={"phase": "parsing", "sequence": sequence},
            )
            self._reconcile_counters()
            self._check_lease()

        for item in items:
            sequence += 1
            section_counts[item.section] = section_counts.get(item.section, 0) + 1

            raw_payload = (
                source_rows.qif_record_to_raw(item)
                if kind == "qif"
                else item.raw_payload
            )
            key = source_rows.make_idempotency_key(
                self.import_run.file_hash,
                item.section,
                item.row_number,
                raw_payload,
            )
            if key in existing_keys:
                continue

            if kind == "qif":
                mapped, failure = self._stage_qif_row(item)
            else:
                mapped, failure = self._stage_tabular_row(item)

            if failure is not None:
                permanent_failures += 1
                self._log(
                    "warning",
                    f"Row validation failed [{item.section}#{item.row_number}]: "
                    f"{failure['code']} - {failure['message']}",
                )
                status = ImportRow.Status.FAILED_PERMANENT
            else:
                status = ImportRow.Status.STAGED

            staged_batch.append(
                ImportRow(
                    run=self.import_run,
                    sequence=sequence,
                    section=item.section,
                    row_number=item.row_number,
                    idempotency_key=key,
                    raw_payload=raw_payload,
                    mapped_payload=mapped,
                    status=status,
                    failure_reason=failure,
                )
            )

            if len(staged_batch) >= self.STAGING_BATCH_SIZE:
                flush()

        flush()

        self.import_run.source_summary = {
            "kind": kind,
            "sections": section_counts,
            "file_hash": self.import_run.file_hash,
            "file_size": self.import_run.file_size,
        }
        self.import_run.save(update_fields=["source_summary"])

        self._log(
            "info",
            f"Parsing complete: {sequence} row(s), "
            f"{permanent_failures} permanent failure(s)",
        )

        if permanent_failures and self.import_run.mode == ImportRun.Mode.STRICT:
            raise FatalImportError(
                "validation_failed",
                f"{permanent_failures} row(s) failed validation",
            )

        return kind

    # ------------------------------------------------------------------
    # Stage 2: commit
    # ------------------------------------------------------------------

    def _hydrate_mapped(self, row: ImportRow) -> Dict[str, Any]:
        import json

        mapped = json.loads(json.dumps(row.mapped_payload or {}))

        if mapped.get("kind") == "qif":
            if mapped.get("date"):
                mapped["date"] = date.fromisoformat(mapped["date"])
            if mapped.get("amount") is not None:
                mapped["amount"] = Decimal(str(mapped["amount"]))
            return mapped

        for mapping in self.mapping.values():
            if self.settings.importing == "transactions":
                field_name = mapping.target
            else:
                field_name = mapping.target.split("_", 1)[1]
            if field_name not in mapped or mapped[field_name] is None:
                continue
            if mapping.coerce_to == "date":
                mapped[field_name] = date.fromisoformat(mapped[field_name])
            elif mapping.coerce_to == "positive_decimal":
                mapped[field_name] = Decimal(str(mapped[field_name]))
        return mapped

    def _create_transaction(self, data: Dict[str, Any]) -> Transaction:
        tags = []
        entities = []

        if "category" in data:
            category_name = data.pop("category")
            category_mapping = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, TransactionCategoryMapping)
                    and m.target == "category"
                ),
                None,
            )

            try:
                if category_mapping:
                    if category_mapping.type == "id":
                        category = TransactionCategory.objects.get(
                            id=category_name
                        )
                    else:  # name
                        if getattr(category_mapping, "create", False):
                            try:
                                category = TransactionCategory.objects.get(
                                    name=category_name
                                )
                            except TransactionCategory.DoesNotExist:
                                category = TransactionCategory(name=category_name)
                                category.save()
                        else:
                            category = TransactionCategory.objects.filter(
                                name=category_name
                            ).first()
                    if category:
                        data["category"] = category
                        self.import_run.categories.add(category)
            except (TransactionCategory.DoesNotExist, ValueError):
                data["category"] = None

        if "account" in data:
            account_id = data.pop("account")
            account_mapping = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, TransactionAccountMapping)
                    and m.target == "account"
                ),
                None,
            )

            try:
                if account_mapping and account_mapping.type == "id":
                    account = Account.objects.filter(id=account_id).first()
                else:  # name
                    account = Account.objects.filter(name=account_id).first()

                if account:
                    data["account"] = account
            except ValueError:
                pass

        if "tags" in data:
            tag_names = data.pop("tags")
            tags_mapping = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, TransactionTagsMapping)
                    and m.target == "tags"
                ),
                None,
            )

            for tag_name in tag_names:
                try:
                    if tags_mapping:
                        if tags_mapping.type == "id":
                            tag = TransactionTag.objects.filter(
                                id=tag_name
                            ).first()
                        else:  # name
                            if getattr(tags_mapping, "create", False):
                                try:
                                    tag = TransactionTag.objects.get(
                                        name=tag_name.strip()
                                    )
                                except TransactionTag.DoesNotExist:
                                    tag = TransactionTag(name=tag_name.strip())
                                    tag.save()
                            else:
                                tag = TransactionTag.objects.filter(
                                    name=tag_name.strip()
                                ).first()

                        if tag:
                            tags.append(tag)
                            self.import_run.tags.add(tag)
                except ValueError:
                    continue

        if "entities" in data:
            entity_names = data.pop("entities")
            entities_mapping = next(
                (
                    m
                    for m in self.mapping.values()
                    if isinstance(m, TransactionEntitiesMapping)
                    and m.target == "entities"
                ),
                None,
            )

            for entity_name in entity_names:
                try:
                    if entities_mapping:
                        if entities_mapping.type == "id":
                            entity = TransactionEntity.objects.filter(
                                id=entity_name
                            ).first()
                        else:  # name
                            if getattr(entities_mapping, "create", False):
                                try:
                                    entity = TransactionEntity.objects.get(
                                        name=entity_name.strip()
                                    )
                                except TransactionEntity.DoesNotExist:
                                    entity = TransactionEntity(
                                        name=entity_name.strip()
                                    )
                                    entity.save()
                            else:
                                entity = TransactionEntity.objects.filter(
                                    name=entity_name.strip()
                                ).first()

                        if entity:
                            entities.append(entity)
                            self.import_run.entities.add(entity)
                except ValueError:
                    continue

        new_transaction = Transaction.objects.create(**data)
        self.import_run.transactions.add(new_transaction)

        if tags:
            new_transaction.tags.set(tags)
        if entities:
            new_transaction.entities.set(entities)

        return new_transaction

    def _create_account(self, data: Dict[str, Any]) -> Account:
        if "group" in data:
            group_name = data.pop("group")
            try:
                group = AccountGroup.objects.get(name=group_name)
            except AccountGroup.DoesNotExist:
                group = AccountGroup(name=group_name)
                group.save()
            data["group"] = group

        if "currency" in data:
            currency = Currency.objects.get(code=data["currency"])
            data["currency"] = currency
            self.import_run.currencies.add(currency)

        if "exchange_currency" in data:
            exchange_currency = Currency.objects.get(
                code=data["exchange_currency"]
            )
            data["exchange_currency"] = exchange_currency
            self.import_run.currencies.add(exchange_currency)

        return Account.objects.create(**data)

    def _create_currency(self, data: Dict[str, Any]) -> Currency:
        if "exchange_currency" in data:
            exchange_currency = Currency.objects.get(
                code=data["exchange_currency"]
            )
            data["exchange_currency"] = exchange_currency
            self.import_run.currencies.add(exchange_currency)

        currency = Currency.objects.create(**data)
        self.import_run.currencies.add(currency)
        return currency

    def _create_category(self, data: Dict[str, Any]) -> TransactionCategory:
        category = TransactionCategory.objects.create(**data)
        self.import_run.categories.add(category)
        return category

    def _create_tag(self, data: Dict[str, Any]) -> TransactionTag:
        tag = TransactionTag.objects.create(**data)
        self.import_run.tags.add(tag)
        return tag

    def _create_entity(self, data: Dict[str, Any]) -> TransactionEntity:
        entity = TransactionEntity.objects.create(**data)
        self.import_run.entities.add(entity)
        return entity

    def _create_qif_transaction(self, data: Dict[str, Any]) -> Transaction:
        """Mirror the historical QIF creation semantics."""
        account = Account.objects.get(id=data["account_id"])

        payload: Dict[str, Any] = {
            "account": account,
            "date": data["date"],
            "type": data["type"],
            "amount": data["amount"],
            "internal_id": data["internal_id"],
            "description": data.get("description") or "",
        }

        entities = []
        payee_name = data.get("payee")
        if payee_name:
            entity, _ = TransactionEntity.objects.get_or_create(name=payee_name)
            entities.append(entity)

        category = None
        tags = []
        label = data.get("label")
        if label:
            if label.startswith("[") and label.endswith("]"):
                payload["description"] = label[1:-1]
            else:
                parts = label.split(":")
                if parts:
                    cat_name = parts[0].strip()
                    if cat_name:
                        category, _ = TransactionCategory.objects.get_or_create(
                            name=cat_name
                        )
                    for tag_name in parts[1:]:
                        tag_name = tag_name.strip()
                        if tag_name:
                            tag, _ = TransactionTag.objects.get_or_create(
                                name=tag_name
                            )
                            tags.append(tag)

        if category:
            payload["category"] = category

        new_trans = Transaction.objects.create(**payload)
        if entities:
            new_trans.entities.set(entities)
            self.import_run.entities.add(*entities)
        if tags:
            new_trans.tags.set(tags)
            self.import_run.tags.add(*tags)
        if category:
            self.import_run.categories.add(category)
        self.import_run.transactions.add(new_trans)
        return new_trans

    def _schedule_rules(self, new_transaction: Transaction) -> None:
        """Enqueue rule evaluation after the current transaction commits.

        Registered inside the domain transaction; on_commit fires only when
        it actually commits. The per-transaction queueing lock makes the
        enqueue exactly-once.
        """
        if not getattr(self.settings, "trigger_transaction_rules", False):
            return

        from apps.common.middleware.thread_local import get_current_user
        from apps.rules.tasks import check_for_transaction_rules

        transaction_id = new_transaction.id

        def _enqueue():
            from procrastinate import exceptions as procrastinate_exceptions

            try:
                user = get_current_user()
                check_for_transaction_rules.defer(
                    instance_id=transaction_id,
                    user_id=user.id if user else None,
                    signal="transaction_created",
                    queueing_lock=f"import-rule-created-{transaction_id}",
                )
            except procrastinate_exceptions.AlreadyEnqueued as e:
                # The queueing lock deduped a duplicate enqueue: the rule
                # job is already queued, exactly-once still holds.
                logger.debug(
                    "Rule enqueue for transaction %s deduped: %s",
                    transaction_id,
                    e,
                )
            except Exception:
                # A real enqueue failure (e.g. broker/DB outage) must not
                # vanish silently: surface it in the run log so it can be
                # diagnosed and re-triggered manually.
                logger.warning(
                    "Failed to enqueue rule evaluation for transaction %s",
                    transaction_id,
                    exc_info=True,
                )
                try:
                    run = ImportRun.objects.filter(id=run_id).first()
                    if run is not None:
                        run.logs = (run.logs or "") + (
                            f"[{timezone.now():%Y-%m-%d %H:%M:%S}] WARNING: "
                            f"Rule enqueue failed for transaction "
                            f"{transaction_id}\n"
                        )
                        run.save(update_fields=["logs"])
                except Exception:
                    logger.debug(
                        "Could not persist rule enqueue failure to run %s",
                        run_id,
                        exc_info=True,
                    )

        run_id = self.import_run.id
        transaction.on_commit(_enqueue)

    # Row states this worker is allowed to pick up for commit.
    _CLAIMABLE_ROW_STATUSES = (
        ImportRow.Status.STAGED,
        ImportRow.Status.FAILED_RETRYABLE,
        ImportRow.Status.PENDING,
    )

    def _commit_row(self, row: ImportRow) -> None:
        """Create the domain object(s) for one staged row.

        Must be called inside an atomic block. The first statement is a
        conditional claim on the row: it doubles as a row-level lock
        against a second worker whose lease was acquired after this
        worker's lease expired. The UPDATE blocks on a competing claim and
        matches zero rows once the other worker has committed (row becomes
        COMMITTED/SKIPPED) — in that case LeaseLostError aborts this
        worker before it can create a duplicate domain object.

        Marks the row COMMITTED or SKIPPED; status/attempt changes are
        part of the domain transaction (so a strict rollback wipes them).
        """
        # The UPDATE itself takes the row lock; a competing worker's
        # conditional UPDATE blocks here and, once we commit, re-evaluates
        # against the new COMMITTED/SKIPPED version and matches zero rows.
        claimed = ImportRow.objects.filter(
            pk=row.pk, status__in=self._CLAIMABLE_ROW_STATUSES
        ).update(status=ImportRow.Status.PENDING, attempts=F("attempts") + 1)
        if not claimed:
            # Another worker already owns/finished this row.
            raise LeaseLostError(
                f"Row {row.id} [{row.section}#{row.row_number}] was claimed "
                "by another worker"
            )

        mapped = self._hydrate_mapped(row)

        if mapped.get("kind") == "qif":
            if Transaction.objects.filter(
                internal_id=mapped["internal_id"]
            ).exists():
                row.status = ImportRow.Status.SKIPPED
                row.save(update_fields=["status"])
                return
            new_transaction = self._create_qif_transaction(mapped)
            row.transaction = new_transaction
            # QIF never triggered rules historically.
        elif self.settings.importing == "transactions":
            if self.deduplication and self._check_duplicate_transaction(mapped):
                row.status = ImportRow.Status.SKIPPED
                row.save(update_fields=["status"])
                return
            new_transaction = self._create_transaction(mapped)
            row.transaction = new_transaction
            self._schedule_rules(new_transaction)
        elif self.settings.importing == "accounts":
            self._create_account(mapped)
        elif self.settings.importing == "currencies":
            self._create_currency(mapped)
        elif self.settings.importing == "categories":
            self._create_category(mapped)
        elif self.settings.importing == "tags":
            self._create_tag(mapped)
        elif self.settings.importing == "entities":
            self._create_entity(mapped)

        row.status = ImportRow.Status.COMMITTED
        row.committed_at = timezone.now()
        row.save(
            update_fields=["status", "transaction", "committed_at"]
        )

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        return isinstance(exc, _TRANSIENT_EXCEPTIONS)

    def _mark_failed_rows(
        self, rows: list[ImportRow], exc: Exception, stage: str
    ) -> str:
        retryable = self._is_retryable(exc)
        status = (
            ImportRow.Status.FAILED_RETRYABLE
            if retryable
            else ImportRow.Status.FAILED_PERMANENT
        )
        reason = {
            "stage": stage,
            "code": type(exc).__name__,
            "message": str(exc),
            "exception": type(exc).__name__,
        }
        for row in rows:
            row.status = status
            row.failure_reason = {
                **reason,
                "section": row.section,
                "line": row.row_number,
            }
            row.committed_at = None
            row.transaction = None
            row.save(
                update_fields=[
                    "status",
                    "failure_reason",
                    "committed_at",
                    "transaction",
                ]
            )
            # The domain attempt was rolled back: count this attempt here,
            # in the independent diagnostics transaction.
            ImportRow.objects.filter(id=row.id).update(
                attempts=F("attempts") + 1
            )
            row.attempts = ImportRow.objects.get(id=row.id).attempts
        self._reconcile_counters()
        return status

    def _actionable_rows(self, include_retryable: bool) -> list[ImportRow]:
        statuses = [ImportRow.Status.STAGED]
        if include_retryable:
            statuses.append(ImportRow.Status.FAILED_RETRYABLE)
        # PENDING rows (interrupted phase 1 has none after resume reparse,
        # but keep them claimable for robustness).
        statuses.append(ImportRow.Status.PENDING)
        return list(
            self.import_run.rows.filter(status__in=statuses).order_by("sequence")
        )

    def commit_rows(self, *, auto_recovery: bool = True) -> None:
        """Phase 2: commit staged rows.

        Strict mode uses a single transaction for the whole batch; fault
        tolerant mode uses one transaction (savepoint) per row.
        """
        self._set_phase(ImportRun.Phase.COMMITTING)
        rows = self._actionable_rows(include_retryable=auto_recovery)

        if not rows:
            return

        strict = self.import_run.mode == ImportRun.Mode.STRICT

        if strict:
            # Fence before entering the long transaction: no heartbeat is
            # visible from inside it (its UPDATEs only become visible at
            # commit), so we must still own the lease right now.
            self._check_lease()
            # Locate the failing row precisely for diagnostics while keeping
            # the batch atomic: current is advanced per row inside the single
            # domain transaction.
            current: ImportRow | None = None
            try:
                with transaction.atomic():
                    for row in rows:
                        current = row
                        self._commit_row(row)
            except LeaseLostError:
                # A competing worker owns the run (row claim lost). Do not
                # write diagnostics or terminal state for anyone; just exit.
                logger.warning(
                    "Strict commit aborted: row claim lost on run %s",
                    self.import_run.id,
                )
                raise
            except Exception as exc:
                # The whole domain batch was rolled back: successful_rows
                # stays 0. Persist diagnostics for the offending row in an
                # independent transaction; sibling rows remain STAGED and
                # will be retried.
                self._log(
                    "error",
                    f"Strict commit failed at row "
                    f"[{current.section if current else '?'}#"
                    f"{current.row_number if current else '?'}]: {exc}",
                )
                if current is not None:
                    current.refresh_from_db()
                    self._mark_failed_rows([current], exc, "commit")
                raise FatalImportError("commit_failed", str(exc)) from exc

            self._reconcile_counters()
            self._save_fields(
                cursor={
                    "phase": "committing",
                    "sequence": rows[-1].sequence if rows else 0,
                }
            )
            self._log(
                "info",
                f"Strict commit successful for {len(rows)} staged row(s)",
            )
            return

        # Fault tolerant: one transaction per row (savepoint semantics).
        processed = 0
        for row in rows:
            try:
                with transaction.atomic():
                    self._commit_row(row)
            except LeaseLostError:
                logger.warning(
                    "Fault tolerant commit aborted after %s row(s): lease "
                    "lost on run %s",
                    processed,
                    self.import_run.id,
                )
                raise
            except Exception as exc:
                status = self._mark_failed_rows([row], exc, "commit")
                self._log(
                    "warning",
                    f"Error processing row [{row.section}#{row.row_number}] "
                    f"({status}): {exc}",
                )
            processed += 1
            if processed % self.STAGING_BATCH_SIZE == 0:
                self._reconcile_counters()
                self._save_fields(
                    cursor={"phase": "committing", "sequence": row.sequence}
                )
                self._check_lease()

        self._reconcile_counters()
        self._save_fields(
            cursor={
                "phase": "committing",
                "sequence": rows[-1].sequence if rows else 0,
            }
        )

    # ------------------------------------------------------------------
    # Counters (derived from persisted row outcomes)
    # ------------------------------------------------------------------

    def _reconcile_counters(self) -> None:
        rows = self.import_run.rows
        aggregates = {
            item["status"]: item["n"]
            for item in rows.values("status").annotate(n=Count("id"))
        }
        committed = aggregates.get(ImportRow.Status.COMMITTED, 0)
        skipped = aggregates.get(ImportRow.Status.SKIPPED, 0)
        failed_permanent = aggregates.get(ImportRow.Status.FAILED_PERMANENT, 0)
        failed_retryable = aggregates.get(ImportRow.Status.FAILED_RETRYABLE, 0)
        failed = failed_permanent + failed_retryable

        ImportRun.objects.filter(id=self.import_run.id).update(
            total_rows=rows.count(),
            successful_rows=committed,
            skipped_rows=skipped,
            failed_rows=failed,
            retryable_rows=failed_retryable,
            processed_rows=committed + skipped + failed,
        )
        self.import_run.total_rows = rows.count()
        self.import_run.successful_rows = committed
        self.import_run.skipped_rows = skipped
        self.import_run.failed_rows = failed
        self.import_run.retryable_rows = failed_retryable
        self.import_run.processed_rows = committed + skipped + failed

    # ------------------------------------------------------------------
    # Path validation / file lifecycle
    # ------------------------------------------------------------------

    def _validate_file_path(self, file_path: str) -> str:
        abs_path = os.path.abspath(file_path)
        if not abs_path.startswith(self.TEMP_DIR):
            raise ValueError(f"Invalid file path. File must be in {self.TEMP_DIR}")
        return abs_path

    @staticmethod
    def _remove_file(file_path: str) -> bool:
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                return True
        except OSError:
            logger.warning(
                "Failed to delete temporary file: %s", file_path, exc_info=True
            )
        return False

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def process_file(self, file_path: str):
        with cachalot_disabled():
            file_path = self._validate_file_path(file_path)

            if not self._acquire_lease():
                self._log(
                    "warning",
                    "Import run is already leased to an active worker; exiting",
                )
                return

            self._save_fields(
                status=ImportRun.Status.PROCESSING,
                started_at=self.import_run.started_at or timezone.now(),
            )
            self._log("info", "Starting import process")

            fatal: FatalImportError | None = None
            lease_lost = False
            try:
                if not os.path.exists(file_path):
                    raise FatalImportError(
                        "source_file_missing",
                        f"Source file no longer exists: {file_path}",
                    )

                self.parse_and_stage(file_path)
                self.commit_rows()
            except LeaseLostError as e:
                lease_lost = True
                fatal = e
            except FatalImportError as e:
                fatal = e
            except Exception as e:
                logger.error("Import pipeline failed", exc_info=True)
                fatal = FatalImportError(type(e).__name__, str(e))
            finally:
                if lease_lost:
                    # Another worker took over (or won a row claim). Do not
                    # touch terminal status, logs counters or the file; free
                    # the lease conditionally so the run becomes re-eligible
                    # immediately and recovery finalizes the actual outcome.
                    logger.warning(
                        "Worker %s exiting run %s after lease loss",
                        self._lease_owner,
                        self.import_run.id,
                    )
                    self._release_lease()
                    return

                if fatal is not None:
                    self._log("error", f"Import failed: {fatal.message}")
                    finalized = self._finalize(
                        status=ImportRun.Status.FAILED,
                        phase=ImportRun.Phase.FAILED,
                        finished_at=timezone.now(),
                    )
                    if not finalized:
                        # Ownership lost mid-failure: the current owner is
                        # responsible for the terminal state. Preserve the
                        # file and exit without raising over its progress.
                        logger.warning(
                            "Worker %s skipped FAILED finalization for run %s: "
                            "lease lost",
                            self._lease_owner,
                            self.import_run.id,
                        )
                        return
                    # Files are retained after failure for diagnosis/retry.
                    self._release_lease()
                    raise Exception("Import failed")

                self._reconcile_counters()
                self._log(
                    "info",
                    f"Import completed successfully. "
                    f"Successful: {self.import_run.successful_rows}, "
                    f"Failed: {self.import_run.failed_rows}, "
                    f"Skipped: {self.import_run.skipped_rows}, "
                    f"Retryable: {self.import_run.retryable_rows}",
                )
                finalized = self._finalize(
                    status=ImportRun.Status.FINISHED,
                    phase=ImportRun.Phase.FINISHED,
                    finished_at=timezone.now(),
                )
                if not finalized:
                    logger.warning(
                        "Worker %s completed work for run %s but lost the "
                        "lease; leaving finalization to the current owner",
                        self._lease_owner,
                        self.import_run.id,
                    )
                    return
                if self._remove_file(file_path):
                    self._log("info", f"Deleted temporary file: {file_path}")
                else:
                    self._log("info", "Cleaning up temporary files: nothing to delete")
                self._release_lease()
