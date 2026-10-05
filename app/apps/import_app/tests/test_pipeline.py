"""Tests for the two-phase durable import pipeline (Tasks 3-6).

Covers:
- unified source readers + persistent staging (phase 1)
- strict single-transaction commit / fault tolerant per-row commits
- counters derived from ImportRow terminal states
- structured failure reasons, retryable classification, attempts
- compare/internal_id dedupe -> SKIPPED
- QIF ZIP section handling
- transaction rule on_commit enqueue semantics
"""

import json
import os
import shutil
import zipfile
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import OperationalError
from django.test import TestCase

from apps.accounts.models import Account, AccountGroup
from apps.common.middleware.thread_local import (
    delete_current_user,
    write_current_user,
)
from apps.currencies.models import Currency
from apps.import_app.models import ImportProfile, ImportRow, ImportRun
from apps.import_app.services.v1 import ImportService
from apps.transactions.models import Transaction

CSV_YAML_STRICT = """
settings:
  file_type: csv
  importing: transactions
  trigger_transaction_rules: false
mapping:
  date_field:
    source: date
    target: date
    format: "%Y-%m-%d"
    required: true
  amount_field:
    source: amount
    target: amount
    required: true
  description_field:
    source: description
    target: description
  account_field:
    source: account
    target: account
    type: name
deduplication:
  - type: compare
    fields: [date, amount, description]
    match_type: lax
"""

CSV_YAML_FAULT = CSV_YAML_STRICT.replace(
    "trigger_transaction_rules: false",
    "trigger_transaction_rules: false\n  skip_errors: true",
)

CSV_YAML_RULES = """
settings:
  file_type: csv
  importing: transactions
mapping:
  date_field:
    source: date
    target: date
    format: "%Y-%m-%d"
    required: true
  amount_field:
    source: amount
    target: amount
    required: true
  description_field:
    source: description
    target: description
  account_field:
    source: account
    target: account
    type: name
"""


class PipelineTestCase(TestCase):
    def setUp(self):
        self.original_temp_dir = ImportService.TEMP_DIR
        self.test_dir = os.path.abspath("temp_test_pipeline")
        ImportService.TEMP_DIR = self.test_dir
        os.makedirs(self.test_dir, exist_ok=True)

        User = get_user_model()
        self.user = User.objects.create_user(
            email="pipeline@example.com", password="password"
        )
        write_current_user(self.user)

        self.currency = Currency.objects.create(
            code="USD", name="US Dollar", decimal_places=2, prefix="$ "
        )
        self.group = AccountGroup.objects.create(name="Group", owner=self.user)
        self.account = Account.objects.create(
            name="Main",
            group=self.group,
            currency=self.currency,
            owner=self.user,
        )
        self.other_account = Account.objects.create(
            name="Savings",
            group=self.group,
            currency=self.currency,
            owner=self.user,
        )

    def tearDown(self):
        delete_current_user()
        ImportService.TEMP_DIR = self.original_temp_dir
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _make_profile(self, name, yaml_config):
        return ImportProfile.objects.create(
            name=name,
            yaml_config=yaml_config,
            version=ImportProfile.Versions.VERSION_1,
        )

    def _make_run(self, profile, file_name="data.csv", mode=None):
        kwargs = {"profile": profile, "file_name": file_name}
        if mode is not None:
            kwargs["mode"] = mode
        return ImportRun.objects.create(**kwargs)

    def _write_csv(self, name, content):
        path = os.path.join(self.test_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    # ------------------------------------------------------------------
    # Task 3: phase 1 staging
    # ------------------------------------------------------------------

    def test_tr31_csv_rows_staged_with_serializable_mapped_payload(self):
        path = self._write_csv(
            "a.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,Lunch,Main\n"
            "2024-01-02,20.50,Dinner,Main\n",
        )
        profile = self._make_profile("p1", CSV_YAML_STRICT)
        run = self._make_run(profile, "a.csv")
        service = ImportService(run)

        kind = service.parse_and_stage(path)

        self.assertEqual(kind, "csv")
        rows = run.rows.order_by("sequence")
        self.assertEqual(rows.count(), 2)
        for row in rows:
            self.assertEqual(row.status, ImportRow.Status.STAGED)
            # JSON-serializable and hydrated values present
            json.dumps(row.mapped_payload)
            self.assertEqual(row.mapped_payload["date"], "2024-01-0" + str(row.row_number + 0))
            self.assertIn("amount", row.mapped_payload)

        run.refresh_from_db()
        self.assertEqual(run.total_rows, 2)
        self.assertEqual(run.phase, ImportRun.Phase.PARSING)
        self.assertEqual(run.cursor["phase"], "parsing")
        self.assertEqual(run.source_summary["kind"], "csv")
        self.assertIn("a.csv", run.source_summary["sections"])
        self.assertEqual(run.source_summary["sections"]["a.csv"], 2)
        self.assertEqual(len(run.file_hash), 64)

    def test_tr32_invalid_row_is_permanent_and_strict_aborts_with_zero_tx(self):
        path = self._write_csv(
            "b.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,Lunch,Main\n"
            ",30.00,Missing date,Main\n",
        )
        profile = self._make_profile("p2", CSV_YAML_STRICT)
        run = self._make_run(profile, "b.csv")
        service = ImportService(run)

        with self.assertRaises(Exception) as cm:
            service.process_file(path)
        self.assertEqual(str(cm.exception), "Import failed")

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FAILED)
        self.assertEqual(Transaction.objects.count(), 0)
        self.assertEqual(run.successful_rows, 0)
        # Diagnostics persisted despite no domain commit.
        self.assertEqual(run.failed_rows, 1)
        self.assertEqual(run.processed_rows, 1)
        bad = run.rows.get(row_number=2)
        self.assertEqual(bad.status, ImportRow.Status.FAILED_PERMANENT)
        self.assertEqual(bad.failure_reason["stage"], "validate")
        self.assertIn("code", bad.failure_reason)
        self.assertIn("message", bad.failure_reason)
        self.assertEqual(bad.failure_reason["line"], 2)

    def test_tr32_missing_reference_rejected_in_strict_stage1(self):
        path = self._write_csv(
            "c.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,Lunch,Ghost Account\n",
        )
        profile = self._make_profile("p3", CSV_YAML_STRICT)
        run = self._make_run(profile, "c.csv")
        service = ImportService(run)

        with self.assertRaises(Exception):
            service.process_file(path)
        self.assertEqual(Transaction.objects.count(), 0)
        bad = run.rows.first()
        self.assertEqual(bad.status, ImportRow.Status.FAILED_PERMANENT)
        self.assertIn("Account", bad.failure_reason["message"])

    def test_tr33_parse_and_stage_is_idempotent_on_reentry(self):
        path = self._write_csv(
            "d.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,Lunch,Main\n"
            ",30.00,Missing date,Main\n",
        )
        profile = self._make_profile("p4", CSV_YAML_FAULT)
        run = self._make_run(
            profile, "d.csv", mode=ImportRun.Mode.FAULT_TOLERANT
        )
        service = ImportService(run)

        service.parse_and_stage(path)
        service.parse_and_stage(path)

        self.assertEqual(run.rows.count(), 2)
        self.assertEqual(
            run.rows.filter(status=ImportRow.Status.FAILED_PERMANENT).count(), 1
        )

    def test_tr31_excel_rows_staged_with_sheet_as_section(self):
        import openpyxl

        path = os.path.join(self.test_dir, "w.xlsx")
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "Ledger"
        sheet.append(["date", "amount", "description", "account"])
        sheet.append(["2024-01-01", 10.0, "Lunch", "Main"])
        sheet.append(["2024-01-02", 20.5, "Dinner", "Main"])
        workbook.save(path)

        yaml_config = CSV_YAML_STRICT.replace("file_type: csv", "file_type: xlsx")
        profile = self._make_profile("p1x", yaml_config)
        run = self._make_run(profile, "w.xlsx")
        service = ImportService(run)

        kind = service.parse_and_stage(path)

        self.assertEqual(kind, "excel")
        self.assertEqual(run.rows.count(), 2)
        self.assertTrue(
            run.rows.filter(section="Ledger", status=ImportRow.Status.STAGED).count()
            == 2
        )
        run.refresh_from_db()
        self.assertEqual(run.source_summary["sections"], {"Ledger": 2})

    # ------------------------------------------------------------------
    # Task 4: phase 2 commit
    # ------------------------------------------------------------------

    def test_tr414345_strict_commit_failure_rolls_back_but_keeps_diagnostics(self):
        path = self._write_csv(
            "e.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,One,Main\n"
            "2024-01-02,20.00,Two,Main\n",
        )
        profile = self._make_profile("p5", CSV_YAML_STRICT)
        run = self._make_run(profile, "e.csv")
        service = ImportService(run)

        original_create = service._create_transaction

        def flaky_create(data):
            flaky_create.calls += 1
            if flaky_create.calls == 2:
                raise ValueError("injected commit failure")
            return original_create(data)

        flaky_create.calls = 0

        with patch.object(service, "_create_transaction", side_effect=flaky_create):
            with self.assertRaises(Exception) as cm:
                service.process_file(path)
        self.assertEqual(str(cm.exception), "Import failed")

        run.refresh_from_db()
        self.assertEqual(Transaction.objects.count(), 0)
        self.assertEqual(run.successful_rows, 0)
        self.assertEqual(run.status, ImportRun.Status.FAILED)
        offending = run.rows.get(row_number=2)
        self.assertEqual(offending.status, ImportRow.Status.FAILED_PERMANENT)
        self.assertEqual(offending.failure_reason["stage"], "commit")
        self.assertEqual(offending.failure_reason["code"], "ValueError")
        # Row 1 went back to STAGED with the rolled-back batch.
        self.assertEqual(
            run.rows.get(row_number=1).status, ImportRow.Status.STAGED
        )
        self.assertEqual(run.failed_rows, 1)
        self.assertEqual(run.processed_rows, 1)
        self.assertIsNone(offending.transaction)

    def test_tr44_transient_error_classified_retryable(self):
        path = self._write_csv(
            "f.csv",
            "date,amount,description,account\n2024-01-01,10.00,One,Main\n",
        )
        profile = self._make_profile("p6", CSV_YAML_STRICT)
        run = self._make_run(profile, "f.csv")
        service = ImportService(run)

        with patch.object(
            service,
            "_create_transaction",
            side_effect=OperationalError("connection reset"),
        ):
            with self.assertRaises(Exception):
                service.process_file(path)

        run.refresh_from_db()
        row = run.rows.first()
        self.assertEqual(row.status, ImportRow.Status.FAILED_RETRYABLE)
        self.assertEqual(run.retryable_rows, 1)
        self.assertEqual(run.failed_rows, 1)
        self.assertEqual(run.successful_rows, 0)

    def test_tr42_fault_tolerant_mixed_rows_partial_commit(self):
        path = self._write_csv(
            "g.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,Good one,Main\n"
            "2024-01-02,20.00,Ghost account,Ghost\n"
            "2024-01-03,30.00,Good two,Main\n",
        )
        profile = self._make_profile("p7", CSV_YAML_FAULT)
        run = self._make_run(
            profile, "g.csv", mode=ImportRun.Mode.FAULT_TOLERANT
        )
        service = ImportService(run)

        service.process_file(path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(Transaction.objects.count(), 2)
        self.assertEqual(run.successful_rows, 2)
        self.assertEqual(run.failed_rows, 1)
        self.assertEqual(run.skipped_rows, 0)
        self.assertEqual(run.processed_rows, 3)
        statuses = list(
            run.rows.order_by("sequence").values_list("status", flat=True)
        )
        self.assertEqual(
            statuses,
            [
                ImportRow.Status.COMMITTED,
                ImportRow.Status.FAILED_PERMANENT,
                ImportRow.Status.COMMITTED,
            ],
        )
        good = run.rows.filter(status=ImportRow.Status.COMMITTED).first()
        self.assertIsNotNone(good.transaction_id)
        self.assertIsNotNone(good.committed_at)

    def test_tr44_attempts_increment_across_retries(self):
        path = self._write_csv(
            "h.csv",
            "date,amount,description,account\n"
            "2024-01-02,20.00,Ghost account,Ghost\n",
        )
        profile = self._make_profile("p8", CSV_YAML_FAULT)
        run = self._make_run(
            profile, "h.csv", mode=ImportRun.Mode.FAULT_TOLERANT
        )
        service = ImportService(run)
        service.parse_and_stage(path)
        service.commit_rows()
        row = run.rows.first()
        self.assertEqual(row.status, ImportRow.Status.FAILED_PERMANENT)
        self.assertEqual(row.attempts, 1)

        # Retry: reset to STAGED like the retry service would.
        ImportRow.objects.filter(id=row.id).update(
            status=ImportRow.Status.STAGED
        )
        service.commit_rows()
        row.refresh_from_db()
        self.assertEqual(row.status, ImportRow.Status.FAILED_PERMANENT)
        self.assertEqual(row.attempts, 2)

    def test_tr46_compare_dedupe_marks_row_skipped(self):
        Transaction.objects.create(
            account=self.account,
            type=Transaction.Type.EXPENSE,
            date=date(2024, 1, 1),
            amount=Decimal("10.00"),
            description="Lunch",
        )
        path = self._write_csv(
            "i.csv",
            "date,amount,description,account\n2024-01-01,10.00,Lunch,Main\n",
        )
        profile = self._make_profile("p9", CSV_YAML_STRICT)
        run = self._make_run(profile, "i.csv")
        service = ImportService(run)

        service.process_file(path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(Transaction.objects.count(), 1)  # only the pre-existing
        self.assertEqual(run.successful_rows, 0)
        self.assertEqual(run.skipped_rows, 1)
        self.assertEqual(run.processed_rows, 1)
        self.assertEqual(run.rows.first().status, ImportRow.Status.SKIPPED)

    # ------------------------------------------------------------------
    # Task 5: QIF ZIP
    # ------------------------------------------------------------------

    def test_tr52_qif_zip_uses_members_as_sections_and_ignores_junk(self):
        zip_path = os.path.join(self.test_dir, "bundle.zip")
        # internal_id is the sha256 of the raw record lines (legacy
        # semantics, account name excluded), so members must differ.
        main_qif = "!Type:Bank\nD04/01/2015\nT100.00\nPmain\n^\n"
        savings_qif = "!Type:Bank\nD05/01/2015\nT200.00\nPsavings\n^\n"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("Main.qif", main_qif)
            zf.writestr("Savings.qif", savings_qif)
            zf.writestr("__MACOSX/._Main.qif", main_qif)
            zf.writestr("notes.txt", "ignore me")

        yaml_config = """
settings:
  file_type: qif
  importing: transactions
  date_format: "%d/%m/%Y"
mapping: {}
"""
        profile = self._make_profile("p10", yaml_config)
        run = self._make_run(profile, "bundle.zip")
        service = ImportService(run)

        service.process_file(zip_path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(Transaction.objects.count(), 2)
        self.assertEqual(run.successful_rows, 2)
        sections = run.source_summary["sections"]
        self.assertEqual(set(sections.keys()), {"Main.qif", "Savings.qif"})
        self.assertTrue(
            run.rows.filter(section="Main.qif").exists()
        )
        self.assertTrue(
            run.rows.filter(section="Savings.qif").exists()
        )

    # ------------------------------------------------------------------
    # Task 6: rule enqueue on commit
    # ------------------------------------------------------------------

    def _run_with_rule_defer(self, yaml_config, path, run, patch_target=None):
        service = ImportService(run)
        with patch(
            "apps.rules.tasks.check_for_transaction_rules.defer"
        ) as mock_defer:
            if patch_target is not None:
                patch_target(service, mock_defer)
            with self.captureOnCommitCallbacks(execute=True):
                service.process_file(path)
        return service, mock_defer

    def test_tr61_fault_mode_defers_rules_once_per_committed_row(self):
        path = self._write_csv(
            "r1.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,One,Main\n"
            "2024-01-02,20.00,Two,Main\n"
            "2024-01-03,30.00,Ghost,Ghost\n",
        )
        yaml_config = CSV_YAML_RULES.replace(
            "settings:\n  file_type: csv",
            "settings:\n  skip_errors: true\n  file_type: csv",
        )
        profile = self._make_profile("rp1", yaml_config)
        run = self._make_run(
            profile, "r1.csv", mode=ImportRun.Mode.FAULT_TOLERANT
        )

        _, mock_defer = self._run_with_rule_defer(yaml_config, path, run)

        self.assertEqual(mock_defer.call_count, 2)
        for call in mock_defer.call_args_list:
            self.assertEqual(call.kwargs["signal"], "transaction_created")
            self.assertTrue(call.kwargs["queueing_lock"].startswith(
                "import-rule-created-"
            ))
            self.assertIn("instance_id", call.kwargs)

    def test_tr62_strict_rollback_defers_zero(self):
        path = self._write_csv(
            "r2.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,One,Main\n"
            "2024-01-02,20.00,Two,Main\n",
        )
        profile = self._make_profile("rp2", CSV_YAML_RULES)
        run = self._make_run(profile, "r2.csv")

        service = ImportService(run)
        original = service._create_transaction

        def flaky(data):
            flaky.calls += 1
            if flaky.calls == 2:
                raise ValueError("boom")
            return original(data)

        flaky.calls = 0

        with patch(
            "apps.rules.tasks.check_for_transaction_rules.defer"
        ) as mock_defer, patch.object(
            service, "_create_transaction", side_effect=flaky
        ):
            with self.assertRaises(Exception):
                with self.captureOnCommitCallbacks(execute=True):
                    service.process_file(path)

        self.assertEqual(mock_defer.call_count, 0)
        self.assertEqual(Transaction.objects.count(), 0)
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FAILED)

    def test_tr62_strict_success_defers_per_transaction(self):
        path = self._write_csv(
            "r3.csv",
            "date,amount,description,account\n"
            "2024-01-01,10.00,One,Main\n"
            "2024-01-02,20.00,Two,Main\n",
        )
        profile = self._make_profile("rp3", CSV_YAML_RULES)
        run = self._make_run(profile, "r3.csv")

        _, mock_defer = self._run_with_rule_defer(CSV_YAML_RULES, path, run)
        self.assertEqual(mock_defer.call_count, 2)

    def test_tr64_trigger_disabled_defers_zero(self):
        path = self._write_csv(
            "r4.csv",
            "date,amount,description,account\n2024-01-01,10.00,One,Main\n",
        )
        profile = self._make_profile("rp4", CSV_YAML_STRICT)
        run = self._make_run(profile, "r4.csv")

        _, mock_defer = self._run_with_rule_defer(
            CSV_YAML_STRICT, path, run
        )
        self.assertEqual(mock_defer.call_count, 0)

    def test_tr54_qif_never_defers_rules(self):
        path = os.path.join(self.test_dir, "Main.qif")
        with open(path, "w", encoding="utf-8") as f:
            f.write("!Type:Bank\nD04/01/2015\nT100.00\nPOK\n^\n")
        yaml_config = """
settings:
  file_type: qif
  importing: transactions
  date_format: "%d/%m/%Y"
mapping: {}
"""
        profile = self._make_profile("rp5", yaml_config)
        run = self._make_run(profile, "Main.qif")

        _, mock_defer = self._run_with_rule_defer(yaml_config, path, run)
        self.assertEqual(mock_defer.call_count, 0)

    def test_tr63_enqueue_conflict_is_swallowed(self):
        path = self._write_csv(
            "r5.csv",
            "date,amount,description,account\n2024-01-01,10.00,One,Main\n",
        )
        profile = self._make_profile("rp6", CSV_YAML_RULES)
        run = self._make_run(profile, "r5.csv")
        service = ImportService(run)

        with patch(
            "apps.rules.tasks.check_for_transaction_rules.defer",
            side_effect=Exception("already enqueued"),
        ):
            # The on_commit callback must not propagate enqueue conflicts.
            with self.captureOnCommitCallbacks(execute=True):
                service.process_file(path)  # no exception

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(run.successful_rows, 1)
        # A real (non-dedup) enqueue failure must leave a run-level trace.
        self.assertIn("Rule enqueue failed for transaction", run.logs)

    def test_tr63b_already_enqueued_is_deduped_silently(self):
        from procrastinate import exceptions as procrastinate_exceptions

        path = self._write_csv(
            "r5b.csv",
            "date,amount,description,account\n2024-01-01,10.00,One,Main\n",
        )
        profile = self._make_profile("rp6b", CSV_YAML_RULES)
        run = self._make_run(profile, "r5b.csv")
        service = ImportService(run)

        with patch(
            "apps.rules.tasks.check_for_transaction_rules.defer",
            side_effect=lambda *args, **kwargs: (
                procrastinate_exceptions.AlreadyEnqueued()
            ),
        ):
            with self.captureOnCommitCallbacks(execute=True):
                service.process_file(path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertNotIn("Rule enqueue failed", run.logs)
