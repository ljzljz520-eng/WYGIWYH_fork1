"""Tests for lease takeover, crash recovery, retries and temp file lifecycle."""

import os
import shutil
import threading
import time
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient
from django.test import Client

from apps.accounts.models import Account, AccountGroup
from apps.common.middleware.thread_local import (
    delete_current_user,
    write_current_user,
)
from apps.currencies.models import Currency
from apps.import_app import tasks as import_tasks
from apps.import_app.models import ImportProfile, ImportRow, ImportRun
from apps.import_app.services import enqueue as enqueue_service
from apps.import_app.services.retry import (
    RetryNotAvailable,
    retry_import_run,
)
from apps.import_app.services.v1 import ImportService
from apps.transactions.models import Transaction

CSV_YAML_FAULT = """
settings:
  file_type: csv
  importing: transactions
  skip_errors: true
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
"""

CSV_YAML_STRICT = CSV_YAML_FAULT.replace("  skip_errors: true\n", "")


CSV_CONTENT = (
    "date,amount,description,account\n"
    "2024-01-01,10.00,One,Main\n"
    "2024-01-02,20.00,Two,Main\n"
    "2024-01-03,30.00,Three,Main\n"
    "2024-01-04,40.00,Four,Main\n"
)


class RecoveryTestCase(TransactionTestCase):
    # TransactionTestCase so the procrastinate task wrapper's
    # close_old_connections() and real on_commit semantics work as in a
    # worker (TestCase wraps tests in an atomic block that forbids this).
    def setUp(self):
        self.original_temp_dir = ImportService.TEMP_DIR
        self.test_dir = os.path.abspath("temp_test_recovery")
        ImportService.TEMP_DIR = self.test_dir
        os.makedirs(self.test_dir, exist_ok=True)

        User = get_user_model()
        self.user = User.objects.create_user(
            email="recovery@example.com", password="password"
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

        self.profile = ImportProfile.objects.create(
            name="recovery-profile",
            yaml_config=CSV_YAML_FAULT,
            version=ImportProfile.Versions.VERSION_1,
        )

    def tearDown(self):
        # The task wrapper clears the thread-local user on successful runs.
        try:
            delete_current_user()
        except AttributeError:
            pass
        ImportService.TEMP_DIR = self.original_temp_dir
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _write_file(self, name="data.csv", content=CSV_CONTENT):
        path = os.path.join(self.test_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def _make_run(self, mode=ImportRun.Mode.FAULT_TOLERANT, **kwargs):
        defaults = {
            "profile": self.profile,
            "file_name": "data.csv",
            "mode": mode,
            "requested_by": self.user,
        }
        defaults.update(kwargs)
        return ImportRun.objects.create(**defaults)

    # ------------------------------------------------------------------
    # TR-7.1: resume from an interrupted commit
    # ------------------------------------------------------------------

    def test_tr71_resume_after_worker_crash_no_duplicates(self):
        path = self._write_file()
        run = self._make_run(stored_file_path=path)
        service = ImportService(run)
        service.parse_and_stage(path)
        rows = list(run.rows.order_by("sequence"))
        self.assertEqual(len(rows), 4)

        # Worker commits the first two rows, then dies: run stays
        # PROCESSING with an expired lease and rows 3-4 STAGED.
        for row in rows[:2]:
            with transaction.atomic():
                service._commit_row(row)
        service._reconcile_counters()
        past = timezone.now() - timedelta(minutes=30)
        ImportRun.objects.filter(id=run.id).update(
            status=ImportRun.Status.PROCESSING,
            lease_owner="dead-worker",
            lease_expires_at=past,
        )
        self.assertEqual(Transaction.objects.count(), 2)

        # Recovery task fires: the new worker takes over and resumes.
        with patch(
            "apps.rules.tasks.check_for_transaction_rules.defer"
        ):
            import_tasks.process_import.func(
                import_run_id=run.id,
                file_path=path,
                user_id=self.user.id,
            )

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(Transaction.objects.count(), 4)
        self.assertEqual(
            run.rows.filter(status=ImportRow.Status.COMMITTED).count(), 4
        )
        self.assertEqual(run.successful_rows, 4)
        self.assertEqual(run.processed_rows, 4)
        self.assertFalse(os.path.exists(path))

    # ------------------------------------------------------------------
    # TR-7.2: fresh lease makes a second worker no-op
    # ------------------------------------------------------------------

    def test_tr72_fresh_lease_second_instance_is_noop(self):
        path = self._write_file()
        run = self._make_run()
        service = ImportService(run)
        service.parse_and_stage(path)
        self.assertEqual(Transaction.objects.count(), 0)

        future = timezone.now() + timedelta(minutes=5)
        ImportRun.objects.filter(id=run.id).update(
            status=ImportRun.Status.PROCESSING,
            lease_owner="other-worker",
            lease_expires_at=future,
        )

        # A second instance must not touch anything.
        ImportService(run).process_file(path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.PROCESSING)
        self.assertEqual(run.lease_owner, "other-worker")
        self.assertEqual(Transaction.objects.count(), 0)
        self.assertIn("already leased", run.logs)

    # ------------------------------------------------------------------
    # TR-7.3: periodic recovery
    # ------------------------------------------------------------------

    def test_tr73_periodic_recovery_defers_only_expired_processing(self):
        path = self._write_file()
        past = timezone.now() - timedelta(minutes=30)
        future = timezone.now() + timedelta(minutes=30)

        stale = self._make_run(
            status=ImportRun.Status.PROCESSING,
            lease_owner="dead",
            lease_expires_at=past,
            stored_file_path=path,
        )
        self._make_run(
            status=ImportRun.Status.PROCESSING,
            lease_owner="alive",
            lease_expires_at=future,
        )
        self._make_run(status=ImportRun.Status.QUEUED)
        self._make_run(status=ImportRun.Status.FINISHED)

        with patch(
            "apps.import_app.tasks.process_import.defer"
        ) as mock_defer:
            result = import_tasks.recover_stale_import_runs.func()

        self.assertEqual(result["candidates"], 1)
        self.assertEqual(result["recovered"], 1)
        self.assertEqual(mock_defer.call_count, 1)
        kwargs = mock_defer.call_args.kwargs
        self.assertEqual(kwargs["import_run_id"], stale.id)
        self.assertEqual(kwargs["queueing_lock"], f"import-recover-run-{stale.id}")

    # ------------------------------------------------------------------
    # TR-7.4: retry service semantics
    # ------------------------------------------------------------------

    def test_tr74_strict_retry_clears_rows_and_requques(self):
        path = self._write_file()
        run = self._make_run(
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.FAILED,
            stored_file_path=path,
            successful_rows=0,
            failed_rows=1,
        )
        ImportRow.objects.create(
            run=run,
            sequence=1,
            section="data.csv",
            row_number=1,
            idempotency_key="x" * 64,
            raw_payload={},
            status=ImportRow.Status.FAILED_PERMANENT,
            failure_reason={"stage": "validate"},
        )

        with patch("apps.import_app.tasks.process_import.defer") as mock_defer:
            retry_import_run(run, user_id=self.user.id)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.QUEUED)
        self.assertEqual(run.phase, ImportRun.Phase.ENQUEUED)
        self.assertEqual(run.rows.count(), 0)
        self.assertEqual(run.failed_rows, 0)
        self.assertIsNone(run.lease_owner)
        mock_defer.assert_called_once()

    def test_tr74_fault_retry_keeps_terminal_resets_retryable(self):
        path = self._write_file()
        run = self._make_run(
            mode=ImportRun.Mode.FAULT_TOLERANT,
            status=ImportRun.Status.FAILED,
            stored_file_path=path,
        )
        committed = ImportRow.objects.create(
            run=run, sequence=1, section="s", row_number=1,
            idempotency_key="a" * 64, status=ImportRow.Status.COMMITTED,
            attempts=1,
        )
        retryable = ImportRow.objects.create(
            run=run, sequence=2, section="s", row_number=2,
            idempotency_key="b" * 64, status=ImportRow.Status.FAILED_RETRYABLE,
            failure_reason={"stage": "commit"}, attempts=3,
        )

        with patch("apps.import_app.tasks.process_import.defer"):
            retry_import_run(run, user_id=self.user.id)

        committed.refresh_from_db()
        retryable.refresh_from_db()
        self.assertEqual(committed.status, ImportRow.Status.COMMITTED)
        self.assertEqual(retryable.status, ImportRow.Status.STAGED)
        self.assertIsNone(retryable.failure_reason)
        self.assertEqual(retryable.attempts, 3)  # attempts preserved

    def test_tr74_finished_and_queued_not_retriable(self):
        path = self._write_file()
        finished = self._make_run(
            status=ImportRun.Status.FINISHED, stored_file_path=path
        )
        queued = self._make_run(status=ImportRun.Status.QUEUED)
        for run in (finished, queued):
            with self.assertRaises(RetryNotAvailable) as cm:
                retry_import_run(run, user_id=self.user.id)
            self.assertEqual(cm.exception.http_status, 409)

    def test_tr74_missing_file_blocks_retry_without_reset(self):
        run = self._make_run(
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.FAILED,
            stored_file_path=os.path.join(self.test_dir, "gone.csv"),
        )
        with self.assertRaises(RetryNotAvailable) as cm:
            retry_import_run(run, user_id=self.user.id)
        self.assertEqual(cm.exception.code, "source_file_missing")
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FAILED)

    # ------------------------------------------------------------------
    # TR-8.1 / TR-8.3: file lifecycle
    # ------------------------------------------------------------------

    def test_tr81_finished_deletes_failed_keeps_file(self):
        good_path = self._write_file("good.csv")
        good_run = self._make_run(stored_file_path=good_path)
        ImportService(good_run).process_file(good_path)
        self.assertFalse(os.path.exists(good_path))

        bad_path = self._write_file(
            "bad.csv",
            "date,amount,description,account\n,10.00,x,Main\n",
        )
        strict_profile = ImportProfile.objects.create(
            name="strict-profile",
            yaml_config=CSV_YAML_STRICT,
            version=ImportProfile.Versions.VERSION_1,
        )
        bad_run = ImportRun.objects.create(
            profile=strict_profile,
            file_name="bad.csv",
            mode=ImportRun.Mode.STRICT,
            stored_file_path=bad_path,
        )
        with self.assertRaises(Exception):
            ImportService(bad_run).process_file(bad_path)
        self.assertTrue(os.path.exists(bad_path))
        bad_run.refresh_from_db()
        self.assertEqual(bad_run.status, ImportRun.Status.FAILED)

    def test_tr83_cleanup_removes_only_old_unreferenced_files(self):
        referenced = os.path.join(self.test_dir, "referenced.csv")
        fresh_orphan = os.path.join(self.test_dir, "fresh.csv")
        old_orphan = os.path.join(self.test_dir, "old.csv")
        for path in (referenced, fresh_orphan, old_orphan):
            with open(path, "w") as f:
                f.write("x")

        # FAILED run keeps its referenced file alive.
        self._make_run(
            status=ImportRun.Status.FAILED, stored_file_path=referenced
        )
        old_ts = time.time() - 25 * 3600
        os.utime(old_orphan, (old_ts, old_ts))

        # The task imports DEFAULT_TEMP_DIR at call time.
        with patch.object(
            enqueue_service, "DEFAULT_TEMP_DIR", self.test_dir
        ):
            result = import_tasks.cleanup_orphan_temp_files.func()

        self.assertTrue(os.path.exists(referenced))
        self.assertTrue(os.path.exists(fresh_orphan))
        self.assertFalse(os.path.exists(old_orphan))
        self.assertEqual(result["removed"], 1)

    # ------------------------------------------------------------------
    # Lease fencing / row-claim mutual exclusion (hardening)
    # ------------------------------------------------------------------

    def test_tr76_concurrent_row_claim_creates_no_duplicate(self):
        """Two workers racing one STAGED row: the loser's claim matches 0
        rows once the winner commits and it aborts with LeaseLostError."""
        from apps.import_app.services.v1 import LeaseLostError

        single = (
            "date,amount,description,account\n"
            "2024-01-01,10.00,One,Main\n"
        )
        path = self._write_file(name="single.csv", content=single)
        run = self._make_run(stored_file_path=path)
        ImportService(run).parse_and_stage(path)
        row = run.rows.get()
        errors = {}

        def worker_a():
            # Simulates the original (slow) worker holding the row inside
            # an open domain transaction on its own DB connection.
            write_current_user(self.user)
            try:
                from django.db import connection as conn

                conn.close()
                service = ImportService(
                    ImportRun.objects.get(id=run.id)
                )
                with transaction.atomic():
                    service._commit_row(
                        ImportRow.objects.get(id=row.id)
                    )
                    time.sleep(1.5)
            except Exception as exc:  # noqa: BLE001 - recorded for asserts
                errors["a"] = repr(exc)
            finally:
                from django.db import connection as conn

                conn.close()

        def worker_b():
            time.sleep(0.5)
            write_current_user(self.user)
            try:
                from django.db import connection as conn

                conn.close()
                service = ImportService(
                    ImportRun.objects.get(id=run.id)
                )
                with transaction.atomic():
                    service._commit_row(
                        ImportRow.objects.get(id=row.id)
                    )
            except LeaseLostError:
                errors["b"] = "lease_lost"
            except Exception as exc:  # noqa: BLE001
                errors["b"] = repr(exc)
            finally:
                from django.db import connection as conn

                conn.close()

        thread_a = threading.Thread(target=worker_a)
        thread_b = threading.Thread(target=worker_b)
        thread_a.start()
        thread_b.start()
        thread_a.join(timeout=10)
        thread_b.join(timeout=10)
        self.assertFalse(thread_a.is_alive())
        self.assertFalse(thread_b.is_alive())
        self.assertNotIn("a", errors, errors.get("a"))
        self.assertEqual(errors.get("b"), "lease_lost", errors)
        # Exactly one transaction despite the overlapping workers.
        self.assertEqual(Transaction.objects.count(), 1)
        self.assertEqual(
            ImportRow.objects.get(id=row.id).status,
            ImportRow.Status.COMMITTED,
        )

    def test_tr77_lease_stolen_before_strict_commit_aborts_quietly(self):
        strict_profile = ImportProfile.objects.create(
            name="strict-recovery-profile",
            yaml_config=CSV_YAML_STRICT,
            version=ImportProfile.Versions.VERSION_1,
        )
        path = self._write_file(name="strict-one.csv")
        run = ImportRun.objects.create(
            profile=strict_profile,
            file_name="strict-one.csv",
            mode=ImportRun.Mode.STRICT,
            stored_file_path=path,
        )
        service = ImportService(run)
        original_parse = service.parse_and_stage

        def steal_lease(file_path):
            original_parse(file_path)
            future = timezone.now() + timedelta(minutes=5)
            ImportRun.objects.filter(id=run.id).update(
                lease_owner="thief-worker", lease_expires_at=future
            )

        service.parse_and_stage = steal_lease

        # Must not raise: fencing says another worker owns the run.
        service.process_file(path)

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.PROCESSING)
        self.assertEqual(Transaction.objects.count(), 0)
        self.assertTrue(os.path.exists(path))

    def test_tr77_lease_stolen_after_fault_work_leaves_finalization(self):
        path = self._write_file()
        run = self._make_run(stored_file_path=path)
        service = ImportService(run)
        original_parse = service.parse_and_stage

        def steal_lease(file_path):
            original_parse(file_path)
            future = timezone.now() + timedelta(minutes=5)
            ImportRun.objects.filter(id=run.id).update(
                lease_owner="thief-worker", lease_expires_at=future
            )

        service.parse_and_stage = steal_lease

        service.process_file(path)  # committed the rows, but cannot finalize

        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.PROCESSING)
        self.assertEqual(Transaction.objects.count(), 4)
        self.assertEqual(run.successful_rows, 4)
        # The source file must survive for the owning worker/recovery.
        self.assertTrue(os.path.exists(path))

    def test_tr78_retry_collision_rolls_back_row_reset(self):
        path = self._write_file()
        run = self._make_run(
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.FAILED,
            stored_file_path=path,
            file_hash="h" * 64,
        )
        ImportRow.objects.create(
            run=run, sequence=1, section="data.csv", row_number=1,
            idempotency_key="f" * 64,
            status=ImportRow.Status.FAILED_PERMANENT,
            failure_reason={"stage": "validate"},
        )
        # A newer active run for the same profile + file content exists.
        ImportRun.objects.create(
            profile=self.profile,
            file_name="data.csv",
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.QUEUED,
            file_hash="h" * 64,
        )

        with patch("apps.import_app.tasks.process_import.defer") as mock_defer:
            with self.assertRaises(RetryNotAvailable) as cm:
                retry_import_run(run, user_id=self.user.id)

        self.assertEqual(cm.exception.code, "active_run_exists")
        mock_defer.assert_not_called()
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FAILED)
        # The strict row deletion must have rolled back.
        self.assertEqual(run.rows.count(), 1)

    def test_tr79_commit_phase_cursor_advances(self):
        path = self._write_file()
        run = self._make_run(stored_file_path=path)
        ImportService(run).process_file(path)
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        self.assertEqual(
            run.cursor, {"phase": "committing", "sequence": 4}
        )


class RetryWebTestCase(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            email="retryweb@example.com", password="password"
        )
        self.client = Client()
        self.client.force_login(self.user)
        self.temp_dir = os.path.abspath("temp_test_retry_web")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.profile = ImportProfile.objects.create(
            name="web-retry-profile",
            yaml_config=CSV_YAML_STRICT,
            version=ImportProfile.Versions.VERSION_1,
        )

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def _run(self, status=ImportRun.Status.FAILED, exists=True):
        path = os.path.join(self.temp_dir, "data.csv")
        if exists:
            with open(path, "w") as f:
                f.write(CSV_CONTENT)
        else:
            path = os.path.join(self.temp_dir, "gone.csv")
        return ImportRun.objects.create(
            profile=self.profile,
            file_name="data.csv",
            mode=ImportRun.Mode.STRICT,
            status=status,
            stored_file_path=path,
        )

    @patch("apps.import_app.tasks.process_import.defer")
    def test_tr75_web_retry_accepted(self, mock_defer):
        run = self._run()
        response = self.client.post(
            f"/import/profiles/{self.profile.id}/runs/{run.id}/retry/",
            HTTP_HX_REQUEST="true",
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            response.headers.get("HX-Trigger"), "updated, hide_offcanvas"
        )
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.QUEUED)
        mock_defer.assert_called_once()

    @patch("apps.import_app.tasks.process_import.defer")
    def test_tr75_web_retry_not_available_stays_204(self, mock_defer):
        run = self._run(status=ImportRun.Status.FINISHED)
        response = self.client.post(
            f"/import/profiles/{self.profile.id}/runs/{run.id}/retry/",
            HTTP_HX_REQUEST="true",
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            response.headers.get("HX-Trigger"), "updated, hide_offcanvas"
        )
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.FINISHED)
        mock_defer.assert_not_called()

    def test_tr82_web_delete_removes_file(self):
        run = self._run()
        self.assertTrue(os.path.exists(run.stored_file_path))
        response = self.client.delete(
            f"/import/profiles/{self.profile.id}/runs/{run.id}/delete/",
            HTTP_HX_REQUEST="true",
        )
        self.assertEqual(response.status_code, 204)
        self.assertFalse(os.path.exists(run.stored_file_path))
        self.assertFalse(ImportRun.objects.filter(id=run.id).exists())

    def test_tr91_run_card_shows_retryable_count_and_retry_button(self):
        failed = self._run()
        failed.retryable_rows = 2
        failed.failed_rows = 3
        failed.save(update_fields=["retryable_rows", "failed_rows"])
        finished = ImportRun.objects.create(
            profile=self.profile,
            file_name="done.csv",
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.FINISHED,
        )

        response = self.client.get(
            f"/import/profiles/{self.profile.id}/runs/list/",
            HTTP_HX_REQUEST="true",
        )
        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Retryable Items", content)
        self.assertIn(
            f"/import/profiles/{self.profile.id}/runs/{failed.id}/retry/",
            content,
        )
        self.assertNotIn(
            f"/import/profiles/{self.profile.id}/runs/{finished.id}/retry/",
            content,
        )

    def test_tr92_log_renders_failed_rows_table(self):
        run = self._run()
        ImportRow.objects.create(
            run=run, sequence=1, section="data.csv", row_number=7,
            idempotency_key="e" * 64,
            status=ImportRow.Status.FAILED_RETRYABLE,
            failure_reason={
                "stage": "commit",
                "code": "database_locked",
                "message": "lock timeout",
                "section": "data.csv",
                "line": 7,
            },
            attempts=2,
        )
        response = self.client.get(
            f"/import/profiles/{self.profile.id}/runs/{run.id}/log/",
            HTTP_HX_REQUEST="true",
        )
        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        self.assertIn("Failed rows", content)
        self.assertIn("data.csv", content)
        self.assertIn("database_locked", content)
        self.assertIn("lock timeout", content)
        compact = "".join(content.split())
        self.assertIn("<td>2</td>", compact)


@override_settings(
    STORAGES={
        "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"
        },
    },
    WHITENOISE_AUTOREFRESH=True,
)
class RetryAPITestCase(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            email="retryapi@example.com", password="password"
        )
        self.client = APIClient()
        self.client.force_authenticate(user=self.user)
        self.temp_dir = os.path.abspath("temp_test_retry_api")
        os.makedirs(self.temp_dir, exist_ok=True)
        self.profile = ImportProfile.objects.create(
            name="api-retry-profile",
            yaml_config=CSV_YAML_STRICT,
            version=ImportProfile.Versions.VERSION_1,
        )

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def _failed_run(self, exists=True):
        path = os.path.join(self.temp_dir, "data.csv")
        if exists:
            with open(path, "w") as f:
                f.write(CSV_CONTENT)
        return ImportRun.objects.create(
            profile=self.profile,
            file_name="data.csv",
            mode=ImportRun.Mode.STRICT,
            status=ImportRun.Status.FAILED,
            stored_file_path=path if exists else os.path.join(self.temp_dir, "nope.csv"),
            requested_by=self.user,
        ), path

    @patch("apps.import_app.tasks.process_import.defer")
    def test_tr74_api_retry_accepted(self, mock_defer):
        run, _ = self._failed_run()
        response = self.client.post(f"/api/import/runs/{run.id}/retry/")
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data["import_run_id"], run.id)
        run.refresh_from_db()
        self.assertEqual(run.status, ImportRun.Status.QUEUED)
        mock_defer.assert_called_once()

    def test_tr74_api_retry_conflict_states(self):
        finished, _ = self._failed_run()
        ImportRun.objects.filter(id=finished.id).update(
            status=ImportRun.Status.FINISHED
        )
        response = self.client.post(f"/api/import/runs/{finished.id}/retry/")
        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)

        missing_run, _ = self._failed_run(exists=False)
        response = self.client.post(
            f"/api/import/runs/{missing_run.id}/retry/"
        )
        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertEqual(response.data["code"], "source_file_missing")

    def test_tr94_run_detail_exposes_diagnostics_fields(self):
        run, _ = self._failed_run()
        run.source_summary = {"kind": "csv", "sections": ["data.csv"]}
        run.cursor = {"phase": "committing", "sequence": 1}
        run.retryable_rows = 1
        run.save(update_fields=["source_summary", "cursor", "retryable_rows"])
        response = self.client.get(f"/api/import/runs/{run.id}/")
        self.assertEqual(response.status_code, 200)
        for key in (
            "mode",
            "phase",
            "retryable_rows",
            "file_hash",
            "cursor",
            "source_summary",
        ):
            self.assertIn(key, response.data)
        self.assertEqual(response.data["retryable_rows"], 1)

    @patch("apps.import_app.tasks.process_import.defer")
    def test_tr93_rows_action_with_status_filter(self, _mock_defer):
        run, _ = self._failed_run()
        ImportRow.objects.create(
            run=run, sequence=1, section="s", row_number=1,
            idempotency_key="c" * 64, status=ImportRow.Status.FAILED_PERMANENT,
            failure_reason={"stage": "validate", "code": "x", "message": "m", "line": 1},
        )
        ImportRow.objects.create(
            run=run, sequence=2, section="s", row_number=2,
            idempotency_key="d" * 64, status=ImportRow.Status.STAGED,
            mapped_payload={"x": 1},
        )
        response = self.client.get(
            f"/api/import/runs/{run.id}/rows/?status=FAILED_PERMANENT"
        )
        self.assertEqual(response.status_code, 200)
        results = response.data["results"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], ImportRow.Status.FAILED_PERMANENT)
        self.assertNotIn("raw_payload", results[0])
        self.assertNotIn("mapped_payload", results[0])
        self.assertEqual(results[0]["failure_reason"]["code"], "x")
