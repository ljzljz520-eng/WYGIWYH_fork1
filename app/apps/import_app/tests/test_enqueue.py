import hashlib
import os
import tempfile
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from apps.import_app.models import ImportProfile, ImportRun
from apps.import_app.services import enqueue as enqueue_service

VALID_QIF_YAML = """
settings:
  file_type: qif
  importing: transactions
  date_format: "%d/%m/%Y"
mapping: {}
"""

VALID_CSV_YAML = """
settings:
  file_type: csv
  importing: transactions
  skip_errors: true
  delimiter: ","
  encoding: utf-8
mapping:
  date:
    source: date
    target: date
    format: "%Y-%m-%d"
"""


class EnqueueImportRunTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            email="enqueue@example.com", password="password"
        )
        self.temp_dir = tempfile.mkdtemp(prefix="wygiwyh-enqueue-")
        self._patches = [
            patch.object(enqueue_service, "DEFAULT_TEMP_DIR", self.temp_dir),
            patch("apps.import_app.tasks.process_import.defer"),
        ]
        self._patches[1].start()
        self._patches[0].start()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        import shutil

        for p in self._patches:
            p.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _profile(self, name="Enqueue Profile", yaml_config=VALID_QIF_YAML):
        return ImportProfile.objects.create(
            name=name,
            yaml_config=yaml_config,
            version=ImportProfile.Versions.VERSION_1,
        )

    def _file(self, content=b"date,desc\n2025-01-01,x", name="data.csv"):
        return SimpleUploadedFile(name, content, content_type="text/csv")

    def test_creates_run_with_summary_snapshot_and_mode(self):
        profile = self._profile(yaml_config=VALID_CSV_YAML)
        content = b"a,b\n1,2\n"
        run, created = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(content),
            user_id=self.user.id,
        )

        self.assertTrue(created)
        self.assertEqual(run.file_hash, hashlib.sha256(content).hexdigest())
        self.assertEqual(run.file_size, len(content))
        self.assertEqual(run.config_snapshot, VALID_CSV_YAML)
        self.assertEqual(run.mode, ImportRun.Mode.FAULT_TOLERANT)
        self.assertEqual(run.phase, ImportRun.Phase.ENQUEUED)
        self.assertTrue(run.stored_file_path.startswith(self.temp_dir))
        self.assertTrue(os.path.exists(run.stored_file_path))

    def test_strict_mode_derived_from_skip_errors_false(self):
        profile = self._profile()
        run, _ = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )
        self.assertEqual(run.mode, ImportRun.Mode.STRICT)

    def test_duplicate_active_file_returns_existing_run(self):
        profile = self._profile()
        first, created_first = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )
        self.assertTrue(created_first)
        files_after_first = set(os.listdir(self.temp_dir))

        second, created_second = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )

        self.assertFalse(created_second)
        self.assertEqual(second.id, first.id)
        self.assertEqual(ImportRun.objects.filter(profile=profile).count(), 1)
        # Duplicate submission must not leave a second staged file behind.
        self.assertEqual(set(os.listdir(self.temp_dir)), files_after_first)

    def test_duplicate_allowed_after_previous_run_finishes(self):
        profile = self._profile()
        first, _ = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )
        first.status = ImportRun.Status.FINISHED
        first.save()

        second, created = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )
        self.assertTrue(created)
        self.assertNotEqual(second.id, first.id)

    def test_web_duplicate_upload_is_204_without_new_run(self):
        from django.test import Client

        profile = self._profile(name="Web Enqueue Profile")
        client = Client()
        client.force_login(self.user)
        url = f"/import/profiles/{profile.id}/runs/add/"

        def _post():
            f = self._file()
            return client.post(
                url,
                {"file": f},
                HTTP_HX_REQUEST="true",
            )

        response1 = _post()
        self.assertEqual(response1.status_code, 204)
        response2 = _post()
        self.assertEqual(response2.status_code, 204)
        self.assertEqual(ImportRun.objects.filter(profile=profile).count(), 1)

    def test_invalid_profile_config_defaults_to_strict(self):
        profile = self._profile(
            name="Broken Profile", yaml_config="settings:\n  bogus: true\n"
        )
        run, created = enqueue_service.enqueue_import_run(
            profile=profile,
            uploaded_file=self._file(),
            user_id=self.user.id,
        )
        self.assertTrue(created)
        self.assertEqual(run.mode, ImportRun.Mode.STRICT)
