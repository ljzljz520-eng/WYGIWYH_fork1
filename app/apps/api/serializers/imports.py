from rest_framework import serializers

from apps.import_app.models import ImportProfile, ImportRun, ImportRow


class ImportProfileSerializer(serializers.ModelSerializer):
    """Serializer for listing import profiles."""

    class Meta:
        model = ImportProfile
        fields = ["id", "name", "version", "yaml_config"]


class ImportRunSerializer(serializers.ModelSerializer):
    """Serializer for listing import runs."""

    class Meta:
        model = ImportRun
        fields = [
            "id",
            "status",
            "mode",
            "phase",
            "profile",
            "file_name",
            "file_hash",
            "file_size",
            "logs",
            "processed_rows",
            "total_rows",
            "successful_rows",
            "skipped_rows",
            "failed_rows",
            "retryable_rows",
            "source_summary",
            "cursor",
            "lease_owner",
            "lease_expires_at",
            "run_attempts",
            "started_at",
            "finished_at",
        ]
        read_only_fields = fields


class ImportRowSerializer(serializers.ModelSerializer):
    """Serializer for row-level import diagnostics.

    Raw and mapped payloads are intentionally excluded: they can be large and
    are not part of the diagnostics contract.
    """

    class Meta:
        model = ImportRow
        fields = [
            "id",
            "sequence",
            "section",
            "row_number",
            "status",
            "failure_reason",
            "attempts",
            "transaction",
            "committed_at",
        ]
        read_only_fields = fields


class ImportFileSerializer(serializers.Serializer):
    """Serializer for uploading a file to import using an existing profile."""

    profile_id = serializers.PrimaryKeyRelatedField(
        queryset=ImportProfile.objects.all(), source="profile"
    )
    file = serializers.FileField()
