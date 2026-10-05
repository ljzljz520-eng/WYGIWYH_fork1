from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema, extend_schema_view, inline_serializer
from django.shortcuts import get_object_or_404
from rest_framework import serializers as drf_serializers
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from apps.api.serializers import (
    ImportFileSerializer,
    ImportProfileSerializer,
    ImportRowSerializer,
    ImportRunSerializer,
)
from apps.import_app.models import ImportProfile, ImportRow, ImportRun
from apps.import_app.services.enqueue import enqueue_import_run
from apps.import_app.services.retry import RetryNotAvailable, retry_import_run


@extend_schema_view(
    list=extend_schema(
        summary="List import profiles",
        description="Returns a paginated list of all available import profiles.",
    ),
    retrieve=extend_schema(
        summary="Get import profile",
        description="Returns the details of a specific import profile by ID.",
    ),
)
class ImportProfileViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for listing and retrieving import profiles."""

    queryset = ImportProfile.objects.all()
    serializer_class = ImportProfileSerializer
    permission_classes = [IsAuthenticated]
    filterset_fields = {
        'name': ['exact', 'icontains'],
        'yaml_config': ['exact', 'icontains'],
        'version': ['exact'],
    }
    search_fields = ['name', 'yaml_config']
    ordering_fields = '__all__'
    ordering = ['name']


@extend_schema_view(
    list=extend_schema(
        summary="List import runs",
        description="Returns a paginated list of import runs. Optionally filter by profile_id.",
        parameters=[
            OpenApiParameter(
                name="profile_id",
                type=int,
                location=OpenApiParameter.QUERY,
                description="Filter runs by profile ID",
                required=False,
            ),
        ],
    ),
    retrieve=extend_schema(
        summary="Get import run",
        description="Returns the details of a specific import run by ID, including status and logs.",
    ),
)
class ImportRunViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for listing and retrieving import runs."""

    queryset = ImportRun.objects.all().order_by("-id")
    serializer_class = ImportRunSerializer
    permission_classes = [IsAuthenticated]
    filterset_fields = {
        'status': ['exact'],
        'mode': ['exact'],
        'phase': ['exact'],
        'profile': ['exact'],
        'file_name': ['exact', 'icontains'],
        'file_hash': ['exact'],
        'logs': ['exact', 'icontains'],
        'processed_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'total_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'successful_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'skipped_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'failed_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'retryable_rows': ['exact', 'gte', 'lte', 'gt', 'lt'],
        'started_at': ['exact', 'gte', 'lte', 'gt', 'lt', 'isnull'],
        'finished_at': ['exact', 'gte', 'lte', 'gt', 'lt', 'isnull'],
    }
    search_fields = ['file_name', 'logs']
    ordering_fields = '__all__'
    ordering = ['-id']

    def get_queryset(self):
        queryset = super().get_queryset()
        profile_id = self.request.query_params.get("profile_id")
        if profile_id:
            queryset = queryset.filter(profile_id=profile_id)
        return queryset

    @extend_schema(
        summary="Retry a failed import run",
        description=(
            "Resets a FAILED run (strict mode rebuilds staging rows; "
            "fault tolerant mode retries only retryable rows) and queues "
            "a new processing task. Returns 409 when the run is not "
            "retriable or its staged source file is missing."
        ),
        request=None,
        responses={
            202: inline_serializer(
                name="ImportRunRetryAccepted",
                fields={
                    "import_run_id": drf_serializers.IntegerField(),
                    "status": drf_serializers.CharField(),
                },
            ),
            409: inline_serializer(
                name="ImportRunRetryConflict",
                fields={
                    "import_run_id": drf_serializers.IntegerField(),
                    "detail": drf_serializers.CharField(),
                    "code": drf_serializers.CharField(),
                },
            ),
        },
    )
    @action(detail=True, methods=["post"])
    def retry(self, request, pk=None):
        run = self.get_object()
        try:
            retry_import_run(run, user_id=request.user.id)
        except RetryNotAvailable as exc:
            return Response(
                {
                    "import_run_id": run.id,
                    "code": exc.code,
                    "detail": exc.message,
                },
                status=status.HTTP_409_CONFLICT,
            )
        return Response(
            {"import_run_id": run.id, "status": ImportRun.Status.QUEUED.lower()},
            status=status.HTTP_202_ACCEPTED,
        )

    @extend_schema(
        summary="List staged rows of an import run",
        description=(
            "Returns the per-source-row staging records with statuses and "
            "structured failure reasons. Filter by status via the query "
            "parameter (e.g. FAILED_RETRYABLE)."
        ),
        parameters=[
            OpenApiParameter(
                name="status",
                type=str,
                location=OpenApiParameter.QUERY,
                description="Filter rows by status",
                required=False,
            ),
        ],
    )
    @action(detail=True, methods=["get"])
    def rows(self, request, pk=None):
        # Do not use self.get_object(): it would run the run-level
        # filterset against our row ``status`` query parameter.
        run = get_object_or_404(ImportRun, pk=pk)
        rows = ImportRow.objects.filter(run=run)
        row_status = request.query_params.get("status")
        if row_status:
            rows = rows.filter(status=row_status)

        page = self.paginate_queryset(rows.order_by("sequence"))
        serializer = ImportRowSerializer(
            page if page is not None else rows, many=True
        )
        if page is not None:
            return self.get_paginated_response(serializer.data)
        return Response(serializer.data)


@extend_schema_view(
    create=extend_schema(
        summary="Import file",
        description="Upload a CSV or XLSX file to import using an existing import profile. The import is queued and processed asynchronously.",
        request={
            "multipart/form-data": {
                "type": "object",
                "properties": {
                    "profile_id": {"type": "integer", "description": "ID of the ImportProfile to use"},
                    "file": {"type": "string", "format": "binary", "description": "CSV or XLSX file to import"},
                },
                "required": ["profile_id", "file"],
            },
        },
        responses={
            202: inline_serializer(
                name="ImportResponse",
                fields={
                    "import_run_id": drf_serializers.IntegerField(),
                    "status": drf_serializers.CharField(),
                },
            ),
        },
    ),
)
class ImportViewSet(viewsets.ViewSet):
    """ViewSet for importing data via file upload."""

    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser]

    def create(self, request):
        serializer = ImportFileSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        profile = serializer.validated_data["profile"]
        uploaded_file = serializer.validated_data["file"]

        import_run, created = enqueue_import_run(
            profile=profile,
            uploaded_file=uploaded_file,
            user_id=request.user.id,
        )

        if not created:
            return Response(
                {
                    "import_run_id": import_run.id,
                    "status": import_run.status.lower(),
                    "detail": (
                        "An identical file is already queued or being processed "
                        "for this import profile."
                    ),
                },
                status=status.HTTP_409_CONFLICT,
            )

        return Response(
            {"import_run_id": import_run.id, "status": "queued"},
            status=status.HTTP_202_ACCEPTED,
        )
