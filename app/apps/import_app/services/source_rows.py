"""Unified source-row readers for CSV, Excel and QIF files.

Each reader yields :class:`RawRow` instances. ``native_payload`` keeps the
native Python values (used for mapping/coercion), while ``raw_payload`` is
JSON-serializable (persisted as staging diagnostics).
"""

import csv
import hashlib
import json
import os
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

import openpyxl
import xlrd


@dataclass
class RawRow:
    section: str
    row_number: int
    native_payload: dict
    raw_payload: dict = field(default_factory=dict)


def make_idempotency_key(
    file_hash: str, section: str, row_number: int, raw_payload: dict
) -> str:
    normalized = json.dumps(
        raw_payload,
        sort_keys=True,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    material = "|".join([file_hash or "", section, str(row_number), normalized])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _safe_row(row: dict) -> dict:
    return {k: _json_safe(v) for k, v in row.items()}


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def iter_csv_rows(file_path: str, settings) -> list[RawRow]:
    section = os.path.basename(file_path)
    rows: list[RawRow] = []

    with open(file_path, "r", encoding=settings.encoding) as csv_file:
        for _ in range(settings.skip_lines):
            next(csv_file)

        reader = csv.DictReader(csv_file, delimiter=settings.delimiter)
        for row_number, row in enumerate(reader, start=1):
            native = dict(row)
            rows.append(
                RawRow(
                    section=section,
                    row_number=row_number,
                    native_payload=native,
                    raw_payload=_safe_row(native),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------


def _selected_sheets(configured, available) -> list[str]:
    if configured == "*":
        return list(available)
    if isinstance(configured, list):
        return configured
    return [configured]


def _iter_xlsx_rows(file_path: str, settings, warnings: list[str]) -> list[RawRow]:
    rows: list[RawRow] = []
    workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    try:
        sheets = _selected_sheets(settings.sheets, workbook.sheetnames)
        for sheet_name in sheets:
            if sheet_name not in workbook.sheetnames:
                warnings.append(f"Sheet '{sheet_name}' not found in the Excel file.")
                continue
            sheet = workbook[sheet_name]
            headers = [
                str(cell.value or "") for cell in sheet[settings.start_row]
            ]
            for row_number, row in enumerate(
                sheet.iter_rows(
                    min_row=settings.start_row + 1, values_only=True
                ),
                start=1,
            ):
                native = {
                    key: (str(value) if value is not None else None)
                    for key, value in zip(headers, row)
                }
                rows.append(
                    RawRow(
                        section=sheet_name,
                        row_number=row_number,
                        native_payload=native,
                        raw_payload=_safe_row(native),
                    )
                )
    finally:
        workbook.close()
    return rows


def _iter_xls_rows(file_path: str, settings, warnings: list[str]) -> list[RawRow]:
    rows: list[RawRow] = []
    workbook = xlrd.open_workbook(file_path)
    sheets = _selected_sheets(settings.sheets, workbook.sheet_names())
    for sheet_name in sheets:
        if sheet_name not in workbook.sheet_names():
            warnings.append(f"Sheet '{sheet_name}' not found in the Excel file.")
            continue
        sheet = workbook.sheet_by_name(sheet_name)
        headers = [
            str(sheet.cell_value(settings.start_row - 1, col) or "")
            for col in range(sheet.ncols)
        ]
        for row_number in range(settings.start_row, sheet.nrows):
            native = {}
            for col, key in enumerate(headers):
                cell_type = sheet.cell_type(row_number, col)
                cell_value = sheet.cell_value(row_number, col)
                if cell_type == xlrd.XL_CELL_DATE:
                    try:
                        native[key] = datetime(
                            *xlrd.xldate_as_tuple(cell_value, workbook.datemode)
                        )
                    except Exception:
                        native[key] = (
                            str(cell_value) if cell_value is not None else None
                        )
                elif cell_value is None:
                    native[key] = None
                else:
                    native[key] = str(cell_value)
            rows.append(
                RawRow(
                    section=sheet_name,
                    row_number=row_number - settings.start_row + 1,
                    native_payload=native,
                    raw_payload=_safe_row(native),
                )
            )
    return rows


def iter_excel_rows(file_path: str, settings) -> tuple[list[RawRow], list[str]]:
    warnings: list[str] = []
    if settings.file_type == "xlsx":
        try:
            return _iter_xlsx_rows(file_path, settings, warnings), warnings
        except Exception as e:
            from openpyxl.utils.exceptions import InvalidFileException

            if isinstance(e, InvalidFileException):
                raise ValueError(f"Invalid XLSX file format: {e}") from e
            raise
    try:
        return _iter_xls_rows(file_path, settings, warnings), warnings
    except xlrd.XLRDError as e:
        raise ValueError(f"Invalid XLS file format: {e}") from e


# ---------------------------------------------------------------------------
# QIF
# ---------------------------------------------------------------------------


@dataclass
class QifRecord:
    section: str
    row_number: int
    account_name: str
    lines: list[str]  # raw non-empty stripped lines, including "!" headers and "^"
    fields: dict  # parsed code -> raw value (D/T/P/M/L/N)


def _qif_records_from_lines(
    lines, section: str
) -> list[QifRecord]:
    account_name = os.path.splitext(os.path.basename(section))[0]
    records: list[QifRecord] = []
    raw_buffer: list[str] = []
    fields: dict[str, str] = {}
    record_index = 0

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        raw_buffer.append(line)

        if line == "^":
            if fields:
                record_index += 1
                records.append(
                    QifRecord(
                        section=section,
                        row_number=record_index,
                        account_name=account_name,
                        lines=list(raw_buffer),
                        fields=dict(fields),
                    )
                )
            raw_buffer = []
            fields = {}
            continue

        if line.startswith("!"):
            continue

        code = line[0]
        value = line[1:]
        if code in ("D", "T", "P", "M", "L", "N"):
            fields[code] = value

    return records


def iter_qif_records(file_path: str, settings) -> tuple[list[QifRecord], list[str]]:
    """Read QIF records from a plain file or a ZIP of QIF members."""
    warnings: list[str] = []
    records: list[QifRecord] = []

    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, "r") as zf:
            for member in zf.namelist():
                if member.lower().endswith(".qif") and not member.startswith(
                    "__MACOSX"
                ):
                    with zf.open(member) as f:
                        content = f.read().decode(settings.encoding)
                    member_records = _qif_records_from_lines(
                        content.splitlines(), member
                    )
                    records.extend(member_records)
    else:
        with open(file_path, "r", encoding=settings.encoding) as f:
            records = _qif_records_from_lines(
                f.readlines(), os.path.basename(file_path)
            )

    return records, warnings


def qif_record_to_raw(record: QifRecord) -> dict:
    return {
        "kind": "qif",
        "account_name": record.account_name,
        "lines": record.lines,
        "fields": dict(record.fields),
    }
