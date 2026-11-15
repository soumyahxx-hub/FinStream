import json

import pytest

from ingestion.consumers.reconciliation import (
    ReconciliationManifest,
    ReconciliationSummary,
)


def test_reconciliation_allows_duplicates_to_be_accounted_for() -> None:
    summary = ReconciliationSummary(
        source_count=10,
        sink_count=8,
        duplicate_count=2,
    )
    assert summary.is_reconciled
    assert summary.unreconciled_count == 0


def test_reconciliation_detects_missing_records() -> None:
    summary = ReconciliationSummary(
        source_count=10,
        sink_count=7,
        duplicate_count=2,
    )
    assert not summary.is_reconciled
    assert summary.unreconciled_count == 1


def test_reconciliation_rejects_negative_counts() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        ReconciliationSummary(
            source_count=10,
            sink_count=-1,
            duplicate_count=0,
        )


def test_reconciliation_rejects_too_many_duplicates() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        ReconciliationSummary(
            source_count=5,
            sink_count=2,
            duplicate_count=6,
        )


def test_manifest_loads_valid_payload(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "paysim",
                "simulation_id": "sample-001",
                "source_count": 1000,
                "duplicate_count": 10,
            }
        ),
        encoding="utf-8",
    )

    manifest = ReconciliationManifest.from_file(path)

    assert manifest.source_system == "paysim"
    assert manifest.simulation_id == "sample-001"
    assert manifest.source_count == 1000
    assert manifest.duplicate_count == 10


def test_manifest_allows_missing_simulation_id(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "synthetic",
                "source_count": 10,
                "duplicate_count": 0,
            }
        ),
        encoding="utf-8",
    )

    manifest = ReconciliationManifest.from_file(path)

    assert manifest.simulation_id is None


def test_manifest_rejects_missing_fields(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps({"source_system": "paysim"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing fields"):
        ReconciliationManifest.from_file(path)


def test_manifest_rejects_unsupported_source(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "unknown",
                "source_count": 10,
                "duplicate_count": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported source_system"):
        ReconciliationManifest.from_file(path)


@pytest.mark.parametrize("invalid_count", [-1, 1.5, True, "10"])
def test_manifest_rejects_invalid_counts(tmp_path, invalid_count) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "paysim",
                "source_count": invalid_count,
                "duplicate_count": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="source_count"):
        ReconciliationManifest.from_file(path)


def test_manifest_rejects_duplicates_above_source_count(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "source_system": "paysim",
                "source_count": 5,
                "duplicate_count": 6,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate_count"):
        ReconciliationManifest.from_file(path)
