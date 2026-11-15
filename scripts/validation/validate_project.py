"""Offline structural checks with no cloud, dataset, or container side effects."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def require(path: str) -> Path:
    candidate = ROOT / path
    if not candidate.exists():
        raise AssertionError(f"Required file is missing: {path}")
    return candidate


def main() -> int:
    required = [
        "pyproject.toml", "docker-compose.yml", "README.md", "PROJECT_STATUS.md",
        "streaming/kafka/topics/topics.yaml", "sql/ddl/001_core_schema.sql",
        "warehouse/dbt/dbt_project.yml", "monitoring/prometheus/prometheus.yml",
    ]
    for item in required:
        require(item)
    with require("pyproject.toml").open("rb") as handle:
        tomllib.load(handle)
    for path in ROOT.rglob("*.yaml"):
        yaml.safe_load(path.read_text(encoding="utf-8"))
    for path in ROOT.rglob("*.yml"):
        yaml.safe_load(path.read_text(encoding="utf-8"))
    for path in ROOT.rglob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))
    for path in ROOT.rglob("*.py"):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
    raw_files = [path for path in (ROOT / "datasets/raw").iterdir() if path.name != ".gitkeep"]
    if raw_files:
        raise AssertionError("Raw datasets must not be committed to this foundation")
    print("Offline structural validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
