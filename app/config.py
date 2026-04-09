from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    znom_dir: Path
    reestr_dir: Path
    db_path: Path
    archive_missing_as_inactive: bool
    file_patterns: list[str]


def load_config(config_path: str | Path = "config.json") -> AppConfig:
    p = Path(config_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    return AppConfig(
        znom_dir=Path(data["znom_dir"]),
        reestr_dir=Path(data["reestr_dir"]),
        db_path=Path(data["db_path"]),
        archive_missing_as_inactive=bool(data.get("archive_missing_as_inactive", True)),
        file_patterns=list(data.get("file_patterns", ["*.xlsx", "*.xlsm", "*.xls"])),
    )
