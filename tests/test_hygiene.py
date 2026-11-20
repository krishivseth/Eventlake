import re
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from eventlake.cli import app

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".git", ".venv", "data", ".mypy_cache", ".ruff_cache", ".pytest_cache", "__pycache__"}
PERSONAL_EMAIL = re.compile(r"[\w.+-]+@(gmail|yahoo|outlook|icloud|proton)\.\w+")
BEARER_TOKEN = re.compile(r"Bearer [A-Za-z0-9]{16,}")


def _text_files() -> list[Path]:
    out: list[Path] = []
    for path in ROOT.rglob("*"):
        rel_parts = path.relative_to(ROOT).parts
        if any(part in SKIP_DIRS for part in rel_parts) or not path.is_file():
            continue
        out.append(path)
    return out


def test_no_personal_emails_or_bearer_tokens() -> None:
    offenders: list[str] = []
    for path in _text_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if PERSONAL_EMAIL.search(text) or BEARER_TOKEN.search(text):
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"secret-like content in: {offenders}"


FIXTURE_LIMIT = 50 * 1024
DEMO_DATA_LIMIT = 1024 * 1024
QUICKSTART = "eventlake demo --offline"


def _tracked_files() -> list[str]:
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    out = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout
    return out.splitlines()


def test_no_env_or_db_files_tracked() -> None:
    offenders: list[str] = []
    for rel in _tracked_files():
        path = Path(rel)
        name = path.name
        is_env = name == ".env" or (name.startswith(".env.") and name != ".env.example")
        is_db = path.suffix == ".duckdb"
        is_parquet = path.suffix == ".parquet" and "demo_data" not in path.parts
        if is_env or is_db or is_parquet:
            offenders.append(rel)
    assert not offenders, f"tracked env or database files: {offenders}"


def test_fixtures_are_small() -> None:
    large = [
        str(p.relative_to(ROOT))
        for p in (ROOT / "tests" / "fixtures").rglob("*")
        if p.is_file() and p.stat().st_size >= FIXTURE_LIMIT
    ]
    assert not large, f"fixtures of 50 KB or more: {large}"
    demo = ROOT / "src" / "eventlake" / "demo_data"
    total = sum(p.stat().st_size for p in demo.rglob("*") if p.is_file())
    assert 0 < total < DEMO_DATA_LIMIT


def test_readme_quickstart_command_exists() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert QUICKSTART in readme
    result = CliRunner().invoke(app, ["demo", "--help"])
    assert result.exit_code == 0
    assert "--offline" in result.output
