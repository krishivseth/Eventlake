import re
from pathlib import Path

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
