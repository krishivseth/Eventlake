"""Plain-text table rendering shared by the CLI and the demo."""

from collections.abc import Sequence

from eventlake.health import FeedStatus


def format_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """Left-aligned plain text table with two spaces between columns."""
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h)
              for i, h in enumerate(headers)]  # fmt: skip

    def line(cells: Sequence[str]) -> str:
        return "  ".join(c.ljust(w) for c, w in zip(cells, widths, strict=True)).rstrip()

    return "\n".join([line(headers), line(["-" * w for w in widths]), *map(line, rows)])


def render_health(statuses: Sequence[FeedStatus]) -> str:
    names = [c.name for c in statuses[0].checks] if statuses else []
    table = format_table(
        ["feed", "status", *names],
        [[s.feed, s.status.upper(), *(c.status for c in s.checks)] for s in statuses],
    )
    issues = [
        f"  {s.feed} {c.name} [{c.status}]: {c.detail}"
        for s in statuses
        for c in s.checks
        if c.status != "ok"
    ]
    return table + ("\n\nIssues:\n" + "\n".join(issues) if issues else "")
