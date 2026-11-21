# Live run of every feed

The offline demo replays recorded data, and the option sample in it is synthetic. This page
records one run of the whole pipeline against the live APIs on 2026-11-21 (a Saturday, about
22:15 to 22:30 UTC), into an empty database.

It found one bug, which is fixed in the same change: see "What the run found".

## Commands

`TRADIER_TOKEN` was set in the environment for the process. It is not stored in the
repository. `EVENTLAKE_CONTACT` was not set, so EDGAR was skipped.

    export TRADIER_BASE=https://api.tradier.com/v1 EVENTLAKE_DB=/tmp/live.duckdb
    uv run eventlake backfill --start 2026-09-01
    uv run eventlake health
    uv run eventlake export /tmp/panel.parquet

## Backfill output

    events: 94 new
    skip edgar: EVENTLAKE_CONTACT is required: the SEC asks automated clients to identify themselves with a contact address
    markets kalshi: 625 new
    markets polymarket: 281 new
    cftc: ok, fetched 12, written 48, quarantined 0
    fred: ok, fetched 140, written 139, quarantined 0
    kalshi: ok, fetched 8320, written 4822, quarantined 0
    noaa: ok, fetched 74, written 74, quarantined 0
    polymarket: ok, fetched 9985, written 9985, quarantined 0
    tradier.options: ok, fetched 6, written 6, quarantined 0
    links: 650 new, unlinked: 256

The `fred` line is from the run after the fix below; in the first pass it read
`fred: failed, fetched 0, written 0, quarantined 0` with a read timeout.

## What was stored

| feed | observations | series | notes |
|---|---|---|---|
| `kalshi` | 4,822 | 619 | 625 markets listed |
| `polymarket` | 9,985 | 228 | 281 markets listed |
| `fred` | 139 | 3 | SP500 57, DFF 80, CPIAUCSL 2 |
| `cftc` | 48 | 4 | 12 weekly reports, 4 position series each |
| `noaa` | 74 | 1 | daily maximum temperature, one station |
| `tradier.options` | 6 | 6 | one snapshot, see below |

Of the 906 markets, 650 were linked to a calendar or weather event and 256 were left
unlinked, which is the intended outcome for a market that matches no rule.

## Health

    feed             status  freshness  last_run  volume  empty_runs  quarantine  gaps
    ---------------  ------  ---------  --------  ------  ----------  ----------  ----
    cftc             OK      ok         ok        ok      ok          ok          ok
    fred             OK      ok         ok        ok      ok          ok          ok
    kalshi           OK      ok         ok        ok      ok          ok          ok
    noaa             WARN    ok         ok        ok      ok          ok          warn
    polymarket       OK      ok         ok        ok      ok          ok          ok
    tradier.options  OK      ok         ok        ok      ok          ok          ok

    Issues:
      noaa gaps [warn]: missing USW00094728:TMAX: 2026-11-16, 2026-11-17, 2026-11-18, 2026-11-19 (weekdays 2026-10-21 to 2026-11-19)
    Not configured (never run): edgar

The NOAA warning is genuine: the daily summaries trail by several days.

## Exported panel

`export` wrote 37 rows, one per FOMC and CPI event up to the run time. The rows inside the
backfill window, each as known the day before its event:

| event | as of (UTC) | top market | market prob |
|---|---|---|---|
| `CPI:2026-09-11` | 2026-09-10 12:30 | kalshi: Will CPI rise more than 0.3% in August 2026? | 60.0% |
| `FOMC:2026-09-16` | 2026-09-15 18:00 | polymarket: Will the Fed increase interest rates by 25 bps after the September 2026 meeting? | 87.5% |
| `CPI:2026-10-14` | 2026-10-13 12:30 | kalshi: Will CPI rise more than 0.5% in September 2026? | 62.0% |
| `FOMC:2026-10-28` | 2026-10-27 18:00 | polymarket: Will there be no change in Fed interest rates after the October 2026 meeting? | 93.5% |
| `CPI:2026-11-10` | 2026-11-09 13:30 | kalshi: Will CPI rise more than 0.2% in October 2026? | 62.0% |

These match the same events in the offline demo, which was recorded a day earlier. The
implied-move column is empty for all of them, because no option snapshot existed before
those events, and the realised move is empty because it is not known the day before. The 32
earlier rows have no market, since the backfill started on 2026-09-01.

## Option snapshot

Each row has `event_time` and `knowledge_time` equal to the fetch time
(2026-11-21T22:25:09Z) and `kt_quality` "observed". The value is the mean of the
at-the-money call and put implied volatility. Quotes are Friday's close.

| series_id | value | spot | strike |
|---|---|---|---|
| `SPY:2026-11-23:atm_iv` | 0.3652 | 689.81 | 690.0 |
| `SPY:2026-11-24:atm_iv` | 0.4241 | 689.81 | 690.0 |
| `SPY:2026-11-25:atm_iv` | 0.4366 | 689.81 | 690.0 |
| `SPY:2026-11-27:atm_iv` | 0.4007 | 689.81 | 690.0 |
| `SPY:2026-11-30:atm_iv` | 0.3714 | 689.81 | 690.0 |
| `SPY:2026-12-01:atm_iv` | 0.3721 | 689.81 | 690.0 |

## What the run found

- **FRED did not answer the collector.** Requests carrying the client's default
  `eventlake/0.1` User-Agent were left unanswered until the 20-second timeout, three times
  in a row, while the same URL returned in under a second with the `requests` library's
  default User-Agent. The run was recorded as `failed` and `health` reported it. The FRED
  collector now sends the library default, with a test. The demo data had been recorded
  before the client's User-Agent was changed, so the offline demo did not catch this.
- **The options collector works on live responses.** Before this run it had only been
  tested against hand-built fixtures.
- **SPY lists an expiry on most trading days,** so the six nearest reach about ten days
  ahead. A store collecting daily holds the expiries around an event only in the last week
  before it.

## What this does not show

- It is one run. No malformed live record was met, so the quarantine paths are covered only
  by the fixture tests.
- EDGAR was not run.
- Nothing here exercises revisions arriving over time; that needs repeated runs.
