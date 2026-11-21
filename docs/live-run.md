# Live run of the options collector

The offline demo uses a synthetic option sample, and the collector's test fixtures are
hand-built. This page records one run of `tradier.options` against the live Tradier API, so
there is evidence the collector works on real responses.

Run on 2026-11-21 at 22:09 UTC (a Saturday, so the quotes are Friday's close), with default
settings: underlying SPY, the six nearest expiries.

## Command

`TRADIER_TOKEN` was set in the environment for this one process. It is not stored in the
repository.

    TRADIER_BASE=https://api.tradier.com/v1 EVENTLAKE_DB=/tmp/live.duckdb \
        uv run eventlake ingest tradier.options

## Output

    tradier.options: ok, fetched 6, written 6, quarantined 0

## Stored observations

Each row has `event_time` and `knowledge_time` equal to the fetch time
(2026-11-21T22:09:37Z) and `kt_quality` "observed". The value is the mean of the
at-the-money call and put implied volatility.

| series_id | value | spot | strike |
|---|---|---|---|
| `SPY:2026-11-23:atm_iv` | 0.3652 | 689.81 | 690.0 |
| `SPY:2026-11-24:atm_iv` | 0.4241 | 689.81 | 690.0 |
| `SPY:2026-11-25:atm_iv` | 0.4366 | 689.81 | 690.0 |
| `SPY:2026-11-27:atm_iv` | 0.4007 | 689.81 | 690.0 |
| `SPY:2026-11-30:atm_iv` | 0.3714 | 689.81 | 690.0 |
| `SPY:2026-12-01:atm_iv` | 0.3721 | 689.81 | 690.0 |

One run row was written to `feed_runs` with status `ok`, 6 fetched, 6 written and 0
quarantined. The quarantine table was empty.

## Health

`uv run eventlake health` against the same store:

    feed             status  freshness  last_run  volume  empty_runs  quarantine  gaps
    ---------------  ------  ---------  --------  ------  ----------  ----------  ----
    tradier.options  OK      ok         ok        ok      ok          ok          ok
    Not configured (never run): cftc, edgar, fred, kalshi, noaa, polymarket

## What this does and does not show

- It shows that the live response shapes parse, the values pass validation and the run and
  health records are written.
- It is one run on one underlying. Malformed live responses were not encountered, so the
  quarantine paths are covered only by the fixture tests.
- SPY lists an expiry on most trading days, so the six nearest reach about ten days ahead.
  A store collecting daily therefore holds the expiries around an event only in the last
  week before it.
