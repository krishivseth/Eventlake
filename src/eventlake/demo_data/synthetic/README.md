# Synthetic option data

`iv_sample.csv` is **illustrative, not market data**. There is no recorded option history in
this repository (the options feed needs a broker token), so the offline demo loads these
invented at-the-money implied vols as `tradier.options` observations with `kt_quality`
"assumed". They let the real implied-move solver run end to end.

The values come from a documented formula in `generate_iv_sample.py` (total variance =
diffuse variance plus a one-off event move). Regenerate with:

    uv run python src/eventlake/demo_data/synthetic/generate_iv_sample.py

Columns: `event_id` (the demo event the row illustrates), `observed_at` (UTC), `underlying`,
`expiry`, `atm_iv`. Every table that shows the derived number labels the column
`implied move (synthetic sample)`.
