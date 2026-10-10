#!/usr/bin/env python3
"""Apply signal data reduction to decoded CSV based on configuration.json.

Reduction methods per signal:
  none       - keep all values
  onChange   - keep value only when it changes from previous
  sampling   - keep value if elapsed >= 'time' ms since last kept sample
  average    - mean over 'time' ms windows
  difference - consecutive value differences

Output: Excel file with two sheets:
  Data       - reduced signal readings with ReductionMethod column
  Statistics - data point counts per signal per reduction method
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Reduction functions
# Each receives a DataFrame slice for one signal (index = Timestamp)
# and returns a reduced DataFrame with the same columns.
# ---------------------------------------------------------------------------

def _reduce_none(df):
    return df.copy()


def _reduce_on_change(df):
    if df.empty:
        return df.copy()
    values = df["DecodedSignalData"]
    mask = values != values.shift()
    mask.iloc[0] = True
    return df[mask].copy()


def _reduce_sampling(df, interval_ms):
    if df.empty:
        return df.copy()
    interval = pd.Timedelta(milliseconds=interval_ms)
    keep = []
    last_ts = None
    for ts in df.index:
        if last_ts is None or (ts - last_ts) >= interval:
            keep.append(ts)
            last_ts = ts
    return df.loc[keep].copy()


def _reduce_average(df, window_ms):
    if df.empty:
        return df.copy()
    window = f"{window_ms}ms"
    series = df["DecodedSignalData"].resample(window).mean().dropna()
    if series.empty:
        return pd.DataFrame(columns=df.columns)
    meta = {c: df[c].iloc[0] for c in df.columns if c != "DecodedSignalData"}
    rows = []
    for ts, val in series.items():
        row = {**meta, "DecodedSignalData": val}
        rows.append(row)
    result = pd.DataFrame(rows, index=series.index)
    result.index.name = "Timestamp"
    return result


def _reduce_difference(df):
    if df.empty or len(df) < 2:
        return pd.DataFrame(columns=df.columns)
    result = df.copy()
    result["DecodedSignalData"] = df["DecodedSignalData"].diff()
    return result.iloc[1:].copy()


def apply_reduction(df_signal, reduction_type, time_ms=None):
    """Dispatch to correct reduction fn. Returns reduced DataFrame."""
    r = reduction_type.lower()
    if r == "none":
        return _reduce_none(df_signal)
    if r == "onchange":
        return _reduce_on_change(df_signal)
    if r == "sampling":
        if time_ms is None:
            print(f"  WARNING: 'sampling' needs 'time' in config, keeping all", file=sys.stderr)
            return _reduce_none(df_signal)
        return _reduce_sampling(df_signal, time_ms)
    if r == "average":
        if time_ms is None:
            print(f"  WARNING: 'average' needs 'time' in config, keeping all", file=sys.stderr)
            return _reduce_none(df_signal)
        return _reduce_average(df_signal, time_ms)
    if r == "difference":
        return _reduce_difference(df_signal)
    print(f"  WARNING: Unknown reduction type '{reduction_type}', keeping all", file=sys.stderr)
    return _reduce_none(df_signal)


# ---------------------------------------------------------------------------
# Config loading + signal rule extraction
# ---------------------------------------------------------------------------

def load_rules(config_path):
    """
    Parse configuration.json → list of rule dicts:
      { signal_name, message_id, message_name, reduction_type,
        time_ms, sending_interval_ms, product_names }
    """
    with open(config_path) as f:
        config = json.load(f)

    rules = []
    for cfg in config.get("configurations", []):
        product_names = cfg.get("product-name", [])
        sending_interval_ms = cfg.get("sending-interval")
        for sig in cfg.get("signals", []):
            dr = sig.get("data-reduction", {})
            raw_id = sig["message-id"]
            msg_id = int(raw_id, 16) if isinstance(raw_id, str) and raw_id.startswith("0x") else int(raw_id)
            rules.append({
                "signal_name":          sig["signal-name"],
                "message_id":           msg_id,
                "message_name":         sig.get("message-name", ""),
                "reduction_type":       dr.get("reduction-type", "none"),
                "time_ms":              dr.get("time"),
                "sending_interval_ms":  sending_interval_ms,
                "product_names":        product_names,
            })
    return rules


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

import re as _re

_CSV_MAX_ROWS = 1_000_000


def _safe_filename(name):
    return _re.sub(r"[^\w\-]", "_", name)


def _write_signal_csvs(df_sig, out_dir, sig, msg_id, suffix, fmt="csv"):
    safe_sig = _safe_filename(sig)
    safe_mid = _safe_filename(str(msg_id))
    if fmt == "parquet":
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}.parquet"
        df_sig.to_parquet(path, index=False)
        return 1
    if df_sig.empty:
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}-0.csv"
        df_sig.to_csv(path, index=False)
        return 1
    n = 0
    for start in range(0, len(df_sig), _CSV_MAX_ROWS):
        chunk = df_sig.iloc[start:start + _CSV_MAX_ROWS]
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}-{n}.csv"
        chunk.to_csv(path, index=False)
        n += 1
    return n


def _build_input_index(input_dir_path):
    """Index decoded files (CSV or parquet) by (safe_sig, safe_mid) from filename.

    Patterns:
      CSV:     {SignalName}-{MessageID}-{stem}-decoded-output-{n}.csv
      Parquet: {SignalName}-{MessageID}-{stem}-decoded-output.parquet
    Returns dict: (safe_sig_lower, safe_mid_lower) -> [Path, ...]
    """
    index = {}
    patterns = ["*-decoded-output-*.csv", "*-decoded-output.parquet"]
    for pat in patterns:
        for p in input_dir_path.glob(pat):
            stem = p.stem  # strip extension
            # strip trailing chunk index for CSV: "...-decoded-output-0" → "...-decoded-output"
            parts = stem.split("-decoded-output")
            if len(parts) < 2:
                continue
            prefix = parts[0]
            tokens = prefix.split("-")
            if len(tokens) < 2:
                continue
            key = (tokens[0].lower(), tokens[1].lower())
            index.setdefault(key, []).append(p)
    return index


def _read_input_files(paths):
    """Read list of CSV or parquet paths into single DataFrame."""
    dfs = []
    for p in sorted(paths):
        if p.suffix == ".parquet":
            dfs.append(pd.read_parquet(p))
        else:
            dfs.append(pd.read_csv(p))
    return pd.concat(dfs, ignore_index=True)


def process(input_dir, config_path, output_dir, fmt="csv"):
    input_dir_path = Path(input_dir)

    all_input = (sorted(input_dir_path.glob("*-decoded-output-*.csv")) +
                 sorted(input_dir_path.glob("*-decoded-output.parquet")))
    if not all_input:
        print(f"ERROR: No decoded files (CSV/parquet) found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(all_input)} decoded file(s) in {input_dir_path}")
    file_index = _build_input_index(input_dir_path)
    print(f"Indexed {len(file_index)} (signal, message) pairs\n")

    rules = load_rules(config_path)
    print(f"Loaded {len(rules)} rule(s) from config\n")

    out_dir_path = Path(output_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    stats_rows = []
    total_files = 0
    matched = 0
    total_rules = len(rules)

    for rule_idx, rule in enumerate(rules, 1):
        sig_name = rule["signal_name"]
        msg_id   = rule["message_id"]
        r_type   = rule["reduction_type"]
        time_ms  = rule["time_ms"]
        products = ", ".join(rule["product_names"])

        safe_sig = _safe_filename(sig_name).lower()
        safe_mid = _safe_filename(f"0x{msg_id:X}").lower()
        key = (safe_sig, safe_mid)

        matched_files = file_index.get(key, [])

        if not matched_files:
            print(f"  [{r_type}] {sig_name} (0x{msg_id:X}) — no files found, writing empty CSV")
            empty_cols = ["Timestamp", "MessageName", "MessageID", "SignalName",
                          "DecodedSignalData", "ReductionMethod", "ProductName"]
            _write_signal_csvs(pd.DataFrame(columns=empty_cols), out_dir_path,
                               sig_name, f"0x{msg_id:X}", "reduced-output", fmt)
            continue

        df = _read_input_files(matched_files)
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)
        df = df.set_index("Timestamp")

        print(f"  [{rule_idx}/{total_rules}] [{r_type}] {sig_name} | {len(df)} → ", end="", flush=True)
        df_reduced = apply_reduction(df, r_type, time_ms)
        print(f"{len(df_reduced)} rows")

        df_reduced = df_reduced.reset_index()
        df_reduced["Timestamp"] = df_reduced["Timestamp"].dt.tz_localize(None)
        df_reduced["ReductionMethod"] = r_type
        df_reduced["ProductName"]     = products

        col_order = ["Timestamp", "MessageName", "MessageID", "SignalName",
                     "DecodedSignalData", "ReductionMethod", "ProductName"]
        df_reduced = df_reduced[[c for c in col_order if c in df_reduced.columns]]

        n = _write_signal_csvs(df_reduced, out_dir_path, sig_name, f"0x{msg_id:X}", "reduced-output", fmt)
        total_files += n
        matched += 1

        stats_rows.append({
            "SignalName":      sig_name,
            "MessageID":       f"0x{msg_id:X}",
            "ReductionMethod": r_type,
            "DataPoints":      len(df_reduced),
        })

    if not stats_rows:
        print("\nERROR: No data after reduction.", file=sys.stderr)
        sys.exit(1)

    stats = pd.DataFrame(stats_rows).sort_values(["SignalName", "MessageID"])
    stats_path = out_dir_path / "statistics.csv"
    stats.to_csv(stats_path, index=False)

    print(f"\nWrote {matched} signal/msgid pairs, {total_files} CSV file(s) → {out_dir_path}")
    print(f"  Statistics → {stats_path}")
    print(f"  Statistics → {stats_path}")
    print("\nStatistics:")
    print(stats.to_string(index=False))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Apply data reduction to decoded MDF4 CSV"
    )
    p.add_argument("--input-dir",  required=True,
                   help="Directory containing decoded_output.xlsx (output dir of decode.py)")
    p.add_argument("--config", required=True, help="configuration.json path")
    p.add_argument("--output-dir", required=True,
                   help="Output directory — reduced_output.xlsx will be written here")
    p.add_argument("--format", choices=["csv", "parquet"], default="csv",
                   help="Output file format (default: csv)")
    return p.parse_args()


def main():
    args = parse_args()
    process(args.input_dir, args.config, args.output_dir, fmt=args.format)


if __name__ == "__main__":
    main()
