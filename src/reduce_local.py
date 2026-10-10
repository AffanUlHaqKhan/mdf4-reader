#!/usr/bin/env python3
"""Apply signal data reduction to local-decoder CSV files.

Input files: {signal_name}.csv with columns: timestamp, signal_name, signal_value
Matches config rules by signal name only (no message ID in data).

Reduction methods per signal:
  none       - keep all values
  onChange   - keep value only when it changes from previous
  sampling   - keep value if elapsed >= 'time' ms since last kept sample
  average    - mean over 'time' ms windows
  difference - consecutive value differences
"""
import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------------------------
# Reduction functions (operate on index=Timestamp, column=signal_value)
# ---------------------------------------------------------------------------

def _reduce_none(df):
    return df.copy()


def _reduce_on_change(df):
    if df.empty:
        return df.copy()
    values = df["signal_value"]
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
    series = df["signal_value"].resample(window).mean().dropna()
    if series.empty:
        return pd.DataFrame(columns=df.columns)
    meta = {c: df[c].iloc[0] for c in df.columns if c != "signal_value"}
    rows = [{**meta, "signal_value": val} for val in series]
    result = pd.DataFrame(rows, index=series.index)
    result.index.name = "timestamp"
    return result


def _reduce_difference(df):
    if df.empty or len(df) < 2:
        return pd.DataFrame(columns=df.columns)
    result = df.copy()
    result["signal_value"] = df["signal_value"].diff()
    return result.iloc[1:].copy()


def apply_reduction(df_signal, reduction_type, time_ms=None):
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
# Config loading — keyed by lower-case signal name for case-insensitive match
# ---------------------------------------------------------------------------

def load_rules(config_path):
    with open(config_path) as f:
        raw = f.read()
    config, _ = json.JSONDecoder().raw_decode(raw)

    rules = {}  # (sig_name.lower(), msg_name.lower()) -> rule dict
    for cfg in config.get("configurations", []):
        product_names = cfg.get("product-name", [])
        sending_interval_ms = cfg.get("sending-interval")
        for sig in cfg.get("signals", []):
            dr = sig.get("data-reduction", {})
            key = (_norm(sig["signal-name"]), _norm(sig.get("message-name", "")))
            rules[key] = {
                "signal_name":         sig["signal-name"],
                "message_name":        sig.get("message-name", ""),
                "reduction_type":      dr.get("reduction-type", "none"),
                "time_ms":             dr.get("time"),
                "sending_interval_ms": sending_interval_ms,
                "product_names":       product_names,
            }
    return rules


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------

_CSV_MAX_ROWS = 1_000_000


def _safe_filename(name):
    return re.sub(r"[^\w\-]", "_", name)


def _write_signal_csvs(df_sig, out_dir, sig_name, msg_name, suffix, fmt="csv"):
    safe = _safe_filename(f"{sig_name}_{msg_name}")
    if fmt == "parquet":
        path = out_dir / f"{safe}-{suffix}.parquet"
        df_sig.to_parquet(path, index=False)
        return 1
    if df_sig.empty:
        path = out_dir / f"{safe}-{suffix}-0.csv"
        df_sig.to_csv(path, index=False)
        return 1
    n = 0
    for start in range(0, len(df_sig), _CSV_MAX_ROWS):
        chunk = df_sig.iloc[start:start + _CSV_MAX_ROWS]
        path = out_dir / f"{safe}-{suffix}-{n}.csv"
        chunk.to_csv(path, index=False)
        n += 1
    return n


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(r'_(base|basis|var\d*)$', re.IGNORECASE)


def _norm(s):
    """Lowercase and strip trailing _BASE / _BASIS / _VARn for fuzzy matching."""
    return _SUFFIX_RE.sub('', s.lower())


def _build_file_index(input_dir_path):
    """Build index: (norm(signal_name_col), norm(message_name_from_stem)) -> Path."""
    import csv as _csv
    index = {}
    for p in input_dir_path.glob("*.csv"):
        try:
            with open(p, newline="") as f:
                row = next(_csv.DictReader(f), None)
            if row is None or "signal_name" not in row:
                continue
            sig_col = row["signal_name"].strip().lower()
            stem = p.stem
            if not stem.lower().startswith(sig_col):
                continue
            msg_part = stem[len(sig_col) + 1:].lower()
            index[(_norm(sig_col), _norm(msg_part))] = p
        except Exception:
            continue
    return index


def process(input_dir, config_path, output_dir, fmt="csv"):
    input_dir_path = Path(input_dir)
    csv_files = sorted(input_dir_path.glob("*.csv"))
    if not csv_files:
        print(f"ERROR: No CSV files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV file(s) in {input_dir_path}")
    file_index = _build_file_index(input_dir_path)
    print(f"Indexed {len(file_index)} (signal, message) pairs\n")

    rules = load_rules(config_path)
    print(f"Loaded {len(rules)} rule(s) from config\n")

    out_dir_path = Path(output_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    stats_rows = []
    total_files = 0

    for (sig_key, msg_key), rule in rules.items():
        csv_path = file_index.get((sig_key, msg_key))

        sig_name = rule["signal_name"]
        msg_name = rule["message_name"]
        r_type   = rule["reduction_type"]
        time_ms  = rule["time_ms"]
        products = ", ".join(rule["product_names"])

        if csv_path is None:
            print(f"  [{r_type}] {sig_name} / {msg_name} — no file found, writing empty CSV")
            empty_df = pd.DataFrame(columns=["timestamp", "signal_name", "signal_value",
                                             "reduction_method", "product_name"])
            _write_signal_csvs(empty_df, out_dir_path, sig_name, msg_name, "reduced", fmt)
            continue

        df = pd.read_csv(csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp")
        df["signal_value"] = pd.to_numeric(df["signal_value"], errors="coerce")

        if df.empty:
            print(f"  [{r_type}] {sig_name} / {msg_name} — empty file, writing empty CSV")
            empty_df = pd.DataFrame(columns=["timestamp", "signal_name", "signal_value",
                                             "reduction_method", "product_name"])
            _write_signal_csvs(empty_df, out_dir_path, sig_name, msg_name, "reduced", fmt)
            continue

        print(f"  [{r_type}] {sig_name} / {msg_name} | {len(df)} → ", end="")
        df_reduced = apply_reduction(df, r_type, time_ms)
        print(f"{len(df_reduced)} rows")

        df_reduced = df_reduced.reset_index()
        df_reduced["timestamp"] = df_reduced["timestamp"].dt.tz_localize(None)
        df_reduced["signal_name"] = sig_name
        df_reduced["reduction_method"] = r_type
        df_reduced["product_name"] = products

        col_order = ["timestamp", "signal_name", "signal_value", "reduction_method", "product_name"]
        df_reduced = df_reduced[[c for c in col_order if c in df_reduced.columns]]

        n = _write_signal_csvs(df_reduced, out_dir_path, sig_name, msg_name, "reduced", fmt)
        total_files += n

        stats_rows.append({
            "SignalName":       sig_name,
            "MessageName":      msg_name,
            "ReductionMethod":  r_type,
            "DataPoints":       len(df_reduced),
        })

    if not stats_rows:
        print("\nERROR: No signals reduced.", file=sys.stderr)
        sys.exit(1)

    stats = pd.DataFrame(stats_rows).sort_values(["SignalName", "MessageName", "ReductionMethod"])
    stats_path = out_dir_path / "statistics.csv"
    stats.to_csv(stats_path, index=False)

    print(f"\nWrote {total_files} CSV file(s) → {out_dir_path}")
    print(f"  Statistics → {stats_path}")
    print("\nStatistics:")
    print(stats.to_string(index=False))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Apply data reduction to local-decoder CSV files"
    )
    p.add_argument("--input-dir",  required=True,
                   help="Directory containing {signal_name}.csv files")
    p.add_argument("--config", required=True, help="configuration.json path")
    p.add_argument("--output-dir", required=True,
                   help="Output directory for reduced CSVs")
    p.add_argument("--format", choices=["csv", "parquet"], default="csv",
                   help="Output file format (default: csv)")
    return p.parse_args()


def main():
    args = parse_args()
    process(args.input_dir, args.config, args.output_dir, fmt=args.format)


if __name__ == "__main__":
    main()
