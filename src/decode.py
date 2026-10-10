#!/usr/bin/env python3
"""Decode MDF4 CAN log files to CSV using DBC files.

Supports: CAN, J1939, UDS, NMEA 2000.
Filters RX-only frames. Auto-detects protocol per file.
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import can_decoder
import canmatrix.formats
import mdf_iter
import asammdf


_CSV_MAX_ROWS = 1_000_000


def _safe_filename(name):
    """Sanitize signal name for use in filenames."""
    import re
    return re.sub(r"[^\w\-]", "_", name)


def _append_signal_csv(df_sig, out_dir, sig, msg_id, suffix, file_idx, rows_in_file, fmt="csv"):
    """Append df_sig to rolling files, splitting at _CSV_MAX_ROWS (CSV only).

    Parquet: always single file, accumulated in-memory state handled by caller.
    Returns (new_file_idx, new_rows_in_file).
    """
    safe_sig = _safe_filename(sig)
    safe_mid = _safe_filename(str(msg_id))
    if fmt == "parquet":
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}.parquet"
        if path.exists():
            import pyarrow.parquet as pq
            import pyarrow as pa
            existing = pq.read_table(path)
            new = pa.Table.from_pandas(df_sig, preserve_index=False)
            pq.write_table(pa.concat_tables([existing, new]), path)
        else:
            df_sig.to_parquet(path, index=False)
        return 0, 0
    pos = 0
    while pos < len(df_sig):
        space = _CSV_MAX_ROWS - rows_in_file
        chunk = df_sig.iloc[pos:pos + space]
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}-{file_idx}.csv"
        first_write = rows_in_file == 0
        chunk.to_csv(path, index=False, mode="a", header=first_write)
        rows_in_file += len(chunk)
        pos += len(chunk)
        if rows_in_file >= _CSV_MAX_ROWS:
            file_idx += 1
            rows_in_file = 0
    return file_idx, rows_in_file


def _write_signal_csvs(df_sig, out_dir, sig, msg_id, suffix, fmt="csv"):
    """Write df_sig to one or more files, splitting at _CSV_MAX_ROWS (CSV only)."""
    safe_sig = _safe_filename(sig)
    safe_mid = _safe_filename(str(msg_id))
    if fmt == "parquet":
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}.parquet"
        df_sig.to_parquet(path, index=False)
        return 1
    n = 0
    for start in range(0, max(1, len(df_sig)), _CSV_MAX_ROWS):
        chunk = df_sig.iloc[start:start + _CSV_MAX_ROWS]
        path = out_dir / f"{safe_sig}-{safe_mid}-{suffix}-{n}.csv"
        chunk.to_csv(path, index=False)
        n += 1
    return n


# ---------------------------------------------------------------------------
# Protocol detection constants
# ---------------------------------------------------------------------------

_NMEA_PGNS = frozenset([
    126983, 126984, 126985, 126986, 126987, 126988, 126996, 127233, 127237,
    127489, 127496, 127497, 127503, 127504, 127506, 127751, 128275, 128520,
    128538, 129029, 129038, 129039, 129040, 129041, 129044, 129284, 129285,
    129301, 129302, 129538, 129540, 129541, 129542, 129545, 129547, 129549,
    129551, 129556, 129792, 129793, 129794, 129795, 129796, 129798, 129799,
    129800, 129801, 129803, 129804, 129805, 129806, 129807, 129808, 129809,
    129810, 129811, 129812, 129813, 129814, 129815, 129816, 130052, 130053,
    130054, 130060, 130061, 130064, 130065, 130067, 130068, 130069, 130070,
    130071, 130072, 130073, 130074, 130320, 130321, 130322, 130323, 130324,
    130564, 130565, 130567, 130569, 130571, 130575, 130577, 130578, 130581,
    130584, 130586,
])

_UDS_RESPONSE_IDS = frozenset([
    1960, 2016, 2025, 2026, 2027, 2028, 2029, 2030, 2031,
    1979, 1992, 1998, 2001, 402522235,
])


# ---------------------------------------------------------------------------
# Transport Protocol reassembly (J1939 BAM, UDS multi-frame, NMEA fast-packet)
# ---------------------------------------------------------------------------

class _MultiFrameDecoder:
    FRAME_STRUCT = {
        "uds": {
            "SINGLE_FRAME_MASK": 0xF0, "FIRST_FRAME_MASK": 0xF0,
            "CONSEQ_FRAME_MASK": 0xF0, "SINGLE_FRAME": 0x00,
            "FIRST_FRAME": 0x10, "CONSEQ_FRAME": 0x20,
            "ff_payload_start": 1, "bam_pgn": -1,
            "res_id_list": [1960, 2016, 2025, 2026, 2027, 2028, 2029,
                            2030, 2031, 1979, 1992, 1998, 2001, 402522235],
            "group": "ID",
        },
        "j1939": {
            "SINGLE_FRAME_MASK": 0xFF, "FIRST_FRAME_MASK": 0xFF,
            "CONSEQ_FRAME_MASK": 0x00, "SINGLE_FRAME": 0xFF,
            "FIRST_FRAME": 0x20, "CONSEQ_FRAME": 0x00,
            "ff_payload_start": 8, "bam_pgn": 60416,
            "res_id_list": [60416, 60160],
            "group": "SA",
        },
        "nmea": {
            "SINGLE_FRAME_MASK": 0xFF, "FIRST_FRAME_MASK": 0x1F,
            "CONSEQ_FRAME_MASK": 0x00, "SINGLE_FRAME": 0xFF,
            "FIRST_FRAME": 0x00, "CONSEQ_FRAME": 0x00,
            "ff_payload_start": 2, "bam_pgn": -1,
            "res_id_list": list(_NMEA_PGNS),
            "group": "ID",
        },
    }

    def __init__(self, tp_type):
        self.tp_type = tp_type

    def _calc_pgn(self, frame_id):
        pgn = (frame_id & 0x03FFFF00) >> 8
        if (pgn & 0xFF00) < 0xF000:
            pgn &= 0xFFFFFF00
        return pgn

    def _calc_sa(self, frame_id):
        return frame_id & 0x000000FF

    def _new_frame(self, base, payload, can_id):
        f = base.copy()
        f["DataBytes"] = payload
        f["DLC"] = 0
        f["DataLength"] = len(payload)
        if can_id:
            f["ID"] = can_id
        return f

    def _pgn_to_can_id(self, row):
        pgn_hex = "".join("{:02x}".format(x) for x in reversed(row.DataBytes[5:8]))
        pgn = int(pgn_hex, 16)
        return (6 << 26) | (pgn << 8) | row.SA

    def _payload_length(self, row):
        if self.tp_type == "uds":
            return (row.DataBytes[0] & 0x0F) << 8 | row.DataBytes[1]
        if self.tp_type == "nmea":
            return row.DataBytes[1]
        # j1939
        return int("".join("{:02x}".format(x) for x in reversed(row.DataBytes[1:2])), 16)

    def _is_first_frame(self, row, bam_pgn, ff_mask, ff_val):
        if self.tp_type == "j1939" and bam_pgn == self._calc_pgn(row.ID):
            return True
        return (row.DataBytes[0] & ff_mask) == ff_val

    def combine(self, df_raw):
        if self.tp_type not in self.FRAME_STRUCT:
            return df_raw

        fs = self.FRAME_STRUCT[self.tp_type]
        res_ids_full = fs["res_id_list"]
        bam_pgn = fs["bam_pgn"]
        ff_start = fs["ff_payload_start"]
        ff_mask = fs["FIRST_FRAME_MASK"]
        ff_val = fs["FIRST_FRAME"]
        sf_mask = fs["SINGLE_FRAME_MASK"]
        sf_val = fs["SINGLE_FRAME"]

        # Split raw into TP / non-TP
        if self.tp_type == "nmea":
            pgns = df_raw["ID"].apply(self._calc_pgn)
            match = pgns.isin(res_ids_full)
            res_ids = pgns[match].drop_duplicates().tolist()
        elif self.tp_type == "j1939":
            pgns = df_raw["ID"].apply(self._calc_pgn)
            match = pgns.isin(res_ids_full)
            res_ids = [r for r in res_ids_full if r != bam_pgn]
            pgns = None
        else:  # uds
            pgns = None
            match = df_raw["ID"].isin(res_ids_full)
            res_ids = df_raw["ID"][match].drop_duplicates().tolist()

        df_tp = df_raw[match]
        df_excl = df_raw[~match]
        result = [df_excl]

        nmea_pgns_series = df_tp["ID"].apply(self._calc_pgn) if self.tp_type == "nmea" else None

        for res_id in res_ids:
            if self.tp_type == "nmea":
                df_res = df_tp[nmea_pgns_series.isin([res_id])]
            elif self.tp_type == "j1939":
                df_res = df_tp.copy()
                df_res["SA"] = df_res["ID"].apply(self._calc_sa)
            else:
                df_res = df_tp[df_tp["ID"].isin([res_id])]

            for _ch, df_ch in df_res.groupby("BusChannel"):
                for _id, df_grp in df_ch.groupby(fs["group"]):
                    base = df_grp.iloc[0]
                    frames, timestamps, payload = [], [], []
                    ff_len, can_id, prev_cf = 0xFFF, None, None
                    frame_ts = None

                    for row in df_grp.itertuples(index=True, name="P"):
                        is_ff = self._is_first_frame(row, bam_pgn, ff_mask, ff_val)
                        b0 = int(row.DataBytes[0])

                        if self.tp_type != "nmea" and (b0 & sf_mask == sf_val):
                            frames.append(self._new_frame(base, row.DataBytes, row.ID).values.tolist())
                            timestamps.append(row.Index)
                        elif is_ff:
                            if len(payload) >= ff_len:
                                frames.append(self._new_frame(base, payload, can_id).values.tolist())
                                timestamps.append(frame_ts)
                            prev_cf = None
                            frame_ts = row.Index
                            if self.tp_type == "j1939":
                                can_id = self._pgn_to_can_id(row)
                            ff_len = self._payload_length(row)
                            payload = list(row.DataBytes[ff_start:])
                        elif prev_cf is None or (b0 - prev_cf) == 1:
                            prev_cf = b0
                            payload = payload + list(row.DataBytes[1:])

                    if frames:
                        result.append(
                            pd.DataFrame(frames, columns=base.index, index=timestamps)
                        )

        df_out = pd.concat(result, join="outer").sort_index()
        df_out.index.name = "TimeStamp"
        return df_out


# ---------------------------------------------------------------------------
# Protocol detection
# ---------------------------------------------------------------------------

def detect_protocol(df_raw):
    """Return one of: 'can', 'j1939', 'uds', 'nmea'."""
    if df_raw.empty:
        return "can"

    has_extended = bool(df_raw["IDE"].any()) if "IDE" in df_raw.columns else False

    if not has_extended:
        ids = set(df_raw["ID"].unique())
        if ids & _UDS_RESPONSE_IDS:
            return "uds"
        return "can"

    # Extended 29-bit IDs
    pgns = df_raw["ID"].apply(lambda x: (x & 0x03FFFF00) >> 8)
    if pgns.isin(_NMEA_PGNS).any():
        return "nmea"

    return "j1939"


# ---------------------------------------------------------------------------
# DBC loading + signal → message lookup
# ---------------------------------------------------------------------------

def load_dbc_files(dbc_folder):
    """Load all *.dbc / *.DBC files from folder. Return (db_list, msg_lookup).

    msg_lookup: can_id (int) -> (msg_name, msg_id_hex)
    Keyed by actual CAN ID so signals shared across messages resolve correctly.
    """
    folder = Path(dbc_folder)
    paths = sorted(set(folder.glob("*.dbc")) | set(folder.glob("*.DBC")))
    if not paths:
        print(f"ERROR: No DBC files found in {dbc_folder}", file=sys.stderr)
        sys.exit(1)

    db_list = []
    msg_lookup = {}  # can_id (int) -> (msg_name, msg_id_hex)

    for p in paths:
        db_list.append(can_decoder.load_dbc(p))
        cm = list(canmatrix.formats.loadp(str(p)).values())[0]
        print(f"  Loaded DBC: {p.name}")
        for frame in cm.frames:
            aid = frame.arbitration_id.id
            msg_lookup[aid] = (frame.name, f"0x{aid:X}")

    print(f"  {len(db_list)} DBC files | {len(msg_lookup)} messages mapped\n")
    return db_list, msg_lookup


# ---------------------------------------------------------------------------
# MF4 decoding
# ---------------------------------------------------------------------------

_CAN_FRAME_CHANNELS = {
    "CAN_DataFrame.ID", "CAN_DataFrame.DataBytes", "CAN_DataFrame.Dir",
    "CAN_DataFrame.IDE", "CAN_DataFrame.BusChannel", "CAN_DataFrame.DataLength",
}

_CHUNK_RECORDS = 500_000  # max records per chunk — tune for available RAM


def _decode_mdf_iter(mf4_path, db_list, msg_lookup):
    """Decode CANedge MDF4 file. Loads raw frames once, chunks at decode stage."""
    with open(mf4_path, "rb") as fh:
        mdf = mdf_iter.MdfFile(fh)
        df_raw = mdf.get_data_frame()

    if df_raw.empty:
        print("  SKIP (empty)")
        return pd.DataFrame()

    # Dir is bool: False=RX, True=TX
    if "Dir" in df_raw.columns:
        rx_mask = df_raw["Dir"].isin([False, 0])
        n_total = len(df_raw)
        df_raw = df_raw[rx_mask]
        print(f"  Frames: {len(df_raw)} RX / {n_total} total")
    else:
        print(f"  Frames: {len(df_raw)} (no Dir column, keeping all)")

    if df_raw.empty:
        print("  SKIP (no RX frames)")
        return pd.DataFrame()

    protocol = detect_protocol(df_raw)
    print(f"  Protocol: {protocol.upper()}")

    # TP reassembly must see all frames before chunking
    if protocol in ("j1939", "uds", "nmea"):
        tp = _MultiFrameDecoder(protocol)
        df_raw = tp.combine(df_raw)

    # Chunk at decode stage to limit peak memory from decoded output accumulation
    all_decoded = []
    chunk_count = 0

    for start in range(0, len(df_raw), _CHUNK_RECORDS):
        chunk = df_raw.iloc[start:start + _CHUNK_RECORDS]
        chunk_count += 1

        parts = []
        for db in db_list:
            decoder = can_decoder.DataFrameDecoder(db)
            for _bus, bus_grp in chunk.groupby("BusChannel"):
                for _len, grp in bus_grp.groupby("DataLength"):
                    try:
                        df_dec = decoder.decode_frame(grp)
                        if not df_dec.empty:
                            parts.append(df_dec)
                    except Exception:
                        continue

        if not parts:
            continue

        df_phys = pd.concat(parts).sort_index()
        df_phys["_ts"] = df_phys.index
        df_phys = df_phys.drop_duplicates(subset=["_ts", "Signal"]).drop(columns=["_ts"])

        msg_names = df_phys["CAN ID"].map(lambda i: msg_lookup.get(i, ("", ""))[0])
        msg_ids   = df_phys["CAN ID"].map(lambda i: msg_lookup.get(i, ("", ""))[1])

        df_out = pd.DataFrame({
            "Timestamp":          df_phys.index,
            "MessageName":        msg_names.values,
            "MessageID":          msg_ids.values,
            "SignalName":         df_phys["Signal"].values,
            "DecodedSignalData":  df_phys["Physical Value"].values,
        })

        print(f"  Chunk {chunk_count}: → {len(df_out)} signal readings")
        all_decoded.append(df_out)

    if not all_decoded:
        print("  No signals decoded")
        return pd.DataFrame()

    result = pd.concat(all_decoded, ignore_index=True).sort_values("Timestamp")
    print(f"  Total decoded: {len(result)} signal readings")
    return result


def _try_open_asammdf(mf4_path):
    """Try to open with asammdf. Returns open MDF object or None."""
    try:
        return asammdf.MDF(str(mf4_path))
    except Exception:
        return None


def _find_can_instances(gp):
    """Return list of {name: ch_idx} dicts, one per CAN_DataFrame instance in group."""
    instances = []
    # Anchor on CAN_DataFrame.ID occurrences; find companions within ±10 channels
    id_indices = [i for i, ch in enumerate(gp.channels) if ch.name == "CAN_DataFrame.ID"]
    for id_idx in id_indices:
        lo, hi = max(0, id_idx - 8), min(len(gp.channels), id_idx + 8)
        local = {}
        for ci in range(lo, hi):
            n = gp.channels[ci].name
            if n in _CAN_FRAME_CHANNELS and n not in local:
                local[n] = ci
        if _CAN_FRAME_CHANNELS.issubset(local):
            instances.append(local)
    return instances


def _iter_groups_asammdf(mdf):
    """Yield CAN-frame DataFrames in chunks. Handles multi-group and multi-instance files."""
    import numpy as _np
    start_epoch = mdf.start_time.timestamp()

    for gp_idx, gp in enumerate(mdf.groups):
        instances = _find_can_instances(gp)
        if not instances:
            continue

        total = gp.channel_group.cycles_nr

        for ch_map in instances:
            offset = 0
            while offset < total:
                count = min(_CHUNK_RECORDS, total - offset)
                try:
                    sig_id   = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.ID"],          record_offset=offset, record_count=count)
                    sig_dir  = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.Dir"],          record_offset=offset, record_count=count)
                    sig_bus  = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.BusChannel"],   record_offset=offset, record_count=count)
                    sig_ide  = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.IDE"],          record_offset=offset, record_count=count)
                    sig_dlen = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.DataLength"],   record_offset=offset, record_count=count)
                    sig_data = mdf.get(group=gp_idx, index=ch_map["CAN_DataFrame.DataBytes"],    record_offset=offset, record_count=count)
                except Exception:
                    break
                dlen = sig_dlen.samples.astype(int)
                ts = pd.to_datetime(sig_id.timestamps + start_epoch, unit="s", utc=True)
                df = pd.DataFrame({
                    "ID":         sig_id.samples.astype(_np.uint32),
                    "DataBytes":  list(sig_data.samples),  # numpy uint8 arrays for can_decoder J1939
                    "Dir":        sig_dir.samples,
                    "IDE":        sig_ide.samples.astype(bool),
                    "BusChannel": sig_bus.samples.astype(int),
                    "DataLength": dlen,
                    "DLC":        dlen,
                }, index=ts)
                df.index.name = "TimeStamp"
                yield df
                offset += count


def _decode_asammdf(mdf, db_list, msg_lookup, out_dir, mf4_stem, signal_filter=None, fmt="csv", merge=False):
    """Decode asammdf MF4 file chunk by chunk, writing files immediately — no accumulation."""
    suffix = "decoded-output" if merge else f"{mf4_stem}-decoded-output"
    # (sig, msg_id) -> (current_file_idx, rows_in_current_file)
    sig_state = {}
    chunk_count = 0
    total_rows = 0

    for df_raw in _iter_groups_asammdf(mdf):
        chunk_count += 1

        if df_raw.empty:
            continue

        dir_col = df_raw["Dir"]
        rx_mask = (dir_col == b'Rx') | (dir_col == 'Rx')
        if not rx_mask.any():
            rx_mask = dir_col.isin([0, False])

        n_total = len(df_raw)
        df_raw = df_raw[rx_mask].copy()

        if df_raw.empty:
            continue

        print(f"  Chunk {chunk_count}: {len(df_raw)} RX / {n_total} frames")

        protocol = detect_protocol(df_raw)
        if protocol in ("j1939", "uds", "nmea"):
            tp = _MultiFrameDecoder(protocol)
            df_raw = tp.combine(df_raw)

        parts = []
        for db in db_list:
            decoder = can_decoder.DataFrameDecoder(db)
            for _bus, bus_grp in df_raw.groupby("BusChannel"):
                for _len, grp in bus_grp.groupby("DataLength"):
                    try:
                        df_dec = decoder.decode_frame(grp)
                        if not df_dec.empty:
                            parts.append(df_dec)
                    except Exception:
                        continue

        if not parts:
            continue

        df_phys = pd.concat(parts).sort_index()
        df_phys["_ts"] = df_phys.index
        df_phys = df_phys.drop_duplicates(subset=["_ts", "Signal"]).drop(columns=["_ts"])

        msg_names = df_phys["CAN ID"].map(lambda i: msg_lookup.get(i, ("", ""))[0])
        msg_ids   = df_phys["CAN ID"].map(lambda i: msg_lookup.get(i, ("", ""))[1])

        df_out = pd.DataFrame({
            "Timestamp":         pd.to_datetime(df_phys.index, utc=True).tz_convert(None),
            "MessageName":       msg_names.values,
            "MessageID":         msg_ids.values,
            "SignalName":        df_phys["Signal"].values,
            "DecodedSignalData": df_phys["Physical Value"].values,
        })

        if signal_filter:
            keys = list(zip(df_out["SignalName"].str.lower(), df_out["MessageName"].str.lower()))
            df_out = df_out[[k in signal_filter for k in keys]]

        if df_out.empty:
            continue

        print(f"    → {len(df_out)} signal readings")
        total_rows += len(df_out)

        for (sig, msg_id), df_sig in df_out.groupby(["SignalName", "MessageID"]):
            key = (sig, msg_id)
            file_idx, rows_in_file = sig_state.get(key, (0, 0))
            file_idx, rows_in_file = _append_signal_csv(
                df_sig, out_dir, sig, msg_id, suffix, file_idx, rows_in_file, fmt
            )
            sig_state[key] = (file_idx, rows_in_file)

    print(f"  Total decoded: {total_rows} signal readings, {len(sig_state)} signal/message pair(s)")
    return len(sig_state)


def decode_mf4(mf4_path, db_list, msg_lookup, out_dir, mf4_stem,
               signal_filter=None, fmt="csv", merge=False):
    """
    Decode one MF4 file, writing per-signal files to out_dir immediately.
    asammdf path streams chunk-by-chunk — no full-file accumulation in memory.
    merge=True: all MF4 files share the same output file per signal.
    Returns number of signal/message pairs written (0 = nothing decoded).
    """
    print(f"Processing: {mf4_path}")

    mdf = _try_open_asammdf(mf4_path)
    if mdf is not None:
        print("  Reader: asammdf (Vector BLF / CAN_DataFrame format)")
        try:
            return _decode_asammdf(mdf, db_list, msg_lookup, out_dir, mf4_stem,
                                   signal_filter, fmt, merge)
        finally:
            try:
                mdf.close()
            except Exception:
                pass

    print("  Reader: mdf_iter (CANedge format)")
    try:
        df = _decode_mdf_iter(mf4_path, db_list, msg_lookup)
    except Exception as exc:
        print(f"  SKIP (read error): {exc}", file=sys.stderr)
        return 0
    if df.empty:
        return 0
    stem = None if merge else mf4_stem
    return _write_file_csvs(df, out_dir, stem or "decoded-output", signal_filter, fmt)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Decode MDF4 CAN/J1939/UDS/NMEA log files to CSV"
    )
    p.add_argument(
        "--mf4-folder", required=True,
        help="Folder containing MF4 log files (searched recursively)",
    )
    p.add_argument(
        "--dbc-folder", required=True,
        help="Folder containing DBC files (all *.dbc files are loaded)",
    )
    p.add_argument(
        "--output-dir", required=True,
        help="Output directory — decoded_output.xlsx will be written here",
    )
    p.add_argument(
        "--signals", default=None,
        help='Path to JSON file with signal filter: {"SignalName": "MessageName", ...}',
    )
    p.add_argument(
        "--format", choices=["csv", "parquet"], default="csv",
        help="Output file format (default: csv)",
    )
    p.add_argument(
        "--merge", action="store_true", default=False,
        help="Merge all MF4 files into same output file per signal (no per-file suffix)",
    )
    return p.parse_args()


def _load_signal_filter(path):
    """Load signal filter JSON → set of (signal_name_lower, message_name_lower).

    Accepts two formats:
      {"SignalName": "MessageName", ...}
      [{"signal": "SignalName", "message": "MessageName"}, ...]
    """
    with open(path) as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return {(e["signal"].lower(), e["message"].lower()) for e in raw}
    return {(k.lower(), v.lower()) for k, v in raw.items()}


def _write_file_csvs(df, out_dir, mf4_stem, signal_filter=None, fmt="csv"):
    """Write decoded DataFrame for one MF4 file to per-signal files immediately."""
    df = df.copy()
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True).dt.tz_convert(None)
    if signal_filter:
        keys = list(zip(df["SignalName"].str.lower(), df["MessageName"].str.lower()))
        df = df[[k in signal_filter for k in keys]]
    groups = sorted(df.groupby(["SignalName", "MessageID"]).groups.keys())
    total_files = 0
    for sig, msg_id in groups:
        df_sig = df[(df["SignalName"] == sig) & (df["MessageID"] == msg_id)].copy()
        suffix = f"{mf4_stem}-decoded-output"
        n = _write_signal_csvs(df_sig, out_dir, sig, msg_id, suffix, fmt)
        total_files += n
    return total_files


def main():
    args = parse_args()

    signal_filter = None
    if args.signals:
        signal_filter = _load_signal_filter(args.signals)
        print(f"=== Signal filter: {len(signal_filter)} signal(s) ===")

    print("=== Loading DBC files ===")
    db_list, msg_lookup = load_dbc_files(args.dbc_folder)

    mf4_files = sorted(
        list(Path(args.mf4_folder).rglob("*.MF4")) +
        list(Path(args.mf4_folder).rglob("*.mf4"))
    )
    if not mf4_files:
        print(f"ERROR: No MF4 files found in {args.mf4_folder}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Decoding {len(mf4_files)} MF4 file(s) ===")
    total_pairs = 0
    decoded_count = 0

    if args.merge:
        print("=== Merge mode: all MF4 files → same output file per signal ===")

    for mf4_path in mf4_files:
        n = decode_mf4(mf4_path, db_list, msg_lookup, out_dir, mf4_path.stem,
                       signal_filter, args.format, args.merge)
        if n > 0:
            total_pairs += n
            decoded_count += 1
            print(f"  Written {n} signal/message pair(s) for {mf4_path.name}")
        print()

    if decoded_count == 0:
        print("ERROR: No data decoded from any file.", file=sys.stderr)
        sys.exit(1)

    print(f"=== Done: {total_pairs} signal/message pair(s) → {out_dir} ===")


if __name__ == "__main__":
    main()
