# MDF4 Reader

Decode MDF4 (MF4) CAN log files using DBC files, then apply configurable signal data reduction. Runs entirely via Docker.

## Features

- **Two MDF4 formats**: CANedge (mdf_iter) and Vector BLF-based (asammdf) — auto-detected
- Auto-detects protocol per file: CAN, J1939, UDS, NMEA 2000
- Filters RX-only frames (drops TX)
- Reassembles multi-frame transport protocol messages (J1939 BAM, UDS, NMEA Fast Packet)
- Loads multiple DBC files from a folder
- Recursive MF4 file discovery
- **Data reduction**: none, onChange, sampling, average, difference — configured per signal via JSON
- **Two reducers**: `reduce.py` for MDF4-decoded CSVs, `reduce_local.py` for local-decoder CSVs

## Pipeline overview

```
MF4 files + DBC files
        ↓
   decode.py       →  per-signal CSVs  (one file per signal/message pair)
        ↓
   reduce.py       →  per-signal reduced CSVs + statistics.csv


Local decoder CSVs  ({signal_name}_{MessageName}.csv)
        ↓
   reduce_local.py →  per-signal reduced CSVs + statistics.csv
```

## Requirements

- Docker + Docker Compose

## Quick start

### Step 1 — Build image

```bash
docker compose build
```

### Step 2 — Decode MF4 files

Mount each folder separately with `-v`. All paths after `--` are container-side paths.

```bash

docker compose run --rm --user $(id -u):$(id -g) \
  -v "/media/affan/Data1/19h0007-23May/mf4_file":/app/mdf4 \
  -v "/media/affan/Data1/19h0007-23May/dbc-file":/app/dbc \
  -v "/media/affan/Data1/19h0007-23May/mf4_decoded_parquet":/app/output \
  mdf4-decoder python src/decode.py \
  --mf4-folder /app/mdf4 \
  --dbc-folder /app/dbc \
  --output-dir /app/output \
  --format parquet \
  2>&1 | tee /tmp/decode.log
```

Omit `--signals` to extract all signals. Omit `--format` to use CSV (default).

Output: per-signal files written to `$OUTPUT_DIR`

### Step 3 — Apply data reduction (MDF4-decoded CSVs)

```bash
INPUT_DIR=/media/affan/Data1/19h0007-23May/mf4_decoded_parquet
OUTPUT_DIR=/media/affan/Data1/19h0007-23May/reduced_data
CONFIG_DIR=/media/affan/Data1/19h0007-23May/configuration

docker compose run --rm --user $(id -u):$(id -g) \
  -v "/media/affan/Data1/19h0007-23May/mf4_decoded_parquet":/app/input \
  -v "/media/affan/Data1/19h0007-23May/reduced_data_parquet":/app/output \
  -v "/media/affan/Data1/19h0007-23May/configuration":/app/config \
  mdf4-decoder python src/reduce.py \
  --input-dir /app/input \
  --config /app/config/r2d2-test.json \
  --output-dir /app/output \
  --format parquet
```

Output: per-signal reduced files + `statistics.csv` written to `$OUTPUT_DIR`

### Step 3 (alternative) — Apply data reduction (local-decoder CSVs)

For input files named `{signal_name}_{MessageName}.csv` with columns `timestamp, signal_name, signal_value`.

```bash
INPUT_DIR=/media/affan/Data1/19h0007-23May/output_local_decoder
OUTPUT_DIR=/media/affan/Data1/19h0007-23May/csv_output_reduced_local
CONFIG_DIR=/media/affan/Data1/19h0007-23May/configuration

docker compose run --rm --user $(id -u):$(id -g) \
  -v "$INPUT_DIR":/app/input \
  -v "$OUTPUT_DIR":/app/output \
  -v "$CONFIG_DIR":/app/config \
  mdf4-decoder python src/reduce_local.py \
  --input-dir /app/input \
  --config /app/config/r2d2-test.json \
  --output-dir /app/output \
  --format parquet
```

Output: per-signal reduced files + `statistics.csv` written to `$OUTPUT_DIR`

> **Note:** Output files are owned by root if run without `--user`. Ensure the output directory is writable by the current user before running.

## Large files

Decoder reads in 500,000-record chunks — safe for multi-GB MF4 files. No config needed.

## Arguments

### decode.py

| Argument | Required | Description |
|----------|----------|-------------|
| `--mf4-folder` | Yes | Folder with MF4 files (searched recursively) |
| `--dbc-folder` | Yes | Folder with DBC files (all `*.dbc` / `*.DBC` loaded) |
| `--output-dir` | Yes | Output directory — per-signal files written here |
| `--signals` | No | Path to JSON file filtering which signals to extract (see below) |
| `--format` | No | Output file format: `csv` (default) or `parquet` |
| `--merge` | No | Merge all MF4 files into one output file per signal (no per-file suffix) |

#### Signal filter (optional)

Create a JSON file mapping signal name → message name. Only matching signals are written to output.

```json
{
  "tachographVehicleSpeed_BASIS": "TCO1_BASIS",
  "CCactive_BASIS": "cruiseControlVehicleSpeed_BASIS"
}
```

Pass it with `--signals /path/to/signals.json`. Omit to extract all signals.

### reduce.py

| Argument | Required | Description |
|----------|----------|-------------|
| `--input-dir` | Yes | Directory containing per-signal decoded CSV or parquet files |
| `--config` | Yes | Path to configuration JSON |
| `--output-dir` | Yes | Output directory — reduced files written here |
| `--format` | No | Output format: `csv` (default) or `parquet` |

### reduce_local.py

| Argument | Required | Description |
|----------|----------|-------------|
| `--input-dir` | Yes | Directory containing `{signal_name}_{MessageName}.csv` files |
| `--config` | Yes | Path to configuration JSON |
| `--output-dir` | Yes | Output directory — reduced files written here |
| `--format` | No | Output format: `csv` (default) or `parquet` |

## Output format

### decode.py output

One file per signal/message pair.
CSV: `{SignalName}-{MessageID}-{stem}-decoded-output-{n}.csv` (split at 1M rows)
Parquet: `{SignalName}-{MessageID}-{stem}-decoded-output.parquet` (single file, no row limit)

| Column | Description |
|--------|-------------|
| `Timestamp` | Absolute timestamp (UTC, from MDF4 file start time + relative offset) |
| `MessageName` | CAN message name from DBC (resolved from actual frame CAN ID) |
| `MessageID` | CAN arbitration ID in hex (e.g. `0xFEF1`) |
| `SignalName` | Signal name from DBC |
| `DecodedSignalData` | Physical value after DBC scaling/offset |

### reduce.py output

One file per signal/message pair + `statistics.csv`.
CSV: `{SignalName}-{MessageID}-reduced-output-{n}.csv` (split at 1M rows)
Parquet: `{SignalName}-{MessageID}-reduced-output.parquet` (single file, no row limit)

Extra columns added:

| Column | Description |
|--------|-------------|
| `ReductionMethod` | Which reduction was applied |
| `ProductName` | Product name(s) from config |

### reduce_local.py output

One file per signal/message pair + `statistics.csv`.
CSV: `{SignalName}_{MessageName}-reduced-{n}.csv` (split at 1M rows)
Parquet: `{SignalName}_{MessageName}-reduced.parquet` (single file, no row limit)

| Column | Description |
|--------|-------------|
| `timestamp` | Timestamp from source file (tz-naive) |
| `signal_name` | Signal name from config |
| `signal_value` | Reduced physical value |
| `reduction_method` | Which reduction was applied |
| `product_name` | Product name(s) from config |

## configuration.json format

```json
{
  "configurations": [
    {
      "product-name": ["my-product"],
      "sending-interval": 8000,
      "signals": [
        {
          "message-id": 218000622,
          "message-name": "TCO1_BASIS",
          "signal-name": "tachographVehicleSpeed_BASIS",
          "data-reduction": {
            "reduction-type": "onChange"
          }
        },
        {
          "message-id": 419361024,
          "message-name": "cruiseControlVehicleSpeed_BASIS",
          "signal-name": "CCactive_BASIS",
          "data-reduction": {
            "reduction-type": "sampling",
            "time": 4000
          }
        }
      ]
    }
  ]
}
```

### Reduction types

| Type | Description | Requires `time` |
|------|-------------|-----------------|
| `none` | Keep all values | No |
| `onChange` | Keep only when value changes | No |
| `sampling` | Keep one sample per N ms | Yes |
| `average` | Mean over N ms windows | Yes |
| `difference` | Consecutive value differences | No |

## MDF4 format detection

| Format | Library | Detection |
|--------|---------|-----------|
| CANedge | mdf_iter | Default |
| Vector BLF-based | asammdf | `VectorBlfEvent` or `CAN_DataFrame.ID` channel present |

Vector BLF files may contain a single group with multiple CAN channel instances (e.g. 20 instances per group). Decoder handles both layouts automatically.

## Protocol detection logic

| Condition | Protocol |
|-----------|----------|
| Standard 11-bit IDs + UDS response ID range | UDS |
| Standard 11-bit IDs | CAN |
| Extended 29-bit IDs + NMEA 2000 PGN range | NMEA 2000 |
| Extended 29-bit IDs | J1939 |

J1939, UDS, and NMEA 2000 automatically get transport protocol reassembly before DBC decoding.

## Folder structure example

```
data/
├── your-dbc-folder/
│   ├── engine.dbc
│   └── body.dbc          # all DBC files in folder loaded
└── your-log-folder/
    ├── DEVICE_A/
    │   └── 00000001/
    │       ├── 00000001.MF4
    │       └── 00000002.MF4
    └── DEVICE_B/
        └── 00000001/
            └── 00000001.MF4   # discovered recursively
```









python3 -m venv ~/mdf4-venv && source ~/mdf4-venv/bin/activate && pip install duckdb matplotlib pandas pyarrow


python3 -m venv ~/mdf4-venv
source ~/mdf4-venv/bin/activate
pip install duckdb matplotlib pandas pyarrow
python3 /path/to/your/script.py
