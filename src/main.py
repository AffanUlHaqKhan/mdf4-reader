import os
import sys
import asammdf


def read_mdf4(filepath: str) -> asammdf.MDF:
    print(f"Reading: {filepath}")
    mdf = asammdf.MDF(filepath)
    print(f"Version: {mdf.version}")
    print(f"Channels: {len(list(mdf.channels_db.keys()))}")
    return mdf


def print_channels(mdf: asammdf.MDF) -> None:
    print("\nChannels:")
    for name in sorted(mdf.channels_db.keys()):
        print(f"  {name}")


def export_to_dataframe(mdf: asammdf.MDF):
    df = mdf.to_dataframe()
    print(f"\nDataFrame shape: {df.shape}")
    print(df.head())
    return df


if __name__ == "__main__":
    filepath = os.environ.get("MDF4_FILE") or (sys.argv[1] if len(sys.argv) > 1 else None)

    if not filepath:
        print("Usage: MDF4_FILE=<path> python src/main.py")
        print("   or: python src/main.py <path>")
        sys.exit(1)

    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        sys.exit(1)

    mdf = read_mdf4(filepath)
    print_channels(mdf)
    export_to_dataframe(mdf)
