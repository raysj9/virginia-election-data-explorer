#!/usr/bin/env python3
"""Read a saved Virginia Department of Elections snapshot into Bronze Parquet.

This job preserves the source files untouched and creates Spark-readable
representations of the JSON results export, results CSV, and turnout CSV.
It deliberately does not normalize, clean, aggregate, or ingest the separate
change-log report.

Run from the project root:
    .venv/bin/python scripts/ingest_va_election_snapshot.py

Use --input-dir and --output-dir to process a different snapshot.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "virginia"
    / "2026-11-03"
    / "snapshot_2026-11-04"
)
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "virginia"
    / "2026-11-03"
    / "snapshot_2026-11-04"
)

SOURCE_FILES = {
    "election_results_json": "election_results.json",
    "election_results_csv": "election_results.csv",
    "election_turnout_csv": "election_turnout.csv",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest a saved Virginia election snapshot into Bronze Parquet."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Directory containing the saved source files (default: {DEFAULT_INPUT_DIR}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"New directory for Parquet outputs (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master (default: local[*]).",
    )
    return parser.parse_args()


def read_source(spark: SparkSession, dataset_name: str, source_path: Path) -> DataFrame:
    if dataset_name == "election_results_json":
        # The export is one large JSON document with nested results arrays.
        return spark.read.option("multiLine", "true").json(str(source_path))

    # Keep CSV values as strings at this stage; type and meaning checks belong
    # in the next milestone. The untouched CSV remains the byte-for-byte source.
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        # These exports contain Windows-1252 accented characters. This saved
        # snapshot has no bytes in the CP1252-only 0x80-0x9F range, so Spark's
        # supported ISO-8859-1 decoder preserves its text without replacement.
        .option("encoding", "ISO-8859-1")
        # Virginia's CSV uses doubled quotes inside quoted values (RFC-style),
        # including names that also contain commas.
        .option("escape", '"')
        .csv(str(source_path))
    )


def add_ingestion_metadata(
    frame: DataFrame, source_path: Path, snapshot_id: str, ingested_at_utc: str
) -> DataFrame:
    return (
        frame.withColumn("_snapshot_id", F.lit(snapshot_id))
        .withColumn("_source_file", F.lit(source_path.name))
        .withColumn("_ingested_at_utc", F.lit(ingested_at_utc))
    )


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    source_paths = {
        dataset_name: input_dir / filename
        for dataset_name, filename in SOURCE_FILES.items()
    }
    missing_paths = [path for path in source_paths.values() if not path.is_file()]
    if missing_paths:
        missing = "\n".join(f"  - {path}" for path in missing_paths)
        raise FileNotFoundError(f"Required source files are missing:\n{missing}")

    if input_dir == output_dir or input_dir in output_dir.parents:
        raise ValueError("--output-dir must be separate from the raw input directory.")
    if output_dir.exists():
        raise FileExistsError(
            f"Output snapshot already exists: {output_dir}. "
            "Choose a new output directory to preserve existing snapshots."
        )

    snapshot_id = input_dir.name
    ingested_at_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    spark = (
        SparkSession.builder.appName("VirginiaElectionSnapshotIngestion")
        .master(args.master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for dataset_name, source_path in source_paths.items():
            output_path = output_dir / dataset_name
            print(f"\nReading {source_path}")
            frame = read_source(spark, dataset_name, source_path)
            frame = add_ingestion_metadata(
                frame,
                source_path=source_path,
                snapshot_id=snapshot_id,
                ingested_at_utc=ingested_at_utc,
            )

            print(f"Schema for {dataset_name}:")
            frame.printSchema()
            row_count = frame.count()
            print(f"Rows in {dataset_name}: {row_count:,}")

            # errorifexists protects a saved snapshot from accidental overwrite.
            frame.write.mode("errorifexists").option("compression", "snappy").parquet(
                str(output_path)
            )
            print(f"Wrote {output_path}")
    except Exception:
        # The output path was confirmed absent above, so any partial directory
        # belongs to this failed run and can be discarded safely.
        if output_dir.exists():
            shutil.rmtree(output_dir)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
