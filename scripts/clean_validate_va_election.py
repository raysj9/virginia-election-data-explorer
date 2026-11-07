#!/usr/bin/env python3
"""Clean the Virginia election Bronze tables and write validation evidence.

This is Milestone 3: it creates typed, consistently named Parquet tables and
reports data-quality findings without changing or dropping source records.
It does not aggregate results into analytics tables or repair discrepancies.

Run from the project root:
    .venv/bin/python scripts/clean_validate_va_election.py
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = "snapshot_2026-11-04"
DEFAULT_BRONZE_DIR = (
    PROJECT_ROOT / "data" / "bronze" / "virginia" / "2026-11-03" / SNAPSHOT
)
DEFAULT_RAW_DIR = (
    PROJECT_ROOT / "data" / "raw" / "virginia" / "2026-11-03" / SNAPSHOT
)
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT / "data" / "curated" / "virginia" / "2026-11-03" / SNAPSHOT
)


def report_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return f"[external]/{resolved.name}"


RESULT_RENAMES = {
    "CandidateId": "candidate_id",
    "CandidateName": "candidate_name",
    "TOTAL_VOTES": "votes",
    "Party": "party",
    "WriteInVote": "write_in_vote",
    "LocalityCode": "locality_code",
    "LocalityName": "locality_name",
    "PrecinctId": "precinct_id",
    "PrecinctName": "precinct_name",
    "DistrictId": "district_id",
    "DistrictType": "district_type",
    "DistrictName": "district_name",
    "OfficeId": "office_id",
    "OfficeTitle": "office_title",
    "ElectionId": "election_id",
    "ElectionType": "election_type",
    "ElectionDate": "election_date",
    "ElectionName": "election_name",
    "NumberOfSeats": "number_of_seats",
}
RESULT_INTEGER_FIELDS = ("votes", "write_in_vote", "number_of_seats")
RESULT_REQUIRED_FIELDS = (
    "election_id",
    "locality_code",
    "precinct_id",
    "office_id",
    "candidate_id",
)
RESULT_KEY = (
    "election_id",
    "locality_code",
    "precinct_id",
    "office_id",
    "district_id",
    "candidate_id",
)

TURNOUT_RENAMES = {
    "election": "election",
    "election_date": "election_date",
    "locality": "locality",
    "precinct": "precinct",
    "Provisional": "provisional",
    "Post-Election": "post_election",
    "Election Day": "election_day",
    "Early Voting": "early_voting",
    "Mailed Absentee": "mailed_absentee",
    "TotalVoteTurnout": "total_vote_turnout",
    "ActiveRegisteredVoters": "active_registered_voters",
    "InactiveRegisteredVoters": "inactive_registered_voters",
    "TotalRegisteredVoters": "total_registered_voters",
}
TURNOUT_METHOD_FIELDS = (
    "provisional",
    "post_election",
    "election_day",
    "early_voting",
    "mailed_absentee",
)
TURNOUT_INTEGER_FIELDS = TURNOUT_METHOD_FIELDS + (
    "total_vote_turnout",
    "active_registered_voters",
    "inactive_registered_voters",
    "total_registered_voters",
)
TURNOUT_REQUIRED_FIELDS = ("election", "election_date", "locality", "precinct")
TURNOUT_KEY = ("election", "election_date", "locality", "precinct")
INTEGER_PATTERN = r"^[+-]?[0-9]+$"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and validate one Virginia election Bronze snapshot."
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args()


def clean_string_columns(frame: DataFrame) -> DataFrame:
    """Trim text and make empty strings null, keeping all other values."""
    for field in frame.schema.fields:
        if isinstance(field.dataType, StringType):
            trimmed = F.trim(F.col(field.name))
            frame = frame.withColumn(
                field.name,
                F.when(trimmed.isNull() | (trimmed == ""), F.lit(None)).otherwise(
                    trimmed
                ),
            )
    return frame


def rename_columns(frame: DataFrame, mapping: dict[str, str]) -> DataFrame:
    for source, destination in mapping.items():
        frame = frame.withColumnRenamed(source, destination)
    return frame


def cast_integer_columns(frame: DataFrame, fields: tuple[str, ...]) -> DataFrame:
    """Cast whole numbers safely and retain a per-field invalid-value flag."""
    for field in fields:
        source = F.trim(F.col(field))
        is_present = source.isNotNull() & (source != "")
        is_integer = source.rlike(INTEGER_PATTERN)
        frame = (
            frame.withColumn(
                f"_invalid_{field}",
                F.when(is_present & ~is_integer, F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                field,
                F.when(is_present & is_integer, source.cast(LongType())).otherwise(
                    F.lit(None).cast(LongType())
                ),
            )
        )
    return frame


def summed_flags(frame: DataFrame, flag_names: tuple[str, ...]) -> dict[str, int]:
    expressions = [
        F.coalesce(F.sum(F.col(name)), F.lit(0)).cast("long").alias(name)
        for name in flag_names
    ]
    return {key: int(value or 0) for key, value in frame.agg(*expressions).first().asDict().items()}


def count_missing_by_field(frame: DataFrame, fields: tuple[str, ...]) -> dict[str, int]:
    expressions = [
        F.coalesce(
            F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)), F.lit(0)
        ).cast("long").alias(name)
        for name in fields
    ]
    return {
        name: int(value or 0)
        for name, value in frame.agg(*expressions).first().asDict().items()
    }


def duplicate_key_metrics(frame: DataFrame, fields: tuple[str, ...]) -> dict[str, int]:
    duplicates = frame.groupBy(*fields).count().filter(F.col("count") > 1)
    totals = duplicates.agg(
        F.count(F.lit(1)).alias("duplicate_key_groups"),
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias(
            "duplicate_excess_rows"
        ),
    ).first()
    return {
        "duplicate_key_groups": int(totals["duplicate_key_groups"] or 0),
        "duplicate_excess_rows": int(totals["duplicate_excess_rows"] or 0),
    }


def make_results(spark: SparkSession, path: Path) -> DataFrame:
    frame = spark.read.parquet(str(path))
    frame = clean_string_columns(rename_columns(frame, RESULT_RENAMES))
    frame = cast_integer_columns(frame, RESULT_INTEGER_FIELDS)
    raw_date = F.col("election_date")
    parsed_date = F.to_date(raw_date, "yyyy-MM-dd")
    frame = (
        frame.withColumn(
            "_invalid_election_date",
            F.when(raw_date.isNotNull() & parsed_date.isNull(), 1).otherwise(0),
        )
        .withColumn("election_date", parsed_date)
    )
    return frame


def make_turnout(spark: SparkSession, path: Path) -> DataFrame:
    frame = spark.read.parquet(str(path))
    frame = clean_string_columns(rename_columns(frame, TURNOUT_RENAMES))
    frame = cast_integer_columns(frame, TURNOUT_INTEGER_FIELDS)
    raw_date = F.col("election_date")
    parsed_date = F.to_date(F.to_timestamp(raw_date, "MM/dd/yyyy HH:mm:ss"))
    frame = (
        frame.withColumn(
            "_invalid_election_date",
            F.when(raw_date.isNotNull() & parsed_date.isNull(), 1).otherwise(0),
        )
        .withColumn("election_date", parsed_date)
    )
    return frame


def statewide_summary_rows(path: Path, snapshot_id: str) -> list[dict[str, Any]]:
    """Flatten state, locality, and precinct totals at their source grains."""
    with path.open(encoding="utf-8") as source:
        document = json.load(source)

    locality_sums: dict[tuple[str, str], int] = {}
    locality_rows: dict[tuple[str, str], int] = {}
    precinct_sums: dict[tuple[str, str], int] = {}
    precinct_rows: dict[tuple[str, str], int] = {}
    precinct_missing: dict[tuple[str, str], int] = {}
    for locality in document.get("localResults") or []:
        for contest in locality.get("ballotItems") or []:
            office_id = str(contest.get("id") or "")
            for option in contest.get("ballotOptions") or []:
                key = (office_id, str(option.get("id") or ""))
                locality_votes = option.get("voteCount")
                if locality_votes is not None:
                    locality_sums[key] = locality_sums.get(key, 0) + int(locality_votes)
                    locality_rows[key] = locality_rows.get(key, 0) + 1
                for precinct in option.get("precinctResults") or []:
                    votes = precinct.get("voteCount")
                    if votes is None:
                        precinct_missing[key] = precinct_missing.get(key, 0) + 1
                    else:
                        precinct_sums[key] = precinct_sums.get(key, 0) + int(votes)
                        precinct_rows[key] = precinct_rows.get(key, 0) + 1

    rows: list[dict[str, Any]] = []
    for contest in (document.get("results") or {}).get("ballotItems") or []:
        for option in contest.get("ballotOptions") or []:
            office_id = str(contest.get("id") or "")
            candidate_id = str(option.get("id") or "")
            key = (office_id, candidate_id)
            local_count = locality_rows.get(key, 0)
            precinct_count = precinct_rows.get(key, 0)
            precinct_missing_count = precinct_missing.get(key, 0)
            rows.append(
                {
                    "snapshot_id": snapshot_id,
                    "office_id": office_id,
                    "office_title": str(contest.get("name") or ""),
                    "candidate_id": candidate_id,
                    "candidate_name": str(option.get("name") or ""),
                    "statewide_summary_votes": (
                        int(option["voteCount"])
                        if option.get("voteCount") is not None
                        else None
                    ),
                    "json_locality_summary_sum": (
                        locality_sums.get(key) if local_count else None
                    ),
                    "json_locality_rows": local_count,
                    "json_precinct_detail_sum": (
                        precinct_sums.get(key) if precinct_count else None
                    ),
                    "json_precinct_rows": precinct_count,
                    "json_precinct_missing_vote_rows": precinct_missing_count,
                }
            )
    return rows


def make_reconciliation(
    spark: SparkSession,
    results: DataFrame,
    raw_json_path: Path,
    snapshot_id: str,
) -> DataFrame:
    summary_schema = StructType(
        [
            StructField("snapshot_id", StringType(), False),
            StructField("office_id", StringType(), False),
            StructField("office_title", StringType(), False),
            StructField("candidate_id", StringType(), False),
            StructField("candidate_name", StringType(), False),
            StructField("statewide_summary_votes", LongType(), True),
            StructField("json_locality_summary_sum", LongType(), True),
            StructField("json_locality_rows", LongType(), False),
            StructField("json_precinct_detail_sum", LongType(), True),
            StructField("json_precinct_rows", LongType(), False),
            StructField("json_precinct_missing_vote_rows", LongType(), False),
        ]
    )
    summary = spark.createDataFrame(
        statewide_summary_rows(raw_json_path, snapshot_id), summary_schema
    )
    csv_totals = results.groupBy("office_id", "candidate_id").agg(
        F.sum("votes").cast(LongType()).alias("csv_precinct_detail_sum"),
        F.count(F.lit(1)).cast(LongType()).alias("csv_rows"),
        F.sum(F.when(F.col("votes").isNull(), 1).otherwise(0))
        .cast(LongType())
        .alias("csv_missing_vote_rows"),
    )
    joined = summary.join(csv_totals, ["office_id", "candidate_id"], "left")
    summary_status = (
        F.when(F.col("json_locality_rows") == 0, F.lit("NO_JSON_LOCALITY_SUMMARY"))
        .when(
            F.col("json_locality_summary_sum") != F.col("statewide_summary_votes"),
            F.lit("MISMATCH"),
        )
        .otherwise(F.lit("MATCH"))
    )
    precinct_status = (
        F.when(F.col("json_precinct_rows") == 0, F.lit("NO_JSON_PRECINCT_DETAIL"))
        .when(F.col("csv_rows").isNull(), F.lit("NO_CSV_MATCH"))
        .when(F.col("csv_missing_vote_rows") > 0, F.lit("INCOMPLETE_CSV_VALUES"))
        .when(
            F.col("json_precinct_missing_vote_rows") > 0,
            F.lit("INCOMPLETE_JSON_PRECINCT_VALUES"),
        )
        .when(
            F.col("csv_precinct_detail_sum") != F.col("json_precinct_detail_sum"),
            F.lit("MISMATCH"),
        )
        .otherwise(F.lit("MATCH"))
    )
    return joined.select(
        "snapshot_id",
        "office_id",
        "office_title",
        "candidate_id",
        "candidate_name",
        "statewide_summary_votes",
        "json_locality_summary_sum",
        (F.col("json_locality_summary_sum") - F.col("statewide_summary_votes"))
        .cast(LongType())
        .alias("locality_minus_statewide_summary"),
        "json_precinct_detail_sum",
        (F.col("statewide_summary_votes") - F.col("json_precinct_detail_sum"))
        .cast(LongType())
        .alias("statewide_votes_outside_precinct_detail"),
        "json_precinct_rows",
        "json_precinct_missing_vote_rows",
        "csv_precinct_detail_sum",
        "csv_rows",
        "csv_missing_vote_rows",
        (F.col("csv_precinct_detail_sum") - F.col("json_precinct_detail_sum"))
        .cast(LongType())
        .alias("csv_minus_json_precinct_detail"),
        summary_status.alias("summary_reconciliation_status"),
        precinct_status.alias("precinct_reconciliation_status"),
    )


def report_reconciliation(frame: DataFrame, status_field: str) -> dict[str, int]:
    return {
        row[status_field]: int(row["count"])
        for row in frame.groupBy(status_field).count().collect()
    }


def write_parquet(frame: DataFrame, path: Path) -> None:
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(
        str(path)
    )


def main() -> None:
    args = parse_args()
    bronze_dir = args.bronze_dir.expanduser().resolve()
    raw_dir = args.raw_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    results_input = bronze_dir / "election_results_csv"
    turnout_input = bronze_dir / "election_turnout_csv"
    raw_json = raw_dir / "election_results.json"

    missing = [p for p in (results_input, turnout_input, raw_json) if not p.exists()]
    if missing:
        raise FileNotFoundError("Required M2 input(s) are missing:\n" + "\n".join(map(str, missing)))
    if output_dir.exists():
        raise FileExistsError(
            f"Output snapshot already exists: {output_dir}. Use a new path to preserve it."
        )

    snapshot_id = bronze_dir.name
    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    spark = (
        SparkSession.builder.appName("VirginiaElectionCleanValidate")
        .master(args.master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        output_dir.mkdir(parents=True)
        print(f"Reading Bronze results: {results_input}")
        results_work = make_results(spark, results_input)
        print(f"Reading Bronze turnout: {turnout_input}")
        turnout_work = make_turnout(spark, turnout_input)

        # Findings are calculated before the internal validation flags are
        # removed from the delivered curated tables.
        result_rows = results_work.count()
        result_missing = count_missing_by_field(results_work, RESULT_REQUIRED_FIELDS)
        result_invalid_numeric = summed_flags(
            results_work, tuple(f"_invalid_{name}" for name in RESULT_INTEGER_FIELDS)
        )
        result_invalid_numeric = {
            name.removeprefix("_invalid_"): count
            for name, count in result_invalid_numeric.items()
        }
        result_bad_dates = summed_flags(results_work, ("_invalid_election_date",))[
            "_invalid_election_date"
        ]
        result_negative_votes = results_work.filter(F.col("votes") < 0).count()
        result_missing_votes = results_work.filter(F.col("votes").isNull()).count()
        result_duplicates = duplicate_key_metrics(results_work, RESULT_KEY)

        turnout_rows = turnout_work.count()
        turnout_missing = count_missing_by_field(turnout_work, TURNOUT_REQUIRED_FIELDS)
        turnout_invalid_numeric = summed_flags(
            turnout_work, tuple(f"_invalid_{name}" for name in TURNOUT_INTEGER_FIELDS)
        )
        turnout_invalid_numeric = {
            name.removeprefix("_invalid_"): count
            for name, count in turnout_invalid_numeric.items()
        }
        turnout_bad_dates = summed_flags(turnout_work, ("_invalid_election_date",))[
            "_invalid_election_date"
        ]
        negative_count = F.lit(0)
        for field in TURNOUT_INTEGER_FIELDS:
            negative_count = negative_count + F.when(F.col(field) < 0, 1).otherwise(0)
        turnout_negative_values = turnout_work.filter(negative_count > 0).count()
        turnout_missing_counts = {
            field: turnout_work.filter(F.col(field).isNull()).count()
            for field in TURNOUT_INTEGER_FIELDS
        }
        turnout_duplicates = duplicate_key_metrics(turnout_work, TURNOUT_KEY)
        complete_methods = F.lit(True)
        method_sum = F.lit(0).cast(LongType())
        for field in TURNOUT_METHOD_FIELDS:
            complete_methods = complete_methods & F.col(field).isNotNull()
            method_sum = method_sum + F.col(field)
        turnout_method_mismatch = turnout_work.filter(
            complete_methods & (method_sum != F.col("total_vote_turnout"))
        ).count()
        turnout_method_comparable = turnout_work.filter(
            complete_methods & F.col("total_vote_turnout").isNotNull()
        ).count()
        turnout_registration_mismatch = turnout_work.filter(
            F.col("active_registered_voters").isNotNull()
            & F.col("inactive_registered_voters").isNotNull()
            & F.col("total_registered_voters").isNotNull()
            & (
                F.col("active_registered_voters")
                + F.col("inactive_registered_voters")
                != F.col("total_registered_voters")
            )
        ).count()

        reconciliation = make_reconciliation(
            spark, results_work, raw_json, snapshot_id
        )
        summary_reconciliation_counts = report_reconciliation(
            reconciliation, "summary_reconciliation_status"
        )
        precinct_reconciliation_counts = report_reconciliation(
            reconciliation, "precinct_reconciliation_status"
        )
        reconciliation_rows = reconciliation.count()

        result_output = results_work.drop(
            *[f"_invalid_{name}" for name in RESULT_INTEGER_FIELDS],
            "_invalid_election_date",
        )
        turnout_output = turnout_work.drop(
            *[f"_invalid_{name}" for name in TURNOUT_INTEGER_FIELDS],
            "_invalid_election_date",
        )
        write_parquet(result_output, output_dir / "results")
        write_parquet(turnout_output, output_dir / "turnout")
        write_parquet(reconciliation, output_dir / "statewide_reconciliation")

        hard_failures = (
            sum(result_missing.values())
            + sum(result_invalid_numeric.values())
            + result_bad_dates
            + result_negative_votes
            + result_duplicates["duplicate_excess_rows"]
            + sum(turnout_missing.values())
            + sum(turnout_invalid_numeric.values())
            + turnout_bad_dates
            + turnout_negative_values
            + turnout_duplicates["duplicate_excess_rows"]
        )
        warning_findings = (
            result_missing_votes
            + turnout_method_mismatch
            + turnout_registration_mismatch
            + summary_reconciliation_counts.get("MISMATCH", 0)
            + summary_reconciliation_counts.get("NO_JSON_LOCALITY_SUMMARY", 0)
            + precinct_reconciliation_counts.get("MISMATCH", 0)
            + precinct_reconciliation_counts.get("NO_CSV_MATCH", 0)
            + precinct_reconciliation_counts.get("NO_JSON_PRECINCT_DETAIL", 0)
            + precinct_reconciliation_counts.get("INCOMPLETE_CSV_VALUES", 0)
            + precinct_reconciliation_counts.get("INCOMPLETE_JSON_PRECINCT_VALUES", 0)
        )
        overall_status = (
            "FAIL" if hard_failures else "WARN" if warning_findings else "PASS"
        )
        report = {
            "milestone": "M3: clean and validate",
            "overall_status": overall_status,
            "snapshot_id": snapshot_id,
            "source_status": "UNOFFICIAL RESULTS",
            "raw_json_source": report_path(raw_json),
            "source_json_created_at": json.loads(raw_json.read_text(encoding="utf-8")).get(
                "createdAt"
            ),
            "run_at_utc": run_at,
            "inputs": {
                "bronze_results": report_path(results_input),
                "bronze_turnout": report_path(turnout_input),
            },
            "outputs": {
                "results": "results/",
                "turnout": "turnout/",
                "statewide_reconciliation": "statewide_reconciliation/",
            },
            "checks": {
                "results": {
                    "rows": result_rows,
                    "required_field_nulls": result_missing,
                    "invalid_integer_values": result_invalid_numeric,
                    "invalid_election_dates": result_bad_dates,
                    "negative_vote_values": result_negative_votes,
                    "rows_with_missing_votes": result_missing_votes,
                    "key_uniqueness": result_duplicates,
                },
                "turnout": {
                    "rows": turnout_rows,
                    "required_field_nulls": turnout_missing,
                    "invalid_integer_values": turnout_invalid_numeric,
                    "invalid_election_dates": turnout_bad_dates,
                    "rows_with_negative_count_values": turnout_negative_values,
                    "missing_count_values_by_field": turnout_missing_counts,
                    "key_uniqueness": turnout_duplicates,
                    "registered_voter_total_mismatches": turnout_registration_mismatch,
                    "complete_vote_method_rows_compared": turnout_method_comparable,
                    "complete_vote_method_total_mismatches": turnout_method_mismatch,
                },
                "statewide_reconciliation": {
                    "summary_choice_rows": reconciliation_rows,
                    "statewide_vs_locality_summary_status_counts": summary_reconciliation_counts,
                    "csv_vs_json_precinct_detail_status_counts": precinct_reconciliation_counts,
                    "rule": (
                        "Compare statewide JSON totals with summed JSON locality totals, and "
                        "compare CSV precinct totals with JSON precinct-detail totals. The "
                        "statewide-to-precinct difference is retained as a coverage measure."
                    ),
                },
            },
            "interpretation": (
                "The full statewide/locality totals include votes that are not present in "
                "the precinct-detail breakdown. CSV precinct rows are checked against the "
                "JSON precinct details at the same level; blank CSV vote values remain null. "
                "No source totals are altered."
            ),
        }
        report_path = output_dir / "validation_report.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Results rows: {result_rows:,}; turnout rows: {turnout_rows:,}")
        print(f"State summary choices reconciled: {reconciliation_rows:,}")
        print(
            "Statewide/locality statuses: "
            f"{json.dumps(summary_reconciliation_counts, sort_keys=True)}"
        )
        print(
            "CSV/JSON precinct-detail statuses: "
            f"{json.dumps(precinct_reconciliation_counts, sort_keys=True)}"
        )
        print(f"Validation status: {overall_status}")
        print(f"Wrote curated snapshot: {output_dir}")
        print(f"Wrote quality report: {report_path}")
    except Exception:
        # The output path was confirmed absent above; a partial directory is
        # therefore owned by this failed run and can be safely removed.
        if output_dir.exists():
            shutil.rmtree(output_dir)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
