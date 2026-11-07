#!/usr/bin/env python3
"""Build locality-level election analytics for one Virginia snapshot.

Milestone 4 keeps full locality results separate from precinct detail, creates
choice totals, and calculates turnout only where its numerator and denominator
are available. It preserves nulls and source metadata.

Run from the project root:
    .venv/bin/python scripts/analyze_va_election_snapshot.py
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


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = "snapshot_2026-11-04"
DEFAULT_BRONZE_DIR = (
    PROJECT_ROOT / "data" / "bronze" / "virginia" / "2026-11-03" / SNAPSHOT
)
DEFAULT_CURATED_DIR = (
    PROJECT_ROOT / "data" / "curated" / "virginia" / "2026-11-03" / SNAPSHOT
)
DEFAULT_ANALYTICS_DIR = (
    PROJECT_ROOT / "data" / "analytics" / "virginia" / "2026-11-03" / SNAPSHOT
)


def report_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return f"[external]/{resolved.name}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build locality-level and turnout analytics for a Virginia snapshot."
    )
    parser.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    parser.add_argument("--curated-dir", type=Path, default=DEFAULT_CURATED_DIR)
    parser.add_argument("--analytics-dir", type=Path, default=DEFAULT_ANALYTICS_DIR)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args()


def clean_text(column: F.Column) -> F.Column:
    value = F.trim(column)
    return F.when(value.isNull() | (value == ""), F.lit(None)).otherwise(value)


def make_locality_results(spark: SparkSession, bronze_json: Path) -> DataFrame:
    """Explode the nested JSON export while retaining full locality totals."""
    root = spark.read.parquet(str(bronze_json))
    localities = root.select(
        "_snapshot_id",
        "_source_file",
        "_ingested_at_utc",
        F.col("createdAt").alias("source_created_at"),
        F.to_date(F.col("electionDate"), "yyyy-MM-dd").alias("election_date"),
        F.col("electionName").alias("election_name"),
        F.explode("localResults").alias("locality"),
    )
    contests = localities.select(
        "_snapshot_id",
        "_source_file",
        "_ingested_at_utc",
        "source_created_at",
        "election_date",
        "election_name",
        F.col("locality.id").alias("locality_code"),
        F.col("locality.name").alias("locality_name"),
        F.explode("locality.ballotItems").alias("contest"),
    )
    choices = contests.select(
        "_snapshot_id",
        "_source_file",
        "_ingested_at_utc",
        "source_created_at",
        "election_date",
        "election_name",
        "locality_code",
        "locality_name",
        F.col("contest.id").alias("office_id"),
        F.col("contest.name").alias("office_title"),
        F.col("contest.contestType").alias("contest_type"),
        F.explode("contest.ballotOptions").alias("choice"),
    )
    valid_groups = F.filter(
        F.col("choice.groupResults"),
        lambda group: group["groupName"].isNotNull()
        & group["voteCount"].isNotNull(),
    )
    method_totals = F.map_from_entries(
        F.transform(
            valid_groups,
            lambda group: F.struct(group["groupName"], group["voteCount"]),
        )
    )
    return choices.select(
        "_snapshot_id",
        F.lit("UNOFFICIAL RESULTS").alias("source_status"),
        "_source_file",
        "_ingested_at_utc",
        "source_created_at",
        "election_date",
        "election_name",
        clean_text(F.col("locality_code")).alias("locality_code"),
        clean_text(F.col("locality_name")).alias("locality_name"),
        clean_text(F.col("office_id")).alias("office_id"),
        clean_text(F.col("office_title")).alias("office_title"),
        clean_text(F.col("contest_type")).alias("contest_type"),
        clean_text(F.col("choice.id")).alias("choice_id"),
        clean_text(F.col("choice.name")).alias("choice_name"),
        clean_text(F.col("choice.politicalParty")).alias("party"),
        F.col("choice.voteCount").cast("long").alias("votes"),
        method_totals.alias("vote_method_totals"),
    )


def status_counts(frame: DataFrame, field: str) -> dict[str, int]:
    return {
        row[field]: int(row["count"])
        for row in frame.groupBy(field).count().collect()
    }


def build_choice_totals(locality_results: DataFrame, curated_dir: Path) -> DataFrame:
    groups = (
        "_snapshot_id",
        "source_status",
        "election_date",
        "election_name",
        "office_id",
        "office_title",
        "contest_type",
        "choice_id",
        "choice_name",
        "party",
    )
    totals = locality_results.groupBy(*groups).agg(
        F.count(F.lit(1)).cast("long").alias("locality_result_rows"),
        F.countDistinct("locality_code").cast("long").alias("localities"),
        F.count("votes").cast("long").alias("locality_rows_with_votes"),
        F.sum(F.when(F.col("votes").isNull(), 1).otherwise(0))
        .cast("long")
        .alias("locality_rows_missing_votes"),
        F.sum("votes").cast("long").alias("summed_locality_votes"),
    )
    state_totals = (
        locality_results.sparkSession.read.parquet(
            str(curated_dir / "statewide_reconciliation")
        )
        .select(
            "office_id",
            F.col("candidate_id").alias("choice_id"),
            "statewide_summary_votes",
        )
    )
    joined = totals.join(state_totals, ["office_id", "choice_id"], "left")
    difference = F.col("summed_locality_votes") - F.col("statewide_summary_votes")
    status = (
        F.when(F.col("statewide_summary_votes").isNull(), "NO_STATEWIDE_SUMMARY")
        .when(F.col("locality_rows_missing_votes") > 0, "INCOMPLETE_LOCALITY_VALUES")
        .when(difference == 0, "MATCH")
        .otherwise("MISMATCH")
    )
    return joined.withColumn(
        "locality_minus_statewide_summary", difference.cast("long")
    ).withColumn("summary_status", status)


def build_locality_precinct_coverage(
    spark: SparkSession, locality_results: DataFrame, curated_results: Path
) -> DataFrame:
    precinct_rows = spark.read.parquet(str(curated_results))
    precinct_sums = (
        precinct_rows.groupBy("locality_code", "office_id", "candidate_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias("precinct_rows"),
            F.count("votes").cast("long").alias("precinct_rows_with_votes"),
            F.sum(F.when(F.col("votes").isNull(), 1).otherwise(0))
            .cast("long")
            .alias("precinct_rows_missing_votes"),
            F.sum("votes").cast("long").alias("precinct_detail_votes"),
        )
        .withColumnRenamed("candidate_id", "choice_id")
    )
    joined = locality_results.join(
        precinct_sums, ["locality_code", "office_id", "choice_id"], "left"
    )
    coverage_status = (
        F.when(F.col("votes").isNull(), "LOCALITY_TOTAL_MISSING")
        .when(F.col("precinct_rows").isNull(), "NO_CSV_PRECINCT_ROWS")
        .when(F.col("precinct_rows_missing_votes") > 0, "INCOMPLETE_PRECINCT_ROWS")
        .otherwise("COMPLETE_PRECINCT_DETAIL")
    )
    complete = F.col("precinct_rows").isNotNull() & (
        F.col("precinct_rows_missing_votes") == 0
    )
    coverage_difference = F.when(
        complete, F.col("votes") - F.col("precinct_detail_votes")
    )
    coverage_share = F.when(
        complete & (F.col("votes") > 0),
        F.col("precinct_detail_votes").cast("double") / F.col("votes"),
    )
    return joined.select(
        "_snapshot_id",
        "source_status",
        "election_date",
        "election_name",
        "locality_code",
        "locality_name",
        "office_id",
        "office_title",
        "contest_type",
        "choice_id",
        "choice_name",
        "party",
        F.col("votes").alias("full_locality_votes"),
        "precinct_rows",
        "precinct_rows_with_votes",
        "precinct_rows_missing_votes",
        "precinct_detail_votes",
        coverage_difference.cast("long").alias("locality_votes_outside_precinct_detail"),
        coverage_share.alias("precinct_detail_share_of_locality_total"),
        coverage_status.alias("coverage_status"),
    )


def build_precinct_turnout_rates(turnout: DataFrame) -> DataFrame:
    denominator = F.col("total_registered_voters")
    numerator = F.col("total_vote_turnout")
    rate = F.when(
        numerator.isNotNull() & denominator.isNotNull() & (denominator > 0),
        numerator.cast("double") / denominator,
    )
    return (
        turnout.withColumn("turnout_rate", rate)
        .withColumn(
            "turnout_rate_formula",
            F.lit("total_vote_turnout / total_registered_voters"),
        )
        .withColumn(
            "turnout_rate_status",
            F.when(numerator.isNull(), "MISSING_TURNOUT_NUMERATOR")
            .when(denominator.isNull() | (denominator <= 0), "INVALID_DENOMINATOR")
            .otherwise("AVAILABLE"),
        )
    )


def build_locality_turnout_summary(precinct_turnout: DataFrame) -> DataFrame:
    summaries = precinct_turnout.groupBy("election", "election_date", "locality").agg(
        F.count(F.lit(1)).cast("long").alias("precinct_rows"),
        F.count("total_vote_turnout").cast("long").alias("precinct_rows_with_turnout"),
        F.count("total_registered_voters")
        .cast("long")
        .alias("precinct_rows_with_registration"),
        F.sum("total_vote_turnout").cast("long").alias("_reported_turnout_sum"),
        F.sum("total_registered_voters").cast("long").alias(
            "_registered_voter_sum"
        ),
    )
    complete_turnout = F.col("precinct_rows_with_turnout") == F.col("precinct_rows")
    complete_registration = (
        F.col("precinct_rows_with_registration") == F.col("precinct_rows")
    )
    return (
        summaries.withColumn(
            "locality_total_vote_turnout",
            F.when(complete_turnout, F.col("_reported_turnout_sum")).otherwise(
                F.lit(None).cast("long")
            ),
        )
        .withColumn(
            "locality_total_registered_voters",
            F.when(complete_registration, F.col("_registered_voter_sum")).otherwise(
                F.lit(None).cast("long")
            ),
        )
        .withColumn(
            "locality_turnout_rate",
            F.when(
                complete_turnout
                & complete_registration
                & (F.col("_registered_voter_sum") > 0),
                F.col("_reported_turnout_sum").cast("double")
                / F.col("_registered_voter_sum"),
            ),
        )
        .withColumn(
            "turnout_coverage_rate",
            F.col("precinct_rows_with_turnout").cast("double")
            / F.col("precinct_rows"),
        )
        .withColumn(
            "locality_turnout_rate_formula",
            F.lit(
                "sum(total_vote_turnout) / sum(total_registered_voters), "
                "only when every precinct numerator and denominator is present"
            ),
        )
        .drop("_reported_turnout_sum", "_registered_voter_sum")
    )


def write_parquet(frame: DataFrame, path: Path) -> None:
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(
        str(path)
    )


def main() -> None:
    args = parse_args()
    bronze_dir = args.bronze_dir.expanduser().resolve()
    curated_dir = args.curated_dir.expanduser().resolve()
    analytics_dir = args.analytics_dir.expanduser().resolve()
    json_input = bronze_dir / "election_results_json"
    curated_results = curated_dir / "results"
    curated_turnout = curated_dir / "turnout"
    statewide_reconciliation = curated_dir / "statewide_reconciliation"
    locality_output = curated_dir / "locality_results"
    required_inputs = (
        json_input,
        curated_results,
        curated_turnout,
        statewide_reconciliation,
    )
    missing = [path for path in required_inputs if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Required Bronze/curated input(s) are missing:\n"
            + "\n".join(str(path) for path in missing)
        )
    if locality_output.exists() or analytics_dir.exists():
        raise FileExistsError(
            "M4 output already exists. Choose new output paths to preserve it: "
            f"{locality_output} ; {analytics_dir}"
        )

    created_outputs: list[Path] = []
    spark: SparkSession | None = None
    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        # Spark creates each Parquet target itself; pre-create only the shared
        # analytics parent directory, not the locality_results target folder.
        for output in (analytics_dir,):
            output.mkdir(parents=True, exist_ok=False)
            created_outputs.append(output)

        spark = (
            SparkSession.builder.appName("VirginiaElectionExploratoryAnalytics")
            .master(args.master)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        print(f"Reading nested JSON Bronze: {json_input}")
        locality_results = make_locality_results(spark, json_input)
        print(f"Reading curated precinct results: {curated_results}")
        print(f"Reading curated turnout: {curated_turnout}")
        turnout_source = spark.read.parquet(str(curated_turnout))

        choice_totals = build_choice_totals(locality_results, curated_dir)
        coverage = build_locality_precinct_coverage(
            spark, locality_results, curated_results
        )
        precinct_turnout = build_precinct_turnout_rates(turnout_source)
        locality_turnout = build_locality_turnout_summary(precinct_turnout)

        locality_rows = locality_results.count()
        locality_count = locality_results.select("locality_code").distinct().count()
        duplicate_groups = (
            locality_results.groupBy(
                "_snapshot_id", "election_date", "locality_code", "office_id", "choice_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        choice_total_rows = choice_totals.count()
        choice_total_statuses = status_counts(choice_totals, "summary_status")
        coverage_rows = coverage.count()
        coverage_statuses = status_counts(coverage, "coverage_status")
        turnout_rows = precinct_turnout.count()
        precinct_rates_available = precinct_turnout.filter(
            F.col("turnout_rate").isNotNull()
        ).count()
        locality_turnout_rows = locality_turnout.count()
        locality_rates_available = locality_turnout.filter(
            F.col("locality_turnout_rate").isNotNull()
        ).count()
        locality_rates_incomplete = locality_turnout.filter(
            F.col("locality_turnout_rate").isNull()
        ).count()

        created_outputs.append(locality_output)
        write_parquet(locality_results, locality_output)
        write_parquet(choice_totals, analytics_dir / "contest_choice_totals")
        write_parquet(coverage, analytics_dir / "locality_precinct_coverage")
        write_parquet(precinct_turnout, analytics_dir / "precinct_turnout_rates")
        write_parquet(locality_turnout, analytics_dir / "locality_turnout_summary")

        hard_failures = duplicate_groups + choice_total_statuses.get("MISMATCH", 0)
        warnings = (
            choice_total_statuses.get("INCOMPLETE_LOCALITY_VALUES", 0)
            + coverage_statuses.get("INCOMPLETE_PRECINCT_ROWS", 0)
            + coverage_statuses.get("LOCALITY_TOTAL_MISSING", 0)
            + coverage_statuses.get("NO_CSV_PRECINCT_ROWS", 0)
            + locality_rates_incomplete
        )
        overall_status = "FAIL" if hard_failures else "WARN" if warnings else "READY"
        report: dict[str, Any] = {
            "milestone": "M4: exploratory analytics",
            "overall_status": overall_status,
            "snapshot_id": bronze_dir.name,
            "source_status": "UNOFFICIAL RESULTS",
            "run_at_utc": run_at,
            "inputs": {
                "nested_json_bronze": report_path(json_input),
                "precinct_results_curated": report_path(curated_results),
                "turnout_curated": report_path(curated_turnout),
                "statewide_reconciliation": report_path(statewide_reconciliation),
            },
            "outputs": {
                "locality_results": report_path(locality_output),
                "contest_choice_totals": "contest_choice_totals/",
                "locality_precinct_coverage": "locality_precinct_coverage/",
                "precinct_turnout_rates": "precinct_turnout_rates/",
                "locality_turnout_summary": "locality_turnout_summary/",
            },
            "checks_and_findings": {
                "locality_results_rows": locality_rows,
                "localities": locality_count,
                "duplicate_locality_result_key_groups": duplicate_groups,
                "contest_choice_total_rows": choice_total_rows,
                "contest_choice_summary_status_counts": choice_total_statuses,
                "locality_precinct_coverage_rows": coverage_rows,
                "locality_precinct_coverage_status_counts": coverage_statuses,
                "precinct_turnout_rows": turnout_rows,
                "precinct_turnout_rates_available": precinct_rates_available,
                "locality_turnout_summaries": locality_turnout_rows,
                "locality_turnout_rates_available": locality_rates_available,
                "locality_turnout_rates_unavailable_due_to_incomplete_inputs": locality_rates_incomplete,
            },
            "definitions": {
                "precinct_turnout_rate": "total_vote_turnout / total_registered_voters",
                "locality_turnout_rate": (
                    "sum(total_vote_turnout) / sum(total_registered_voters), only when "
                    "every precinct numerator and denominator is present"
                ),
                "locality_precinct_coverage": (
                    "Full JSON locality votes and CSV precinct-detail sums are shown at "
                    "their own grains; the difference is a coverage measure, not a forced reconciliation."
                ),
                "contest_choice_summary_status": (
                    "NO_STATEWIDE_SUMMARY is expected for choices without a statewide "
                    "counterpart in the source; their locality totals remain available."
                ),
                "missing_values": "Blank source counts remain null; they are not treated as zero.",
            },
            "interpretation": (
                "Use locality_results for full locality-level candidate or ballot-choice totals. "
                "Use the M3 results table for precinct detail. Turnout rates are available "
                "only where both the reported turnout numerator and registration denominator "
                "are present; incomplete locality totals are left null. A missing statewide "
                "summary for a local contest is expected and is not a failed reconciliation."
            ),
        }
        report_path = analytics_dir / "analytics_report.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Locality result rows: {locality_rows:,} across {locality_count:,} localities")
        print(f"Statewide choice aggregation statuses: {json.dumps(choice_total_statuses, sort_keys=True)}")
        print(f"Locality/precinct coverage statuses: {json.dumps(coverage_statuses, sort_keys=True)}")
        print(
            f"Precinct turnout rates available: {precinct_rates_available:,} of "
            f"{turnout_rows:,} rows"
        )
        print(
            f"Locality turnout rates available: {locality_rates_available:,} of "
            f"{locality_turnout_rows:,} localities"
        )
        print(f"M4 status: {overall_status}")
        print(f"Wrote locality table: {locality_output}")
        print(f"Wrote analytics: {analytics_dir}")
    except Exception:
        for output in reversed(created_outputs):
            if output.exists():
                shutil.rmtree(output)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
