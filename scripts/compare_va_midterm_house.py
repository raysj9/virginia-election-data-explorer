#!/usr/bin/env python3
"""Compare Virginia's 2022 and 2026 midterm U.S. House results by district.

The official 2022 source has statewide district summaries. The 2026 source is
the saved Department of Elections JSON snapshot. The job groups candidates by
party and compares two-party vote share within each of Virginia's 11
congressional districts. It retains source status and candidate-level context
in the party totals; it does not calculate turnout or imply individual vote
switching.

Run from the project root:
    .venv/bin/python scripts/compare_va_midterm_house.py
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
HISTORICAL_SNAPSHOT_ID = "snapshot_2026-11-05"
CURRENT_SNAPSHOT_ID = "snapshot_2026-11-04"
HISTORICAL_URL = (
    "https://results.elections.virginia.gov/vaelections/2022%20November%20General/"
    "Json/Congress.json"
)
CURRENT_URL = (
    "https://enr.elections.virginia.gov/results/public/virginia/elections/"
    "2026-November-General"
)
DEFAULT_HISTORICAL_RESULTS = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "virginia"
    / "2022-11-08"
    / HISTORICAL_SNAPSHOT_ID
    / "congress.json"
)
DEFAULT_CURRENT_RESULTS = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "virginia"
    / "2026-11-03"
    / CURRENT_SNAPSHOT_ID
    / "election_results.json"
)
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "analytics"
    / "virginia"
    / "2022-11-08_vs_2026-11-03"
    / "snapshot_2026-11-05"
)


def report_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return f"[external]/{resolved.name}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Virginia 2022 and 2026 midterm House results by district."
    )
    parser.add_argument("--historical-results", type=Path, default=DEFAULT_HISTORICAL_RESULTS)
    parser.add_argument("--current-results", type=Path, default=DEFAULT_CURRENT_RESULTS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args()


def read_2022_candidates(spark: SparkSession, path: Path) -> DataFrame:
    """Flatten the official historical JSON to one row per candidate/choice."""
    document = spark.read.option("multiLine", "true").json(str(path))
    races = document.select(
        F.lit(HISTORICAL_SNAPSHOT_ID).alias("_snapshot_id"),
        F.lit("OFFICIAL RESULTS").alias("source_status"),
        F.lit(HISTORICAL_URL).alias("source_url"),
        F.col("ElectionName").alias("election_name"),
        F.to_date(F.col("ElectionDate")).alias("election_date"),
        F.explode_outer("Races").alias("race"),
    )
    choices = races.select(
        "_snapshot_id",
        "source_status",
        "source_url",
        "election_name",
        "election_date",
        F.col("race.RaceName").alias("contest_name"),
        F.explode_outer("race.Candidates").alias("candidate"),
    ).filter(F.col("contest_name").contains("House of Representatives"))

    district_number = F.regexp_extract(
        F.col("contest_name"), r"\(([0-9]{2})\)$", 1
    )
    return choices.select(
        "_snapshot_id",
        "source_status",
        "source_url",
        F.lit(2022).alias("election_year"),
        "election_date",
        "election_name",
        F.lpad(district_number, 2, "0").alias("district_id"),
        F.trim(F.col("candidate.BallotName")).alias("candidate_name"),
        F.trim(F.col("candidate.PoliticalParty")).alias("party"),
        F.col("candidate.Votes").cast("long").alias("votes"),
    )


def read_2026_candidates(spark: SparkSession, path: Path) -> DataFrame:
    """Flatten statewide results from the saved 2026 Department JSON export."""
    document = spark.read.option("multiLine", "true").json(str(path))
    contests = document.select(
        F.lit(CURRENT_SNAPSHOT_ID).alias("_snapshot_id"),
        F.lit("UNOFFICIAL RESULTS").alias("source_status"),
        F.lit(CURRENT_URL).alias("source_url"),
        F.col("electionName").alias("election_name"),
        F.to_date(F.col("electionDate")).alias("election_date"),
        F.explode_outer("results.ballotItems").alias("contest"),
    )
    choices = contests.select(
        "_snapshot_id",
        "source_status",
        "source_url",
        "election_name",
        "election_date",
        F.col("contest.name").alias("contest_name"),
        F.explode_outer("contest.ballotOptions").alias("candidate"),
    ).filter(F.col("contest_name").contains("House of Representatives"))

    district_number = F.regexp_extract(
        F.col("contest_name"), r"\(([0-9]{1,2})(?:st|nd|rd|th) District\)$", 1
    )
    candidate_name = F.trim(F.col("candidate.name"))
    party = F.trim(F.col("candidate.politicalParty"))
    normalized_party = F.when(
        party.isNull() | (party == ""),
        F.when(F.lower(candidate_name).contains("write"), F.lit("Write-In"))
        .otherwise(F.lit("UNKNOWN")),
    ).otherwise(party)
    return choices.select(
        "_snapshot_id",
        "source_status",
        "source_url",
        F.lit(2026).alias("election_year"),
        "election_date",
        "election_name",
        F.lpad(district_number, 2, "0").alias("district_id"),
        candidate_name.alias("candidate_name"),
        normalized_party.alias("party"),
        F.col("candidate.voteCount").cast("long").alias("votes"),
    )


def make_party_totals(candidate_rows: DataFrame) -> DataFrame:
    """Aggregate candidate votes by party without dropping smaller parties."""
    return candidate_rows.groupBy(
        "_snapshot_id",
        "source_status",
        "source_url",
        "election_year",
        "election_date",
        "election_name",
        "district_id",
        "party",
    ).agg(
        F.countDistinct("candidate_name").cast("long").alias("candidate_count"),
        F.sort_array(F.collect_set("candidate_name")).alias("candidate_names"),
        F.count("votes").cast("long").alias("candidate_rows_with_votes"),
        F.sum("votes").cast("long").alias("party_votes"),
    )


def make_district_metrics(party_totals: DataFrame) -> DataFrame:
    """Calculate two-party shares and the Democratic-Republican margin."""
    sums = party_totals.groupBy(
        "_snapshot_id",
        "source_status",
        "source_url",
        "election_year",
        "election_date",
        "election_name",
        "district_id",
    ).agg(
        F.coalesce(
            F.sum(F.when(F.col("party") == "Democratic", F.col("party_votes"))),
            F.lit(0),
        )
        .cast("long")
        .alias("democratic_votes"),
        F.coalesce(
            F.sum(F.when(F.col("party") == "Republican", F.col("party_votes"))),
            F.lit(0),
        )
        .cast("long")
        .alias("republican_votes"),
        F.coalesce(
            F.sum(
                F.when(
                    ~F.col("party").isin("Democratic", "Republican"),
                    F.col("party_votes"),
                )
            ),
            F.lit(0),
        )
        .cast("long")
        .alias("other_party_and_write_in_votes"),
    )
    two_party_votes = F.col("democratic_votes") + F.col("republican_votes")
    return (
        sums.withColumn("two_party_votes", two_party_votes.cast("long"))
        .withColumn(
            "democratic_two_party_share",
            F.when(
                two_party_votes > 0,
                F.col("democratic_votes").cast("double") / two_party_votes,
            ),
        )
        .withColumn(
            "republican_two_party_share",
            F.when(
                two_party_votes > 0,
                F.col("republican_votes").cast("double") / two_party_votes,
            ),
        )
        .withColumn(
            "democratic_minus_republican_margin_pp",
            F.when(
                two_party_votes > 0,
                (F.col("democratic_votes") - F.col("republican_votes"))
                .cast("double")
                / two_party_votes
                * F.lit(100.0),
            ),
        )
        .withColumn(
            "two_party_leader",
            F.when(F.col("democratic_votes") > F.col("republican_votes"), "Democratic")
            .when(F.col("republican_votes") > F.col("democratic_votes"), "Republican")
            .otherwise("TIE"),
        )
    )


def make_comparison(district_metrics: DataFrame) -> DataFrame:
    historical = district_metrics.filter(F.col("election_year") == 2022).alias("h")
    current = district_metrics.filter(F.col("election_year") == 2026).alias("c")
    joined = historical.join(current, "district_id", "full_outer")
    comparison = joined.select(
        F.col("district_id"),
        F.col("h.source_status").alias("status_2022"),
        F.col("c.source_status").alias("status_2026"),
        F.col("h.source_url").alias("source_url_2022"),
        F.col("c.source_url").alias("source_url_2026"),
        F.col("h.democratic_votes").alias("democratic_votes_2022"),
        F.col("c.democratic_votes").alias("democratic_votes_2026"),
        F.col("h.republican_votes").alias("republican_votes_2022"),
        F.col("c.republican_votes").alias("republican_votes_2026"),
        F.col("h.other_party_and_write_in_votes").alias("other_votes_2022"),
        F.col("c.other_party_and_write_in_votes").alias("other_votes_2026"),
        F.col("h.two_party_votes").alias("two_party_votes_2022"),
        F.col("c.two_party_votes").alias("two_party_votes_2026"),
        F.col("h.democratic_two_party_share").alias("democratic_share_2022"),
        F.col("c.democratic_two_party_share").alias("democratic_share_2026"),
        F.col("h.republican_two_party_share").alias("republican_share_2022"),
        F.col("c.republican_two_party_share").alias("republican_share_2026"),
        F.col("h.democratic_minus_republican_margin_pp").alias("margin_pp_2022"),
        F.col("c.democratic_minus_republican_margin_pp").alias("margin_pp_2026"),
        F.col("h.two_party_leader").alias("leader_2022"),
        F.col("c.two_party_leader").alias("leader_2026"),
    )
    return (
        comparison.withColumn(
            "democratic_share_change_pp",
            (F.col("democratic_share_2026") - F.col("democratic_share_2022"))
            * F.lit(100.0),
        )
        .withColumn(
            "margin_change_pp",
            F.col("margin_pp_2026") - F.col("margin_pp_2022"),
        )
        .orderBy("district_id")
    )


def row_to_dict(row: Any) -> dict[str, Any]:
    values = row.asDict(recursive=True)
    return {
        key: (float(value) if isinstance(value, float) else value)
        for key, value in values.items()
    }


def build_report(
    candidates_2022: DataFrame,
    candidates_2026: DataFrame,
    party_totals: DataFrame,
    metrics: DataFrame,
    comparison: DataFrame,
    historical_path: Path,
    current_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    candidate_counts = {
        2022: candidates_2022.count(),
        2026: candidates_2026.count(),
    }
    districts = {
        2022: sorted(row["district_id"] for row in metrics.filter("election_year = 2022").select("district_id").distinct().collect()),
        2026: sorted(row["district_id"] for row in metrics.filter("election_year = 2026").select("district_id").distinct().collect()),
    }
    missing_major_party = {
        year: int(
            metrics.filter(
                (F.col("election_year") == year)
                & ((F.col("democratic_votes") == 0) | (F.col("republican_votes") == 0))
            ).count()
        )
        for year in (2022, 2026)
    }
    comparison_rows = comparison.count()
    unmatched_2022 = sorted(set(districts[2022]) - set(districts[2026]))
    unmatched_2026 = sorted(set(districts[2026]) - set(districts[2022]))
    data_status = (
        "PASS"
        if comparison_rows == 11
        and len(districts[2022]) == 11
        and len(districts[2026]) == 11
        and not unmatched_2022
        and not unmatched_2026
        and missing_major_party == {2022: 0, 2026: 0}
        else "WARN"
    )
    largest_shifts = comparison.orderBy(
        F.abs(F.col("democratic_share_change_pp")).desc()
    ).select(
        "district_id",
        "democratic_share_2022",
        "democratic_share_2026",
        "democratic_share_change_pp",
        "leader_2022",
        "leader_2026",
    ).limit(3).collect()

    party_sums = metrics.groupBy("election_year").agg(
        F.sum("democratic_votes").cast("long").alias("democratic_votes"),
        F.sum("republican_votes").cast("long").alias("republican_votes"),
        F.sum("other_party_and_write_in_votes")
        .cast("long")
        .alias("other_party_and_write_in_votes"),
    )
    statewide_party_totals = {
        str(row["election_year"]): {
            "democratic_votes": int(row["democratic_votes"] or 0),
            "republican_votes": int(row["republican_votes"] or 0),
            "other_party_and_write_in_votes": int(
                row["other_party_and_write_in_votes"] or 0
            ),
        }
        for row in party_sums.collect()
    }
    duplicate_party_groups = (
        party_totals.groupBy("election_year", "district_id", "party")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    negative_vote_rows = (
        candidates_2022.unionByName(candidates_2026)
        .filter(F.col("votes") < 0)
        .count()
    )

    return {
        "milestone": "M5: midterm House comparison",
        "overall_status": data_status,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sources": {
            "2022": {
                "election_date": "2022-11-08",
                "snapshot_id": HISTORICAL_SNAPSHOT_ID,
                "source_status": "OFFICIAL RESULTS",
                "source_url": HISTORICAL_URL,
                "local_file": report_path(historical_path),
            },
            "2026": {
                "election_date": "2026-11-03",
                "snapshot_id": CURRENT_SNAPSHOT_ID,
                "source_status": "UNOFFICIAL RESULTS",
                "source_url": CURRENT_URL,
                "local_file": report_path(current_path),
            },
        },
        "comparison_grain": "election × congressional district × party",
        "candidate_detail_grain": "election × congressional district × candidate/choice",
        "method": {
            "party_totals": "Sum each candidate's votes by reported party within each congressional district; retain non-Democratic and non-Republican choices in the output.",
            "two_party_share": "party votes / (Democratic votes + Republican votes); calculated separately for each year and district.",
            "margin": "(Democratic votes - Republican votes) / two-party votes × 100, in percentage points.",
            "change": "2026 Democratic two-party share minus 2022 Democratic two-party share, in percentage points.",
        },
        "checks_and_findings": {
            "candidate_choice_rows": {str(k): int(v) for k, v in candidate_counts.items()},
            "district_ids": {str(k): v for k, v in districts.items()},
            "matched_districts": int(comparison_rows),
            "unmatched_2022_districts": unmatched_2022,
            "unmatched_2026_districts": unmatched_2026,
            "districts_missing_democratic_or_republican_votes": {
                str(k): v for k, v in missing_major_party.items()
            },
            "duplicate_district_year_party_groups": int(duplicate_party_groups),
            "negative_candidate_vote_rows": int(negative_vote_rows),
            "statewide_house_votes_summed_across_districts": statewide_party_totals,
            "largest_absolute_democratic_share_changes": [
                row_to_dict(row) for row in largest_shifts
            ],
        },
        "comparability": {
            "cycle": "Both are November midterm general elections.",
            "office": "Both compare U.S. House general-election results.",
            "geography": "The Department of Elections stated that the congressional district boundaries in force before the voided April 2026 referendum remained in place for the 2026 primary and general elections; these are the same district numbers used in 2022.",
            "snapshot_status_difference": "2022 is official; the 2026 input is the project's saved unofficial-results snapshot.",
            "limits": [
                "Different candidates ran in most districts; this measures party vote-share change, not individual voter movement.",
                "The 2022 source is a district-level statewide summary and does not provide locality or precinct rows in this JSON export.",
                "Candidate vote totals are not a turnout measure; this job does not calculate turnout.",
                "The 2026 results may have changed after the saved snapshot and are not presented as certified results.",
            ],
        },
        "outputs": {
            "candidate_results": report_path(output_dir / "candidate_results"),
            "district_party_totals": report_path(output_dir / "district_party_totals"),
            "district_metrics": report_path(output_dir / "district_metrics"),
            "district_comparison": report_path(output_dir / "district_comparison"),
            "report": report_path(output_dir / "analytics_report.json"),
        },
    }


def main() -> None:
    args = parse_args()
    historical_path = args.historical_results.expanduser().resolve()
    current_path = args.current_results.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    for source_path in (historical_path, current_path):
        if not source_path.is_file():
            raise FileNotFoundError(f"Source file does not exist: {source_path}")
    if output_dir.exists():
        raise FileExistsError(
            f"Output already exists: {output_dir}. Choose a new snapshot directory "
            "to preserve existing analytics."
        )

    spark = (
        SparkSession.builder.appName("VirginiaMidtermHouseComparison")
        .master(args.master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        candidates_2022 = read_2022_candidates(spark, historical_path)
        candidates_2026 = read_2026_candidates(spark, current_path)
        candidate_rows = candidates_2022.unionByName(candidates_2026)
        candidate_results = candidate_rows.select(
            F.col("_snapshot_id").alias("snapshot_id"),
            "source_status",
            "source_url",
            "election_year",
            "election_date",
            "election_name",
            "district_id",
            "candidate_name",
            "party",
            "votes",
        )
        party_totals = make_party_totals(candidate_rows)
        district_metrics = make_district_metrics(party_totals)
        comparison = make_comparison(district_metrics)

        output_dir.mkdir(parents=True)
        try:
            (
                candidate_results.coalesce(1)
                .write.mode("errorifexists")
                .option("compression", "snappy")
                .parquet(str(output_dir / "candidate_results"))
            )
            (
                party_totals.coalesce(1)
                .write.mode("errorifexists")
                .option("compression", "snappy")
                .parquet(str(output_dir / "district_party_totals"))
            )
            (
                district_metrics.coalesce(1)
                .write.mode("errorifexists")
                .option("compression", "snappy")
                .parquet(str(output_dir / "district_metrics"))
            )
            (
                comparison.coalesce(1)
                .write.mode("errorifexists")
                .option("compression", "snappy")
                .parquet(str(output_dir / "district_comparison"))
            )
            report = build_report(
                candidates_2022,
                candidates_2026,
                party_totals,
                district_metrics,
                comparison,
                historical_path,
                current_path,
                output_dir,
            )
            (output_dir / "analytics_report.json").write_text(
                json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            print(json.dumps(report, indent=2, ensure_ascii=False))
        except Exception:
            shutil.rmtree(output_dir)
            raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
