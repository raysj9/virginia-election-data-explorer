"""Serve the M5 Virginia House comparison from its Parquet snapshot.

Run from the project root with:
    .venv/bin/uvicorn api.main:app --reload
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .assistant import AssistantAnswer, AssistantQuestion, answer_question


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = (
    PROJECT_ROOT
    / "data"
    / "analytics"
    / "virginia"
    / "2022-11-08_vs_2026-11-03"
    / "snapshot_2026-11-05_candidate_detail"
)
DATA_DIR = Path(
    os.environ.get("VA_HOUSE_COMPARISON_DATA_DIR", str(DEFAULT_DATA_DIR))
).expanduser().resolve()
COMPARISON_DIR = DATA_DIR / "district_comparison"
CANDIDATE_RESULTS_DIR = DATA_DIR / "candidate_results"
REPORT_PATH = DATA_DIR / "analytics_report.json"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"


class SourceMetadata(BaseModel):
    election_date: str
    snapshot_id: str
    source_status: str
    source_url: str


class CandidateVote(BaseModel):
    candidate_name: str | None
    party: str | None
    votes: int | None


class DistrictComparison(BaseModel):
    district_id: str
    status_2022: str | None
    status_2026: str | None
    source_url_2022: str | None
    source_url_2026: str | None
    democratic_votes_2022: int | None
    democratic_votes_2026: int | None
    republican_votes_2022: int | None
    republican_votes_2026: int | None
    other_votes_2022: int | None
    other_votes_2026: int | None
    two_party_votes_2022: int | None
    two_party_votes_2026: int | None
    democratic_share_2022: float | None
    democratic_share_2026: float | None
    republican_share_2022: float | None
    republican_share_2026: float | None
    margin_pp_2022: float | None
    margin_pp_2026: float | None
    leader_2022: str | None
    leader_2026: str | None
    democratic_share_change_pp: float | None
    margin_change_pp: float | None
    candidates_2022: list[CandidateVote]
    candidates_2026: list[CandidateVote]


class HouseComparisonResponse(BaseModel):
    overall_status: str
    comparison_grain: str
    sources: dict[str, SourceMetadata]
    comparability: dict[str, Any]
    results: list[DistrictComparison]


app = FastAPI(
    title="Virginia Election Results API",
    description=(
        "Read-only access to the saved 2022 and 2026 Virginia U.S. House "
        "district comparison, including candidate-level vote totals. Source "
        "status and comparison caveats accompany every response."
    ),
    version="1.0.0",
)
app.mount(
    "/dashboard-assets",
    StaticFiles(directory=str(DASHBOARD_DIR)),
    name="dashboard-assets",
)


def _load_report() -> dict[str, Any]:
    """Load the M5 report, which carries provenance and comparison caveats."""
    try:
        return json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.exception("Unable to read the comparison report at %s", REPORT_PATH)
        raise HTTPException(
            status_code=503,
            detail="The comparison report is unavailable; check the configured data snapshot.",
        ) from exc


def _normalize_district_id(raw_id: str) -> str:
    if not raw_id.isdecimal():
        raise HTTPException(
            status_code=422,
            detail="district_id must be a number from 1 through 11.",
        )
    district_number = int(raw_id)
    if not 1 <= district_number <= 11:
        raise HTTPException(
            status_code=422,
            detail="district_id must be a number from 1 through 11.",
        )
    return f"{district_number:02d}"


def _read_dataset_rows(
    dataset_dir: Path, district_id: str | None
) -> list[dict[str, Any]]:
    parquet_files = sorted(dataset_dir.glob("*.parquet"))
    if not parquet_files:
        raise HTTPException(
            status_code=503,
            detail=f"The {dataset_dir.name} Parquet dataset is unavailable; check the configured data snapshot.",
        )

    sql = "SELECT * FROM read_parquet(?)"
    parameters: list[Any] = [str(dataset_dir / "*.parquet")]
    if district_id is not None:
        sql += " WHERE district_id = ?"
        parameters.append(district_id)
    if dataset_dir == CANDIDATE_RESULTS_DIR:
        sql += " ORDER BY district_id, election_year, party, candidate_name"
    else:
        sql += " ORDER BY district_id"

    connection = duckdb.connect(database=":memory:")
    try:
        cursor = connection.execute(sql, parameters)
        column_names = [column[0] for column in cursor.description]
        return [dict(zip(column_names, row, strict=True)) for row in cursor.fetchall()]
    except duckdb.Error as exc:
        LOGGER.exception("Unable to query the %s Parquet dataset", dataset_dir.name)
        raise HTTPException(
            status_code=503,
            detail=f"The {dataset_dir.name} dataset could not be queried.",
        ) from exc
    finally:
        connection.close()


@app.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    """Serve the interactive results dashboard from this API's origin."""
    return FileResponse(DASHBOARD_DIR / "index.html")


@app.get("/health", tags=["operations"])
def health() -> dict[str, str]:
    """Report whether the saved comparison and its provenance report are available."""
    report = _load_report()
    if not list(COMPARISON_DIR.glob("*.parquet")) or not list(
        CANDIDATE_RESULTS_DIR.glob("*.parquet")
    ):
        raise HTTPException(
            status_code=503,
            detail="The comparison or candidate Parquet dataset is unavailable.",
        )
    return {
        "status": "ok",
        "dataset_status": str(report.get("overall_status", "UNKNOWN")),
        "snapshot_id_2026": str(report["sources"]["2026"]["snapshot_id"]),
        "result_status_2026": str(report["sources"]["2026"]["source_status"]),
    }


@app.get(
    "/api/v1/house-comparisons",
    response_model=HouseComparisonResponse,
    tags=["election results"],
    summary="Get the House comparison for all districts or one district",
)
def get_house_comparisons(
    district_id: str | None = Query(
        default=None,
        description="Optional Virginia congressional district number, 1 through 11.",
    ),
) -> HouseComparisonResponse:
    """Return M5 comparison and candidate-level results without recalculating analytics."""
    normalized_id = (
        _normalize_district_id(district_id) if district_id is not None else None
    )
    report = _load_report()
    rows = _read_dataset_rows(COMPARISON_DIR, normalized_id)
    candidate_rows = _read_dataset_rows(CANDIDATE_RESULTS_DIR, normalized_id)
    candidates_by_district_and_year: dict[tuple[str, int], list[dict[str, Any]]] = (
        defaultdict(list)
    )
    for candidate in candidate_rows:
        candidates_by_district_and_year[
            (candidate["district_id"], int(candidate["election_year"]))
        ].append(
            {
                "candidate_name": candidate["candidate_name"],
                "party": candidate["party"],
                "votes": candidate["votes"],
            }
        )
    for row in rows:
        row["candidates_2022"] = candidates_by_district_and_year.get(
            (row["district_id"], 2022), []
        )
        row["candidates_2026"] = candidates_by_district_and_year.get(
            (row["district_id"], 2026), []
        )
    return HouseComparisonResponse(
        overall_status=report["overall_status"],
        comparison_grain=report["comparison_grain"],
        sources=report["sources"],
        comparability=report["comparability"],
        results=rows,
    )


@app.post(
    "/api/v1/assistant",
    response_model=AssistantAnswer,
    tags=["election assistant"],
    summary="Answer a question from the saved House comparison",
)
def ask_assistant(request: AssistantQuestion) -> AssistantAnswer:
    """Answer from the same comparison response served to the dashboard."""
    if not request.question.strip():
        raise HTTPException(status_code=422, detail="question must contain non-whitespace text.")
    comparison = get_house_comparisons(district_id=None)
    return answer_question(request, comparison)
