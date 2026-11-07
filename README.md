# Virginia Election Data Explorer

A local project for exploring Virginia election data. The dashboard compares
U.S. House results by congressional district for the 2022 and 2026 November
general elections. It includes candidate vote totals, source links, and an
optional assistant for questions about the saved comparison.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

The assistant works with limited local lookups by default. For OpenAI answers,
copy `.env.example` to `.env` and add `OPENAI_API_KEY` before starting the
server. With a key configured, questions and saved public comparison data are
sent to OpenAI; API usage may incur charges. The `.env` file is ignored by Git.

Start the app:

```bash
.venv/bin/uvicorn api.main:app --reload
```

Open <http://127.0.0.1:8000/dashboard>. API docs are at
<http://127.0.0.1:8000/docs>.

## Data

The 2022 results are official. The 2026 results are a saved **unofficial**
snapshot retrieved November 4, 2026, not a live feed. Two-party share uses
Democratic and Republican votes only. Candidate vote totals are not turnout,
and aggregate changes do not show how individuals voted.

The Spark pipeline also creates locality and turnout analysis, but those
results are not currently shown in the dashboard or served by the API.

## API

- `GET /api/v1/house-comparisons` returns all districts.
- `GET /api/v1/house-comparisons?district_id=07` returns one district.
- `POST /api/v1/assistant` answers from the saved comparison.

Source files are in `data/raw/`; Spark jobs are in `scripts/`.
