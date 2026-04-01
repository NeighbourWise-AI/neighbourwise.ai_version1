# NeighbourWise Search Agent (Serper + Claude)

## Pipeline
```
User Query
   │
   ▼
Serper API  ──►  Live Google Search Results (organic, answer box, PAA)
   │
   ▼
Claude (claude-opus-4-5)  ──►  Synthesized answer with citations
   │
   ▼
Temp Storage  ──►  Session-scoped RAG chunks (ready for Snowflake ingestion)
```

## Setup

### 1. Get your Serper API key
Sign up at https://serper.dev — free tier gives 2,500 searches/month.

### 2. Install dependencies
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### 3. Set environment variables
```bash
# Mac/Linux
export ANTHROPIC_API_KEY=sk-ant-...
export SERPER_API_KEY=your-serper-key

# Windows CMD
set ANTHROPIC_API_KEY=sk-ant-...
set SERPER_API_KEY=your-serper-key

# Windows PowerShell
$env:ANTHROPIC_API_KEY="sk-ant-..."
$env:SERPER_API_KEY="your-serper-key"
```

### 4. Run
```bash
python app.py
```

Visit: http://localhost:5000

## Project structure
```
neighbourwise-agent/
├── app.py                  # Flask server — Serper call + Claude synthesis
├── static/
│   └── index.html          # Frontend UI
├── requirements.txt
└── README.md
```

## API endpoint
`POST /search`
```json
// Request
{ "query": "safest neighborhoods in Boston", "domain": "Crime/Safety" }

// Response
{
  "result": "..synthesized answer..",
  "query": "safest neighborhoods in Boston",
  "domain": "Crime/Safety",
  "sources_fetched": 8,
  "input_tokens": 1240,
  "output_tokens": 387
}
```

## Extending to Snowflake
In `app.py`, after getting `result_text`, add a chunking + insert step:
```python
from pipeline import chunk_and_insert  # your existing pipeline
chunk_and_insert(
    text=result_text,
    table="RAW_CRIME_SAFETY_CHUNKS",
    source_url=f"serper:{query}"
)
```
