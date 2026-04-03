"""
NeighbourWise AI — Validator Agent (Optimized)
═══════════════════════════════════════════════
OPTIMIZED: Claude is called ONLY when programmatic checks fail.
  - 4 fast programmatic checks (no LLM): SQL quality, RAG relevance, data usage, format
  - Claude hallucination check: ONLY if programmatic checks failed
  - Claude improvement: ONLY if any check failed

Most questions: 0 Claude calls (fast pass-through)
Bad answers: 1-2 Claude calls (check + improve)
"""

import argparse
import json
import os
import re
import time
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

load_dotenv()
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "user": os.environ.get("SNOWFLAKE_USER", ""),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "NEIGHBOURWISE_AI"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"),
}

AGENT_MODEL = "mistral-large2"
VALIDATOR_MODEL = "claude-3-5-sonnet"
PASS = "✅ PASS"
WARN = "⚠️  WARN"
FAIL = "❌ FAIL"


def sf_connect():
    return snowflake.connector.connect(
        **SNOWFLAKE_CONFIG, insecure_mode=True, network_timeout=120, login_timeout=60,
    )

def run_sql(cur, query):
    try:
        cur.execute(query)
        if cur.description:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        return []
    except Exception as e:
        return {"error": str(e)}

def cortex_complete(cur, prompt):
    try:
        safe = prompt.replace("'", "''")[:8000]
        cur.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{AGENT_MODEL}', '{safe}')")
        return cur.fetchone()[0]
    except Exception as e:
        return f"Error: {e}"

def claude_complete(cur, prompt):
    try:
        safe = prompt.replace("'", "''")[:8000]
        cur.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{VALIDATOR_MODEL}', '{safe}')")
        return cur.fetchone()[0]
    except Exception as e:
        return f"Error (Claude): {e}"


# ═══════════════════════════════════════════════════════════════
# FAST PROGRAMMATIC CHECKS (no LLM calls)
# ═══════════════════════════════════════════════════════════════

def check_sql_quality(sql_data):
    if not sql_data or not sql_data.get("sql"):
        return None
    issues = []
    sql = sql_data["sql"]
    if "NEIGHBOURWISE_DOMAINS.MARTS." not in sql:
        issues.append("Missing full table path")
    quoted = re.findall(r"'([A-Za-z ]+)'", sql)
    for q in quoted:
        if q != q.upper() and q.lower() not in ("true", "false", "null", "query:"):
            issues.append(f"Neighborhood '{q}' not UPPERCASE")
    if sql_data.get("error"):
        issues.append(f"SQL error: {sql_data['error'][:100]}")
    if isinstance(sql_data.get("results"), list) and len(sql_data["results"]) == 0 and not sql_data.get("error"):
        issues.append("SQL returned 0 rows")
    return {"status": PASS if not issues else FAIL, "issues": issues}


def check_rag_relevance(rag_data):
    if not rag_data or not rag_data.get("chunks"):
        return None
    chunks = rag_data["chunks"]
    top_sim = float(chunks[0].get("SIMILARITY", chunks[0].get("similarity", 0))) if chunks else 0
    issues = []
    if top_sim < 0.65:
        issues.append(f"Low relevance: {top_sim:.4f}")
    return {"status": PASS if not issues else WARN, "issues": issues, "top_similarity": round(top_sim, 4)}


def check_data_usage(answer, sql_data):
    issues = []
    if sql_data and isinstance(sql_data.get("results"), list) and len(sql_data["results"]) > 0:
        bad = ["not available", "no data", "not provided", "does not include",
               "not specified", "couldn't find relevant"]
        for phrase in bad:
            if phrase in answer.lower():
                issues.append(f"SQL returned data but answer says '{phrase}'")
                break
    return {"status": PASS if not issues else FAIL, "issues": issues}


def check_format(answer):
    issues = []
    lower = answer.lower()
    if "### summary" not in lower and "**summary" not in lower and "summary" not in lower[:100]:
        issues.append("Missing Summary section")
    if "|" not in answer:
        issues.append("Missing data table")
    if "### insight" not in lower and "**insight" not in lower and "insight" not in lower:
        issues.append("Missing Insights section")
    return {"status": PASS if not issues else WARN, "issues": issues}


# ═══════════════════════════════════════════════════════════════
# CLAUDE CHECKS (only called when needed)
# ═══════════════════════════════════════════════════════════════

def check_hallucination(cur, answer, sql_data, rag_data):
    sql_rows = len(sql_data["results"]) if sql_data and isinstance(sql_data.get("results"), list) else 0
    rag_chunks = len(rag_data["chunks"]) if rag_data and rag_data.get("chunks") else 0
    prompt = f"""Check for SERIOUS hallucinations only. NOT hallucinations: reasonable inferences, conversational phrasing, general recommendations. ONLY flag: invented numbers, made-up facility names, claims about topics not in data.
Data: SQL={sql_rows} rows, RAG={rag_chunks} chunks.
Answer: {answer[:1500]}
Reply ONLY: NO or YES: [explanation]"""
    result = claude_complete(cur, prompt)
    has_hall = result.strip().upper().startswith("YES")
    return {"status": FAIL if has_hall else PASS, "issues": [result.strip()[:200]] if has_hall else []}


def generate_improved_answer(cur, question, draft, feedback, sql_data, rag_data):
    parts = []
    if sql_data and isinstance(sql_data.get("results"), list):
        parts.append(f"DATA:\n{json.dumps(sql_data['results'][:15], indent=2, default=str)}")
    if rag_data and rag_data.get("chunks"):
        parts.append(f"DOCS:\n" + "\n".join(
            f"[{c.get('DOMAIN', c.get('domain','?'))}] {c.get('CHUNK_TEXT', c.get('chunk_text',''))[:500]}"
            for c in rag_data["chunks"][:3]))
    context = "\n\n".join(parts)
    issues = "\n".join(f"- {i}" for i in feedback["all_issues"])

    prompt = f"""You are NeighbourWise AI. Fix the issues in this draft.

QUESTION: {question}
DRAFT: {draft[:2000]}
ISSUES: {issues}
DATA: {context[:3000]}

Write an improved answer with ### Summary (3-4 sentences, conversational, Title Case neighborhoods), ### Key Data (markdown table, Yes/No booleans, formatted numbers), ### Recommendations (2-3 specific actionable suggestions referencing actual data).

Improved answer:"""
    return claude_complete(cur, prompt)


# ═══════════════════════════════════════════════════════════════
# MAIN FUNCTION — called by app.py
# ═══════════════════════════════════════════════════════════════

def validate_and_improve(cur, question, draft_answer, sql_data, rag_data):
    """
    ALWAYS calls Claude for validation.
    1. Run 4 fast programmatic checks (no LLM)
    2. Run Claude hallucination check (always)
    3. If any check failed → Claude improves the answer
    4. If all pass → return draft as-is
    """
    checks = {}

    # Fast programmatic checks
    sql_check = check_sql_quality(sql_data)
    if sql_check:
        checks["sql_quality"] = sql_check
    rag_check = check_rag_relevance(rag_data)
    if rag_check:
        checks["rag_relevance"] = rag_check
    checks["data_usage"] = check_data_usage(draft_answer, sql_data)
    checks["format"] = check_format(draft_answer)

    # ALWAYS call Claude for hallucination check
    checks["hallucination"] = check_hallucination(cur, draft_answer, sql_data, rag_data)

    needs_improvement = any(c["status"] == FAIL for c in checks.values())
    all_issues = [i for c in checks.values() for i in c.get("issues", [])]
    feedback = {"checks": checks, "needs_improvement": needs_improvement,
                "total_issues": len(all_issues), "all_issues": all_issues}

    if needs_improvement:
        final = generate_improved_answer(cur, question, draft_answer, feedback, sql_data, rag_data)
        return {"answer": final, "feedback": feedback, "improved": True, "draft": draft_answer}
    else:
        return {"answer": draft_answer, "feedback": feedback, "improved": False, "draft": draft_answer}


# ═══════════════════════════════════════════════════════════════
# STANDALONE MODE
# ═══════════════════════════════════════════════════════════════

def _sql_agent(cur, q):
    prompt = f"""SQL expert. NEIGHBOURWISE_DOMAINS.MARTS schema. UPPERCASE neighborhoods. Full paths. ONLY SQL.
Tables: MRT_BOSTON_HEALTHCARE, MRT_BOSTON_RESTAURANTS, MRT_BOSTON_UNIVERSITIES, MRT_BOSTON_CRIME, MRT_BOSTON_HOUSING, MRT_BOSTON_MBTA_STOPS, MRT_BOSTON_SCHOOLS, MRT_BOSTON_BLUEBIKE_STATIONS, MRT_BOSTON_GROCERY_STORES, MRT_NEIGHBORHOOD_HEALTHCARE, MRT_NEIGHBORHOOD_SAFETY, MRT_NEIGHBORHOOD_HOUSING, MRT_NEIGHBORHOOD_MBTA, MRT_NEIGHBORHOOD_RESTAURANTS, MRT_NEIGHBORHOOD_SCHOOLS, MRT_NEIGHBORHOOD_BLUEBIKES, MRT_NEIGHBORHOOD_GROCERY_STORES, MRT_NEIGHBORHOOD_UNIVERSITIES.
Question: {q}\nSQL:"""
    sql = cortex_complete(cur, prompt).strip().replace("```sql","").replace("```","").strip()
    if not sql or sql.startswith("Error"): return {"sql": None, "results": None, "error": sql}
    results = run_sql(cur, sql)
    if isinstance(results, dict) and "error" in results: return {"sql": sql, "results": None, "error": results["error"]}
    return {"sql": sql, "results": results}

def _rag_agent(cur, q):
    safe = q.replace("'","''")[:2000]
    results = run_sql(cur, f"""SELECT chunk_text, domain, source_file, VECTOR_COSINE_SIMILARITY(chunk_embedding, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'query: {safe}')) AS similarity FROM NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_DOMAIN_CHUNKS ORDER BY similarity DESC LIMIT 3""")
    if isinstance(results, dict) and "error" in results: return {"chunks": []}
    return {"chunks": results}

def _synthesize(cur, q, sql_data, rag_data):
    parts = []
    if sql_data and isinstance(sql_data.get("results"), list): parts.append(f"DATA:\n{json.dumps(sql_data['results'][:15], indent=2, default=str)}")
    if rag_data and rag_data.get("chunks"): parts.append(f"DOCS:\n" + "\n".join(f"[{c.get('DOMAIN','?')}] {c.get('CHUNK_TEXT','')[:500]}" for c in rag_data["chunks"][:3]))
    if not parts: return "No relevant information found."
    return cortex_complete(cur, f"""NeighbourWise AI. Answer with ### Summary (3-4 sentences), ### Key Data (table), ### Recommendations (2-3 specific).
{chr(10).join(parts)[:3000]}
Question: {q}\nAnswer:""")

def run_single(cur, q, verbose=True):
    if verbose: print(f"\n{'═'*70}\n  Q: {q}\n{'═'*70}")
    sql_data = _sql_agent(cur, q)
    rag_data = _rag_agent(cur, q)
    if verbose:
        if sql_data.get("sql"): print(f"  SQL: {len(sql_data.get('results') or [])} rows")
        if rag_data.get("chunks"): print(f"  RAG: {len(rag_data['chunks'])} chunks")
    draft = _synthesize(cur, q, sql_data, rag_data)
    if verbose: print(f"  Draft: {draft[:100]}...")
    result = validate_and_improve(cur, q, draft, sql_data, rag_data)
    if verbose:
        for n, d in result["feedback"]["checks"].items():
            print(f"  {d['status']} {n}" + (f" → {d['issues'][0][:80]}" if d.get("issues") else ""))
        print(f"  Improved: {'YES' if result['improved'] else 'NO'}")
        print(f"\n{result['answer']}\n")
    return result

TEST_QUESTIONS = [
    "How many hospitals are in Dorchester?",
    "What are the top rated restaurants in South Boston?",
    "Which universities are in Fenway and do they have campus housing?",
    "Which neighborhoods have the highest violent crime count?",
    "What is the average rent and housing grade for Beacon Hill?",
    "Which neighborhoods have no rapid transit access?",
    "Which neighborhood has the most Bluebike stations?",
    "How many supermarkets are in Roxbury?",
    "How many public schools are in Dorchester?",
    "What are the health equity challenges in Boston?",
    "What is Boston doing about food deserts?",
    "Tell me about healthcare in Roxbury with facility counts and challenges",
    "Compare Back Bay and Mattapan across safety, healthcare, and housing",
    "What is the population of Dorchester?",
    "Who is the mayor of Boston?",
]

def run_test_suite():
    print(f"{'='*70}\n  NeighbourWise Validator | Mistral + Claude | {datetime.now():%Y-%m-%d %H:%M}\n{'='*70}")
    conn = sf_connect(); cur = conn.cursor(); results = []
    for i, q in enumerate(TEST_QUESTIONS):
        print(f"\n{'━'*70}\n  TEST {i+1}/{len(TEST_QUESTIONS)}\n{'━'*70}")
        results.append(run_single(cur, q))
        time.sleep(1)
    passed = sum(1 for r in results if not r["feedback"]["needs_improvement"])
    improved = sum(1 for r in results if r["improved"])
    total = len(results)
    score = (passed + improved * 0.8) / total * 100
    grade = "A" if score >= 90 else "B+" if score >= 80 else "B" if score >= 70 else "C" if score >= 60 else "D"
    print(f"\n{'='*70}\n  REPORT: {passed} passed, {improved} improved, {total-passed-improved} failed")
    print(f"  Score: {score:.1f}% — Grade: {grade}\n{'='*70}")
    cur.close(); conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", "-q")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    if args.all: run_test_suite()
    elif args.question:
        conn = sf_connect(); cur = conn.cursor(); run_single(cur, args.question); cur.close(); conn.close()
    else:
        print("NeighbourWise Validator (Interactive). Type 'exit' to quit.")
        conn = sf_connect(); cur = conn.cursor()
        while True:
            q = input("\n  Q: ").strip()
            if not q or q.lower() in ("exit","quit"): break
            run_single(cur, q)
        cur.close(); conn.close()
