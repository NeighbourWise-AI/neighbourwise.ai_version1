"""
NeighbourWise AI — Streamlit App (Final Version)
══════════════════════════════════════════════════
Three-agent system:
  Agent 1: SQL Agent      → Cortex mistral-large2 → structured data
  Agent 2: RAG Agent      → Cortex e5-base-v2     → unstructured data
  Agent 3: Validator Agent → Cortex claude-3-5-sonnet → imported from neighbourwise_validator.py

Flow: Question → Classify → SQL/RAG agents → Draft → Validator (Claude) → Final Answer
"""

import streamlit as st
import json
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# IMPORT VALIDATOR AGENT (uses Claude via Cortex)
# ═══════════════════════════════════════════════════════════════
from neighbourwise_validator import validate_and_improve

load_dotenv()
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

st.set_page_config(page_title="NeighbourWise AI", page_icon="🏘️", layout="wide")

SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "user": os.environ.get("SNOWFLAKE_USER", ""),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", ""),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "NEIGHBOURWISE_AI"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "NEIGHBOURWISE_DOMAINS"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "TRAINING_ROLE"),
}

LLM_MODEL = "mistral-large2"


# ═══════════════════════════════════════════════════════════════
# SNOWFLAKE CONNECTION
# ═══════════════════════════════════════════════════════════════

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        **SNOWFLAKE_CONFIG, insecure_mode=True, network_timeout=120, login_timeout=60,
    )


def run_sql(query):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        if cur.description:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        return []
    except Exception as e:
        return {"error": str(e)}
    finally:
        cur.close()


def cortex_complete(prompt):
    conn = get_connection()
    cur = conn.cursor()
    try:
        safe = prompt.replace("'", "''")[:8000]
        cur.execute(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', '{safe}')")
        return cur.fetchone()[0]
    except Exception as e:
        return f"Error: {e}"
    finally:
        cur.close()


# ═══════════════════════════════════════════════════════════════
# AGENT 1: SQL AGENT (Cortex Mistral)
# ═══════════════════════════════════════════════════════════════

def sql_agent(question):
    prompt = f"""You are a SQL expert for the NeighbourWise Boston neighborhood database.

CRITICAL RULES:
1. ALWAYS use full table path: NEIGHBOURWISE_DOMAINS.MARTS.<table_name>
2. NEIGHBORHOOD_NAME values are ALWAYS UPPERCASE (e.g. 'DORCHESTER', 'BACK BAY', 'FENWAY')
3. When comparing neighborhoods across domains, JOIN tables using LOCATION_ID
4. Generate ONLY the SQL query. No explanation, no markdown, no backticks.

Neighborhood tables (LOCATION_ID, NEIGHBORHOOD_NAME, CITY, _GRADE, _SCORE):
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HEALTHCARE: TOTAL_FACILITIES, HOSPITAL_COUNT, CLINIC_COUNT, TOTAL_BED_CAPACITY, HEALTHCARE_GRADE, HEALTHCARE_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY: TOTAL_INCIDENTS, VIOLENT_CRIME_COUNT, PCT_VIOLENT, SAFETY_GRADE, SAFETY_SCORE, MOST_COMMON_OFFENSE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING: TOTAL_PROPERTIES, AVG_ASSESSED_VALUE, AVG_ESTIMATED_RENT, AVG_PRICE_PER_SQFT, HOUSING_GRADE, HOUSING_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_MBTA: TOTAL_STOPS, TOTAL_ROUTES, HAS_RAPID_TRANSIT, TRANSIT_GRADE, TRANSIT_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_RESTAURANTS: TOTAL_RESTAURANTS, AVG_RATING, CUISINE_DIVERSITY, RESTAURANT_GRADE, RESTAURANT_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SCHOOLS: TOTAL_SCHOOLS, PUBLIC_SCHOOL_COUNT, SCHOOL_GRADE, SCHOOL_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_BLUEBIKES: TOTAL_STATIONS, TOTAL_DOCKS, BIKESHARE_GRADE, BIKESHARE_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_GROCERY_STORES: TOTAL_STORES, SUPERMARKET_COUNT, GROCERY_GRADE, GROCERY_SCORE
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_UNIVERSITIES: TOTAL_UNIVERSITIES, EDUCATION_GRADE, EDUCATION_SCORE, UNIVERSITY_NAMES

Facility tables:
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_HEALTHCARE: FACILITY_NAME, FACILITY_TYPE, NEIGHBORHOOD_NAME, BED_COUNT, IS_HOSPITAL, IS_CLINIC, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_RESTAURANTS: RESTAURANT_NAME, CUISINE_CATEGORY, NEIGHBORHOOD_NAME, RATING, REVIEW_COUNT, PRICE_LABEL, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_UNIVERSITIES: COLLEGE_NAME, INSTITUTION_TYPE, NEIGHBORHOOD_NAME, HAS_CAMPUS_HOUSING, LARGEST_PROGRAM, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_CRIME: OFFENSE_DESCRIPTION, NEIGHBORHOOD_NAME, CRIME_SEVERITY_LABEL, IS_VIOLENT_CRIME, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_HOUSING: BUILDING_TYPE, NEIGHBORHOOD_NAME, TOTAL_ASSESSED_VALUE, ESTIMATED_RENT, BEDROOMS, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_MBTA_STOPS: STOP_NAME, NEIGHBORHOOD_NAME, SERVES_HEAVY_RAIL, IS_WHEELCHAIR_ACCESSIBLE, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_SCHOOLS: SCHOOL_NAME, SCHOOL_TYPE_DESC, NEIGHBORHOOD_NAME, IS_PUBLIC, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_BLUEBIKE_STATIONS: STATION_NAME, NEIGHBORHOOD_NAME, TOTAL_DOCKS, CAPACITY_TIER, LOCATION_ID
- NEIGHBOURWISE_DOMAINS.MARTS.MRT_BOSTON_GROCERY_STORES: STORE_NAME, STORE_TYPE, NEIGHBORHOOD_NAME, LOCATION_ID

Example cross-domain JOIN:
SELECT h.NEIGHBORHOOD_NAME, h.HEALTHCARE_GRADE, s.SAFETY_GRADE, ho.HOUSING_GRADE
FROM NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HEALTHCARE h
JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_SAFETY s ON h.LOCATION_ID = s.LOCATION_ID
JOIN NEIGHBOURWISE_DOMAINS.MARTS.MRT_NEIGHBORHOOD_HOUSING ho ON h.LOCATION_ID = ho.LOCATION_ID
WHERE h.NEIGHBORHOOD_NAME IN ('BACK BAY', 'ROXBURY')

Question: {question}
SQL:"""

    sql_text = cortex_complete(prompt).strip().replace("```sql", "").replace("```", "").strip()

    if not sql_text or sql_text.startswith("Error"):
        return {"source": "sql_agent", "sql": None, "results": None, "error": sql_text}

    results = run_sql(sql_text)
    if isinstance(results, dict) and "error" in results:
        return {"source": "sql_agent", "sql": sql_text, "results": None, "error": results["error"]}

    return {"source": "sql_agent", "sql": sql_text, "results": results}


# ═══════════════════════════════════════════════════════════════
# AGENT 2: RAG AGENT (Cortex e5-base-v2 embeddings)
# ═══════════════════════════════════════════════════════════════

def rag_agent(question, domain_filter=None, top_k=5):
    conn = get_connection()
    cur = conn.cursor()
    try:
        safe_q = question.replace("'", "''")[:2000]
        prefixed_q = f"query: {safe_q}"
        domain_clause = f"AND domain = '{domain_filter}'" if domain_filter and domain_filter != "ALL" else ""

        sql = f"""
            SELECT chunk_text, domain, source_file,
                VECTOR_COSINE_SIMILARITY(
                    chunk_embedding,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', '{prefixed_q}')
                ) AS similarity
            FROM NEIGHBOURWISE_DOMAINS.RAW_UNSTRUCTURED.RAW_DOMAIN_CHUNKS
            WHERE 1=1 {domain_clause}
            ORDER BY similarity DESC LIMIT {top_k}
        """
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return {"source": "rag_agent", "chunks": rows, "query": question}
    except Exception as e:
        return {"source": "rag_agent", "chunks": [], "error": str(e)}
    finally:
        cur.close()


# ═══════════════════════════════════════════════════════════════
# CLASSIFIER (Cortex Mistral)
# ═══════════════════════════════════════════════════════════════

def classify_question(question):
    prompt = f"""Classify this question about Boston neighborhoods into ONE category:
SQL — needs numbers, counts, rankings, grades, data lookups
RAG — needs explanations, context, policies, reports, qualitative info
BOTH — needs numbers AND context together
Question: {question}
Reply with ONLY one word: SQL or RAG or BOTH"""

    result = cortex_complete(prompt).strip().upper()
    if "BOTH" in result:
        return "BOTH"
    elif "RAG" in result:
        return "RAG"
    elif "SQL" in result:
        return "SQL"
    return "BOTH"


# ═══════════════════════════════════════════════════════════════
# SYNTHESIZER — draft answer (Cortex Mistral)
# ═══════════════════════════════════════════════════════════════

def synthesize_answer(question, sql_data, rag_data):
    parts = []
    if sql_data and isinstance(sql_data.get("results"), list) and sql_data["results"]:
        parts.append(f"STRUCTURED DATA:\n{json.dumps(sql_data['results'][:20], indent=2, default=str)}")
    if rag_data and rag_data.get("chunks"):
        chunks = "\n\n".join([
            f"[{c.get('DOMAIN', c.get('domain', '?'))}] {c.get('CHUNK_TEXT', c.get('chunk_text', ''))[:500]}"
            for c in rag_data["chunks"][:5]
        ])
        parts.append(f"DOCUMENT CONTEXT:\n{chunks}")

    if not parts:
        return "I couldn't find relevant information to answer your question. Try being more specific about a Boston neighborhood or domain."

    context = "\n\n".join(parts)
    prompt = f"""You are NeighbourWise AI, a Boston neighborhood analyst.

Your answer MUST have exactly THREE sections:
### Summary
2-3 conversational sentences. Lead with the key insight. Convert UPPERCASE neighborhoods to Title Case.
### Key Data
Clean markdown table. Booleans as Yes/No. Numbers formatted nicely. No IDs or timestamps.
### Recommendations
2-3 specific, actionable suggestions based on the data.

CONTEXT DATA:
{context}

QUESTION: {question}

Answer:"""
    return cortex_complete(prompt)


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATOR: Classify → Agents → Draft → VALIDATOR → Answer
# ═══════════════════════════════════════════════════════════════

def ask_neighbourwise(question, domain_filter=None):
    """
    Full pipeline:
    1. Classify question (Mistral)
    2. Run SQL and/or RAG agents (Mistral + e5)
    3. Generate draft answer (Mistral)
    4. Call Validator Agent (Claude) — imported from neighbourwise_validator.py
    5. Return validated/improved answer
    """

    # Step 1: Classify
    classification = classify_question(question)

    # Step 2: Run agents
    sql_data = None
    rag_data = None
    if classification in ("SQL", "BOTH"):
        sql_data = sql_agent(question)
    if classification in ("RAG", "BOTH"):
        rag_data = rag_agent(question, domain_filter=domain_filter)

    # Step 3: Generate draft answer (Mistral)
    draft_answer = synthesize_answer(question, sql_data, rag_data)

    # Step 4: Call Validator Agent (Claude) — the key integration
    conn = get_connection()
    cur = conn.cursor()
    try:
        validated = validate_and_improve(cur, question, draft_answer, sql_data, rag_data)
    finally:
        cur.close()

    # Step 5: Return the validated answer
    return {
        "answer": validated["answer"],         # Final answer (improved by Claude if needed)
        "classification": classification,
        "sql_data": sql_data,
        "rag_data": rag_data,
        "validation": validated["feedback"],   # Validation details
        "improved": validated["improved"],     # Was it improved by Claude?
        "draft": validated["draft"],           # Original Mistral draft
    }


# ═══════════════════════════════════════════════════════════════
# STREAMLIT UI
# ═══════════════════════════════════════════════════════════════

def main():
    # Sidebar
    with st.sidebar:
        st.title("🏘️ NeighbourWise AI")
        st.caption("Boston Neighborhood Intelligence")
        st.divider()

        domain_filter = st.selectbox(
            "Filter by domain (for document search)",
            ["ALL", "HEALTHCARE", "RESTAURANTS", "UNIVERSITIES", "CRIME",
             "HOUSING", "TRANSIT", "BLUEBIKES", "GROCERY", "SCHOOLS", "GENERAL"],
        )
        st.divider()

        st.markdown("**Example questions:**")
        examples = [
            "How many hospitals are in Dorchester?",
            "Which neighborhood is the safest?",
            "Tell me about healthcare in Roxbury",
            "Compare Back Bay and Roxbury across all domains",
            "What are the mental health challenges in Boston?",
            "Best rated restaurants in Fenway",
            "Which neighborhoods have no subway access?",
            "What is Boston doing about food deserts?",
            "Average rent in each neighborhood",
            "Universities in Fenway with campus housing",
        ]
        for q in examples:
            if st.button(q, key=f"ex_{q}", use_container_width=True):
                st.session_state.user_question = q

        st.divider()
        st.caption("Agents: SQL (Mistral) + RAG (e5) + Validator (Claude)")
        st.caption("20 mart tables + 5,702 document chunks")

    # Main area
    st.header("Ask anything about Boston neighborhoods")
    st.caption("SQL + RAG agents generate a draft → Claude validates and improves → you get the best answer")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sql"):
                with st.expander("📊 SQL Query"):
                    st.code(msg["sql"], language="sql")
            if msg.get("results"):
                with st.expander("📋 Data Results"):
                    st.dataframe(pd.DataFrame(msg["results"][:50]), use_container_width=True)
            if msg.get("chunks"):
                with st.expander("📄 Document Sources"):
                    for i, c in enumerate(msg["chunks"][:5]):
                        d = c.get("DOMAIN", c.get("domain", "?"))
                        s = c.get("SOURCE_FILE", c.get("source_file", "?"))
                        t = c.get("CHUNK_TEXT", c.get("chunk_text", ""))[:300]
                        st.markdown(f"**[{d}]** {s}")
                        st.caption(t + "...")
                        if i < len(msg["chunks"]) - 1:
                            st.divider()
            if msg.get("validation"):
                v = msg["validation"]
                if msg.get("improved"):
                    with st.expander("🔍 Validator (Claude): Answer was improved"):
                        for name, data in v["checks"].items():
                            st.markdown(f"{data['status']} **{name}**")
                            for issue in data.get("issues", []):
                                st.caption(f"  → {issue[:150]}")
                else:
                    with st.expander("✅ Validator (Claude): Passed all checks"):
                        st.caption("No issues found.")

    # Chat input
    user_input = st.chat_input("Ask about any Boston neighborhood...")

    if "user_question" in st.session_state:
        user_input = st.session_state.user_question
        del st.session_state.user_question

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("🔄 SQL + RAG agents working... then Claude validates..."):
                result = ask_neighbourwise(user_input, domain_filter=domain_filter)

            st.markdown(result["answer"])

            # Source badge
            cls = result["classification"]
            badge = "📊 SQL" if cls == "SQL" else "📄 RAG" if cls == "RAG" else "📊📄 Hybrid"
            tag = " → 🔍 Improved by Claude" if result.get("improved") else " → ✅ Passed Claude validation"
            st.caption(f"{badge}{tag}")

            # SQL details
            sql_query = None
            sql_results = None
            if result.get("sql_data") and result["sql_data"].get("sql"):
                sql_query = result["sql_data"]["sql"]
                sql_results = result["sql_data"].get("results")
                with st.expander("📊 SQL Query"):
                    st.code(sql_query, language="sql")
                if sql_results and isinstance(sql_results, list):
                    with st.expander("📋 Data Results"):
                        st.dataframe(pd.DataFrame(sql_results[:50]), use_container_width=True)

            # RAG details
            rag_chunks = None
            if result.get("rag_data") and result["rag_data"].get("chunks"):
                rag_chunks = result["rag_data"]["chunks"]
                with st.expander("📄 Document Sources"):
                    for i, c in enumerate(rag_chunks[:5]):
                        d = c.get("DOMAIN", c.get("domain", "?"))
                        s = c.get("SOURCE_FILE", c.get("source_file", "?"))
                        t = c.get("CHUNK_TEXT", c.get("chunk_text", ""))[:300]
                        st.markdown(f"**[{d}]** {s}")
                        st.caption(t + "...")
                        if i < len(rag_chunks) - 1:
                            st.divider()

            # Validation details
            if result.get("validation"):
                v = result["validation"]
                if result.get("improved"):
                    with st.expander("🔍 Validator (Claude): Answer was improved"):
                        for name, data in v["checks"].items():
                            st.markdown(f"{data['status']} **{name}**")
                            for issue in data.get("issues", []):
                                st.caption(f"  → {issue[:150]}")
                else:
                    with st.expander("✅ Validator (Claude): Passed all checks"):
                        st.caption("No issues found.")

        st.session_state.messages.append({
            "role": "assistant",
            "content": result["answer"],
            "sql": sql_query,
            "results": sql_results,
            "chunks": rag_chunks,
            "validation": result.get("validation"),
            "improved": result.get("improved"),
        })


if __name__ == "__main__":
    main()
