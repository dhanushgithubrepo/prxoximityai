from __future__ import annotations

from datetime import date, datetime
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional
import re

import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()
from pydantic import BaseModel, Field
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

# Import LangSmith tracer
from langsmith_tracer import tracer

# Import advanced agents
from advanced_agents import run_parallel_agent_workflow, get_customer_memory, get_all_memories, clear_all_memories, ChannelType

# Import emailer
from emailer import emailer, send_agent_action_emails

# Import agent components
from agent import run_agent_workflow, CustomerAction, ActionType
from cache import cache

app = FastAPI(title="Proximity AI", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
REQUIRED_COLUMNS = ["name", "email", "spend", "visits", "last_order"]



def _norm_col(name: str) -> str:
    s = str(name).strip().lower()
    s = s.replace("₹", "")
    s = re.sub(r"\([^\)]*\)", "", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    original_cols = list(df.columns)
    normalized = {_norm_col(c): c for c in original_cols}

    mapping: Dict[str, str] = {}

    for key in ("name", "customer_name", "full_name"):
        if key in normalized:
            mapping[normalized[key]] = "name"
            break

    for key in ("email", "email_address", "mail"):
        if key in normalized:
            mapping[normalized[key]] = "email"
            break

    for key in ("spend", "amount", "revenue", "total_spend", "total_amount", "spend_inr"):
        if key in normalized:
            mapping[normalized[key]] = "spend"
            break

    for key in ("visits", "visit", "orders", "order_count", "frequency"):
        if key in normalized:
            mapping[normalized[key]] = "visits"
            break

    for key in ("last_order", "last_order_date", "last_purchase", "last_purchase_date", "last_seen", "last_active", "last_activity"):
        if key in normalized:
            mapping[normalized[key]] = "last_order"
            break

    if mapping:
        df = df.rename(columns=mapping)

    return df

def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def load_and_validate_df(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    missing=[c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")
    df=df.copy()
    df["name"] = df["name"].astype(str).fillna("")
    df["email"] = df["email"].astype(str).str.strip().str.lower()
 
    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0.0)
    df["visits"] = pd.to_numeric(df["visits"], errors="coerce").fillna(0).astype(int)
 
    df["last_order"] = df["last_order"].apply(_parse_date)
 
    bad_email = df["email"].isna() | df["email"].eq("")
    if bad_email.any():
        raise ValueError("Some rows have empty email. Email is required.")
 
    return df


def build_data_profile(df: pd.DataFrame) -> Dict[str, Any]:
    last_order_non_null = df["last_order"].dropna()
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "missing_by_column": {c: int(df[c].isna().sum()) for c in df.columns},
        "duplicate_rows": int(df.duplicated().sum()),
        "last_order_min": None if last_order_non_null.empty else str(last_order_non_null.min()),
        "last_order_max": None if last_order_non_null.empty else str(last_order_non_null.max()),
        "spend_min": float(df["spend"].min()),
        "spend_max": float(df["spend"].max()),
        "visits_min": int(df["visits"].min()),
        "visits_max": int(df["visits"].max()),
    }


def df_preview(df: pd.DataFrame, limit: int = 25) -> List[Dict[str, Any]]:
    preview = df.head(limit).copy()
    if "last_order" in preview.columns:
        preview["last_order"] = preview["last_order"].apply(lambda d: None if d is None else str(d))
    return preview.to_dict(orient="records")
 
@app.post("/analyze")
async def analyze(file: UploadFile = File(...)) -> Dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
    if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")
    try:
        raw = await file.read()
        filename = file.filename.lower()

        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(raw))
        else:
            text: Optional[str] = None
            for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue

            if text is None:
                raise ValueError("Could not decode CSV file. Try exporting as UTF-8.")

            df = pd.read_csv(StringIO(text))

        df = load_and_validate_df(df)
        data_profile = build_data_profile(df)

        return {
            "data_profile": data_profile,
            "preview": df_preview(df, limit=25),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {type(e).__name__}: {e}")
@app.post("/analyze/rfm")
async def analyze_rfm(file: UploadFile = File(...), use_ai: bool = False) -> Dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
    if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

    try:
        raw = await file.read()
        filename = file.filename.lower()

        # Generate file hash for caching
        file_hash = cache.get_file_hash(raw)
        
        # Check cache first
        cached_result = cache.get_rfm_result(file_hash)
        if cached_result:
            # Return cached result
            return cached_result

        if filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(raw))
        else:
            text: Optional[str] = None
            for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue

            if text is None:
                raise ValueError("Could not decode CSV file. Try exporting as UTF-8.")

            df = pd.read_csv(StringIO(text))

        df = load_and_validate_df(df)
        today = date.today()

        scored = compute_rfm_and_churn(df, today=today)
        summary = build_rfm_summary(scored)

        customers = scored.sort_values("churn_risk", ascending=False).to_dict(orient="records")

        # Make dates JSON-friendly
        for c in customers:
            if "last_order" in c and c["last_order"] is not None:
                c["last_order"] = str(c["last_order"])

        # LangChain/Gemini hook with caching
        ai_insights = None
        if use_ai:
            print("DEBUG: GEMINI_API_KEY loaded:", bool(get_gemini_api_key()))
            api_key = get_gemini_api_key()
            if api_key:
                top_at_risk = customers[:10]
                # Check cache for AI insights
                cached_ai = cache.get_ai_insights(summary, top_at_risk)
                if cached_ai:
                    ai_insights = InsightSummary(**cached_ai)
                else:
                    ai_insights = generate_gemini_insights(summary, top_at_risk, api_key=api_key)
                    if ai_insights:
                        cache.set_ai_insights(summary, top_at_risk, ai_insights.dict())
            else:
                ai_insights = InsightSummary(
                    insights_summary="AI disabled: set GEMINI_API_KEY to enable.",
                    recommended_actions=["Add key to enable AI insights."],
                    email_templates=[],
                )
        else:
            ai_insights = InsightSummary(
                insights_summary="AI layer not requested.",
                recommended_actions=["Enable use_ai=true to get AI insights."],
                email_templates=[],
            )

        # Agent workflow (D/E/F) with caching
        agent_state = run_agent_workflow(customers)
        
        # Check cache for agent actions
        cached_agent = cache.get_agent_actions(customers)
        if cached_agent:
            agent_state = type(agent_state)(**cached_agent)
        else:
            agent_state = run_agent_workflow(customers)
            cache.set_agent_actions(customers, agent_state.dict())

        ai_insights_dict = None
        if ai_insights:
            try:
                ai_insights_dict = ai_insights.dict()
            except Exception:
                ai_insights_dict = {
                    "insights_summary": "AI parsing failed.",
                    "recommended_actions": ["Retry or check API key."],
                    "email_templates": [],
                }

        # Prepare agent actions for JSON
        agent_actions = []
        for action in agent_state.actions:
            action_dict = action.dict()
            # Convert datetime to string
            if action_dict.get("scheduled_at"):
                action_dict["scheduled_at"] = action_dict["scheduled_at"].isoformat()
            agent_actions.append(action_dict)

        result = {
            "summary": summary,
            "customers": customers,
            "ai_insights": ai_insights_dict,
            "agent": {
                "metadata": agent_state.metadata,
                "actions": agent_actions,
                "discount_codes": agent_state.discount_codes,
            },
            "cached": False,  # Indicate this was computed, not cached
        }
        
        # Cache the full result
        cache.set_rfm_result(file_hash, result)
        
        return result

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {type(e).__name__}: {e}") 

@app.get("/cache/stats")
def get_cache_stats():
    """Get cache statistics."""
    try:
        stats = cache.get_cache_stats()
        return stats
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        return {"error": str(e), "cache_type": "Error", "details": error_details}

@app.delete("/cache/clear")
def clear_cache():
    """Clear all cache entries (admin only)."""
    cache.clear_cache()
    return {"message": "Cache cleared"}

# LangSmith observability endpoints
@app.get("/traces")
def get_traces(limit: int = 20):
    """Get recent agent traces for debugging."""
    return {
        "traces": tracer.get_recent_traces(limit=limit),
        "total_traced": len(tracer.traces)
    }

@app.get("/traces/stats")
def get_trace_stats():
    """Get agent trace statistics."""
    return tracer.get_trace_stats()

@app.get("/agent/config")
def get_agent_config():
    """Get agent configuration and routing rules."""
    return {
        "routing_rules": {
            "At-Risk + churn_risk >= 80": "EMAIL_DISCOUNT",
            "VIP Inactive": "VIP_WINBACK", 
            "At-Risk or churn_risk >= 70": "EMAIL_REENGAGEMENT",
            "default": "MONITOR_ONLY"
        },
        "priority_weights": {
            "EMAIL_DISCOUNT": 80,
            "VIP_WINBACK": 90,
            "EMAIL_REENGAGEMENT": 60,
            "MONITOR_ONLY": 10
        },
        "discount_generation": True,
        "email_templates": True,
        "tracer_enabled": tracer.enabled
    }

@app.post("/agent/execute")
async def execute_agent_action(action_id: str, confirm: bool = False):
    """Execute a specific agent action (manual trigger)."""
    # This would integrate with email service in production
    return {
        "action_id": action_id,
        "status": "pending" if not confirm else "executed",
        "message": "Action queued for execution" if confirm else "Confirmation required"
    }

# Advanced Agent Endpoints
@app.post("/agent/parallel")
async def run_parallel_agents(file: UploadFile = File(...), workers: int = 4):
    """Run agent workflow in parallel with memory and escalation."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file uploaded")
    
    try:
        raw = await file.read()
        
        if file.filename.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(raw))
        else:
            df = pd.read_csv(StringIO(raw.decode("utf-8")))
        
        df = load_and_validate_df(df)
        today = date.today()
        scored = compute_rfm_and_churn(df, today=today)
        customers = scored.to_dict(orient="records")
        
        # Run parallel agent workflow
        result = run_parallel_agent_workflow(customers, max_workers=workers)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")

@app.get("/agent/memory/{customer_email}")
def get_customer_memory_endpoint(customer_email: str):
    """Get memory/history for a specific customer."""
    memory = get_customer_memory(customer_email)
    if not memory:
        raise HTTPException(status_code=404, detail="No memory found for this customer")
    return memory

@app.get("/agent/memories")
def get_all_memories_endpoint():
    """Get all customer memories summary."""
    return get_all_memories()

@app.delete("/agent/memories/clear")
def clear_memories():
    """Clear all agent memories (for testing)."""
    clear_all_memories()
    return {"message": "All agent memories cleared"}

@app.get("/agent/channels")
def get_channel_types():
    """Get available communication channels."""
    return {
        "channels": [ch.value for ch in ChannelType],
        "escalation_chain": ["email", "sms", "whatsapp"],
        "description": "Email → SMS → WhatsApp escalation based on engagement"
    }

# SendGrid Email Endpoints
@app.get("/email/status")
def get_email_status():
    """Get SendGrid email service status."""
    return emailer.get_status()

@app.post("/email/send")
async def send_single_email(to: str, subject: str, body: str):
    """Send a single email via SendGrid."""
    result = emailer.send_email(to, subject, body)
    if result.success:
        return {"success": True, "message_id": result.message_id}
    else:
        raise HTTPException(status_code=500, detail=result.error)

@app.post("/email/send-bulk")
async def send_bulk_emails(actions: List[Dict[str, Any]]):
    """Send emails for all agent actions."""
    result = send_agent_action_emails(actions)
    return result

@app.post("/agent/execute-and-send")
async def execute_and_send(action_id: str, customer_email: str):
    """Execute action and send email via SendGrid."""
    # Get customer memory
    memory = get_customer_memory(customer_email)
    
    # Send email
    result = emailer.send_email(
        customer_email,
        "Your personalized offer from Proximity AI",
        "Check your dashboard for your personalized discount and recommendations!"
    )
    
    return {
        "action_id": action_id,
        "email_sent": result.success,
        "message_id": result.message_id,
        "customer_memory": memory
    }

def compute_rfm_and_churn(df: pd.DataFrame, today: date) -> pd.DataFrame:
    out = df.copy()

    last_order_dt = pd.to_datetime(out["last_order"], errors="coerce")
    today_ts = pd.Timestamp(today)

    out["recency_days"] = (today_ts - last_order_dt).dt.days
    out["recency_days"] = out["recency_days"].fillna(9999).astype(int)

    out["frequency"] = out["visits"].astype(int)
    out["monetary"] = out["spend"].astype(float)

    # R score: lower recency_days => higher R
    out["r_score"] = pd.cut(
        out["recency_days"],
        bins=[-1, 7, 30, 60, 90, 10**9],
        labels=[5, 4, 3, 2, 1],
    ).astype(int)

    # F score
    out["f_score"] = pd.cut(
        out["frequency"],
        bins=[-1, 1, 3, 7, 14, 10**9],
        labels=[1, 2, 3, 4, 5],
    ).astype(int)

    # M score
    out["m_score"] = pd.cut(
        out["monetary"],
        bins=[-1, 50, 200, 500, 1000, 10**9],
        labels=[1, 2, 3, 4, 5],
    ).astype(int)

    out["rfm_score"] = out["r_score"].astype(str) + "-" + out["f_score"].astype(str) + "-" + out["m_score"].astype(str)

    # Simple segments (you can refine later)
    conditions = [
        (out["m_score"] >= 4) & (out["r_score"] <= 2),
        (out["m_score"] >= 4),
        (out["r_score"] <= 2) & (out["f_score"] <= 2),
        (out["r_score"] >= 4) & (out["f_score"] >= 3),
    ]
    choices = ["VIP Inactive", "VIP", "At-Risk Low Engagement", "Engaged"]
    out["segment"] = np.select(conditions, choices, default="Regular")

    # Churn risk heuristic (0..100)
    churn_points = 0
    churn_points += np.select(
        [
            out["recency_days"] > 90,
            out["recency_days"] > 60,
            out["recency_days"] > 30,
        ],
        [50, 35, 20],
        default=0,
    )

    churn_points += np.select(
        [
            out["frequency"] <= 1,
            out["frequency"] <= 3,
        ],
        [20, 10],
        default=0,
    )

    churn_points += np.select(
        [out["monetary"] < 50],
        [10],
        default=0,
    )

    out["churn_risk"] = np.clip(churn_points, 0, 100).astype(int)

    out["tier"] = np.select(
        [out["churn_risk"] >= 70, out["churn_risk"] >= 40],
        ["At-Risk", "Watchlist"],
        default="Healthy",
    )

    return out


def build_rfm_summary(scored: pd.DataFrame) -> Dict[str, Any]:
    tier_counts = scored["tier"].value_counts(dropna=False).to_dict()
    segment_counts = scored["segment"].value_counts(dropna=False).to_dict()

    return {
        "tier_counts": {str(k): int(v) for k, v in tier_counts.items()},
        "segment_counts": {str(k): int(v) for k, v in segment_counts.items()},
        "top_at_risk_emails": scored.sort_values("churn_risk", ascending=False)["email"].head(10).tolist(),
    }

def get_gemini_api_key() -> Optional[str]:
    key = os.getenv("GEMINI_API_KEY")
    if key:
        return key.strip() or None
    return None

class InsightSummary(BaseModel):
    insights_summary: str = Field(..., description="1-2 sentence executive summary of the dataset")
    recommended_actions: List[str] = Field(..., description="3-5 recommended next actions for the business")
    email_templates: List[Dict[str, str]] = Field(..., description="Email templates for top at-risk customers")

def generate_gemini_insights(
    summary: Dict[str, Any],
    top_at_risk_customers: List[Dict[str, Any]],
    api_key: str,
    model: str = "gemini-2.5-flash-lite",
) -> Optional[InsightSummary]:
    try:
        print("DEBUG: LangChain imports starting...")
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.messages import HumanMessage, SystemMessage
        from langchain_core.output_parsers import PydanticOutputParser
        print("DEBUG: LangChain imports succeeded")
    except Exception as e:
        print("DEBUG: LangChain import error:", e)
        return None

    try:
        print("DEBUG: About to call Gemini...")
        llm = ChatGoogleGenerativeAI(model=model, api_key=api_key, temperature=0.3)
        parser = PydanticOutputParser(pydantic_object=InsightSummary)

        system_msg = SystemMessage(
            content=(
                "You are a customer intelligence analyst. "
                "Given RFM + churn summary and top at-risk customers, "
                "return concise insights and actionable recommendations. "
                "Return ONLY valid JSON matching the schema."
            )
        )

        human_msg = HumanMessage(
            content=(
                f"Dataset summary:\n{summary}\n\n"
                f"Top at-risk customers (name, email, churn_risk, segment, rfm_score):\n"
                f"{top_at_risk_customers[:5]}\n\n"
                "Generate insights, recommended actions, and personalized email templates for these customers."
            )
        )

        response = llm.invoke([system_msg, human_msg])
        print("DEBUG: Gemini response:", response.content[:200])
        raw = response.content
        # Try to parse as InsightSummary; if it fails, try to map from Gemini's format
        try:
            return parser.parse(raw)
        except Exception:
            return map_gemini_to_insightsummary(raw)
    except Exception as e:
        print("DEBUG: Gemini call error:", e)
        return None

def map_gemini_to_insightsummary(raw: str) -> InsightSummary:
    import json
    print("DEBUG: Raw Gemini response:", raw[:500])
    try:
        # Remove possible markdown code blocks
        if raw.strip().startswith("```"):
            raw = "\n".join(raw.split("\n")[1:-1])
        data = json.loads(raw)
        print("DEBUG: Parsed JSON keys:", list(data.keys()))
    except Exception as e:
        print("DEBUG: JSON parse error:", e)
        raise ValueError("Failed to parse JSON from Gemini response")

    # Map fields
    insights_list = data.get("insights", [])
    if insights_list:
        # Handle both string and dict formats
        if isinstance(insights_list[0], dict):
            insights_summary = ". ".join(
                item.get("description", item.get("title", str(item))) for item in insights_list
            )
        else:
            insights_summary = ". ".join(str(item) for item in insights_list)
    else:
        insights_summary = "No insights available."

    recommendations = data.get("recommendations", [])
    # Handle both object and string formats
    recommended_actions = []
    for r in recommendations:
        if isinstance(r, dict):
            recommended_actions.append(r.get("action", str(r)))
        else:
            recommended_actions.append(str(r))

    emails = data.get("personalized_emails", [])
    email_templates = [
        {
            "to": e.get("customer_email", ""),
            "subject": e.get("subject", ""),
            "body": e.get("body", ""),
        }
        for e in emails
    ]

    return InsightSummary(
        insights_summary=insights_summary,
        recommended_actions=recommended_actions,
        email_templates=email_templates
    )