"""
Proximity AI - Gradio Interface
Replaces the complex HTML frontend with a simple Python UI
"""

import gradio as gr
import pandas as pd
from datetime import date
import os
import sys

# Add proximity module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'proximity'))

from main import (
    load_and_validate_df,
    build_data_profile,
    compute_rfm_and_churn,
    build_rfm_summary,
    get_gemini_api_key,
    generate_gemini_insights,
    InsightSummary
)
from agent import run_agent_workflow
from cache import cache

def run_analysis(file_obj, use_ai=False):
    """Run full RFM analysis on uploaded file"""
    
    if file_obj is None:
        return "No file uploaded", None, None, None, None, None
    
    try:
        # Read file
        if hasattr(file_obj, 'name'):
            filepath = file_obj.name
            if filepath.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(filepath)
            else:
                df = pd.read_csv(filepath)
        else:
            return "Invalid file format", None, None, None, None, None
        
        # Validate and process
        df = load_and_validate_df(df)
        today = date.today()
        scored = compute_rfm_and_churn(df, today=today)
        summary = build_rfm_summary(scored)
        
        # Convert customers for display
        customers = scored.copy()
        if 'last_order' in customers.columns:
            customers['last_order'] = customers['last_order'].apply(lambda d: str(d) if d else '')
        
        # Prepare customer list for agent
        customers_list = customers.to_dict(orient='records')
        
        # Run agent workflow
        agent_state = run_agent_workflow(customers_list)
        
        # AI Insights (if enabled and key available)
        ai_insights_dict = None
        if use_ai:
            api_key = get_gemini_api_key()
            if api_key:
                # Check cache
                cached = cache.get_ai_insights(summary, customers_list[:10])
                if cached:
                    ai_insights_dict = cached
                else:
                    ai_insights = generate_gemini_insights(summary, customers_list[:10], api_key=api_key)
                    if ai_insights:
                        ai_insights_dict = ai_insights.dict()
                        cache.set_ai_insights(summary, customers_list[:10], ai_insights_dict)
        
        # Build output sections
        
        # 1. Overview metrics
        tc = summary.get('tier_counts', {})
        total = sum(tc.values())
        overview_md = f"""
## Overview

| Metric | Value | Status |
|--------|-------|--------|
| **Total Customers** | {total} | |
| **At-Risk** | {tc.get('At-Risk', 0)} | 🔴 Action needed |
| **Watchlist** | {tc.get('Watchlist', 0)} | 🟡 Monitoring |
| **Healthy** | {tc.get('Healthy', 0)} | 🟢 Good |
| **Campaigns** | {len(agent_state.actions)} | Generated |
| **Discount Codes** | {len(agent_state.discount_codes)} | Ready |
        """
        
        # Segments breakdown
        sc = summary.get('segment_counts', {})
        segments_md = "### Segments\n\n"
        for seg, count in sorted(sc.items(), key=lambda x: -x[1]):
            pct = (count / total * 100) if total > 0 else 0
            bar = "█" * int(pct / 5)
            segments_md += f"- **{seg}**: {count} ({pct:.1f}%) {bar}\n"
        
        # 2. Customers table
        display_cols = ['name', 'email', 'tier', 'segment', 'churn_risk', 'rfm_score', 'spend', 'visits']
        available_cols = [c for c in display_cols if c in customers.columns]
        customers_df = customers[available_cols].head(50)
        
        # 3. Campaigns
        actions = agent_state.actions
        campaigns_md = f"## Campaigns ({len(actions)} generated)\n\n"
        
        if actions:
            for action in actions[:20]:  # Limit to 20
                name = action.customer_name or action.customer_email
                campaigns_md += f"""
### {name}
- **Action**: {action.action_type}
- **Tier**: {action.tier} | **Risk**: {action.churn_risk}%
"""
                if action.discount_code:
                    campaigns_md += f"- **Code**: `{action.discount_code}`\n"
                if action.email_subject:
                    campaigns_md += f"- **Subject**: {action.email_subject}\n"
                if action.email_body:
                    body = action.email_body.replace('\n', ' ')[:100]
                    campaigns_md += f"- **Preview**: {body}...\n"
                campaigns_md += "\n---\n"
        else:
            campaigns_md += "No campaigns needed — all customers are healthy."
        
        # 4. Insights
        if ai_insights_dict:
            insights_md = f"""
## AI Insights

### Summary
{ai_insights_dict.get('insights_summary', 'No summary available.')}

### Recommended Actions
"""
            for i, action in enumerate(ai_insights_dict.get('recommended_actions', []), 1):
                insights_md += f"{i}. {action}\n"
            
            # Email templates
            templates = ai_insights_dict.get('email_templates', [])
            if templates:
                insights_md += f"\n### Email Templates ({len(templates)})\n"
                for t in templates[:3]:
                    insights_md += f"\n**To**: {t.get('to', 'N/A')}\n"
                    insights_md += f"**Subject**: {t.get('subject', 'N/A')}\n"
        else:
            if use_ai and not get_gemini_api_key():
                insights_md = """
## AI Insights

⚠️ **GEMINI_API_KEY not set**

Add your API key to `proximity/.env`:
```
GEMINI_API_KEY=your_key_here
```

Get a key from: https://makersuite.google.com/app/apikey
"""
            elif use_ai:
                insights_md = """
## AI Insights

⏳ AI was requested but no insights were generated. This may be due to:
- API rate limits
- Network timeout
- No high-risk customers to analyze

Try again or check the backend logs.
"""
            else:
                insights_md = """
## AI Insights

ℹ️ **AI disabled**

Enable the "Use AI Insights" checkbox to get Gemini-powered analysis.
"""
        
        # Top at-risk list
        top_risk = summary.get('top_at_risk_emails', [])[:10]
        risk_md = "### Priority Outreach (Top At-Risk)\n\n"
        for i, email in enumerate(top_risk, 1):
            risk_md += f"{i}. `{email}`\n"
        
        insights_full = insights_md + "\n" + risk_md
        
        return (
            "✅ Analysis complete!",
            overview_md + "\n" + segments_md,
            customers_df,
            campaigns_md,
            insights_full,
            f"Cached: {ai_insights_dict is not None and use_ai}"
        )
        
    except Exception as e:
        return f"❌ Error: {str(e)}", None, None, None, None, None


def create_ui():
    """Create Gradio interface"""
    
    with gr.Blocks(title="Proximity AI — Customer Intelligence", css="""
        .tab-content { padding: 20px; }
        .input-section { background: #f7f7f5; padding: 20px; border-radius: 8px; }
        .status-box { font-family: monospace; font-size: 12px; }
    """) as demo:
        
        gr.Markdown("""
        # 🎯 Proximity AI
        ### Customer Intelligence Platform — RFM Analysis & Churn Prediction
        """)
        
        with gr.Row():
            with gr.Column(scale=1, elem_classes="input-section"):
                gr.Markdown("### Upload Data")
                
                file_input = gr.File(
                    label="CSV or Excel file",
                    file_types=['.csv', '.xlsx', '.xls'],
                    type="filepath"
                )
                
                use_ai = gr.Checkbox(
                    label="Use AI Insights (Gemini)",
                    value=False,
                    info="Requires GEMINI_API_KEY in .env file"
                )
                
                run_btn = gr.Button(
                    "🚀 Run Analysis",
                    variant="primary",
                    size="lg"
                )
                
                status = gr.Textbox(
                    label="Status",
                    interactive=False,
                    elem_classes="status-box"
                )
                
                cache_info = gr.Textbox(
                    label="Cache Info",
                    interactive=False,
                    visible=False
                )
            
            with gr.Column(scale=3):
                with gr.Tabs():
                    with gr.TabItem("📊 Overview"):
                        overview = gr.Markdown("Upload a file to see results")
                    
                    with gr.TabItem("👥 Customers"):
                        customers_table = gr.Dataframe(
                            label="Customer List (Top 50)",
                            interactive=False
                        )
                    
                    with gr.TabItem("📧 Campaigns"):
                        campaigns = gr.Markdown("Campaigns will appear here")
                    
                    with gr.TabItem("💡 Insights"):
                        insights = gr.Markdown("AI insights will appear here")
        
        # Footer info
        gr.Markdown("""
        ---
        **Required columns**: `name`, `email`, `spend`, `visits`, `last_order`  
        Auto-detects variations like `total_spend`, `order_count`, `last_purchase_date`
        """)
        
        # Event handlers
        run_btn.click(
            fn=run_analysis,
            inputs=[file_input, use_ai],
            outputs=[status, overview, customers_table, campaigns, insights, cache_info]
        )
    
    return demo


if __name__ == "__main__":
    demo = create_ui()
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        show_error=True,
        inbrowser=True
    )
