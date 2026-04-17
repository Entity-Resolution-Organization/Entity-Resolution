"""
demo_app.py
===========
Streamlit demo UI for Entity Resolution - Google HQ Presentation.

Four tabs:
  1. Resolve  - Interactive pair comparison with gauges + similarity bars
  2. Unify    - Tilores-style golden record view (scattered records -> one identity)
  3. Batch    - CSV upload with color-coded results table
  4. Pipeline - Model metrics, architecture, endpoint status

Run:
    streamlit run scripts/demo_app.py --server.port 8501
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Fix import path so scripts.model_client works when launched from repo root
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.model_client import get_client  # noqa: E402
from scripts.preprocess import InferencePreprocessor  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Entity Resolution | Group 13",
    page_icon="\U0001f517",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Cached resources
# ---------------------------------------------------------------------------
@st.cache_resource
def load_client():
    return get_client()


@st.cache_resource
def load_preprocessor():
    return InferencePreprocessor()


CLIENT = load_client()
PREPROCESSOR = load_preprocessor()

# ---------------------------------------------------------------------------
# Preset demo scenarios
# ---------------------------------------------------------------------------
PRESETS = {
    "-- Select a scenario --": {},
    "Exact Match": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Robert Smith",
        "address2": "123 Main Street",
    },
    "Nickname (Bob / Robert)": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Bob Smith",
        "address2": "123 Main Street",
    },
    "Typo in Name": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Robrt Smith",
        "address2": "123 Main Street",
    },
    "Person Moved (diff address)": {
        "name1": "Robert Smith",
        "address1": "123 Main Street, NY",
        "name2": "Robert Smith",
        "address2": "789 Oak Avenue, LA",
    },
    "Name Reordered": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Smith Robert",
        "address2": "123 Main Street",
    },
    "Address Abbreviation": {
        "name1": "John Doe",
        "address1": "123 Main Street",
        "name2": "John Doe",
        "address2": "123 Main St",
    },
    "Non-ASCII Name": {
        "name1": "Mohammad Al-Rashid",
        "address1": "45 Desert Rd, Dubai",
        "name2": "Mohammed Al Rashid",
        "address2": "45 Desert Road, Dubai",
    },
    "Different People, Same Address": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "James Wilson",
        "address2": "123 Main Street",
    },
    "Completely Different": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Maria Garcia",
        "address2": "456 Oak Avenue",
    },
    "First Initial Only": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "R. Smith",
        "address2": "123 Main Street",
    },
    "Middle Name Added": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Robert James Smith",
        "address2": "123 Main Street",
    },
    "Similar Name, Diff Address": {
        "name1": "Robert Smith",
        "address1": "123 Main Street",
        "name2": "Robert Smyth",
        "address2": "999 Oak Blvd",
    },
}

# ---------------------------------------------------------------------------
# Unify tab: sample scattered records
# ---------------------------------------------------------------------------
UNIFY_SCENARIOS = {
    "Robert Smith (3 records)": [
        {"source": "NC Voters", "name": "Robert Smith", "address": "123 Main St, Raleigh NC"},
        {"source": "Pseudopeople", "name": "Bob Smith", "address": "123 Main Street"},
        {"source": "OFAC SDN", "name": "SMITH, ROBERT J", "address": "123 Main St, Raleigh"},
    ],
    "Mohammad Al-Rashid (4 records)": [
        {"source": "NC Voters", "name": "Mohammad Al-Rashid", "address": "45 Desert Rd"},
        {"source": "Pseudopeople", "name": "Mohammed Al Rashid", "address": "45 Desert Road"},
        {"source": "OFAC SDN", "name": "AL-RASHID, MOHAMMAD", "address": "45 Desert Rd, Dubai"},
        {"source": "Bank CRM", "name": "Mohamad Alrashid", "address": "45 Desert Road, Dubai"},
    ],
    "Maria Garcia (3 records)": [
        {"source": "NC Voters", "name": "Maria Garcia", "address": "789 Elm Ave, Durham NC"},
        {"source": "Pseudopeople", "name": "Maria L. Garcia", "address": "789 Elm Avenue"},
        {"source": "Bank CRM", "name": "GARCIA, MARIA LUISA", "address": "789 Elm Ave"},
    ],
}


# ===================================================================
#  HELPER: run prediction
# ===================================================================
def predict_pair(name1, addr1, name2, addr2):
    pair = PREPROCESSOR.prepare_pair(name1, addr1, name2, addr2)
    results = CLIENT.predict([pair])
    return results[0]


# ===================================================================
#  HELPER: plotly gauge
# ===================================================================
def make_gauge(probability, decision):
    colors = {"MATCH": "#10b981", "REVIEW": "#f59e0b", "NO-MATCH": "#ef4444"}
    color = colors.get(decision, "#6b7280")

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=probability * 100,
            number={"suffix": "%", "font": {"size": 48}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"color": color, "thickness": 0.6},
                "bgcolor": "rgba(0,0,0,0)",
                "steps": [
                    {"range": [0, 20], "color": "rgba(239,68,68,0.15)"},
                    {"range": [20, 45], "color": "rgba(245,158,11,0.15)"},
                    {"range": [45, 100], "color": "rgba(16,185,129,0.15)"},
                ],
                "threshold": {
                    "line": {"color": color, "width": 3},
                    "thickness": 0.8,
                    "value": probability * 100,
                },
            },
        )
    )
    fig.update_layout(
        height=250,
        margin=dict(t=40, b=0, l=30, r=30),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ===================================================================
#  HELPER: similarity bars
# ===================================================================
def render_similarity_bars(field_sims):
    for field, score in field_sims.items():
        label = field.replace("_", " ").title()
        pct = score * 100
        if pct >= 80:
            color = "#10b981"
        elif pct >= 50:
            color = "#f59e0b"
        else:
            color = "#ef4444"

        st.markdown(
            f'<div style="margin-bottom:6px">'
            f'<div style="display:flex;justify-content:space-between;font-size:13px">'
            f"<span>{label}</span><span><b>{pct:.0f}%</b></span></div>"
            f'<div style="background:#e5e7eb;border-radius:6px;height:10px">'
            f'<div style="background:{color};width:{min(pct, 100):.0f}%;height:10px;'
            f'border-radius:6px"></div></div></div>',
            unsafe_allow_html=True,
        )


# ===================================================================
#  HELPER: decision badge
# ===================================================================
def decision_badge(decision, confidence):
    colors = {
        "MATCH": ("#065f46", "#d1fae5"),
        "REVIEW": ("#78350f", "#fef3c7"),
        "NO-MATCH": ("#7f1d1d", "#fee2e2"),
    }
    fg, bg = colors.get(decision, ("#374151", "#f3f4f6"))
    st.markdown(
        f'<div style="text-align:center;margin:8px 0">'
        f'<span style="background:{bg};color:{fg};padding:8px 24px;border-radius:20px;'
        f'font-weight:700;font-size:20px;letter-spacing:1px">{decision}</span>'
        f'<div style="color:#6b7280;font-size:13px;margin-top:4px">{confidence} confidence</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


# ===================================================================
#  HELPER: entity card
# ===================================================================
def entity_card(label, name, address, color="#3b82f6"):
    name_display = name if name else '<span style="color:#9ca3af">empty</span>'
    addr_display = address if address else '<span style="color:#9ca3af">empty</span>'
    st.markdown(
        f'<div style="border:2px solid {color};border-radius:12px;padding:16px;'
        f'background:rgba(59,130,246,0.04)">'
        f'<div style="color:{color};font-weight:700;font-size:13px;'
        f'text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">{label}</div>'
        f'<div style="font-size:20px;font-weight:600;margin-bottom:4px">{name_display}</div>'
        f'<div style="color:#6b7280;font-size:14px">{addr_display}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


# ===================================================================
#  TAB 1: Resolve
# ===================================================================
def tab_resolve():
    st.markdown("### Compare Two Entity Records")
    st.markdown(
        "Enter two records below or pick a preset scenario to see how the model resolves them."
    )

    preset = st.selectbox("Demo scenarios", list(PRESETS.keys()))
    defaults = PRESETS.get(preset, {})

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Entity A**")
        n1 = st.text_input("Name", value=defaults.get("name1", ""), key="r_n1")
        a1 = st.text_input("Address", value=defaults.get("address1", ""), key="r_a1")
    with col2:
        st.markdown("**Entity B**")
        n2 = st.text_input("Name", value=defaults.get("name2", ""), key="r_n2")
        a2 = st.text_input("Address", value=defaults.get("address2", ""), key="r_a2")

    if st.button("Resolve", type="primary", use_container_width=True):
        if not n1 and not n2:
            st.warning("Enter at least one name on each side.")
            return

        with st.spinner("Resolving..."):
            t0 = time.time()
            result = predict_pair(n1, a1, n2, a2)
            elapsed = (time.time() - t0) * 1000

        # --- Cards ---
        c1, c_mid, c2 = st.columns([2, 1, 2])
        with c1:
            entity_card("Entity A", n1, a1, "#3b82f6")
        with c_mid:
            decision_badge(result.decision, result.confidence_level)
        with c2:
            entity_card("Entity B", n2, a2, "#8b5cf6")

        st.markdown("---")

        # --- Gauge + Similarities ---
        g_col, s_col = st.columns([1, 1])
        with g_col:
            st.markdown("#### Match Probability")
            st.plotly_chart(
                make_gauge(result.probability, result.decision), use_container_width=True
            )
            st.caption(f"Latency: {elapsed:.0f} ms | Threshold: 0.45")
        with s_col:
            st.markdown("#### Field Similarity Breakdown")
            render_similarity_bars(result.field_similarities)

        # --- Raw JSON ---
        with st.expander("Raw prediction JSON"):
            st.json(result.to_dict())


# ===================================================================
#  TAB 2: Unify
# ===================================================================
def tab_unify():
    st.markdown("### Scattered Records -> Unified Identity")
    st.markdown(
        "Real-world databases contain the **same person** stored differently across systems. "
        "Entity resolution unifies them into a single **golden record**."
    )

    scenario = st.selectbox("Choose an identity", list(UNIFY_SCENARIOS.keys()))
    records = UNIFY_SCENARIOS[scenario]

    st.markdown("#### Before: Fragmented Records")
    cols = st.columns(len(records))
    for i, rec in enumerate(records):
        with cols[i]:
            st.markdown(
                f'<div style="border:1px solid #e5e7eb;border-radius:10px;padding:14px;'
                f'min-height:140px;background:#fff">'
                f'<div style="font-size:11px;color:#6b7280;text-transform:uppercase;'
                f'letter-spacing:1px;margin-bottom:6px">{rec["source"]}</div>'
                f'<div style="font-size:16px;font-weight:600">{rec["name"]}</div>'
                f'<div style="font-size:13px;color:#6b7280;margin-top:4px">{rec["address"]}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

    if st.button("Unify Records", type="primary", use_container_width=True):
        with st.spinner("Resolving all pairs..."):
            anchor = records[0]
            pair_results = []
            for rec in records[1:]:
                result = predict_pair(
                    anchor["name"], anchor["address"], rec["name"], rec["address"]
                )
                pair_results.append((rec, result))

        # --- Arrow ---
        st.markdown(
            '<div style="text-align:center;font-size:36px;color:#10b981;margin:12px 0">'
            "\u2b07</div>",
            unsafe_allow_html=True,
        )

        # --- Golden Record ---
        st.markdown("#### After: Unified Golden Record")
        avg_prob = sum(r.probability for _, r in pair_results) / len(pair_results)
        all_match = all(r.decision == "MATCH" for _, r in pair_results)
        golden_color = "#10b981" if all_match else "#f59e0b"

        # Build source lines
        source_lines = ""
        for rec, result in pair_results:
            badge_color = "#10b981" if result.decision == "MATCH" else "#f59e0b"
            source_lines += (
                f'<div style="display:flex;justify-content:space-between;align-items:center;'
                f'padding:4px 0;font-size:13px">'
                f'<span><b>{rec["source"]}</b>: {rec["name"]}</span>'
                f'<span style="color:{badge_color};font-weight:600">'
                f"{result.probability * 100:.0f}% {result.decision}</span></div>"
            )

        st.markdown(
            f'<div style="border:3px solid {golden_color};border-radius:14px;padding:20px;'
            f'background:linear-gradient(135deg, rgba(16,185,129,0.05), rgba(59,130,246,0.05))">'
            f'<div style="display:flex;justify-content:space-between;align-items:center">'
            f"<div>"
            f'<div style="font-size:11px;color:#6b7280;text-transform:uppercase;'
            f'letter-spacing:2px;margin-bottom:4px">Golden Record</div>'
            f'<div style="font-size:24px;font-weight:700">{anchor["name"]}</div>'
            f'<div style="color:#6b7280;font-size:14px;margin-top:2px">{anchor["address"]}</div>'
            f"</div>"
            f'<div style="text-align:right">'
            f'<div style="font-size:32px;font-weight:700;color:{golden_color}">'
            f"{avg_prob * 100:.0f}%</div>"
            f'<div style="font-size:12px;color:#6b7280">avg match confidence</div>'
            f"</div></div>"
            f'<div style="margin-top:12px;border-top:1px solid #e5e7eb;padding-top:12px">'
            f'<div style="font-size:12px;color:#6b7280;margin-bottom:6px">'
            f"Linked from {len(records)} sources:</div>"
            f"{source_lines}</div></div>",
            unsafe_allow_html=True,
        )


# ===================================================================
#  TAB 3: Batch
# ===================================================================
SAMPLE_CSV = """name1,address1,name2,address2
Robert Smith,123 Main Street,Bob Smith,123 Main St
John Doe,456 Elm Avenue,John Doe,456 Elm Ave
Maria Garcia,789 Oak Blvd,Maria Garcia,100 Pine Road
Mohammad Al-Rashid,45 Desert Rd,Mohammed Al Rashid,45 Desert Road
Alice Johnson,200 Park Ave,Robert Smith,200 Park Ave"""


def tab_batch():
    st.markdown("### Batch Entity Resolution")
    st.markdown(
        "Upload a CSV with columns `name1, address1, name2, address2` or use the sample data."
    )

    use_sample = st.checkbox("Use sample data", value=True)

    if use_sample:
        df = pd.read_csv(pd.io.common.StringIO(SAMPLE_CSV.strip()))
        st.dataframe(df, use_container_width=True)
    else:
        uploaded = st.file_uploader("Upload CSV", type=["csv"])
        if uploaded is None:
            return
        df = pd.read_csv(uploaded)
        st.dataframe(df.head(10), use_container_width=True)

    required = {"name1", "address1", "name2", "address2"}
    if not required.issubset(set(df.columns)):
        st.error(f"CSV must have columns: {required}")
        return

    if st.button("Run Batch Resolution", type="primary", use_container_width=True):
        progress = st.progress(0, text="Resolving pairs...")
        results = []
        total = len(df)

        for i, row in df.iterrows():
            result = predict_pair(
                str(row["name1"]),
                str(row["address1"]),
                str(row["name2"]),
                str(row["address2"]),
            )
            results.append(
                {
                    "name1": row["name1"],
                    "name2": row["name2"],
                    "probability": round(result.probability, 4),
                    "decision": result.decision,
                    "confidence": result.confidence_level,
                    "name_sim": round(result.field_similarities.get("name_jaro_winkler", 0), 3),
                    "addr_sim": round(result.field_similarities.get("address_token_overlap", 0), 3),
                }
            )
            progress.progress((i + 1) / total, text=f"Resolved {i + 1}/{total} pairs")

        progress.empty()
        result_df = pd.DataFrame(results)

        # --- Summary stats ---
        st.markdown("---")
        s1, s2, s3, s4 = st.columns(4)
        n_match = int((result_df["decision"] == "MATCH").sum())
        n_review = int((result_df["decision"] == "REVIEW").sum())
        n_nomatch = int((result_df["decision"] == "NO-MATCH").sum())

        s1.metric("Total Pairs", total)
        s2.metric("Matches", n_match)
        s3.metric("Review", n_review)
        s4.metric("No Match", n_nomatch)

        # --- Color-coded results table ---
        def color_decision(val):
            cmap = {
                "MATCH": "background-color: #d1fae5",
                "REVIEW": "background-color: #fef3c7",
                "NO-MATCH": "background-color: #fee2e2",
            }
            return cmap.get(val, "")

        styled = result_df.style.applymap(color_decision, subset=["decision"])
        st.dataframe(styled, use_container_width=True)

        # --- Download ---
        csv_out = result_df.to_csv(index=False)
        st.download_button("Download Results CSV", csv_out, "er_results.csv", "text/csv")


# ===================================================================
#  TAB 4: Pipeline
# ===================================================================
def tab_pipeline():
    st.markdown("### Model & Pipeline Metrics")

    # --- Try to fetch metrics from GCS ---
    metrics = None
    try:
        from google.cloud import storage as gcs_lib

        bucket_name = os.environ.get("GCP_BUCKET_NAME", "entity-resolution-bucket-1")
        project_id = os.environ.get("GCP_PROJECT_ID", "entity-resolution-487121")
        gcs_client = gcs_lib.Client(project=project_id)
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob("pipeline-results/models/person/results/test_metrics.json")
        if blob.exists():
            metrics = json.loads(blob.download_as_text())
    except Exception as e:
        logger.warning(f"Could not load metrics from GCS: {e}")

    if metrics:
        st.markdown("#### Test Set Performance")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("F1 Score", f"{metrics.get('test_f1', 0):.4f}")
        m2.metric("Precision", f"{metrics.get('test_precision', 0):.4f}")
        m3.metric("Recall", f"{metrics.get('test_recall', 0):.4f}")
        m4.metric("AUC", f"{metrics.get('test_auc', 0):.4f}")

        m5, m6, m7, _ = st.columns(4)
        m5.metric("Test Samples", f"{metrics.get('total_samples', 0):,}")
        m6.metric("Threshold", f"{metrics.get('threshold', 0.45)}")
        m7.metric("Positive Rate", f"{metrics.get('positive_rate', 0):.1%}")
    else:
        st.info(
            "Connect to GCS to display pipeline metrics (set GCP_PROJECT_ID and GCP_BUCKET_NAME)."
        )

    st.markdown("---")

    # --- Endpoint info ---
    st.markdown("#### Live Endpoint")
    endpoint_info = None
    try:
        from google.cloud import storage as gcs_lib

        bucket_name = os.environ.get("GCP_BUCKET_NAME", "entity-resolution-bucket-1")
        project_id = os.environ.get("GCP_PROJECT_ID", "entity-resolution-487121")
        gcs_client = gcs_lib.Client(project=project_id)
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob("pipeline-results/endpoint_info.json")
        if blob.exists():
            endpoint_info = json.loads(blob.download_as_text())
    except Exception:
        pass

    if endpoint_info:
        e1, e2 = st.columns(2)
        e1.code(f"Endpoint: {endpoint_info.get('endpoint_resource_name', 'N/A')}")
        e2.code(f"Region: {endpoint_info.get('region', 'us-central1')}")
    else:
        st.info("Endpoint info not available from GCS.")

    st.markdown("---")

    # --- Architecture ---
    st.markdown("#### System Architecture")
    st.code(
        "Data Sources (Pseudopeople, NC Voters, OFAC SDN)\n"
        "      |\n"
        "      v\n"
        "+-----------------+     +------------------+     +-------------------+\n"
        "|  Data Pipeline  | --> |  Model Pipeline  | --> | Inference Pipeline|\n"
        "|  (Airflow DAG)  |     |  (Vertex AI KFP) |     |  (FastAPI + UI)   |\n"
        "+-----------------+     +------------------+     +-------------------+\n"
        "      |                        |                        |\n"
        " DVC versioning          MLflow tracking          Vertex AI endpoint\n"
        " GCS storage             Bias detection           Streamlit demo\n"
        " Schema validation       Sensitivity analysis     Batch inference\n"
        " Bias detection          Quality gate/rollback    Airflow DAG\n",
        language=None,
    )

    st.markdown("---")
    st.markdown("#### Model Details")
    st.markdown(
        "| Component | Value |\n"
        "|---|---|\n"
        "| Base Model | microsoft/deberta-v3-base |\n"
        "| Fine-tuning | LoRA (r=8, alpha=16) |\n"
        "| Target Modules | query_proj, value_proj |\n"
        "| Training Data | 130K pairs (Pseudopeople, NC Voters, OFAC SDN) |\n"
        "| Classification Threshold | 0.45 |\n"
        "| Serving | Vertex AI Endpoint (n1-standard-4, autoscaling 1-3) |"
    )


# ===================================================================
#  SIDEBAR
# ===================================================================
def sidebar():
    st.sidebar.markdown("## Entity Resolution")
    st.sidebar.markdown("**Group 13** | MLOps Spring 2026")
    st.sidebar.markdown("Northeastern University")
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "DeBERTa-v3 + LoRA fine-tuned model for person entity matching. "
        "Deployed on Google Cloud Vertex AI."
    )
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Team**")
    st.sidebar.markdown(
        "Aparup Behera, Sushritha B.D.S., "
        "Sai Pranav Krovvidi, Nishi Patel, "
        "Gouri Rajesh, Fatima Zehrah"
    )
    st.sidebar.markdown("---")
    client_type = "Vertex AI" if "VertexAI" in type(CLIENT).__name__ else "Mock (Jaro-Winkler)"
    st.sidebar.markdown(f"**Client**: {client_type}")


# ===================================================================
#  MAIN
# ===================================================================
def main():
    sidebar()

    st.title("Entity Resolution Demo")
    st.caption("Resolve duplicate identity records using DeBERTa + LoRA | Presented at Google HQ")

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Resolve",
            "Unify",
            "Batch",
            "Pipeline",
        ]
    )

    with tab1:
        tab_resolve()
    with tab2:
        tab_unify()
    with tab3:
        tab_batch()
    with tab4:
        tab_pipeline()


if __name__ == "__main__":
    main()
