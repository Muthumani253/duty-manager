#!/usr/bin/env python3
"""Duty Manager v5 — MUTHUMANI S, LECTURER-EEE, GPT KARUR | 9443100811"""
from __future__ import annotations
import os, uuid, base64
from datetime import datetime, timedelta, date
import re
from io import BytesIO

import streamlit as st
import pandas as pd

try:
    from reportlab.lib import colors as RC
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                    Paragraph, Spacer, PageBreak, HRFlowable)
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    RPDF = True
except ImportError:
    RPDF = False

DATA_DIR         = "data"
PANEL_PATH       = os.path.join(DATA_DIR, "panel.csv")
PANEL_DATED_PATH = os.path.join(DATA_DIR, "panel_dated.csv")
STAFF_PATH       = os.path.join(DATA_DIR, "staff.csv")
SUBMAP_PATH      = os.path.join(DATA_DIR, "submap.csv")
SUBJMAP_PATH     = os.path.join(DATA_DIR, "subjmap.csv")
os.makedirs(DATA_DIR, exist_ok=True)

CREATOR    = "MUTHUMANI S | LECTURER-EEE | GPT KARUR | 9443100811"
PANEL_COLS = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","ERROR","__rowid"]
PDATE_COLS = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_COLS = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone","__rowid"]
SMAP_COLS  = ["Staff_Last_Staff_ID","Staff_Name","Department","Department_Code",
              "Subject_Type","Subject_Code","Subject_Name","Subject_Remarks"]

st.set_page_config(page_title="Duty Manager v5", page_icon="🗂️",
                   layout="wide", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════════════════
#  LIGHT / WHITE THEME CSS
# ═══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── Base background ── */
.stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"],
section[data-testid="stMain"], .main, body {
    background-color:#f4f6fb !important;
}
[data-testid="stSidebar"], [data-testid="collapsedControl"],
header[data-testid="stHeader"], #MainMenu, footer { display:none !important; }
.main .block-container { padding:0 1.5rem 2.5rem !important; max-width:100% !important; }

/* ── Typography ── */
body, .stApp { color:#1e293b !important; }
p,li,span,div,label,td,th,caption,small,strong,b,i,em,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] td,
[data-testid="stMarkdownContainer"] th { color:#1e293b !important; }
h1,h2,h3,h4,h5,h6 { color:#0f172a !important; }

/* ── Tables in markdown ── */
[data-testid="stMarkdownContainer"] table { border-collapse:collapse; width:100%; }
[data-testid="stMarkdownContainer"] th {
    background:#e8edf5 !important; color:#0f172a !important;
    padding:8px 12px !important; border:1px solid #cbd5e1 !important; font-weight:700;
}
[data-testid="stMarkdownContainer"] td {
    background:#ffffff !important; color:#1e293b !important;
    padding:6px 12px !important; border:1px solid #e2e8f0 !important;
}
[data-testid="stMarkdownContainer"] tr:nth-child(even) td { background:#f8fafc !important; }

/* ── Code ── */
code, pre { background:#eef2ff !important; color:#3730a3 !important;
    font-size:.79rem !important; border-radius:5px !important; }

/* ── Topbar ── */
.topbar {
    background:linear-gradient(135deg,#4f46e5,#7c3aed);
    padding:0 24px; display:flex; align-items:center;
    justify-content:space-between; height:62px;
    margin:0 -1.5rem 0; border-radius:0 0 12px 12px;
    box-shadow:0 4px 16px rgba(79,70,229,.25);
}
.tb-logo { background:rgba(255,255,255,.2); border-radius:10px;
    padding:8px 10px; font-size:1.3rem; line-height:1; }
.tb-title { color:#ffffff !important; font-weight:800; font-size:1.1rem; }
.tb-sub   { color:rgba(255,255,255,.75) !important; font-size:.68rem; }
.tb-badge { color:#ffffff !important; font-size:.72rem;
    background:rgba(255,255,255,.15); border:1px solid rgba(255,255,255,.3);
    border-radius:20px; padding:4px 14px; white-space:nowrap; }

/* ── Stats bar ── */
.statsbar {
    display:flex; gap:8px; flex-wrap:wrap; background:#ffffff;
    border-bottom:1px solid #e2e8f0; padding:10px 24px;
    margin:0 -1.5rem 1.2rem;
    box-shadow:0 1px 4px rgba(0,0,0,.06);
}
.sc { background:#f1f5f9; border:1px solid #e2e8f0; border-radius:8px;
    padding:4px 14px; font-size:.74rem; color:#475569 !important; white-space:nowrap; }
.sc b { color:#1e293b !important; font-size:.88rem; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background:#ffffff !important; border-bottom:2px solid #e2e8f0 !important;
    gap:0 !important; padding:0 6px !important;
    margin:0 -1.5rem 1.4rem !important; overflow-x:auto !important;
    box-shadow:0 1px 4px rgba(0,0,0,.05);
}
.stTabs [data-baseweb="tab"] {
    background:transparent !important; color:#64748b !important;
    border:none !important; border-bottom:3px solid transparent !important;
    border-radius:0 !important; font-size:.9rem !important; font-weight:600 !important;
    padding:14px 24px !important; transition:all .15s !important; white-space:nowrap !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color:#4f46e5 !important; background:#f5f3ff !important;
}
.stTabs [aria-selected="true"] {
    color:#4f46e5 !important; border-bottom-color:#4f46e5 !important;
    background:#f5f3ff !important;
}

/* ── Section headers ── */
.sec-hdr {
    color:#0f172a; font-weight:700; font-size:1rem;
    margin:14px 0 8px; padding-bottom:6px;
    border-bottom:2px solid #e8edf5; letter-spacing:.01em;
}
.sub-hdr { color:#334155; font-weight:600; font-size:.9rem; margin:8px 0 5px; }

/* ── HR ── */
hr.thin { border:none; border-top:1px solid #e2e8f0; margin:16px 0; }

/* ── Cards ── */
.ok-card   { background:#f0fdf4; border:1px solid #86efac; border-radius:8px;
    padding:10px 16px; color:#15803d !important; font-size:.83rem; margin:5px 0; }
.warn-card { background:#fffbeb; border:1px solid #fcd34d; border-radius:8px;
    padding:10px 16px; color:#92400e !important; font-size:.83rem; margin:5px 0; }
.err-card  { background:#fef2f2; border:1px solid #fca5a5; border-radius:8px;
    padding:10px 16px; color:#b91c1c !important; font-size:.83rem; margin:5px 0; }
.info-card { background:#eff6ff; border:1px solid #93c5fd; border-radius:8px;
    padding:10px 16px; color:#1d4ed8 !important; font-size:.83rem; margin:5px 0; }
.alloc-card { background:#ffffff; border:1px solid #e2e8f0; border-radius:12px;
    padding:16px 18px; margin:10px 0;
    box-shadow:0 1px 6px rgba(0,0,0,.06); }

/* ── Badges ── */
.badge-green  { background:#dcfce7; border:1px solid #86efac; border-radius:12px;
    padding:2px 10px; color:#15803d !important; font-size:.74rem; }
.badge-yellow { background:#fef9c3; border:1px solid #fde047; border-radius:12px;
    padding:2px 10px; color:#854d0e !important; font-size:.74rem; }
.badge-red    { background:#fee2e2; border:1px solid #fca5a5; border-radius:12px;
    padding:2px 10px; color:#b91c1c !important; font-size:.74rem; }
.badge-grey   { background:#f1f5f9; border:1px solid #cbd5e1; border-radius:12px;
    padding:2px 10px; color:#475569 !important; font-size:.74rem; }

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    background:#f8fafc !important; border:2px dashed #cbd5e1 !important;
    border-radius:10px !important;
}
[data-testid="stFileUploader"] label { color:#64748b !important; }

/* ── Inputs ── */
div[data-testid="stSelectbox"] label,
div[data-testid="stTextInput"] label,
div[data-testid="stNumberInput"] label { color:#475569 !important; font-size:.83rem !important; }
div[data-testid="stSelectbox"] > div > div,
div[data-testid="stTextInput"] input,
div[data-testid="stNumberInput"] input {
    background:#ffffff !important; border:1.5px solid #cbd5e1 !important;
    color:#1e293b !important; border-radius:8px !important;
}
div[data-testid="stSelectbox"] > div > div:focus-within,
div[data-testid="stTextInput"] input:focus {
    border-color:#4f46e5 !important; box-shadow:0 0 0 3px rgba(79,70,229,.12) !important;
}
div[data-testid="stSelectbox"] svg { fill:#64748b !important; }

/* ── Buttons ── */
.stButton > button {
    background:#ffffff !important; border:1.5px solid #cbd5e1 !important;
    color:#334155 !important; border-radius:8px !important;
    font-size:.87rem !important; font-weight:600 !important;
    padding:8px 18px !important; transition:all .15s !important;
}
.stButton > button:hover {
    background:#f5f3ff !important; border-color:#4f46e5 !important;
    color:#4f46e5 !important; box-shadow:0 2px 8px rgba(79,70,229,.15) !important;
}
.stButton > button[kind="primary"] {
    background:linear-gradient(135deg,#4f46e5,#7c3aed) !important;
    border:none !important; color:#ffffff !important;
    box-shadow:0 2px 8px rgba(79,70,229,.3) !important;
}
.stButton > button[kind="primary"]:hover {
    opacity:.9 !important; box-shadow:0 4px 14px rgba(79,70,229,.4) !important;
}
.stDownloadButton > button {
    background:#4f46e5 !important; border:none !important;
    color:#ffffff !important; border-radius:8px !important;
    font-size:.85rem !important; font-weight:600 !important;
    box-shadow:0 2px 8px rgba(79,70,229,.3) !important;
}
.stDownloadButton > button:hover {
    background:#4338ca !important; box-shadow:0 4px 14px rgba(79,70,229,.45) !important;
}

/* ── DataFrame ── */
.stDataFrame { border:1px solid #e2e8f0 !important; border-radius:10px !important;
    box-shadow:0 1px 4px rgba(0,0,0,.05) !important; }
[data-testid="stDataFrameResizable"] { background:#ffffff !important; }

/* ── Metrics ── */
[data-testid="stMetricValue"] { color:#1e293b !important; font-size:1.5rem !important; font-weight:700 !important; }
[data-testid="stMetricLabel"] { color:#64748b !important; font-size:.78rem !important; }
[data-testid="metric-container"] { background:#ffffff !important; border:1px solid #e2e8f0 !important;
    border-radius:10px !important; padding:14px !important; box-shadow:0 1px 4px rgba(0,0,0,.05) !important; }

/* ── Expander ── */
[data-testid="stExpander"] { border:1px solid #e2e8f0 !important; border-radius:10px !important;
    background:#ffffff !important; box-shadow:0 1px 4px rgba(0,0,0,.04) !important; }
[data-testid="stExpanderHeader"] { color:#334155 !important; font-size:.88rem !important; font-weight:600 !important; }

/* ── Checkbox ── */
.stCheckbox label { color:#334155 !important; font-size:.86rem !important; }

/* ── Caption ── */
[data-testid="stCaptionContainer"] p { color:#94a3b8 !important; font-size:.75rem !important; }

/* ── int card ── */
.int-card { background:#f5f3ff; border-left:4px solid #6366f1; border-radius:0 8px 8px 0;
    padding:8px 14px; margin:4px 0; }
.ext-sel-card { background:#f0fdf4; border-left:4px solid #22c55e; border-radius:0 8px 8px 0;
    padding:10px 16px; margin:4px 0; }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════
def parse_date(s):
    if s is None: return None
    try:
        if pd.isna(s): return None
    except: pass
    if isinstance(s, (datetime, date, pd.Timestamp)):
        return s.date() if hasattr(s, "date") else None
    t = str(s).strip()
    if not t: return None
    for f in ["%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try: return datetime.strptime(t, f).date()
        except: pass
    try: return pd.to_datetime(t, dayfirst=True).date()
    except: return None

def d2s(d):
    if d is None: return ""
    if isinstance(d, (datetime, pd.Timestamp)): d = d.date()
    return d.strftime("%d.%m.%Y")

def rowid(df, pre="r"):
    df = df.copy()
    if "__rowid" not in df.columns:
        df["__rowid"] = [f"{pre}_{uuid.uuid4().hex}" for _ in range(len(df))]
    else:
        df["__rowid"] = df["__rowid"].astype(str)
        m = df["__rowid"].str.strip() == ""
        if m.any():
            df.loc[m, "__rowid"] = [f"{pre}_{uuid.uuid4().hex}" for _ in range(m.sum())]
    return df

def load_csv(path, cols):
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, dtype=object).fillna("")
            for c in cols:
                if c not in df.columns: df[c] = ""
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def save_csv(df, path):
    try: df.to_csv(path, index=False); return True
    except Exception as e: st.error(f"Save failed: {e}"); return False

SPLIT_RE = re.compile(r"[,\uFF0C;|\-/\\_\s]+")

def norm_id(v):
    if v is None: return ""
    try:
        if isinstance(v, float) and v != v: return ""
    except: pass
    s = str(v).strip()
    if s in ("", "0", "0.0", "0.00"): return ""
    if re.fullmatch(r"-?\d+\.\d+", s):
        try:
            fv = float(s)
            if abs(fv - int(fv)) < 1e-9: s = str(int(fv))
        except: pass
    return "" if s == "0" else s.upper()

def is_zero(v): return str(v).strip() in ("0", "0.0", "0.00") if v else False

def split_toks(v):
    if not v: return []
    s = str(v).strip()
    return [p.strip() for p in SPLIT_RE.split(s) if p.strip()] if s else []

def is_busy(t):
    t2 = str(t).strip().upper()
    return t2 == "B" or bool(re.match(r"^B[\W_]*\d+$", t2))

def inscode_from_sid(sid):
    s = str(sid).strip()
    return s[1:4] if len(s) >= 4 else ""

def get_col(sf, sid, col):
    sid = norm_id(sid)
    if not sid or sf.empty: return ""
    try:
        m = sf["Staff ID"].astype(str).str.upper() == sid
        return str(sf.loc[m, col].iloc[0]) if m.any() else ""
    except: return ""

def get_name(sf, sid):  return get_col(sf, sid, "Name of the Staff")
def get_phone(sf, sid): return get_col(sf, sid, "Phone")
def get_desig(sf, sid): return get_col(sf, sid, "Designation")
def get_instt(sf, sid): return get_col(sf, sid, "INSTT")
def get_dep(sf, sid):   return get_col(sf, sid, "Department")

def get_subname(sm, code):
    if sm is None or sm.empty: return ""
    m = sm[sm["SUBCODE"].astype(str) == str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def priority_icon(count):
    if count == 0: return "🟢"
    elif count <= 2: return "🟡"
    else: return "🔴"

def priority_class(count):
    if count == 0: return "badge-green"
    elif count <= 2: return "badge-yellow"
    else: return "badge-red"

# ═══════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════
for key, path, cols, pre in [
    ("panel", PANEL_PATH,       PANEL_COLS, "p"),
    ("pdate", PANEL_DATED_PATH, PDATE_COLS, "d"),
    ("staff", STAFF_PATH,       STAFF_COLS, "s"),
]:
    if key not in st.session_state:
        df = load_csv(path, cols); df = rowid(df, pre)
        for c in cols:
            if c not in df.columns: df[c] = ""
        st.session_state[key] = df.copy()

if "submap" not in st.session_state:
    st.session_state.submap = load_csv(SUBMAP_PATH, ["SUBCODE","SUBNAME"]).copy()
if "ssmap" not in st.session_state:
    sm2 = load_csv(SUBJMAP_PATH, SMAP_COLS)
    for c in SMAP_COLS:
        if c not in sm2.columns: sm2[c] = ""
    st.session_state.ssmap = sm2.copy()
if "staged" not in st.session_state: st.session_state.staged = {}
if "errors" not in st.session_state: st.session_state.errors = {}

def P():  st.session_state.panel = rowid(st.session_state.panel,"p");  save_csv(st.session_state.panel, PANEL_PATH)
def PD(): st.session_state.pdate = rowid(st.session_state.pdate,"d");  save_csv(st.session_state.pdate, PANEL_DATED_PATH)
def S():  st.session_state.staff = rowid(st.session_state.staff,"s");  save_csv(st.session_state.staff, STAFF_PATH)
def SM(): save_csv(st.session_state.submap, SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap, SUBJMAP_PATH)

# ═══════════════════════════════════════════════════════
# LOGIC
# ═══════════════════════════════════════════════════════
def duty_stats(sf):
    stats = {}
    if sf is None or sf.empty: return stats
    dcols = [c for c in sf.columns if c != "__rowid" and isinstance(c, str)
             and len(c.split(".")) == 3 and all(p.isdigit() for p in c.split("."))]
    for _, row in sf.iterrows():
        sid = norm_id(row.get("Staff ID"))
        if not sid: continue
        cnt = sum(1 for dc in dcols for t in split_toks(row.get(dc,"")) if not is_busy(t))
        stats[sid] = {"count": cnt, "INSTT": row.get("INSTT",""), "dep": row.get("dep code",""),
                      "name": row.get("Name of the Staff",""), "desig": row.get("Designation",""),
                      "phone": row.get("Phone","")}
    return stats

def ext_suggestions(panel_row, sf, ssmap):
    """3-tier: 🟢 Willing (SubjectMap), 🟡 Same Dept, ⚪ Others — sorted least duties first per tier"""
    p_ins  = str(panel_row.get("INSCODE","")).strip()
    sub    = str(panel_row.get("SUBCODE","")).strip().upper()
    p_dep  = str(panel_row.get("NCNO","")).strip()
    stats  = duty_stats(sf)
    if ssmap is not None and not ssmap.empty:
        mapped     = ssmap[ssmap["Subject_Code"].astype(str).str.strip().str.upper() == sub]
        mapped_ids = set(mapped["Staff_Last_Staff_ID"].apply(norm_id).unique())
    else:
        mapped_ids = set()
    willing, same_dep, others = [], [], []
    for _, row in sf.iterrows():
        sid   = norm_id(row.get("Staff ID"))
        if not sid: continue
        instt = str(row.get("INSTT","")).strip()
        if instt == p_ins: continue
        dep   = str(row.get("dep code","")).strip()
        se    = stats.get(sid, {})
        cnt   = se.get("count", 0)
        entry = {"sid": sid, "name": row.get("Name of the Staff",""),
                 "desig": row.get("Designation",""), "instt": instt,
                 "dep": dep, "phone": row.get("Phone",""), "count": cnt}
        if sid in mapped_ids:
            entry.update({"group":"willing","icon":"🟢","cls":"badge-green"})
            willing.append(entry)
        elif dep == p_dep:
            entry.update({"group":"same_dep","icon":"🟡","cls":"badge-yellow"})
            same_dep.append(entry)
        else:
            entry.update({"group":"other","icon":"⚪","cls":"badge-grey"})
            others.append(entry)
    willing.sort(key=lambda x: x["count"])
    same_dep.sort(key=lambda x: x["count"])
    others.sort(key=lambda x: x["count"])
    return willing + same_dep + others

def make_dropdown_label(s):
    return f"{s['icon']} {s['instt']}-{s['dep']} | {s['sid']}-{s['name']} | {s['desig']} | Duties:{s['count']}"

def extract_sid(label):
    l = str(label).strip()
    l = re.sub(r'^[🟢🟡🔴⚪]\s*','', l)
    if "|" in l:
        parts = l.split("|")
        if len(parts) >= 2:
            sid_name = parts[1].strip()
            if "-" in sid_name:
                return norm_id(sid_name.split("-")[0].strip())
            return norm_id(sid_name)
    return norm_id(l.split()[0] if l.split() else "")

def auto_allocate(candidates, sf, ssmap):
    res, skip = {}, {}
    for pidx, row in candidates.iterrows():
        suggs = ext_suggestions(row, sf, ssmap)
        if suggs:
            res[pidx] = make_dropdown_label(suggs[0])
        else:
            skip[pidx] = f"No eligible external staff for SUBCODE {row.get('SUBCODE','?')}"
    return res, skip

def check_errors(pdf, sf):
    errs = {i: [] for i in pdf.index}
    sd   = {}
    for idx, row in pdf.iterrows():
        d1  = parse_date(row.get("DATE_FROM")); d2 = parse_date(row.get("DATE_TO"))
        sc  = str(row.get("SUBCODE","")).strip(); ins = str(row.get("INSCODE","")).strip()
        for role, fld in [("INT","INTID"),("EXT","EXTID")]:
            sid = norm_id(row.get(fld,""))
            if is_zero(row.get(fld,"")): sid = ""
            if not sid: continue
            s_ins = inscode_from_sid(sid)
            if role == "INT" and s_ins and s_ins != ins:
                errs[idx].append(f"❌ INTID {sid}: home {s_ins} ≠ exam {ins}")
            if role == "EXT" and s_ins and s_ins == ins:
                errs[idx].append(f"❌ EXTID {sid}: home {s_ins} == exam {ins} (must differ)")
            sd.setdefault(sid,[]).append((idx, sc, d1, d2, role))
    for sid, duties in sd.items():
        for i in range(len(duties)):
            ia, sca, d1a, d2a, _ = duties[i]
            if not (d1a and d2a): continue
            for j in range(i+1, len(duties)):
                ib, scb, d1b, d2b, _ = duties[j]
                if not (d1b and d2b): continue
                if max(d1a,d1b) <= min(d2a,d2b) and sca != scb:
                    msg = f"⚠️ {sid} CLASH: {sca}({d2s(d1a)}→{d2s(d2a)}) overlaps {scb}({d2s(d1b)}→{d2s(d2b)})"
                    errs[ia].append(msg); errs[ib].append(msg)
    return {k: v for k, v in errs.items() if v}

# ═══════════════════════════════════════════════════════
# PDF GENERATION  (returns bytes directly)
# ═══════════════════════════════════════════════════════
def generate_pdf_rl(panel_df, sf, submap):
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=1.5*cm, rightMargin=1.5*cm,
                            topMargin=1.5*cm, bottomMargin=1.5*cm)
    H1  = ParagraphStyle("H1", fontSize=13, fontName="Helvetica-Bold",
                         spaceAfter=3, alignment=TA_CENTER, textColor=RC.HexColor("#1e1b4b"))
    H2  = ParagraphStyle("H2", fontSize=8, fontName="Helvetica",
                         spaceAfter=2, alignment=TA_CENTER, textColor=RC.HexColor("#6b7280"))
    SML = ParagraphStyle("SML", fontSize=7, fontName="Helvetica",
                         textColor=RC.HexColor("#9ca3af"), alignment=TA_CENTER)
    story = []; sd = {}

    for _, row in panel_df.iterrows():
        sc  = str(row.get("SUBCODE","")).strip(); sn = get_subname(submap, sc)
        ins = str(row.get("INSCODE","")).strip()
        for role, fld in [("INT","INTID"),("EXT","EXTID")]:
            sid = norm_id(row.get(fld,""))
            if not sid: continue
            sd.setdefault(sid,[]).append({
                "ins": ins, "sc": sc, "sn": sn, "role": role,
                "cid": norm_id(row.get("EXTID" if role=="INT" else "INTID",""))
            })

    for sid in sorted(sd.keys(), key=lambda s: get_name(sf, s)):
        duties = sd.get(sid, [])
        if not duties: continue
        name  = get_name(sf, sid);  phone = get_phone(sf, sid)
        m     = sf[sf["Staff ID"].astype(str).str.upper() == sid]
        desig = str(m.iloc[0]["Designation"]) if not m.empty else ""
        dept  = str(m.iloc[0]["Department"])  if not m.empty else ""
        instt = str(m.iloc[0]["INSTT"])       if not m.empty else ""

        story.append(Paragraph("PRACTICAL EXAM DUTY ORDER", H1))
        story.append(Paragraph(CREATOR, H2))
        story.append(Spacer(1, .3*cm))

        # Staff info table — light styling
        ht = Table([
            ["Staff ID", sid,   "Name",        name],
            ["Institution", instt, "Phone",    phone],
            ["Department", dept,  "Designation", desig],
        ], colWidths=[2.5*cm, 4.5*cm, 2.8*cm, 7*cm])
        ht.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (0,-1), RC.HexColor("#eef2ff")),
            ("BACKGROUND", (2,0), (2,-1), RC.HexColor("#eef2ff")),
            ("BACKGROUND", (1,0), (1,-1), RC.white),
            ("BACKGROUND", (3,0), (3,-1), RC.white),
            ("TEXTCOLOR",  (0,0), (-1,-1), RC.HexColor("#1e293b")),
            ("FONTNAME",   (0,0), (0,-1), "Helvetica-Bold"),
            ("FONTNAME",   (2,0), (2,-1), "Helvetica-Bold"),
            ("FONTSIZE",   (0,0), (-1,-1), 8),
            ("GRID",       (0,0), (-1,-1), .5, RC.HexColor("#cbd5e1")),
            ("PADDING",    (0,0), (-1,-1), 6),
            ("VALIGN",     (0,0), (-1,-1), "MIDDLE"),
        ]))
        story.append(ht); story.append(Spacer(1, .35*cm))

        # Duty table
        tr = [["S.No", "Duty\nINSCODE", "SubCode", "Subject Name", "Role",
                "Partner\nID", "Partner Name", "Partner\nPhone", "Date From", "Date To"]]
        for sno, d in enumerate(duties, 1):
            pid = d["cid"]
            pn  = get_name(sf, pid)  if pid else ""
            pp  = get_phone(sf, pid) if pid else ""
            tr.append([str(sno), d["ins"], d["sc"], d["sn"] or d["sc"], d["role"],
                        pid or "-", pn or "-", pp or "-", "", ""])

        dt = Table(tr,
                   colWidths=[.8*cm, 1.9*cm, 1.9*cm, 4.2*cm, 1.1*cm,
                               2*cm, 3.5*cm, 2*cm, 2*cm, 2*cm],
                   repeatRows=1)
        dt.setStyle(TableStyle([
            ("BACKGROUND",   (0,0), (-1,0),  RC.HexColor("#4f46e5")),
            ("TEXTCOLOR",    (0,0), (-1,0),  RC.white),
            ("FONTNAME",     (0,0), (-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",     (0,0), (-1,-1), 7),
            ("ALIGN",        (0,0), (-1,-1), "CENTER"),
            ("ALIGN",        (3,1), (3,-1),  "LEFT"),
            ("ALIGN",        (6,1), (6,-1),  "LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [RC.white, RC.HexColor("#f8fafc")]),
            ("GRID",         (0,0), (-1,-1), .4, RC.HexColor("#cbd5e1")),
            ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
            ("PADDING",      (0,0), (-1,-1), 4),
        ]))
        story.append(dt); story.append(Spacer(1, .3*cm))
        story.append(Paragraph("* Date From / Date To to be filled by Flying Squad Officer at the time of duty.", SML))
        story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()

# ═══════════════════════════════════════════════════════
# TOP BAR + STATS
# ═══════════════════════════════════════════════════════
panel_c  = len(st.session_state.panel)
staff_c  = len(st.session_state.staff)
pdate_c  = len(st.session_state.pdate)
filled_c = int((st.session_state.panel["EXTID"].apply(norm_id) != "").sum()) if panel_c else 0

st.markdown(f"""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:14px">
    <div class="tb-logo">🗂️</div>
    <div>
      <div class="tb-title">Duty Manager v5</div>
      <div class="tb-sub">Practical Exam Duty Allocation System</div>
    </div>
  </div>
  <div class="tb-badge">{CREATOR}</div>
</div>
<div class="statsbar">
  <div class="sc">📋 Panel <b>{panel_c}</b></div>
  <div class="sc">✅ EXT Filled <b>{filled_c}</b></div>
  <div class="sc">⏳ Pending <b>{panel_c - filled_c}</b></div>
  <div class="sc">🧑‍🏫 Staff <b>{staff_c}</b></div>
  <div class="sc">🗓️ Dated Rows <b>{pdate_c}</b></div>
  <div class="sc">🔖 Staged <b>{len(st.session_state.staged)}</b></div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# MAIN TABS
# ═══════════════════════════════════════════════════════
tab_up, tab_ext, tab_duty, tab_dl = st.tabs([
    "  📥  Upload Centre  ",
    "  🎯  EXT Allocate  ",
    "  ▶️   Duty Marking  ",
    "  📦  Downloads  ",
])

# ═══════════════════════════════════════════════════════
# TAB 1 — UPLOAD CENTRE
# ═══════════════════════════════════════════════════════
with tab_up:
    st.markdown('<div class="sec-hdr">📥 Upload Centre</div>', unsafe_allow_html=True)
    u1, u2 = st.columns([1, 1], gap="large")

    with u1:
        st.markdown('<span class="sub-hdr">📋 Panel CSV / XLSX</span>', unsafe_allow_html=True)
        st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID", language="")
        up = st.file_uploader("", type=["csv","xlsx"], key="pan_up", label_visibility="collapsed")
        cl = st.checkbox("Clear existing panel before import", key="pan_cl")
        if up:
            try:
                tmp = (pd.read_csv(up, dtype=object) if up.name.lower().endswith(".csv")
                       else pd.read_excel(up, dtype=object, sheet_name=0)).fillna("")
                req  = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                miss = [c for c in req if c not in tmp.columns]
                if miss: st.error(f"❌ Missing columns: {', '.join(miss)}")
                else:
                    tmp = tmp[req].copy(); tmp["ERROR"] = ""; tmp = rowid(tmp,"p")
                    if cl:
                        st.session_state.panel = rowid(tmp.reset_index(drop=True),"p")
                    else:
                        ins_up = [str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                        bk = st.session_state.panel.copy()
                        bk = bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                        st.session_state.panel = rowid(pd.concat([bk,tmp],ignore_index=True),"p")
                    P(); st.success(f"✅ {len(tmp)} panel rows loaded")
            except Exception as e: st.error(f"❌ {e}")

        st.markdown('<hr class="thin">', unsafe_allow_html=True)
        pv = st.session_state.panel.copy()
        if not st.session_state.submap.empty:
            pv = pv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        show_p = [c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID"] if c in pv.columns]
        st.markdown(f'<span class="sub-hdr">📊 Current Panel — {len(pv)} rows</span>', unsafe_allow_html=True)
        st.dataframe(pv[show_p].fillna(""), use_container_width=True, height=280)

    with u2:
        st.markdown('<span class="sub-hdr">🧑‍🏫 Staff CSV / XLSX</span>', unsafe_allow_html=True)
        st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation  Phone", language="")
        us = st.file_uploader("", type=["csv","xlsx"], key="stf_up", label_visibility="collapsed")
        cs = st.checkbox("Clear existing staff before import", key="stf_cl")
        if us:
            try:
                tmp = (pd.read_csv(us, dtype=object) if us.name.lower().endswith(".csv")
                       else pd.read_excel(us, dtype=object, sheet_name=0)).fillna("")
                req2  = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"]
                miss2 = [c for c in req2 if c not in tmp.columns]
                if miss2: st.error(f"❌ Missing: {', '.join(miss2)}")
                else:
                    tmp = tmp[req2].copy(); tmp = rowid(tmp,"s")
                    if cs:
                        st.session_state.staff = rowid(tmp.reset_index(drop=True),"s")
                    else:
                        existing  = st.session_state.staff.copy()
                        new_ids   = set(tmp["Staff ID"].apply(norm_id))
                        existing  = existing[~existing["Staff ID"].apply(norm_id).isin(new_ids)]
                        st.session_state.staff = rowid(pd.concat([existing,tmp],ignore_index=True),"s")
                    S(); st.success(f"✅ {len(tmp)} staff records loaded")
            except Exception as e: st.error(f"❌ {e}")

        st.markdown('<hr class="thin">', unsafe_allow_html=True)
        sf_show = [c for c in ["Staff ID","INSTT","Name of the Staff","dep code","Designation"] if c in st.session_state.staff.columns]
        st.markdown(f'<span class="sub-hdr">👥 Current Staff — {len(st.session_state.staff)} records</span>', unsafe_allow_html=True)
        st.dataframe(st.session_state.staff[sf_show].fillna(""), use_container_width=True, height=280)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)
    s1, s2 = st.columns(2, gap="large")

    with s1:
        st.markdown('<span class="sub-hdr">📘 Subject-Staff Map (SubjectMap)</span>', unsafe_allow_html=True)
        st.code("Staff_Last_Staff_ID  Staff_Name  Department  Department_Code  Subject_Type  Subject_Code  Subject_Name  Subject_Remarks", language="")
        uss = st.file_uploader("", type=["csv","xlsx"], key="ss_up", label_visibility="collapsed")
        if uss:
            try:
                tmp = (pd.read_csv(uss, dtype=object) if uss.name.lower().endswith(".csv")
                       else pd.read_excel(uss, dtype=object, sheet_name=0)).fillna("")
                for c in SMAP_COLS:
                    if c not in tmp.columns: tmp[c] = ""
                st.session_state.ssmap = tmp[SMAP_COLS].copy()
                SS(); st.success(f"✅ {len(tmp)} subject-staff rows loaded")
            except Exception as e: st.error(f"❌ {e}")
        if not st.session_state.ssmap.empty:
            st.dataframe(st.session_state.ssmap[SMAP_COLS].fillna(""), use_container_width=True, height=180)
        else:
            st.markdown('<div class="info-card">ℹ️ No SubjectMap loaded — Auto/Manual will use 🟡 Same-Dept tier.</div>', unsafe_allow_html=True)

    with s2:
        st.markdown('<span class="sub-hdr">📗 SubCode Name Map</span>', unsafe_allow_html=True)
        st.code("SUBCODE  SUBNAME", language="")
        usm = st.file_uploader("", type=["csv","xlsx"], key="sm_up", label_visibility="collapsed")
        if usm:
            try:
                tmp = (pd.read_csv(usm, dtype=object) if usm.name.lower().endswith(".csv")
                       else pd.read_excel(usm, dtype=object, sheet_name=0)).fillna("")
                if "SUBCODE" not in tmp.columns or "SUBNAME" not in tmp.columns:
                    st.error("❌ Need SUBCODE and SUBNAME columns")
                else:
                    st.session_state.submap = tmp[["SUBCODE","SUBNAME"]].copy()
                    SM(); st.success(f"✅ {len(tmp)} subject names loaded")
            except Exception as e: st.error(f"❌ {e}")
        if not st.session_state.submap.empty:
            st.dataframe(st.session_state.submap.fillna(""), use_container_width=True, height=180)

# ═══════════════════════════════════════════════════════
# TAB 2 — EXT ALLOCATE
# ═══════════════════════════════════════════════════════
with tab_ext:
    panel  = st.session_state.panel.copy()
    sf     = st.session_state.staff.copy()
    ssmap  = st.session_state.ssmap.copy()
    submap = st.session_state.submap.copy()

    def needs_ext(r): return norm_id(r.get("EXTID","")) == ""
    def has_ext(r):   return norm_id(r.get("EXTID","")) != ""

    # top metrics
    all_cands = panel[panel.apply(needs_ext, axis=1)]
    mc = st.columns(4)
    mc[0].metric("📋 Pending EXTID",   len(all_cands))
    mc[1].metric("🧑‍🏫 Staff Loaded",    len(sf))
    mc[2].metric("📘 SubjectMap Rows", len(ssmap))
    mc[3].metric("🔖 Staged",           len(st.session_state.staged))
    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    sub_auto, sub_manual, sub_dl_ext = st.tabs([
        "  🤖  Auto Allocate  ",
        "  📝  Manual Allocation  ",
        "  📥  Download  ",
    ])

    # ────────────────────────────────────────
    # SUB-TAB A — AUTO
    # ────────────────────────────────────────
    with sub_auto:
        with st.expander("ℹ️ Allocation Rules", expanded=False):
            st.markdown("""
| # | Rule | Detail |
|---|------|--------|
| 1 | **SubjectMap** | SUBCODE match via Subject-Staff Mapping → 🟢 Willing |
| 2 | **External Rule** | Staff INSTT ≠ panel INSCODE |
| 3 | **Same Dept** | dep code == panel NCNO → 🟡 if not in SubjectMap |
| 4 | **Others** | All remaining external staff → ⚪ |
| 5 | **Priority** | Least duties first within each tier |
            """)

        fc1, fc2, fc3 = st.columns([2,2,2])
        ins_f   = fc1.selectbox("🏫 Filter INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))), key="ea_i")
        nc_f    = fc2.selectbox("🏭 Filter NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),    key="ea_n")
        show_f  = fc3.selectbox("👁️ Show",["Pending Only","All Rows","Filled Only"], key="ea_sh")

        filt_panel = panel.copy()
        if ins_f != "All": filt_panel = filt_panel[filt_panel["INSCODE"].astype(str)==ins_f]
        if nc_f  != "All": filt_panel = filt_panel[filt_panel["NCNO"].astype(str)==nc_f]
        candidates = filt_panel[filt_panel.apply(needs_ext, axis=1)].copy()

        if show_f == "Pending Only":  view_panel = candidates.copy()
        elif show_f == "Filled Only": view_panel = filt_panel[filt_panel.apply(has_ext, axis=1)].copy()
        else:                         view_panel = filt_panel.copy()

        st.markdown('<div class="sec-hdr">📊 Panel Preview</div>', unsafe_allow_html=True)
        if not view_panel.empty:
            pv2 = view_panel.copy()
            if not submap.empty:
                pv2 = pv2.merge(submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
            pv2["INT_NAME"] = pv2["INTID"].apply(lambda x: get_name(sf, x))
            pv2["EXT_NAME"] = pv2["EXTID"].apply(lambda x: get_name(sf, x))
            pv2["STATUS"]   = pv2.apply(lambda r: "✅ Filled" if has_ext(r) else "⏳ Pending", axis=1)
            show_cols = [c for c in ["STATUS","INSCODE","NCNO","SUBCODE","SUBNAME",
                                     "NOC","INTID","INT_NAME","EXTID","EXT_NAME"] if c in pv2.columns]
            def sty_status(v): return "background-color:#dcfce7;color:#15803d" if v=="✅ Filled" else "background-color:#fee2e2;color:#b91c1c"
            def sty_ext(v):
                v2 = str(v).strip()
                return ("background-color:#dcfce7;color:#15803d" if v2 and not is_zero(v2)
                        else "background-color:#fee2e2;color:#b91c1c")
            styled = pv2[show_cols].fillna("").style\
                .applymap(sty_status, subset=["STATUS"])\
                .applymap(sty_ext,    subset=["EXTID"])
            st.dataframe(styled, use_container_width=True, height=240)
        else:
            st.markdown('<div class="info-card">ℹ️ No rows for current filter.</div>', unsafe_allow_html=True)

        st.markdown('<hr class="thin">', unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🤖 Auto-Allocate</div>', unsafe_allow_html=True)
        st.markdown('<div class="info-card">Picks 🟢 Willing (SubjectMap) first → 🟡 Same-Dept → ⚪ Others · Least duties top priority within each group</div>', unsafe_allow_html=True)

        if st.button("🤖 Auto-Allocate ALL Pending", type="primary"):
            if sf.empty: st.error("❌ Upload staff data first!")
            else:
                res, skip = auto_allocate(candidates, sf, ssmap if not ssmap.empty else None)
                for k, v in res.items(): st.session_state.staged[str(k)] = v
                st.success(f"✅ Auto-staged {len(res)} rows.")
                if skip:
                    with st.expander(f"⚠️ {len(skip)} rows skipped"):
                        st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]), use_container_width=True)
            st.rerun()

        staged_map = st.session_state.staged
        if staged_map:
            st.markdown('<hr class="thin">', unsafe_allow_html=True)
            st.markdown('<div class="sec-hdr">🚀 Apply All Staged</div>', unsafe_allow_html=True)
            with st.expander(f"👁️ Preview {len(staged_map)} staged assignments"):
                rows = []
                for k, v in list(staged_map.items())[:40]:
                    try:
                        pi    = int(k)
                        r     = st.session_state.panel.loc[pi] if pi in st.session_state.panel.index else {}
                        sid_v = extract_sid(v) if "|" in v else norm_id(v)
                        cnt_d = duty_stats(sf).get(sid_v,{}).get("count",0)
                        rows.append({"Row":k, "INSCODE":r.get("INSCODE","?"),
                                     "SUBCODE":r.get("SUBCODE","?"),
                                     "→ EXTID":sid_v, "Name":get_name(sf,sid_v),
                                     "Priority":f"{priority_icon(cnt_d)} {cnt_d} duties"})
                    except: rows.append({"Row":k, "→ EXTID":v})
                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=220)

            a1, a2 = st.columns(2)
            if a1.button("✅ Apply ALL Staged", type="primary", use_container_width=True):
                ok_c, fc_ = [], []
                for k, v in list(staged_map.items()):
                    try: pi = int(k)
                    except: fc_.append(k); continue
                    if pi not in st.session_state.panel.index: fc_.append(k); continue
                    sid_c = extract_sid(v) if "|" in v else norm_id(v)
                    if sid_c:
                        st.session_state.panel.at[pi,"EXTID"] = sid_c
                        st.session_state.staged.pop(k, None); ok_c.append(k)
                    else: fc_.append(k)
                P(); st.success(f"✅ Applied {len(ok_c)} · ❌ Failed {len(fc_)}")
                st.rerun()
            if a2.button("🗑️ Clear All Staged", use_container_width=True):
                st.session_state.staged = {}; st.success("✅ Cleared"); st.rerun()

    # ────────────────────────────────────────
    # SUB-TAB B — MANUAL ALLOCATION
    # ────────────────────────────────────────
    with sub_manual:
        mf1, mf2, mf3 = st.columns([2,2,2])
        m_ins = mf1.selectbox("🏫 Filter INSCODE", ["All"]+sorted(set(panel["INSCODE"].astype(str))), key="ma_i")
        m_nc  = mf2.selectbox("🏭 Filter NCNO",    ["All"]+sorted(set(panel["NCNO"].astype(str))),    key="ma_n")
        m_sh  = mf3.selectbox("👁️ Show", ["Pending Only","All Rows","Filled Only"], key="ma_sh")

        m_filt  = panel.copy()
        if m_ins != "All": m_filt = m_filt[m_filt["INSCODE"].astype(str)==m_ins]
        if m_nc  != "All": m_filt = m_filt[m_filt["NCNO"].astype(str)==m_nc]
        m_cands = m_filt[m_filt.apply(needs_ext, axis=1)].copy()

        if m_sh == "Pending Only":  m_view = m_cands.copy()
        elif m_sh == "Filled Only": m_view = m_filt[m_filt.apply(has_ext, axis=1)].copy()
        else:                       m_view = m_filt.copy()

        st.markdown(
            f'<div class="info-card">📋 Showing <b>{len(m_view)}</b> rows &nbsp;·&nbsp; '
            f'⏳ Pending <b>{len(m_cands)}</b> &nbsp;·&nbsp; '
            f'✅ Filled <b>{len(m_filt)-len(m_cands)}</b></div>',
            unsafe_allow_html=True)
        st.markdown('<hr class="thin">', unsafe_allow_html=True)

        if m_view.empty:
            st.markdown('<div class="ok-card">🎉 No rows to show for current filter!</div>', unsafe_allow_html=True)
        else:
            for _, row in m_view.reset_index().iterrows():
                pidx      = int(row["index"])
                sc        = str(row.get("SUBCODE","")).strip()
                sn        = get_subname(submap, sc)
                ins       = str(row.get("INSCODE","")).strip()
                nc        = str(row.get("NCNO","")).strip()
                noc       = str(row.get("NOC","")).strip()
                nob       = str(row.get("NOB","")).strip()
                intid     = norm_id(row.get("INTID",""))
                intname   = get_name(sf, intid)
                int_desig = get_desig(sf, intid)
                int_phone = get_phone(sf, intid)
                cur_ext   = norm_id(row.get("EXTID",""))
                sv_val    = st.session_state.staged.get(str(pidx),"")

                suggs    = ext_suggestions(row, sf, ssmap if not ssmap.empty else None)
                s_labels = ["— Select External Examiner —"] + [make_dropdown_label(s) for s in suggs]
                w_cnt    = sum(1 for s in suggs if s.get("group")=="willing")
                sd_cnt   = sum(1 for s in suggs if s.get("group")=="same_dep")
                ot_cnt   = sum(1 for s in suggs if s.get("group")=="other")

                with st.container():
                    # Card header
                    st.markdown(f"""
<div class="alloc-card">
  <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:10px">
    <span style="background:#eef2ff;border:1px solid #c7d2fe;border-radius:6px;
                 padding:4px 12px;font-size:.83rem;color:#3730a3;font-weight:600">🏫 {ins}</span>
    <span style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:6px;
                 padding:4px 12px;font-size:.83rem;color:#5b21b6;font-weight:600">🏭 {nc}</span>
    <code style="background:#fef3c7;color:#92400e;padding:3px 10px;border-radius:4px;
                 font-size:.86rem;font-weight:700">{sc}</code>
    {"<span style='font-size:.81rem;color:#64748b'>"+sn+"</span>" if sn else ""}
    <span style="margin-left:auto;font-size:.77rem;color:#94a3b8">
      👥 {noc} students · {nob} batches
    </span>
  </div>
  <!-- Internal Examiner -->
  <div class="int-card" style="display:flex;flex-wrap:wrap;gap:10px;align-items:center">
    <span style="font-size:.75rem;color:#6d28d9;font-weight:600">🎓 Internal Examiner:</span>
    {"<code style='background:#ede9fe;color:#5b21b6;padding:2px 9px;border-radius:4px;font-size:.88rem;font-weight:700'>"+intid+"</code>" if intid else "<span style='color:#94a3b8;font-size:.8rem'>No INTID assigned</span>"}
    {"<span style='color:#1e293b;font-size:.9rem;font-weight:700'>&nbsp;"+intname+"</span>" if intname else ""}
    {"<span style='color:#475569;font-size:.79rem'>&nbsp;·&nbsp;"+int_desig+"</span>" if int_desig else ""}
    {"<span style='color:#94a3b8;font-size:.76rem'>&nbsp;·&nbsp;📞 "+int_phone+"</span>" if int_phone else ""}
  </div>
</div>
""", unsafe_allow_html=True)

                    # Status badge
                    if cur_ext:
                        ext_name  = get_name(sf, cur_ext)
                        ext_desig = get_desig(sf, cur_ext)
                        ext_ph    = get_phone(sf, cur_ext)
                        st.markdown(
                            f'<div class="ok-card">✅ <b>EXTID: {cur_ext}</b> — {ext_name}'
                            f'{"&nbsp;·&nbsp;"+ext_desig if ext_desig else ""}'
                            f'{"&nbsp;📞 "+ext_ph if ext_ph else ""}</div>',
                            unsafe_allow_html=True)
                    elif sv_val:
                        sv_id   = extract_sid(sv_val) if "|" in sv_val else norm_id(sv_val)
                        sv_name = get_name(sf, sv_id)
                        st.markdown(
                            f'<div class="warn-card">🟡 Staged (unsaved): <b>{sv_id}</b>'
                            f'{"&nbsp;— "+sv_name if sv_name else ""}</div>',
                            unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="err-card">⏳ Not assigned yet</div>', unsafe_allow_html=True)

                    # Legend
                    st.markdown(
                        f'<div style="font-size:.73rem;color:#94a3b8;margin-bottom:4px">'
                        f'🟢 {w_cnt} Willing (SubjectMap) &nbsp;·&nbsp; '
                        f'🟡 {sd_cnt} Same-Dept &nbsp;·&nbsp; ⚪ {ot_cnt} Others &nbsp;·&nbsp; '
                        f'Sorted: least duties first within each group</div>',
                        unsafe_allow_html=True)

                    # Dropdown
                    cur_lbl = sv_val if sv_val in s_labels else s_labels[0]
                    di      = s_labels.index(cur_lbl) if cur_lbl in s_labels else 0
                    sel     = st.selectbox(
                        f"🔽 Select External Examiner — {len(suggs)} available",
                        s_labels, index=di, key=f"sel_{pidx}",
                        help="🟢 Willing (SubjectMap) | 🟡 Same Dept | ⚪ Others | Least duties first per group")

                    # Manual + Apply
                    mc1, mc2 = st.columns([5,1])
                    man       = mc1.text_input(
                        "", value="", key=f"man_{pidx}",
                        placeholder="✏️ Or type Staff ID manually",
                        label_visibility="collapsed",
                        help="Manually type Staff ID — overrides dropdown selection")
                    apply_now = mc2.button("▶ Apply", key=f"app_{pidx}", use_container_width=True)

                    if sel and sel != s_labels[0]:
                        st.session_state.staged[str(pidx)] = sel
                    if man.strip():
                        st.session_state.staged[str(pidx)] = man.strip()

                    if apply_now:
                        chosen = sv_val or (sel if sel != s_labels[0] else "") or man.strip()
                        if not chosen:
                            st.warning("⚠️ Select or enter a Staff ID first")
                        else:
                            sid_c = extract_sid(chosen) if "|" in chosen else norm_id(chosen)
                            if sid_c:
                                st.session_state.panel.at[pidx,"EXTID"] = sid_c; P()
                                st.session_state.staged.pop(str(pidx), None)
                                st.success(f"✅ EXTID {sid_c} — {get_name(sf,sid_c)} applied!")
                                st.rerun()
                            else:
                                st.error("❌ Invalid Staff ID")

                    # Selected staff card
                    if sel and sel != s_labels[0]:
                        parts     = [p.strip() for p in re.sub(r'^[🟢🟡🔴⚪]\s*','',sel).split("|")]
                        instt_dep = parts[0] if parts else ""
                        instt_s   = instt_dep.split("-")[0].strip() if "-" in instt_dep else instt_dep
                        dep_s     = instt_dep.split("-")[1].strip() if "-" in instt_dep else ""
                        sid_name  = parts[1].strip() if len(parts)>1 else ""
                        sid_s     = norm_id(sid_name.split("-")[0]) if "-" in sid_name else norm_id(sid_name)
                        name_s    = "-".join(sid_name.split("-")[1:]) if "-" in sid_name else ""
                        desig_s   = parts[2].strip() if len(parts)>2 else ""
                        duties_s  = parts[3].replace("Duties:","").strip() if len(parts)>3 else ""
                        cnt_v     = int(duties_s) if duties_s.isdigit() else 0
                        badge     = priority_class(cnt_v)
                        ph_s      = get_phone(sf, sid_s)
                        st.markdown(
                            f'<div class="ext-sel-card">'
                            f'<div style="display:flex;flex-wrap:wrap;gap:14px;align-items:center">'
                            f'<b style="color:#166534">👤 {sid_s}</b>'
                            f'<span style="color:#1e293b;font-weight:700">{name_s}</span>'
                            f'<span style="color:#475569">{desig_s}</span>'
                            f'<span style="color:#64748b">🏫 {instt_s}</span>'
                            f'{"<span style=color:#64748b>Dept:"+dep_s+"</span>" if dep_s else ""}'
                            f'{"<span style=color:#94a3b8>📞 "+ph_s+"</span>" if ph_s else ""}'
                            f'<span class="{badge}" style="margin-left:auto">Duties: {duties_s}</span>'
                            f'</div></div>', unsafe_allow_html=True)

                    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ────────────────────────────────────────
    # SUB-TAB C — DOWNLOAD (inside EXT page)
    # ────────────────────────────────────────
    with sub_dl_ext:
        st.markdown('<div class="sec-hdr">📥 Download Allocated Data & PDF</div>', unsafe_allow_html=True)

        dl_panel = st.session_state.panel.copy()
        dl_sf    = st.session_state.staff.copy()
        dl_sub   = st.session_state.submap.copy()

        de1, de2 = st.columns([2,2])
        dl_ins   = de1.selectbox("🏫 Filter INSCODE", ["All"]+sorted(set(dl_panel["INSCODE"].astype(str))), key="dl_ext_i")
        dl_nc    = de2.selectbox("🏭 Filter NCNO",    ["All"]+sorted(set(dl_panel["NCNO"].astype(str))),    key="dl_ext_n")

        dl_data   = dl_panel.copy()
        if dl_ins != "All": dl_data = dl_data[dl_data["INSCODE"].astype(str)==dl_ins]
        if dl_nc  != "All": dl_data = dl_data[dl_data["NCNO"].astype(str)==dl_nc]
        dl_filled = dl_data[dl_data["EXTID"].apply(norm_id) != ""]
        dl_pend   = dl_data[dl_data["EXTID"].apply(norm_id) == ""]
        dl_exp    = [c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in dl_data.columns]

        st.markdown(
            f'<div class="info-card">📋 Total: <b>{len(dl_data)}</b> &nbsp;·&nbsp; '
            f'✅ Filled: <b>{len(dl_filled)}</b> &nbsp;·&nbsp; '
            f'⏳ Pending: <b>{len(dl_pend)}</b></div>',
            unsafe_allow_html=True)

        dd1, dd2, dd3 = st.columns(3)
        with dd1:
            st.download_button(
                f"📥 Allocated CSV ({len(dl_filled)} rows)",
                data=dl_filled[dl_exp].to_csv(index=False).encode(),
                file_name=f"allocated{'_'+dl_ins if dl_ins!='All' else ''}.csv",
                mime="text/csv", use_container_width=True, key="dl_alloc_ext")
        with dd2:
            st.download_button(
                f"📥 Full Panel CSV ({len(dl_data)} rows)",
                data=dl_data[dl_exp].to_csv(index=False).encode(),
                file_name=f"panel_full{'_'+dl_ins if dl_ins!='All' else ''}.csv",
                mime="text/csv", use_container_width=True, key="dl_full_ext")
        with dd3:
            st.download_button(
                f"📥 Pending CSV ({len(dl_pend)} rows)",
                data=dl_pend[dl_exp].to_csv(index=False).encode(),
                file_name=f"pending{'_'+dl_ins if dl_ins!='All' else ''}.csv",
                mime="text/csv", use_container_width=True, key="dl_pend_ext")

        st.markdown('<hr class="thin">', unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🖨️ PDF Duty Sheets — Direct Download</div>', unsafe_allow_html=True)

        if dl_filled.empty:
            st.markdown('<div class="warn-card">⚠️ No rows with EXTID filled for selected filter. Assign EXTIDs first.</div>', unsafe_allow_html=True)
        elif not RPDF:
            st.markdown('<div class="err-card">❌ reportlab not installed. Check requirements.txt</div>', unsafe_allow_html=True)
        else:
            with st.spinner("⚙️ Building PDF..."):
                try:
                    pdf_bytes = generate_pdf_rl(dl_filled, dl_sf, dl_sub)
                    fname     = f"duty_sheets{'_'+dl_ins if dl_ins!='All' else ''}.pdf"
                    st.download_button(
                        label=f"📄 Download PDF Duty Sheets ({len(dl_filled)} staff records)",
                        data=pdf_bytes,
                        file_name=fname,
                        mime="application/pdf",
                        use_container_width=True,
                        type="primary",
                        key="dl_pdf_ext")
                    st.markdown(
                        f'<div class="ok-card">✅ PDF ready — <b>{len(dl_filled)}</b> duty sheets compiled. Click above to download.</div>',
                        unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ PDF generation error: {e}")

# ═══════════════════════════════════════════════════════
# TAB 3 — DUTY MARKING
# ═══════════════════════════════════════════════════════
with tab_duty:
    st.markdown('<div class="sec-hdr">▶️ Duty Marking — Upload Dated Panel & Validate</div>', unsafe_allow_html=True)
    with st.expander("ℹ️ Error-Check Rules"):
        st.markdown("""
| # | Check | Rule |
|---|-------|------|
| 🔴 1 | **Institution Rule** | INTID chars[1:4] == INSCODE; EXTID chars[1:4] ≠ INSCODE |
| 🔴 2 | **Date Clash** | Same staff · overlapping dates · different SUBCODE = ❌ |
        """)

    d1c, d2c = st.columns([1,1], gap="large")
    with d1c:
        st.markdown('<span class="sub-hdr">📂 Upload Dated Panel CSV / XLSX</span>', unsafe_allow_html=True)
        st.code("INSCODE NCNO SUBCODE REGL NOC NOB INTID EXTID DATE_FROM DATE_TO", language="")
        udp = st.file_uploader("", type=["csv","xlsx"], key="dp_up", label_visibility="collapsed")
        cl2 = st.checkbox("Clear existing dated panel", key="dp_cl")
        if udp:
            try:
                tmp = (pd.read_csv(udp, dtype=object) if udp.name.lower().endswith(".csv")
                       else pd.read_excel(udp, dtype=object, sheet_name=0)).fillna("")
                req  = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
                miss = [c for c in req if c not in tmp.columns]
                if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                else:
                    tmp = tmp[req].copy(); tmp["ERROR"] = ""; tmp = rowid(tmp,"d")
                    if cl2:
                        st.session_state.pdate = rowid(tmp.reset_index(drop=True),"d")
                    else:
                        ins_up = [str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                        bk = st.session_state.pdate.copy()
                        bk = bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                        st.session_state.pdate = rowid(pd.concat([bk,tmp],ignore_index=True),"d")
                    PD(); st.success(f"✅ {len(tmp)} dated rows loaded")
            except Exception as e: st.error(f"❌ {e}")

    with d2c:
        pdv = st.session_state.pdate.copy()
        pdv["_d"] = pdv["DATE_FROM"].apply(parse_date)
        pdv = pdv.sort_values("_d", na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdv = pdv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        show = [c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID",
                             "DATE_FROM","DATE_TO","ERROR"] if c in pdv.columns]
        st.markdown(f'<span class="sub-hdr">🗓️ Dated Panel — {len(pdv)} rows</span>', unsafe_allow_html=True)
        st.dataframe(pdv[show].fillna(""), use_container_width=True, height=280)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)
    gc1, gc2, gc3 = st.columns([2,2,2])
    ins_g = gc1.selectbox("🏫 INSCODE",["All"]+sorted(set(st.session_state.pdate["INSCODE"].astype(str))), key="dm_i")
    nc_g  = gc2.selectbox("🏭 NCNO",   ["All"]+sorted(set(st.session_state.pdate["NCNO"].astype(str))),   key="dm_n")
    filt2 = st.session_state.pdate.copy()
    if ins_g != "All": filt2 = filt2[filt2["INSCODE"].astype(str)==ins_g]
    if nc_g  != "All": filt2 = filt2[filt2["NCNO"].astype(str)==nc_g]
    with gc3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔍 Run Error Check", type="primary", use_container_width=True):
            if st.session_state.pdate.empty:
                st.error("❌ Upload dated panel first!")
            else:
                with st.spinner("Running checks..."):
                    err_map = check_errors(filt2, st.session_state.staff)
                for idx in filt2.index:
                    if idx in st.session_state.pdate.index:
                        msgs = err_map.get(idx,[])
                        st.session_state.pdate.at[idx,"ERROR"] = " | ".join(msgs) if msgs else ""
                PD(); st.session_state.errors = err_map
                total = sum(len(v) for v in err_map.values())
                if total == 0:
                    st.markdown('<div class="ok-card">✅ All checks passed! No clashes found.</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="err-card">🔴 {total} issue(s) in {len(err_map)} rows.</div>', unsafe_allow_html=True)

    if st.session_state.errors:
        st.markdown('<hr class="thin">', unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🔴 Error Report</div>', unsafe_allow_html=True)
        for idx, msgs in st.session_state.errors.items():
            r = st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
            with st.expander(f"🔴 Row {idx} · 🏫{r.get('INSCODE','?')} · 📚{r.get('SUBCODE','?')} · {len(msgs)} issue(s)"):
                for m in msgs:
                    st.markdown(f'<div class="err-card">{m}</div>', unsafe_allow_html=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">📊 Duty Count per Staff</div>', unsafe_allow_html=True)
    if not st.session_state.pdate.empty:
        dc_d = {}
        for _, row in st.session_state.pdate.iterrows():
            for fld in ["INTID","EXTID"]:
                sid = norm_id(row.get(fld,""))
                if sid: dc_d[sid] = dc_d.get(sid,0) + 1
        if dc_d:
            df_ch = pd.DataFrame(list(dc_d.items()), columns=["Staff ID","Duties"])
            df_ch["Name"]  = df_ch["Staff ID"].apply(lambda s: get_name(st.session_state.staff, s))
            df_ch["Label"] = df_ch["Staff ID"] + " — " + df_ch["Name"]
            df_ch = df_ch.sort_values("Duties", ascending=False).head(30)
            st.bar_chart(df_ch.set_index("Label")["Duties"])
        else:
            st.markdown('<div class="info-card">ℹ️ No staff in dated panel yet.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="info-card">ℹ️ Upload dated panel to see duty chart.</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# TAB 4 — DOWNLOADS
# ═══════════════════════════════════════════════════════
with tab_dl:
    st.markdown('<div class="sec-hdr">📦 Downloads — CSVs & PDF Duty Sheets</div>', unsafe_allow_html=True)

    all_p  = st.session_state.panel.copy()
    all_d  = st.session_state.pdate.copy()
    sf_dl  = st.session_state.staff.copy()
    sub_dl = st.session_state.submap.copy()

    exp_p = [c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in all_p.columns]
    exp_d = [c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID",
                          "DATE_FROM","DATE_TO","ERROR"] if c in all_d.columns]

    # ── Panel CSVs ──
    st.markdown('<span class="sub-hdr">📋 Panel (No Dates)</span>', unsafe_allow_html=True)
    if all_p.empty:
        st.markdown('<div class="info-card">ℹ️ No panel data loaded.</div>', unsafe_allow_html=True)
    else:
        inscodes_p = sorted(set(all_p["INSCODE"].astype(str)))
        dc1, dc2, dc3 = st.columns(3)
        with dc1:
            st.download_button(
                f"📥 Full Panel CSV — {len(all_p)} rows",
                data=all_p[exp_p].to_csv(index=False).encode(),
                file_name="panel_full.csv", mime="text/csv", use_container_width=True)
        with dc2:
            pf2 = all_p.copy()
            if not sub_dl.empty:
                pf2 = pf2.merge(sub_dl[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
            exp2 = [c for c in exp_p+["SUBNAME"] if c in pf2.columns]
            st.download_button(
                f"📥 Panel + SUBNAME — {len(pf2)} rows",
                data=pf2[exp2].to_csv(index=False).encode(),
                file_name="panel_full_subname.csv", mime="text/csv", use_container_width=True)
        with dc3:
            pend = all_p[all_p["EXTID"].apply(norm_id)==""]
            st.download_button(
                f"📥 Pending EXTID — {len(pend)} rows",
                data=pend[exp_p].to_csv(index=False).encode(),
                file_name="panel_pending_extid.csv", mime="text/csv", use_container_width=True)

        st.markdown('<span class="sub-hdr" style="font-size:.82rem">Per Institution</span>', unsafe_allow_html=True)
        for chunk in [inscodes_p[i:i+4] for i in range(0,len(inscodes_p),4)]:
            cols = st.columns(4)
            for ci, ins in enumerate(chunk):
                df_i = all_p[all_p["INSCODE"].astype(str)==ins][exp_p]
                ef_i = df_i["EXTID"].apply(norm_id).ne("").sum()
                cols[ci].download_button(
                    label=f"📥 {ins}\n({ef_i}/{len(df_i)} filled)",
                    data=df_i.to_csv(index=False).encode(),
                    file_name=f"panel_{ins}.csv", mime="text/csv",
                    key=f"dl_p_{ins}", use_container_width=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── Dated Panel CSVs ──
    st.markdown('<span class="sub-hdr">🗓️ Dated Panel</span>', unsafe_allow_html=True)
    if all_d.empty:
        st.markdown('<div class="info-card">ℹ️ No dated panel loaded.</div>', unsafe_allow_html=True)
    else:
        inscodes_d = sorted(set(all_d["INSCODE"].astype(str)))
        dd1, dd2 = st.columns(2)
        with dd1:
            st.download_button(
                f"📥 Full Dated Panel — {len(all_d)} rows",
                data=all_d[exp_d].to_csv(index=False).encode(),
                file_name="dated_panel_full.csv", mime="text/csv", use_container_width=True)
        with dd2:
            errd = all_d[all_d["ERROR"].astype(str).str.strip()!=""]
            st.download_button(
                f"📥 Errors Only — {len(errd)} rows",
                data=errd[exp_d].to_csv(index=False).encode(),
                file_name="dated_panel_errors.csv", mime="text/csv", use_container_width=True)

        for chunk in [inscodes_d[i:i+4] for i in range(0,len(inscodes_d),4)]:
            cols = st.columns(4)
            for ci, ins in enumerate(chunk):
                df_i = all_d[all_d["INSCODE"].astype(str)==ins][exp_d]
                cols[ci].download_button(
                    label=f"📥 Dated {ins} ({len(df_i)} rows)",
                    data=df_i.to_csv(index=False).encode(),
                    file_name=f"dated_{ins}.csv", mime="text/csv",
                    key=f"dl_d_{ins}", use_container_width=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── PDF Duty Sheets — DIRECT DOWNLOAD ──
    st.markdown('<div class="sec-hdr">🖨️ PDF Duty Sheets — Direct Download</div>', unsafe_allow_html=True)
    if all_p.empty:
        st.markdown('<div class="warn-card">⚠️ Upload panel data first.</div>', unsafe_allow_html=True)
    elif not RPDF:
        st.markdown('<div class="err-card">❌ reportlab not installed. Add to requirements.txt</div>', unsafe_allow_html=True)
    else:
        pdf_ins_f = st.selectbox("🏫 Filter by INSCODE",
                                  ["All"]+sorted(set(all_p["INSCODE"].astype(str))), key="pdf_ins")
        pdf_data   = all_p.copy()
        if pdf_ins_f != "All": pdf_data = pdf_data[pdf_data["INSCODE"].astype(str)==pdf_ins_f]
        pdf_filled = pdf_data[pdf_data["EXTID"].apply(norm_id)!=""]

        st.markdown(
            f'<div class="info-card">📄 <b>{len(pdf_filled)}</b> rows with EXTID filled '
            f'(out of {len(pdf_data)} total) — PDF will be generated automatically below.</div>',
            unsafe_allow_html=True)

        if pdf_filled.empty:
            st.markdown('<div class="warn-card">⚠️ No filled EXTID rows for selected filter.</div>', unsafe_allow_html=True)
        else:
            with st.spinner("⚙️ Building PDF..."):
                try:
                    pdf_bytes = generate_pdf_rl(pdf_filled, sf_dl, sub_dl)
                    fname     = f"duty_sheets{'_'+pdf_ins_f if pdf_ins_f!='All' else ''}.pdf"
                    st.download_button(
                        label=f"📄 Download PDF Duty Sheets — {len(pdf_filled)} Records",
                        data=pdf_bytes,
                        file_name=fname,
                        mime="application/pdf",
                        use_container_width=True,
                        type="primary",
                        key="dl_pdf_main")
                    st.markdown(
                        f'<div class="ok-card">✅ PDF ready! Click the button above to download <b>{fname}</b>.</div>',
                        unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"❌ PDF error: {e}")

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── Staff CSV ──
    st.markdown('<span class="sub-hdr">🧑‍🏫 Staff Data</span>', unsafe_allow_html=True)
    if not sf_dl.empty:
        sf_exp = [c for c in ["Staff ID","INSTT","Name of the Staff","Department",
                               "dep code","Designation","Phone"] if c in sf_dl.columns]
        st.download_button(
            f"📥 Staff CSV — {len(sf_dl)} records",
            data=sf_dl[sf_exp].to_csv(index=False).encode(),
            file_name="staff_all.csv", mime="text/csv")
    else:
        st.markdown('<div class="info-card">ℹ️ No staff data loaded.</div>', unsafe_allow_html=True)

    st.markdown(
        f'<div style="text-align:center;margin-top:30px">'
        f'<span style="background:#f1f5f9;border:1px solid #e2e8f0;border-radius:20px;'
        f'padding:6px 20px;color:#94a3b8;font-size:.75rem">✨ {CREATOR}</span></div>',
        unsafe_allow_html=True)
