#!/usr/bin/env python3
# streamlit_app.py — Duty Manager v3
"""Practical Exam Panel Scheduling System
Created by MUTHUMANI S, LECTURER-EEE, GPT KARUR | 9443100811"""
from __future__ import annotations
import os, uuid, base64
from datetime import datetime, timedelta, date
import re
import streamlit as st
import pandas as pd
from io import BytesIO

try:
    from reportlab.lib import colors as RC
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                    Paragraph, Spacer, PageBreak)
    from reportlab.lib.enums import TA_CENTER
    RPDF = True
except ImportError:
    RPDF = False

# ─────────────────────────────────────────────
DATA_DIR          = "data"
PANEL_PATH        = os.path.join(DATA_DIR, "panel.csv")
PANEL_DATED_PATH  = os.path.join(DATA_DIR, "panel_dated.csv")
STAFF_PATH        = os.path.join(DATA_DIR, "staff.csv")
SUBMAP_PATH       = os.path.join(DATA_DIR, "submap.csv")
SUBJMAP_PATH      = os.path.join(DATA_DIR, "subjmap.csv")
os.makedirs(DATA_DIR, exist_ok=True)

CREATOR      = "MUTHUMANI S | LECTURER-EEE | GPT KARUR | 9443100811"
PANEL_COLS   = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","ERROR","__rowid"]
PDATE_COLS   = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_COLS   = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone","__rowid"]
SMAP_COLS    = ["Staff_Last_Staff_ID","Staff_Name","Department","Department_Code",
                "Subject_Type","Subject_Code","Subject_Name","Subject_Remarks"]

st.set_page_config(page_title="Duty Manager", page_icon="🗂️",
                   layout="wide", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════
# CSS  — Works on Streamlit Cloud (light/dark both)
# ═══════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── FORCE dark background everywhere ── */
.stApp,[data-testid="stAppViewContainer"],
[data-testid="stMain"],section[data-testid="stMain"],
[data-testid="stVerticalBlock"],.main,body {
    background-color:#0d1117 !important;
}

/* ── Hide sidebar & chrome ── */
[data-testid="stSidebar"],
[data-testid="collapsedControl"],
header[data-testid="stHeader"],
#MainMenu, footer { display:none !important; }

/* ── Layout ── */
.main .block-container { padding:0 1.4rem 2rem !important; max-width:100% !important; }

/* ══ ALL TEXT → visible on dark ══ */
*, *::before, *::after { box-sizing:border-box; }
body, .stApp { color:#c9d1d9 !important; }
p,li,span,div,label,td,th,caption,small,strong,b,i,em,
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span,
[data-testid="stMarkdownContainer"] li,
[data-testid="stMarkdownContainer"] td,
[data-testid="stMarkdownContainer"] th,
[data-testid="stText"] { color:#c9d1d9 !important; }
h1,h2,h3,h4,h5,h6 { color:#e6edf3 !important; }
a { color:#6366f1 !important; }

/* markdown tables */
[data-testid="stMarkdownContainer"] table { border-collapse:collapse; width:100%; }
[data-testid="stMarkdownContainer"] th {
    background:#1c2333 !important; color:#e6edf3 !important;
    padding:8px 12px !important; border:1px solid #30363d !important; font-weight:700;
}
[data-testid="stMarkdownContainer"] td {
    background:#0d1117 !important; color:#c9d1d9 !important;
    padding:6px 12px !important; border:1px solid #21262d !important;
}
[data-testid="stMarkdownContainer"] tr:nth-child(even) td { background:#0f1923 !important; }

/* code */
code,pre,.stCode code,
[data-testid="stCode"] code {
    background:#010409 !important; color:#79c0ff !important;
    font-size:.8rem !important; border-radius:6px !important;
}

/* caption */
[data-testid="stCaptionContainer"] p { color:#6e7681 !important; font-size:.76rem !important; }

/* ══ TOP NAVBAR ══ */
.topbar {
    background:#010409; border-bottom:1px solid #21262d;
    padding:0 20px; display:flex; align-items:center;
    justify-content:space-between; height:58px;
    margin:0 -1.4rem 0;
}
.tb-logo {
    background:linear-gradient(135deg,#6366f1,#8b5cf6);
    border-radius:9px; padding:7px 9px; font-size:1.2rem; line-height:1;
}
.tb-title { color:#e6edf3 !important; font-weight:700; font-size:1.05rem; line-height:1.15; }
.tb-sub   { color:#6e7681 !important; font-size:.68rem; font-weight:400; }
.tb-badge {
    color:#8b949e !important; font-size:.72rem;
    background:#161b22; border:1px solid #21262d;
    border-radius:20px; padding:4px 14px; white-space:nowrap;
}

/* ══ STATS BAR ══ */
.statsbar {
    display:flex; gap:6px; flex-wrap:wrap;
    background:#010409; border-bottom:1px solid #21262d;
    padding:8px 20px; margin:0 -1.4rem 1.1rem;
}
.sc {
    background:#161b22; border:1px solid #21262d; border-radius:6px;
    padding:3px 11px; font-size:.74rem; color:#8b949e !important; white-space:nowrap;
}
.sc b { color:#e6edf3 !important; font-size:.86rem; }

/* ══ TAB NAVIGATION ══ */
.stTabs [data-baseweb="tab-list"] {
    background:#010409 !important; border-bottom:1px solid #21262d !important;
    gap:0 !important; padding:0 4px !important; margin:0 -1.4rem 1.2rem !important;
    overflow-x:auto !important;
}
.stTabs [data-baseweb="tab"] {
    background:transparent !important; color:#8b949e !important;
    border:none !important; border-bottom:2px solid transparent !important;
    border-radius:0 !important; font-size:.88rem !important; font-weight:600 !important;
    padding:13px 22px !important; transition:all .15s !important; white-space:nowrap !important;
}
.stTabs [data-baseweb="tab"]:hover   { color:#e6edf3 !important; background:#161b22 !important; }
.stTabs [aria-selected="true"]       { color:#6366f1 !important; border-bottom-color:#6366f1 !important; }
.stTabs [data-baseweb="tab"] p       { color:inherit !important; font-size:inherit !important; font-weight:inherit !important; }
[data-testid="stTabsContent"]        { padding:0 !important; border:none !important; background:transparent !important; }

/* ══ SECTION HEADERS ══ */
.sec-hdr {
    background:linear-gradient(90deg,#6366f1,#8b5cf6);
    color:#ffffff !important; padding:8px 18px; border-radius:8px;
    font-weight:700; font-size:.96rem; margin:10px 0 8px;
    display:flex; align-items:center; gap:8px;
}
.sec-hdr *  { color:#ffffff !important; }
.sub-hdr    {
    color:#e6edf3 !important; font-size:.9rem; font-weight:700;
    padding:0 0 5px; border-bottom:1px solid #21262d; margin:10px 0 6px;
    display:block;
}

/* ══ CARDS ══ */
.err-card  { background:#2d1515; border-left:3px solid #ef4444; border-radius:6px; padding:8px 12px; margin:3px 0; font-size:.83rem; }
.err-card  *,.err-card  { color:#fca5a5 !important; }
.ok-card   { background:#0d2218; border-left:3px solid #22c55e; border-radius:6px; padding:8px 12px; margin:3px 0; font-size:.85rem; }
.ok-card   *,.ok-card   { color:#86efac !important; }
.warn-card { background:#2a1f0a; border-left:3px solid #f59e0b; border-radius:6px; padding:8px 12px; margin:3px 0; font-size:.83rem; }
.warn-card *,.warn-card { color:#fcd34d !important; }
.info-card { background:#0c1a2e; border-left:3px solid #3b82f6; border-radius:6px; padding:8px 12px; margin:3px 0; font-size:.83rem; }
.info-card *,.info-card { color:#93c5fd !important; }

/* ══ INPUTS ══ */
/* Selectbox */
[data-testid="stSelectbox"]>div>div {
    background:#161b22 !important; border:1px solid #30363d !important;
    border-radius:6px !important; color:#e6edf3 !important;
}
[data-testid="stSelectbox"] span { color:#e6edf3 !important; }
[data-testid="stSelectbox"] label p { color:#8b949e !important; font-size:.82rem !important; }
/* Dropdown */
[data-baseweb="popover"] ul,[data-baseweb="menu"]  { background:#161b22 !important; border:1px solid #21262d !important; }
[data-baseweb="menu"] li      { color:#c9d1d9 !important; background:#161b22 !important; }
[data-baseweb="menu"] li:hover{ background:#21262d !important; }
/* Text input */
[data-testid="stTextInput"] input {
    background:#161b22 !important; border:1px solid #30363d !important;
    color:#e6edf3 !important; border-radius:6px !important;
}
[data-testid="stTextInput"] label p { color:#8b949e !important; font-size:.82rem !important; }
/* Checkbox */
[data-testid="stCheckbox"] label p { color:#c9d1d9 !important; }
/* File uploader */
[data-testid="stFileUploader"]                { background:#161b22 !important; border:1px solid #21262d !important; border-radius:8px !important; }
[data-testid="stFileUploaderDropzone"]        { background:#0d1117 !important; border:1px dashed #30363d !important; border-radius:6px !important; }
[data-testid="stFileUploaderDropzone"] p,
[data-testid="stFileUploaderDropzone"] span   { color:#6e7681 !important; }
[data-testid="stFileUploaderDropzone"] button { background:#21262d !important; color:#c9d1d9 !important; border:1px solid #30363d !important; border-radius:6px !important; }
[data-testid="stFileUploaderFileName"]        { color:#c9d1d9 !important; }

/* ══ BUTTONS ══ */
.stButton>button { border-radius:7px !important; font-weight:600 !important; font-size:.85rem !important; transition:all .15s !important; }
.stButton>button[kind="primary"]   { background:linear-gradient(135deg,#6366f1,#8b5cf6) !important; border:none !important; color:#ffffff !important; }
.stButton>button[kind="primary"]:hover { opacity:.88 !important; transform:translateY(-1px) !important; }
.stButton>button[kind="secondary"] { background:#161b22 !important; border:1px solid #30363d !important; color:#e6edf3 !important; }
.stButton>button[kind="secondary"]:hover { border-color:#6366f1 !important; }

/* ══ DATA EDITOR ══ */
div[data-testid="stDataFrame"],
div[data-testid="stDataEditor"] { border-radius:8px !important; overflow:hidden !important; }

/* ══ EXPANDER ══ */
[data-testid="stExpander"]          { background:#161b22 !important; border:1px solid #21262d !important; border-radius:8px !important; }
[data-testid="stExpander"] summary  { background:#161b22 !important; border-radius:8px !important; padding:8px 16px !important; }
[data-testid="stExpander"] summary p{ color:#e6edf3 !important; font-weight:600 !important; }
[data-testid="stExpander"] svg      { fill:#8b949e !important; }
.streamlit-expanderContent          { background:#0d1117 !important; padding:12px !important; }

/* ══ METRICS ══ */
[data-testid="stMetric"]        { background:#161b22 !important; border:1px solid #21262d !important; border-radius:8px !important; padding:12px 14px !important; }
[data-testid="stMetricLabel"] p { color:#8b949e !important; font-size:.78rem !important; }
[data-testid="stMetricValue"] div,
[data-testid="stMetricValue"]   { color:#e6edf3 !important; }

/* ══ ALERTS ══ */
[data-testid="stAlertContainer"]>div { border-radius:8px !important; }
div[data-testid="stAlert"][data-baseweb="notification"] { background:#161b22 !important; border:1px solid #30363d !important; }

/* ══ SCROLLBAR ══ */
::-webkit-scrollbar            { width:5px; height:5px; }
::-webkit-scrollbar-track      { background:#0d1117; }
::-webkit-scrollbar-thumb      { background:#30363d; border-radius:3px; }
::-webkit-scrollbar-thumb:hover{ background:#6366f1; }

hr.thin { border:none; border-top:1px solid #21262d; margin:8px 0; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def parse_date(s):
    if s is None: return None
    try:
        if pd.isna(s): return None
    except: pass
    if isinstance(s,(datetime,date,pd.Timestamp)): return s.date() if hasattr(s,"date") else None
    t=str(s).strip()
    if not t: return None
    for f in ["%d.%m.%Y","%d/%m/%Y","%Y-%m-%d"]:
        try: return datetime.strptime(t,f).date()
        except: pass
    try: return pd.to_datetime(t,dayfirst=True).date()
    except: return None

def d2s(d):
    if d is None: return ""
    if isinstance(d,(datetime,pd.Timestamp)): d=d.date()
    return d.strftime("%d.%m.%Y")

def rowid(df,pre="r"):
    df=df.copy()
    if "__rowid" not in df.columns:
        df["__rowid"]=[f"{pre}_{uuid.uuid4().hex}" for _ in range(len(df))]
    else:
        df["__rowid"]=df["__rowid"].astype(str)
        m=df["__rowid"].str.strip()==""
        if m.any(): df.loc[m,"__rowid"]=[f"{pre}_{uuid.uuid4().hex}" for _ in range(m.sum())]
    return df

def load_csv(path,cols):
    if os.path.exists(path):
        try:
            df=pd.read_csv(path,dtype=object).fillna("")
            for c in cols:
                if c not in df.columns: df[c]=""
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def save_csv(df,path):
    try: df.to_csv(path,index=False); return True
    except Exception as e: st.error(f"Save failed: {e}"); return False

SPLIT_RE=re.compile(r"[,\uFF0C;|\-/\\_\s]+")

def norm_id(v):
    if v is None: return ""
    try:
        if isinstance(v,float) and v!=v: return ""
    except: pass
    s=str(v).strip()
    if s in ("","0","0.0","0.00"): return ""
    if re.fullmatch(r"-?\d+\.\d+",s):
        try:
            fv=float(s)
            if abs(fv-int(fv))<1e-9: s=str(int(fv))
        except: pass
    return "" if s=="0" else s.upper()

def is_zero(v): return str(v).strip() in ("0","0.0","0.00") if v else False
def split_toks(v):
    if not v: return []
    s=str(v).strip()
    return [p.strip() for p in SPLIT_RE.split(s) if p.strip()] if s else []
def is_busy(t):
    t2=str(t).strip().upper()
    return t2=="B" or bool(re.match(r"^B[\W_]*\d+$",t2))
def inscode_from_sid(sid):
    s=str(sid).strip()
    return s[1:4] if len(s)>=4 else ""
def get_name(sf,sid):
    sid=norm_id(sid)
    if not sid or sf.empty: return ""
    try:
        m=sf["Staff ID"].astype(str).str.upper()==sid
        return str(sf.loc[m,"Name of the Staff"].iloc[0]) if m.any() else ""
    except: return ""
def get_phone(sf,sid):
    sid=norm_id(sid)
    if not sid or sf.empty: return ""
    try:
        m=sf["Staff ID"].astype(str).str.upper()==sid
        return str(sf.loc[m,"Phone"].iloc[0]) if m.any() else ""
    except: return ""
def get_subname(sm,code):
    if sm is None or sm.empty: return ""
    m=sm[sm["SUBCODE"].astype(str)==str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def dl_link(df,fname,label,color="#6366f1"):
    b64=base64.b64encode(df.to_csv(index=False).encode()).decode()
    return (f'<a href="data:file/csv;base64,{b64}" download="{fname}" '
            f'style="background:{color};color:#fff;padding:6px 14px;border-radius:6px;'
            f'text-decoration:none;font-size:.8rem;display:inline-block;margin:2px 4px 2px 0;font-weight:600">{label}</a>')

def pdf_dl(pdf_b,fname,label):
    b64=base64.b64encode(pdf_b).decode()
    return (f'<a href="data:application/pdf;base64,{b64}" download="{fname}" '
            f'style="background:#ef4444;color:#fff;padding:6px 14px;border-radius:6px;'
            f'text-decoration:none;font-size:.8rem;display:inline-block;margin:2px 4px 2px 0;font-weight:600">{label}</a>')

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
for key,path,cols,pre in [
    ("panel",  PANEL_PATH,       PANEL_COLS,  "p"),
    ("pdate",  PANEL_DATED_PATH, PDATE_COLS,  "d"),
    ("staff",  STAFF_PATH,       STAFF_COLS,  "s"),
]:
    if key not in st.session_state:
        df=load_csv(path,cols); df=rowid(df,pre)
        for c in cols:
            if c not in df.columns: df[c]=""
        st.session_state[key]=df.copy()

if "submap" not in st.session_state:
    st.session_state.submap=load_csv(SUBMAP_PATH,["SUBCODE","SUBNAME"]).copy()
if "ssmap" not in st.session_state:
    sm2=load_csv(SUBJMAP_PATH,SMAP_COLS)
    for c in SMAP_COLS:
        if c not in sm2.columns: sm2[c]=""
    st.session_state.ssmap=sm2.copy()
if "staged" not in st.session_state: st.session_state.staged={}
if "errors" not in st.session_state: st.session_state.errors={}

def P():  st.session_state.panel=rowid(st.session_state.panel,"p"); save_csv(st.session_state.panel,PANEL_PATH)
def PD(): st.session_state.pdate=rowid(st.session_state.pdate,"d"); save_csv(st.session_state.pdate,PANEL_DATED_PATH)
def S():  st.session_state.staff=rowid(st.session_state.staff,"s"); save_csv(st.session_state.staff,STAFF_PATH)
def SM(): save_csv(st.session_state.submap,SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap,SUBJMAP_PATH)

# ─────────────────────────────────────────────
# LOGIC
# ─────────────────────────────────────────────
def duty_stats(sf):
    stats={}
    if sf is None or sf.empty: return stats
    dcols=[c for c in sf.columns if c!="__rowid" and isinstance(c,str)
           and len(c.split("."))==3 and all(p.isdigit() for p in c.split("."))]
    for _,row in sf.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        cnt=sum(1 for dc in dcols for t in split_toks(row.get(dc,"")) if not is_busy(t))
        stats[sid]={"count":cnt,"INSTT":row.get("INSTT",""),"dep":row.get("dep code",""),
                    "name":row.get("Name of the Staff",""),"desig":row.get("Designation",""),
                    "phone":row.get("Phone","")}
    return stats

def ext_suggestions(panel_row,sf,ssmap):
    p_ins=str(panel_row.get("INSCODE","")).strip()
    sub=str(panel_row.get("SUBCODE","")).strip().upper()
    p_dep=str(panel_row.get("NCNO","")).strip()
    stats=duty_stats(sf)
    if ssmap is not None and not ssmap.empty:
        mapped=ssmap[ssmap["Subject_Code"].astype(str).str.strip().str.upper()==sub]
        mapped_ids=set(mapped["Staff_Last_Staff_ID"].apply(norm_id).unique())
    else:
        mapped_ids=None
    res=[]
    for _,row in sf.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        instt=str(row.get("INSTT","")).strip()
        if instt==p_ins: continue
        dep=str(row.get("dep code","")).strip()
        if mapped_ids is not None:
            if sid not in mapped_ids: continue
        elif dep!=p_dep: continue
        se=stats.get(sid,{})
        res.append({"sid":sid,"name":row.get("Name of the Staff",""),
                    "desig":row.get("Designation",""),"instt":instt,
                    "dep":dep,"phone":row.get("Phone",""),"count":se.get("count",0)})
    res.sort(key=lambda x:x["count"])
    return res

def make_lbl(s): return f"🟢 {s['sid']} — {s['name']} — {s['desig']} — {s['instt']}"

def auto_allocate(candidates,sf,ssmap):
    res,skip={},{}
    for pidx,row in candidates.iterrows():
        suggs=ext_suggestions(row,sf,ssmap)
        if suggs: res[pidx]=make_lbl(suggs[0])
        else: skip[pidx]=f"No eligible external staff for SUBCODE {row.get('SUBCODE','?')}"
    return res,skip

def check_errors(pdf,sf):
    errs={i:[] for i in pdf.index}
    sd={}
    for idx,row in pdf.iterrows():
        d1=parse_date(row.get("DATE_FROM")); d2=parse_date(row.get("DATE_TO"))
        sc=str(row.get("SUBCODE","")).strip()
        ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if is_zero(row.get(fld,"")): sid=""
            if not sid: continue
            s_ins=inscode_from_sid(sid)
            if role=="INT" and s_ins and s_ins!=ins:
                errs[idx].append(f"❌ INTID {sid}: home-inst {s_ins} ≠ exam-inst {ins}")
            if role=="EXT" and s_ins and s_ins==ins:
                errs[idx].append(f"❌ EXTID {sid}: home-inst {s_ins} == exam-inst {ins} (must differ)")
            sd.setdefault(sid,[]).append((idx,sc,d1,d2,role))
    for sid,duties in sd.items():
        for i in range(len(duties)):
            ia,sca,d1a,d2a,ra=duties[i]
            if not(d1a and d2a): continue
            for j in range(i+1,len(duties)):
                ib,scb,d1b,d2b,rb=duties[j]
                if not(d1b and d2b): continue
                if max(d1a,d1b)<=min(d2a,d2b) and sca!=scb:
                    msg=f"⚠️ {sid} CLASH: {sca}({d2s(d1a)}→{d2s(d2a)}) overlaps {scb}({d2s(d1b)}→{d2s(d2b)})"
                    errs[ia].append(msg); errs[ib].append(msg)
    return {k:v for k,v in errs.items() if v}

# ─────────────────────────────────────────────
# PDF
# ─────────────────────────────────────────────
def generate_pdf(panel_df,sf,submap):
    buf=BytesIO()
    if not RPDF:
        buf.write(b"Install: pip install reportlab"); return buf.getvalue()
    doc=SimpleDocTemplate(buf,pagesize=A4,
        leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=1.5*cm,bottomMargin=1.5*cm)
    H1=ParagraphStyle("H1",fontSize=12,fontName="Helvetica-Bold",spaceAfter=4,alignment=TA_CENTER)
    SML=ParagraphStyle("SML",fontSize=7,fontName="Helvetica",textColor=RC.grey,alignment=TA_CENTER)
    story=[]
    sd={}
    for _,row in panel_df.iterrows():
        sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
        ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if not sid: continue
            sd.setdefault(sid,[]).append({"ins":ins,"sc":sc,"sn":sn,"role":role,
                "cid":norm_id(row.get("EXTID" if role=="INT" else "INTID",""))})
    for sid in sorted(sd.keys(),key=lambda s:get_name(sf,s)):
        duties=sd.get(sid,[])
        if not duties: continue
        name=get_name(sf,sid); phone=get_phone(sf,sid)
        m=sf[sf["Staff ID"].astype(str).str.upper()==sid]
        desig=str(m.iloc[0]["Designation"]) if not m.empty else ""
        dept =str(m.iloc[0]["Department"])  if not m.empty else ""
        instt=str(m.iloc[0]["INSTT"])       if not m.empty else ""
        story.append(Paragraph("PRACTICAL EXAM DUTY ORDER",H1))
        story.append(Paragraph(CREATOR,SML)); story.append(Spacer(1,.3*cm))
        ht=Table([["Staff ID",sid,"Name",name],["Institution",instt,"Phone",phone],
                  ["Department",dept,"Designation",desig]],
                 colWidths=[2.5*cm,4.5*cm,2.5*cm,7*cm])
        ht.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1),RC.HexColor("#0d1117")),
            ("TEXTCOLOR",(0,0),(-1,-1),RC.white),
            ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),("GRID",(0,0),(-1,-1),.4,RC.HexColor("#21262d")),
            ("PADDING",(0,0),(-1,-1),5),
        ]))
        story.append(ht); story.append(Spacer(1,.4*cm))
        tr=[["S.No","Duty\nINSCODE","SubCode","Subject Name","Role",
             "Partner ID","Partner Name","Partner Phone","Date From","Date To"]]
        for sno,d in enumerate(duties,1):
            pid=d["cid"]; pn=get_name(sf,pid) if pid else ""; pp=get_phone(sf,pid) if pid else ""
            tr.append([str(sno),d["ins"],d["sc"],d["sn"] or d["sc"],d["role"],
                       pid or "-",pn or "-",pp or "-","",""])
        dt=Table(tr,colWidths=[.9*cm,2*cm,2*cm,4.2*cm,1.2*cm,2.2*cm,3.5*cm,2.2*cm,2*cm,2*cm],repeatRows=1)
        dt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),RC.HexColor("#6366f1")),("TEXTCOLOR",(0,0),(-1,0),RC.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),7),
            ("ALIGN",(0,0),(-1,-1),"CENTER"),("ALIGN",(3,1),(3,-1),"LEFT"),("ALIGN",(6,1),(6,-1),"LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[RC.HexColor("#f8fafc"),RC.HexColor("#e2e8f0")]),
            ("GRID",(0,0),(-1,-1),.4,RC.HexColor("#94a3b8")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("PADDING",(0,0),(-1,-1),4),
        ]))
        story.append(dt); story.append(Spacer(1,.3*cm))
        story.append(Paragraph("Date From/To to be filled by Flying Squad at duty.",SML))
        story.append(PageBreak())
    doc.build(story)
    return buf.getvalue()

# ─────────────────────────────────────────────
# TOP BAR + STATS (always visible)
# ─────────────────────────────────────────────
pn=len(st.session_state.panel); pdn=len(st.session_state.pdate)
ef=st.session_state.panel["EXTID"].apply(lambda v:norm_id(v)!="").sum() if pn else 0
ep=pn-ef; sc2=len(st.session_state.staff); sm2c=len(st.session_state.ssmap)
stg=len(st.session_state.staged)

st.markdown(f"""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:10px">
    <div class="tb-logo">🗂️</div>
    <div>
      <div class="tb-title">DUTY MANAGER</div>
      <div class="tb-sub">PRACTICAL EXAM PANEL</div>
    </div>
  </div>
  <div class="tb-badge">👤 {CREATOR}</div>
</div>
<div class="statsbar">
  <div class="sc">📋 Panel (No Date) <b>{pn}</b></div>
  <div class="sc">✅ EXTID Filled <b style="color:#22c55e">{ef}</b></div>
  <div class="sc">⏳ Pending <b style="color:#f59e0b">{ep}</b></div>
  <div class="sc">🗓️ Dated Panel <b style="color:#8b5cf6">{pdn}</b></div>
  <div class="sc">🧑‍🏫 Staff <b style="color:#3b82f6">{sc2}</b></div>
  <div class="sc">📘 SubjectMap <b style="color:#ec4899">{sm2c}</b></div>
  <div class="sc">🔖 Staged <b style="color:#f59e0b">{stg}</b></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────
tab_up, tab_ext, tab_duty = st.tabs([
    "  📥  Upload Centre  ",
    "  🎯  EXT Allocate  ",
    "  ▶️   Duty Marking  ",
])

# ═══════════════════════════════════════════════════════
# TAB 1 — UPLOAD CENTRE
# ═══════════════════════════════════════════════════════
with tab_up:
    st.markdown('<div class="sec-hdr">📥 Upload Centre — Panel · Staff · Subject-Staff Mapping</div>',unsafe_allow_html=True)
    s1,s2,s3=st.tabs(["  📋  Panel (No Dates)  ","  🧑‍🏫  Staff Details  ","  📘  Subject-Staff Mapping  "])

    with s1:
        ul,ur=st.columns([1,1],gap="medium")
        with ul:
            st.markdown('<span class="sub-hdr">📂 Upload Panel CSV / XLSX</span>',unsafe_allow_html=True)
            st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID",language="")
            uf=st.file_uploader("",type=["csv","xlsx"],key="p_up",label_visibility="collapsed")
            cl=st.checkbox("Clear ALL rows before upload",key="p_cl")
            if uf:
                try:
                    tmp=(pd.read_csv(uf,dtype=object) if uf.name.lower().endswith(".csv")
                         else pd.read_excel(uf,dtype=object,sheet_name=0)).fillna("")
                    req=["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                    miss=[c for c in req if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing columns: {', '.join(miss)}")
                    else:
                        tmp=tmp[req].copy(); tmp["ERROR"]=""; tmp=rowid(tmp,"p")
                        if cl:
                            st.session_state.panel=rowid(tmp.reset_index(drop=True),"p")
                        else:
                            ins_up=[str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                            bk=st.session_state.panel.copy()
                            bk=bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                            bk=pd.concat([bk,tmp],ignore_index=True)
                            st.session_state.panel=rowid(bk.reset_index(drop=True),"p")
                        P(); st.success(f"✅ {len(tmp)} rows uploaded")
                except Exception as e: st.error(f"❌ {e}")

            st.markdown('<hr class="thin">',unsafe_allow_html=True)
            st.markdown('<span class="sub-hdr">📂 SUBCODE → SUBNAME Mapping</span>',unsafe_allow_html=True)
            sf2=st.file_uploader("",type=["csv","xlsx"],key="sub_up",label_visibility="collapsed")
            if sf2:
                try:
                    sm2=(pd.read_csv(sf2,dtype=object) if sf2.name.lower().endswith(".csv")
                         else pd.read_excel(sf2,dtype=object,sheet_name=0)).fillna("")
                    if "SUBCODE" not in sm2.columns or "SUBNAME" not in sm2.columns:
                        if sm2.shape[1]>=2:
                            sm2=pd.DataFrame({"SUBCODE":sm2.iloc[:,0].astype(str),"SUBNAME":sm2.iloc[:,1].astype(str)})
                    st.session_state.submap=sm2[["SUBCODE","SUBNAME"]].copy(); SM()
                    st.success("✅ SUBNAME mapping saved")
                except Exception as e: st.error(f"❌ {e}")

            st.markdown('<span class="sub-hdr">✏️ Edit SUBCODE → SUBNAME</span>',unsafe_allow_html=True)
            smv=st.session_state.submap.copy()
            esm=st.data_editor(
                smv[["SUBCODE","SUBNAME"]].fillna("") if not smv.empty else pd.DataFrame(columns=["SUBCODE","SUBNAME"]),
                key="sm_ed",use_container_width=True,height=160,num_rows="dynamic")
            if st.button("💾 Save SUBNAME Mapping",key="sm_sv",use_container_width=True):
                st.session_state.submap=esm.copy(); SM(); st.success("✅ Saved")

        with ur:
            pv=st.session_state.panel.copy()
            if not st.session_state.submap.empty:
                pv=pv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            show=[c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID","EXTID","ERROR"] if c in pv.columns]
            fi_c,fd_c=st.columns(2)
            pfi=fi_c.selectbox("🏫 INSCODE",["All"]+sorted(set(pv["INSCODE"].astype(str))),key="pf_i")
            pfd=fd_c.selectbox("🏭 NCNO",   ["All"]+sorted(set(pv["NCNO"].astype(str))),   key="pf_n")
            pv2=pv.copy()
            if pfi!="All": pv2=pv2[pv2["INSCODE"].astype(str)==pfi]
            if pfd!="All": pv2=pv2[pv2["NCNO"].astype(str)==pfd]
            st.markdown(f'<span class="sub-hdr">📋 Panel Preview — {len(pv2)} rows &nbsp;<small style="color:#6e7681;font-weight:400">(inline editable)</small></span>',unsafe_allow_html=True)
            ep2=st.data_editor(pv2[show].fillna(""),key="p_ed",use_container_width=True,height=350,num_rows="dynamic")
            if st.button("💾 Save Panel Changes",key="p_sv",use_container_width=True):
                try:
                    bk=st.session_state.panel.copy()
                    if "__rowid" not in ep2.columns: ep2["__rowid"]=""
                    ed=rowid(ep2.copy(),"p")
                    if "SUBNAME" in ed.columns: ed=ed.drop(columns=["SUBNAME"])
                    if "ERROR" not in ed.columns: ed["ERROR"]=""
                    bk_i=bk.set_index("__rowid",drop=False); ed_i=ed.set_index("__rowid",drop=False)
                    for rid in bk_i.index.intersection(ed_i.index):
                        for c in ed_i.columns: bk_i.at[rid,c]=ed_i.at[rid,c]
                    new=[r for r in ed_i.index if r not in bk_i.index]
                    if new:
                        bk_i=pd.concat([bk_i.reset_index(drop=True),ed_i.loc[new].reset_index(drop=True)],ignore_index=True)
                    st.session_state.panel=rowid(bk_i.reset_index(drop=True),"p")
                    P(); st.success("✅ Panel saved")
                except Exception as e: st.error(f"❌ {e}")

    with s2:
        sl,sr=st.columns([1,1],gap="medium")
        with sl:
            st.markdown('<span class="sub-hdr">📂 Upload Staff CSV / XLSX</span>',unsafe_allow_html=True)
            st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation  Phone",language="")
            st.markdown('<div class="info-card">📌 <b>Phone</b> column required for printed duty sheets</div>',unsafe_allow_html=True)
            sample_s=pd.DataFrame([{"Staff ID":"X123EEE1","INSTT":"123","Name of the Staff":"KUMAR S",
                "Department":"EEE","dep code":"1030","Designation":"Lecturer","Phone":"9876543210"}])
            st.markdown(dl_link(sample_s,"sample_staff.csv","📥 Sample Staff CSV"),unsafe_allow_html=True)
            st.markdown("")
            usf=st.file_uploader("",type=["csv","xlsx"],key="s_up",label_visibility="collapsed")
            if usf:
                try:
                    tmp=(pd.read_csv(usf,dtype=object) if usf.name.lower().endswith(".csv")
                         else pd.read_excel(usf,dtype=object,sheet_name=0)).fillna("")
                    req=["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation"]
                    miss=[c for c in req if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp["Staff ID"]=tmp["Staff ID"].apply(norm_id)
                        if "Phone" not in tmp.columns: tmp["Phone"]=""
                        for c in STAFF_COLS:
                            if c not in tmp.columns: tmp[c]=""
                        st.session_state.staff=rowid(tmp,"s")[STAFF_COLS].copy()
                        S(); st.success(f"✅ {len(tmp)} staff loaded")
                except Exception as e: st.error(f"❌ {e}")
        with sr:
            sv=st.session_state.staff.copy()
            fi2,fd2=st.columns(2)
            fi_s=fi2.selectbox("🏫 INSTT",["All"]+sorted(set(sv["INSTT"].astype(str))),key="sf_i")
            fd_s=fd2.selectbox("🏭 Dept", ["All"]+sorted(set(sv["Department"].astype(str))),key="sf_d")
            if fi_s!="All": sv=sv[sv["INSTT"].astype(str)==fi_s]
            if fd_s!="All": sv=sv[sv["Department"].astype(str)==fd_s]
            dcols=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"] if c in sv.columns]
            st.markdown(f'<span class="sub-hdr">🧑‍🏫 Staff Preview — {len(sv)} rows &nbsp;<small style="color:#6e7681;font-weight:400">(inline editable)</small></span>',unsafe_allow_html=True)
            es=st.data_editor(sv[dcols],key="s_ed",use_container_width=True,height=400,num_rows="dynamic")
            if st.button("💾 Save Staff Changes",key="s_sv",use_container_width=True):
                try:
                    bk=st.session_state.staff.copy()
                    if "__rowid" not in es.columns: es["__rowid"]=""
                    ed=rowid(es.copy(),"s")
                    bk_i=bk.set_index("__rowid",drop=False); ed_i=ed.set_index("__rowid",drop=False)
                    for rid in bk_i.index.intersection(ed_i.index):
                        for c in ed_i.columns: bk_i.at[rid,c]=ed_i.at[rid,c]
                    new=[r for r in ed_i.index if r not in bk_i.index]
                    if new:
                        bk_i=pd.concat([bk_i.reset_index(drop=True),ed_i.loc[new].reset_index(drop=True)],ignore_index=True)
                    st.session_state.staff=rowid(bk_i.reset_index(drop=True),"s")
                    S(); st.success("✅ Staff saved")
                except Exception as e: st.error(f"❌ {e}")

    with s3:
        ml,mr=st.columns([1,1],gap="medium")
        with ml:
            st.markdown('<span class="sub-hdr">📂 Upload Subject-Staff Mapping</span>',unsafe_allow_html=True)
            st.code("\n".join(SMAP_COLS),language="")
            sample_sm=pd.DataFrame([{"Staff_Last_Staff_ID":"X123EEE1","Staff_Name":"KUMAR S",
                "Department":"EEE","Department_Code":"1030","Subject_Type":"Core",
                "Subject_Code":"P3401","Subject_Name":"Basic Electrical Lab","Subject_Remarks":""}])
            st.markdown(
                dl_link(pd.DataFrame(columns=SMAP_COLS),"ssmap_template.csv","📥 Empty Template")
                +"&nbsp;"+dl_link(sample_sm,"ssmap_sample.csv","📥 Sample CSV","#8b5cf6"),
                unsafe_allow_html=True)
            st.markdown("")
            ussm=st.file_uploader("",type=["csv","xlsx"],key="ssm_up",label_visibility="collapsed")
            if ussm:
                try:
                    tmp=(pd.read_csv(ussm,dtype=object) if ussm.name.lower().endswith(".csv")
                         else pd.read_excel(ussm,dtype=object,sheet_name=0)).fillna("")
                    miss=[c for c in SMAP_COLS if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp["Staff_Last_Staff_ID"]=tmp["Staff_Last_Staff_ID"].apply(norm_id)
                        tmp["Subject_Code"]=tmp["Subject_Code"].astype(str).str.strip().str.upper()
                        st.session_state.ssmap=tmp[SMAP_COLS].copy()
                        SS(); st.success(f"✅ {len(tmp)} mapping rows loaded")
                except Exception as e: st.error(f"❌ {e}")
        with mr:
            ssv=st.session_state.ssmap.copy()
            sf3,sf4=st.columns(2)
            dm_f=sf3.selectbox("🏭 Dept",["All"]+sorted(set(ssv["Department"].astype(str))),key="ssm_d")
            sc_f=sf4.text_input("","",key="ssm_s",placeholder="🔍 Subject Code...")
            if dm_f!="All": ssv=ssv[ssv["Department"]==dm_f]
            if sc_f.strip(): ssv=ssv[ssv["Subject_Code"].str.contains(sc_f.strip().upper(),na=False)]
            st.markdown(f'<span class="sub-hdr">📘 Mapping Preview — {len(ssv)} rows &nbsp;<small style="color:#6e7681;font-weight:400">(inline editable)</small></span>',unsafe_allow_html=True)
            essm=st.data_editor(ssv.fillna(""),key="ssm_ed",use_container_width=True,height=400,num_rows="dynamic")
            if st.button("💾 Save Mapping Changes",key="ssm_sv",use_container_width=True):
                try:
                    ed=essm.copy()
                    for c in SMAP_COLS:
                        if c not in ed.columns: ed[c]=""
                    ed_c=ed[SMAP_COLS].copy()
                    ed_c["Subject_Code"]=ed_c["Subject_Code"].astype(str).str.strip().str.upper()
                    bk=st.session_state.ssmap.copy()
                    if not bk.empty:
                        bk_i=bk.set_index("Subject_Code",drop=False)
                        ed_i=ed_c.set_index("Subject_Code",drop=False)
                        for rid in bk_i.index.intersection(ed_i.index):
                            for c in ed_i.columns: bk_i.at[rid,c]=ed_i.at[rid,c]
                        new=[r for r in ed_i.index if r not in bk_i.index]
                        if new:
                            bk_i=pd.concat([bk_i.reset_index(drop=True),ed_i.loc[new].reset_index(drop=True)],ignore_index=True)
                        st.session_state.ssmap=bk_i.reset_index(drop=True)
                    else:
                        st.session_state.ssmap=ed_c.copy()
                    SS(); st.success("✅ Mapping saved")
                except Exception as e: st.error(f"❌ {e}")

# ═══════════════════════════════════════════════════════
# TAB 2 — EXT ALLOCATE
# ═══════════════════════════════════════════════════════
with tab_ext:
    st.markdown('<div class="sec-hdr">🎯 EXT Allocate — Assign External Examiners (No Date Check)</div>',unsafe_allow_html=True)
    with st.expander("ℹ️ Allocation Logic Used"):
        st.markdown("""
| # | Rule | Detail |
|---|------|--------|
| 1 | **Subject Match** | Staff mapped to panel SUBCODE via Subject-Staff Mapping |
| 2 | **External Rule** | Staff INSTT must differ from panel INSCODE |
| 3 | **Least Duty** | Staff with minimum duty count is preferred |
| 4 | **Fallback** | No SubjectMap → matches `dep code` == panel `NCNO` |
        """)

    panel=st.session_state.panel.copy()
    sf=st.session_state.staff.copy()
    ssmap=st.session_state.ssmap.copy()
    submap=st.session_state.submap.copy()

    def needs_ext2(r):
        return str(r.get("INTID","")).strip()!="" and (str(r.get("EXTID","")).strip()=="" or is_zero(r.get("EXTID","")))

    candidates=panel[panel.apply(needs_ext2,axis=1)].copy()
    fc1,fc2=st.columns(2)
    ins_f=fc1.selectbox("🏫 Filter INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ea_i")
    nc_f =fc2.selectbox("🏭 Filter NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ea_n")
    if ins_f!="All": candidates=candidates[candidates["INSCODE"].astype(str)==ins_f]
    if nc_f !="All": candidates=candidates[candidates["NCNO"].astype(str)==nc_f]

    m1,m2,m3,m4=st.columns(4)
    m1.metric("📋 Pending EXTID",len(candidates))
    m2.metric("🧑‍🏫 Staff Loaded",len(sf))
    m3.metric("📘 SubjectMap",len(ssmap))
    m4.metric("🔖 Staged",len(st.session_state.staged))

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">🤖 Auto-Allocate</div>',unsafe_allow_html=True)
    st.markdown('<div class="info-card">Matches SUBCODE → SubjectMap → Different INSTT → Least Duty Count → Stages all visible rows automatically</div>',unsafe_allow_html=True)
    if st.button("🤖 Auto-Allocate ALL Visible Rows",type="primary"):
        if sf.empty: st.error("❌ Upload staff data first!")
        else:
            res,skip=auto_allocate(candidates,sf,ssmap if not ssmap.empty else None)
            for k,v in res.items(): st.session_state.staged[str(k)]=v
            st.success(f"✅ Auto-staged {len(res)} rows.")
            if skip:
                with st.expander(f"⚠️ {len(skip)} rows skipped"):
                    st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]),use_container_width=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">📝 Per-Row Manual Allocation</div>',unsafe_allow_html=True)

    if candidates.empty:
        st.markdown('<div class="ok-card">🎉 All visible rows have EXTID assigned!</div>',unsafe_allow_html=True)
    else:
        for _,row in candidates.reset_index().iterrows():
            pidx=int(row["index"])
            sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
            ins=str(row.get("INSCODE","")).strip(); intid=str(row.get("INTID","")).strip()
            intname=get_name(sf,intid); sv_val=st.session_state.staged.get(str(pidx),"")
            suggs=ext_suggestions(row,sf,ssmap if not ssmap.empty else None)
            slabels=["— Select Staff —"]+[make_lbl(s) for s in suggs]
            with st.container():
                h1,h2,h3=st.columns([3,3,2])
                h1.markdown(f'<div style="color:#e6edf3;font-size:.87rem;padding:2px 0">🏫 <b>{ins}</b> · 🏭 {row.get("NCNO","")} · 📚 <code style="background:#161b22;padding:1px 6px;border-radius:4px;color:#79c0ff">{sc}</code>{(" · "+sn) if sn else ""}</div>',unsafe_allow_html=True)
                h2.markdown(f'<div style="color:#8b949e;font-size:.82rem;padding:2px 0">👥 {row.get("NOC","")} · INT: <code style="background:#161b22;padding:1px 5px;border-radius:4px;color:#fbbf24">{intid}</code> <span style="color:#c9d1d9">{intname}</span></div>',unsafe_allow_html=True)
                sv_short=(sv_val[:38]+"…") if len(sv_val)>38 else sv_val
                h3.markdown(f'<div style="font-size:.8rem;color:{"#22c55e" if sv_val else "#6e7681"};padding:2px 0">{"✅ "+sv_short if sv_val else "⬜ Not staged"}</div>',unsafe_allow_html=True)
                r1,r2,r3=st.columns([4,3,1])
                di=slabels.index(sv_val) if sv_val in slabels else 0
                sel=r1.selectbox("",slabels,index=di,key=f"sel_{pidx}_{sv_val[:8]}",label_visibility="collapsed")
                man=r2.text_input("","",key=f"man_{pidx}",placeholder="✏️ Manual Staff ID",label_visibility="collapsed")
                if sel and sel!="— Select Staff —": st.session_state.staged[str(pidx)]=sel
                if man.strip(): st.session_state.staged[str(pidx)]=man.strip()
                if r3.button("▶",key=f"app_{pidx}"):
                    chosen=sv_val or (sel if sel!="— Select Staff —" else "") or man.strip()
                    if not chosen: st.warning("⚠️ Select or enter a Staff ID")
                    else:
                        lc=str(chosen).replace("🟢 ","").split("—")
                        sid_c=norm_id(lc[0].strip()) if lc else ""
                        if sid_c:
                            st.session_state.panel.at[pidx,"EXTID"]=sid_c; P()
                            st.session_state.staged.pop(str(pidx),None); st.success(f"✅ EXTID {sid_c} applied")
                        else: st.error("❌ Invalid Staff ID")
                st.markdown('<hr class="thin">',unsafe_allow_html=True)

    staged_map=st.session_state.staged
    if staged_map:
        st.markdown('<hr class="thin">',unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🚀 Apply All Staged</div>',unsafe_allow_html=True)
        with st.expander(f"👁️ Preview {len(staged_map)} staged rows"):
            rows=[]
            for k,v in list(staged_map.items())[:30]:
                try:
                    pi=int(k); r=st.session_state.panel.loc[pi] if pi in st.session_state.panel.index else {}
                    rows.append({"Idx":k,"INSCODE":r.get("INSCODE","?"),"SUBCODE":r.get("SUBCODE","?"),"→ EXTID":v})
                except: rows.append({"Idx":k,"→ EXTID":v})
            st.dataframe(pd.DataFrame(rows),use_container_width=True,height=200)
        a1,a2=st.columns(2)
        if a1.button("✅ Apply ALL Staged",type="primary",use_container_width=True):
            ok_c,fc2b=[],[]
            for k,v in list(staged_map.items()):
                try: pi=int(k)
                except: fc2b.append(k); continue
                if pi not in st.session_state.panel.index: fc2b.append(k); continue
                lc=str(v).replace("🟢 ","").split("—")
                sid_c=norm_id(lc[0].strip()) if lc else ""
                if sid_c:
                    st.session_state.panel.at[pi,"EXTID"]=sid_c
                    st.session_state.staged.pop(k,None); ok_c.append(k)
                else: fc2b.append(k)
            P(); st.success(f"✅ Applied {len(ok_c)} · ❌ Failed {len(fc2b)}")
        if a2.button("🗑️ Clear All Staged",use_container_width=True):
            st.session_state.staged={}; st.success("✅ Staged cleared")

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">📥 Downloads</div>',unsafe_allow_html=True)
    all_p=st.session_state.panel.copy()
    if not all_p.empty:
        dc1,dc2,dc3=st.columns(3)
        inscodes=sorted(set(all_p["INSCODE"].astype(str)))
        exp=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in all_p.columns]
        with dc1:
            st.markdown('<span class="sub-hdr">📊 CSV per Institution</span>',unsafe_allow_html=True)
            for ins in inscodes:
                df_i=all_p[all_p["INSCODE"].astype(str)==ins][exp]
                st.markdown(dl_link(df_i,f"panel_{ins}.csv",f"📥 INSCODE {ins}"),unsafe_allow_html=True)
        with dc2:
            st.markdown('<span class="sub-hdr">📄 Full Panel CSV</span>',unsafe_allow_html=True)
            st.markdown(dl_link(all_p[exp],"panel_full.csv","📥 Full Panel CSV"),unsafe_allow_html=True)
        with dc3:
            st.markdown('<span class="sub-hdr">🖨️ PDF Duty Sheets</span>',unsafe_allow_html=True)
            if RPDF:
                if st.button("⚙️ Generate PDF Duty Sheets",use_container_width=True):
                    with st.spinner("Building PDF..."):
                        pdf_b=generate_pdf(all_p,sf,submap)
                    st.markdown(pdf_dl(pdf_b,"duty_sheets.pdf","📄 Download PDF"),unsafe_allow_html=True)
            else:
                st.markdown('<div class="warn-card">⚠️ pip install reportlab</div>',unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# TAB 3 — DUTY MARKING
# ═══════════════════════════════════════════════════════
with tab_duty:
    st.markdown('<div class="sec-hdr">▶️ Duty Marking — Upload Dated Panel & Validate Clashes</div>',unsafe_allow_html=True)
    with st.expander("ℹ️ Error-Check Rules"):
        st.markdown("""
| # | Check | Rule |
|---|-------|------|
| 🔴 1 | **Institution Rule** | INTID chars[1:4] == INSCODE; EXTID chars[1:4] ≠ INSCODE |
| 🔴 2 | **Single-Day Clash** | Same staff · same date · different SUBCODE = ❌ |
| 🔴 3 | **Multi-Day Overlap** | Overlapping date ranges same staff different duties = ❌ |
        """)

    d1c,d2c=st.columns([1,1],gap="medium")
    with d1c:
        st.markdown('<span class="sub-hdr">📂 Upload Dated Panel CSV / XLSX</span>',unsafe_allow_html=True)
        st.code("INSCODE NCNO SUBCODE REGL NOC NOB INTID EXTID DATE_FROM DATE_TO",language="")
        udp=st.file_uploader("",type=["csv","xlsx"],key="dp_up",label_visibility="collapsed")
        cl2=st.checkbox("Clear existing dated panel",key="dp_cl")
        if udp:
            try:
                tmp=(pd.read_csv(udp,dtype=object) if udp.name.lower().endswith(".csv")
                     else pd.read_excel(udp,dtype=object,sheet_name=0)).fillna("")
                req=["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
                miss=[c for c in req if c not in tmp.columns]
                if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                else:
                    tmp=tmp[req].copy(); tmp["ERROR"]=""; tmp=rowid(tmp,"d")
                    if cl2:
                        st.session_state.pdate=rowid(tmp.reset_index(drop=True),"d")
                    else:
                        ins_up=[str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                        bk=st.session_state.pdate.copy()
                        bk=bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                        bk=pd.concat([bk,tmp],ignore_index=True)
                        st.session_state.pdate=rowid(bk.reset_index(drop=True),"d")
                    PD(); st.success(f"✅ {len(tmp)} dated rows loaded")
            except Exception as e: st.error(f"❌ {e}")

    with d2c:
        pdv=st.session_state.pdate.copy()
        pdv["_d"]=pdv["DATE_FROM"].apply(parse_date)
        pdv=pdv.sort_values("_d",na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdv=pdv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
        show=[c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"] if c in pdv.columns]
        st.markdown(f'<span class="sub-hdr">🗓️ Dated Panel — {len(pdv)} rows</span>',unsafe_allow_html=True)
        st.dataframe(pdv[show].fillna(""),use_container_width=True,height=280)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    gc1,gc2,gc3=st.columns([2,2,2])
    ins_g=gc1.selectbox("🏫 INSCODE",["All"]+sorted(set(st.session_state.pdate["INSCODE"].astype(str))),key="dm_i")
    nc_g =gc2.selectbox("🏭 NCNO",   ["All"]+sorted(set(st.session_state.pdate["NCNO"].astype(str))),   key="dm_n")
    filt=st.session_state.pdate.copy()
    if ins_g!="All": filt=filt[filt["INSCODE"].astype(str)==ins_g]
    if nc_g !="All": filt=filt[filt["NCNO"].astype(str)==nc_g]
    with gc3:
        st.markdown("<br>",unsafe_allow_html=True)
        if st.button("🔍 Run Error Check",type="primary",use_container_width=True):
            if st.session_state.pdate.empty:
                st.error("❌ Upload dated panel first!")
            else:
                with st.spinner("Running all checks..."):
                    err_map=check_errors(filt,st.session_state.staff)
                for idx in filt.index:
                    if idx in st.session_state.pdate.index:
                        msgs=err_map.get(idx,[])
                        st.session_state.pdate.at[idx,"ERROR"]=" | ".join(msgs) if msgs else ""
                PD(); st.session_state.errors=err_map
                total=sum(len(v) for v in err_map.values())
                if total==0:
                    st.markdown('<div class="ok-card">✅ All checks passed! No clashes found.</div>',unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="err-card">🔴 Found {total} issue(s) across {len(err_map)} rows — see Error Report below.</div>',unsafe_allow_html=True)

    if st.session_state.errors:
        st.markdown('<hr class="thin">',unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🔴 Error Report</div>',unsafe_allow_html=True)
        for idx,msgs in st.session_state.errors.items():
            r=st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
            ins_r=r.get("INSCODE","?"); sc_r=r.get("SUBCODE","?")
            d1_r=r.get("DATE_FROM","?"); d2_r=r.get("DATE_TO","?")
            with st.expander(f"🔴 Row {idx} · 🏫 {ins_r} · 📚 {sc_r} · 📅 {d1_r}→{d2_r} · {len(msgs)} issue(s)"):
                for m in msgs:
                    st.markdown(f'<div class="err-card">{m}</div>',unsafe_allow_html=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">📊 Duty Count Overview</div>',unsafe_allow_html=True)
    if not st.session_state.pdate.empty:
        dc_d={}
        for _,row in st.session_state.pdate.iterrows():
            for fld in ["INTID","EXTID"]:
                sid=norm_id(row.get(fld,""))
                if sid: dc_d[sid]=dc_d.get(sid,0)+1
        if dc_d:
            df_ch=pd.DataFrame(list(dc_d.items()),columns=["Staff ID","Duties"])
            df_ch["Name"]=df_ch["Staff ID"].apply(lambda s:get_name(st.session_state.staff,s))
            df_ch["Label"]=df_ch["Staff ID"]+" — "+df_ch["Name"]
            df_ch=df_ch.sort_values("Duties",ascending=False).head(30)
            st.bar_chart(df_ch.set_index("Label")["Duties"])
        else:
            st.markdown('<div class="info-card">ℹ️ No staff assignments in dated panel.</div>',unsafe_allow_html=True)
    else:
        st.markdown('<div class="info-card">ℹ️ Upload dated panel to see duty count chart.</div>',unsafe_allow_html=True)

    st.markdown(
        f'<div style="text-align:center;margin-top:20px">'
        f'<span style="background:#161b22;border:1px solid #30363d;border-radius:20px;'
        f'padding:5px 18px;color:#8b949e;font-size:.75rem">✨ {CREATOR}</span></div>',
        unsafe_allow_html=True)
