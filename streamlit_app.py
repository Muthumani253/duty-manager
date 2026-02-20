#!/usr/bin/env python3
"""Duty Manager v4 — MUTHUMANI S, LECTURER-EEE, GPT KARUR | 9443100811"""
from __future__ import annotations
import os, uuid, base64
from datetime import datetime, timedelta, date
import re
from io import BytesIO

import streamlit as st
import pandas as pd

try:
    from reportlab.lib import colors as RC
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                    Paragraph, Spacer, PageBreak)
    from reportlab.lib.enums import TA_CENTER
    RPDF = True
except ImportError:
    RPDF = False

DATA_DIR         = "data"
PANEL_PATH       = os.path.join(DATA_DIR,"panel.csv")
PANEL_DATED_PATH = os.path.join(DATA_DIR,"panel_dated.csv")
STAFF_PATH       = os.path.join(DATA_DIR,"staff.csv")
SUBMAP_PATH      = os.path.join(DATA_DIR,"submap.csv")
SUBJMAP_PATH     = os.path.join(DATA_DIR,"subjmap.csv")
os.makedirs(DATA_DIR, exist_ok=True)

CREATOR    = "MUTHUMANI S | LECTURER-EEE | GPT KARUR | 9443100811"
PANEL_COLS = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","ERROR","__rowid"]
PDATE_COLS = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_COLS = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone","__rowid"]
SMAP_COLS  = ["Staff_Last_Staff_ID","Staff_Name","Department","Department_Code",
              "Subject_Type","Subject_Code","Subject_Name","Subject_Remarks"]

st.set_page_config(page_title="Duty Manager",page_icon="🗂️",layout="wide",initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════ CSS
st.markdown("""
<style>
.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],
section[data-testid="stMain"],[data-testid="stVerticalBlock"],.main,body{
    background-color:#0d1117 !important;}
[data-testid="stSidebar"],[data-testid="collapsedControl"],
header[data-testid="stHeader"],#MainMenu,footer{display:none !important;}
.main .block-container{padding:0 1.4rem 2rem !important;max-width:100% !important;}
body,.stApp{color:#c9d1d9 !important;}
p,li,span,div,label,td,th,caption,small,strong,b,i,em,
[data-testid="stMarkdownContainer"] p,[data-testid="stMarkdownContainer"] span,
[data-testid="stMarkdownContainer"] li,[data-testid="stMarkdownContainer"] td,
[data-testid="stMarkdownContainer"] th{color:#c9d1d9 !important;}
h1,h2,h3,h4,h5,h6{color:#e6edf3 !important;}
[data-testid="stMarkdownContainer"] table{border-collapse:collapse;width:100%;}
[data-testid="stMarkdownContainer"] th{background:#1c2333 !important;color:#e6edf3 !important;padding:7px 12px !important;border:1px solid #30363d !important;font-weight:700;}
[data-testid="stMarkdownContainer"] td{background:#0d1117 !important;color:#c9d1d9 !important;padding:5px 12px !important;border:1px solid #21262d !important;}
[data-testid="stMarkdownContainer"] tr:nth-child(even) td{background:#0f1923 !important;}
code,pre,[data-testid="stCode"] code{background:#010409 !important;color:#79c0ff !important;font-size:.79rem !important;border-radius:5px !important;}
[data-testid="stCaptionContainer"] p{color:#6e7681 !important;font-size:.75rem !important;}
/* TOPBAR */
.topbar{background:#010409;border-bottom:1px solid #21262d;padding:0 20px;
    display:flex;align-items:center;justify-content:space-between;height:58px;margin:0 -1.4rem 0;}
.tb-logo{background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:9px;padding:7px 9px;font-size:1.2rem;line-height:1;}
.tb-title{color:#e6edf3 !important;font-weight:700;font-size:1.05rem;line-height:1.15;}
.tb-sub{color:#6e7681 !important;font-size:.68rem;}
.tb-badge{color:#8b949e !important;font-size:.72rem;background:#161b22;border:1px solid #21262d;border-radius:20px;padding:4px 14px;white-space:nowrap;}
/* STATS */
.statsbar{display:flex;gap:6px;flex-wrap:wrap;background:#010409;border-bottom:1px solid #21262d;padding:8px 20px;margin:0 -1.4rem 1rem;}
.sc{background:#161b22;border:1px solid #21262d;border-radius:6px;padding:3px 11px;font-size:.74rem;color:#8b949e !important;white-space:nowrap;}
.sc b{color:#e6edf3 !important;font-size:.86rem;}
/* TABS */
.stTabs [data-baseweb="tab-list"]{background:#010409 !important;border-bottom:1px solid #21262d !important;gap:0 !important;padding:0 4px !important;margin:0 -1.4rem 1.2rem !important;overflow-x:auto !important;}
.stTabs [data-baseweb="tab"]{background:transparent !important;color:#8b949e !important;border:none !important;border-bottom:2px solid transparent !important;border-radius:0 !important;font-size:.88rem !important;font-weight:600 !important;padding:13px 22px !important;transition:all .15s !important;white-space:nowrap !important;}
.stTabs [data-baseweb="tab"]:hover{color:#e6edf3 !important;background:#161b22 !important;}
.stTabs [aria-selected="true"]{color:#6366f1 !important;border-bottom-color:#6366f1 !important;}
.stTabs [data-baseweb="tab"] p{color:inherit !important;font-size:inherit !important;font-weight:inherit !important;}
[data-testid="stTabsContent"]{padding:0 !important;border:none !important;background:transparent !important;}
/* HEADERS */
.sec-hdr{background:linear-gradient(90deg,#6366f1,#8b5cf6);color:#fff !important;padding:8px 18px;border-radius:8px;font-weight:700;font-size:.96rem;margin:10px 0 8px;display:flex;align-items:center;gap:8px;}
.sec-hdr *{color:#fff !important;}
.sub-hdr{color:#e6edf3 !important;font-size:.9rem;font-weight:700;padding:0 0 5px;border-bottom:1px solid #21262d;margin:10px 0 6px;display:block;}
/* CARDS */
.err-card{background:#2d1515;border-left:3px solid #ef4444;border-radius:6px;padding:8px 12px;margin:3px 0;color:#fca5a5 !important;}
.err-card *{color:#fca5a5 !important;}
.ok-card{background:#0d2218;border-left:3px solid #22c55e;border-radius:6px;padding:8px 12px;margin:3px 0;color:#86efac !important;}
.ok-card *{color:#86efac !important;}
.warn-card{background:#2a1f0a;border-left:3px solid #f59e0b;border-radius:6px;padding:8px 12px;margin:3px 0;color:#fcd34d !important;}
.warn-card *{color:#fcd34d !important;}
.info-card{background:#0c1a2e;border-left:3px solid #3b82f6;border-radius:6px;padding:8px 12px;margin:3px 0;color:#93c5fd !important;}
.info-card *{color:#93c5fd !important;}
/* EXT ROW CARD */
.ext-row{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:10px 14px;margin:6px 0;}
.ext-row-pending{border-left:4px solid #ef4444 !important;}
.ext-row-done{border-left:4px solid #22c55e !important;}
/* INPUTS */
[data-testid="stSelectbox"]>div>div{background:#161b22 !important;border:1px solid #30363d !important;border-radius:6px !important;color:#e6edf3 !important;}
[data-testid="stSelectbox"] span{color:#e6edf3 !important;}
[data-testid="stSelectbox"] label p{color:#8b949e !important;font-size:.82rem !important;}
[data-baseweb="popover"] ul,[data-baseweb="menu"]{background:#161b22 !important;border:1px solid #21262d !important;}
[data-baseweb="menu"] li{color:#c9d1d9 !important;background:#161b22 !important;}
[data-baseweb="menu"] li:hover{background:#21262d !important;}
[data-testid="stTextInput"] input{background:#161b22 !important;border:1px solid #30363d !important;color:#e6edf3 !important;border-radius:6px !important;}
[data-testid="stTextInput"] label p{color:#8b949e !important;font-size:.82rem !important;}
[data-testid="stCheckbox"] label p{color:#c9d1d9 !important;}
[data-testid="stFileUploader"]{background:#161b22 !important;border:1px solid #21262d !important;border-radius:8px !important;}
[data-testid="stFileUploaderDropzone"]{background:#0d1117 !important;border:1px dashed #30363d !important;border-radius:6px !important;}
[data-testid="stFileUploaderDropzone"] p,[data-testid="stFileUploaderDropzone"] span{color:#6e7681 !important;}
[data-testid="stFileUploaderDropzone"] button{background:#21262d !important;color:#c9d1d9 !important;border:1px solid #30363d !important;border-radius:6px !important;}
/* DOWNLOAD BUTTONS */
[data-testid="stDownloadButton"] button{background:linear-gradient(135deg,#6366f1,#8b5cf6) !important;color:#ffffff !important;border:none !important;border-radius:7px !important;font-weight:600 !important;font-size:.82rem !important;}
[data-testid="stDownloadButton"] button:hover{opacity:.88 !important;}
[data-testid="stDownloadButton"] button p{color:#ffffff !important;font-size:.82rem !important;}
/* BUTTONS */
.stButton>button{border-radius:7px !important;font-weight:600 !important;font-size:.85rem !important;}
.stButton>button[kind="primary"]{background:linear-gradient(135deg,#6366f1,#8b5cf6) !important;border:none !important;color:#fff !important;}
.stButton>button[kind="primary"]:hover{opacity:.88 !important;}
.stButton>button[kind="secondary"]{background:#161b22 !important;border:1px solid #30363d !important;color:#e6edf3 !important;}
.stButton>button[kind="secondary"]:hover{border-color:#6366f1 !important;}
/* DATA TABLES */
div[data-testid="stDataFrame"],div[data-testid="stDataEditor"]{border-radius:8px !important;overflow:hidden !important;}
/* EXPANDER */
[data-testid="stExpander"]{background:#161b22 !important;border:1px solid #21262d !important;border-radius:8px !important;}
[data-testid="stExpander"] summary{background:#161b22 !important;border-radius:8px !important;padding:8px 16px !important;}
[data-testid="stExpander"] summary p{color:#e6edf3 !important;font-weight:600 !important;}
[data-testid="stExpander"] svg{fill:#8b949e !important;}
.streamlit-expanderContent{background:#0d1117 !important;padding:12px !important;}
/* METRICS */
[data-testid="stMetric"]{background:#161b22 !important;border:1px solid #21262d !important;border-radius:8px !important;padding:12px 14px !important;}
[data-testid="stMetricLabel"] p{color:#8b949e !important;font-size:.78rem !important;}
[data-testid="stMetricValue"] div{color:#e6edf3 !important;}
/* PRIORITY BADGES */
.badge-green{background:#0d2218;color:#22c55e;border:1px solid #22c55e;border-radius:12px;padding:1px 8px;font-size:.72rem;font-weight:700;}
.badge-yellow{background:#2a1f0a;color:#f59e0b;border:1px solid #f59e0b;border-radius:12px;padding:1px 8px;font-size:.72rem;font-weight:700;}
.badge-red{background:#2d1515;color:#ef4444;border:1px solid #ef4444;border-radius:12px;padding:1px 8px;font-size:.72rem;font-weight:700;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:#0d1117;}
::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px;}
::-webkit-scrollbar-thumb:hover{background:#6366f1;}
hr.thin{border:none;border-top:1px solid #21262d;margin:8px 0;}
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

def get_col(sf,sid,col):
    sid=norm_id(sid)
    if not sid or sf.empty: return ""
    try:
        m=sf["Staff ID"].astype(str).str.upper()==sid
        return str(sf.loc[m,col].iloc[0]) if m.any() else ""
    except: return ""

def get_name(sf,sid):  return get_col(sf,sid,"Name of the Staff")
def get_phone(sf,sid): return get_col(sf,sid,"Phone")
def get_desig(sf,sid): return get_col(sf,sid,"Designation")
def get_instt(sf,sid): return get_col(sf,sid,"INSTT")
def get_dep(sf,sid):   return get_col(sf,sid,"Department")

def get_subname(sm,code):
    if sm is None or sm.empty: return ""
    m=sm[sm["SUBCODE"].astype(str)==str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def priority_icon(count):
    """Return priority color icon based on duty count"""
    if count==0:   return "🟢"
    elif count<=2: return "🟡"
    else:          return "🔴"

def priority_class(count):
    if count==0:   return "badge-green"
    elif count<=2: return "badge-yellow"
    else:          return "badge-red"

# ═══════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════
for key,path,cols,pre in [
    ("panel",  PANEL_PATH,       PANEL_COLS, "p"),
    ("pdate",  PANEL_DATED_PATH, PDATE_COLS, "d"),
    ("staff",  STAFF_PATH,       STAFF_COLS, "s"),
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
if "staged"  not in st.session_state: st.session_state.staged={}
if "errors"  not in st.session_state: st.session_state.errors={}

def P():  st.session_state.panel=rowid(st.session_state.panel,"p"); save_csv(st.session_state.panel,PANEL_PATH)
def PD(): st.session_state.pdate=rowid(st.session_state.pdate,"d"); save_csv(st.session_state.pdate,PANEL_DATED_PATH)
def S():  st.session_state.staff=rowid(st.session_state.staff,"s"); save_csv(st.session_state.staff,STAFF_PATH)
def SM(): save_csv(st.session_state.submap,SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap,SUBJMAP_PATH)

# ═══════════════════════════════════════════════════════
# LOGIC
# ═══════════════════════════════════════════════════════
def duty_stats(sf):
    """Count duties per staff from date columns (INSTT tokens)"""
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
    """Get eligible external staff sorted by priority (least duties first)"""
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
        if instt==p_ins: continue          # must be external (different institution)
        dep=str(row.get("dep code","")).strip()
        if mapped_ids is not None:
            if sid not in mapped_ids: continue   # must be mapped to subject
        elif dep!=p_dep: continue                # fallback: match dept code
        se=stats.get(sid,{})
        cnt=se.get("count",0)
        res.append({"sid":sid,"name":row.get("Name of the Staff",""),
                    "desig":row.get("Designation",""),"instt":instt,
                    "dep":dep,"phone":row.get("Phone",""),"count":cnt,
                    "icon":priority_icon(cnt),"cls":priority_class(cnt)})
    res.sort(key=lambda x:x["count"])
    return res

def make_dropdown_label(s):
    """Rich label: priority icon + staff info"""
    return f"{s['icon']} {s['sid']} | {s['name']} | {s['desig']} | 🏫{s['instt']} | Duties:{s['count']}"

def extract_sid(label):
    """Extract staff ID from dropdown label"""
    # Remove priority icon (first char if emoji)
    l=str(label).strip()
    # Remove leading icon characters
    l=re.sub(r'^[🟢🟡🔴]\s*','',l)
    return norm_id(l.split("|")[0].strip())

def auto_allocate(candidates,sf,ssmap):
    res,skip={},{}
    for pidx,row in candidates.iterrows():
        suggs=ext_suggestions(row,sf,ssmap)
        if suggs: res[pidx]=make_dropdown_label(suggs[0])
        else: skip[pidx]=f"No eligible external staff for SUBCODE {row.get('SUBCODE','?')}"
    return res,skip

def check_errors(pdf,sf):
    errs={i:[] for i in pdf.index}
    sd={}
    for idx,row in pdf.iterrows():
        d1=parse_date(row.get("DATE_FROM")); d2=parse_date(row.get("DATE_TO"))
        sc=str(row.get("SUBCODE","")).strip(); ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if is_zero(row.get(fld,"")): sid=""
            if not sid: continue
            s_ins=inscode_from_sid(sid)
            if role=="INT" and s_ins and s_ins!=ins:
                errs[idx].append(f"❌ INTID {sid}: home {s_ins} ≠ exam {ins}")
            if role=="EXT" and s_ins and s_ins==ins:
                errs[idx].append(f"❌ EXTID {sid}: home {s_ins} == exam {ins} (must differ)")
            sd.setdefault(sid,[]).append((idx,sc,d1,d2,role))
    for sid,duties in sd.items():
        for i in range(len(duties)):
            ia,sca,d1a,d2a,_=duties[i]
            if not(d1a and d2a): continue
            for j in range(i+1,len(duties)):
                ib,scb,d1b,d2b,_=duties[j]
                if not(d1b and d2b): continue
                if max(d1a,d1b)<=min(d2a,d2b) and sca!=scb:
                    msg=f"⚠️ {sid} CLASH: {sca}({d2s(d1a)}→{d2s(d2a)}) overlaps {scb}({d2s(d1b)}→{d2s(d2b)})"
                    errs[ia].append(msg); errs[ib].append(msg)
    return {k:v for k,v in errs.items() if v}

# ═══════════════════════════════════════════════════════
# PDF / HTML GENERATION
# ═══════════════════════════════════════════════════════
def generate_pdf_rl(panel_df,sf,submap):
    buf=BytesIO()
    doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=1.5*cm,bottomMargin=1.5*cm)
    H1=ParagraphStyle("H1",fontSize=12,fontName="Helvetica-Bold",spaceAfter=4,alignment=TA_CENTER)
    SML=ParagraphStyle("SML",fontSize=7,fontName="Helvetica",textColor=RC.grey,alignment=TA_CENTER)
    story=[]; sd={}
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
            ("BACKGROUND",(0,0),(-1,-1),RC.HexColor("#0d1117")),("TEXTCOLOR",(0,0),(-1,-1),RC.white),
            ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),("GRID",(0,0),(-1,-1),.4,RC.HexColor("#21262d")),("PADDING",(0,0),(-1,-1),5),
        ]))
        story.append(ht); story.append(Spacer(1,.4*cm))
        tr=[["S.No","Duty INSCODE","SubCode","Subject Name","Role","Partner ID","Partner Name","Partner Phone","Date From","Date To"]]
        for sno,d in enumerate(duties,1):
            pid=d["cid"]; pn=get_name(sf,pid) if pid else ""; pp=get_phone(sf,pid) if pid else ""
            tr.append([str(sno),d["ins"],d["sc"],d["sn"] or d["sc"],d["role"],pid or "-",pn or "-",pp or "-","",""])
        dt=Table(tr,colWidths=[.9*cm,2*cm,2*cm,4*cm,1.2*cm,2.2*cm,3.5*cm,2.2*cm,2*cm,2*cm],repeatRows=1)
        dt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),RC.HexColor("#6366f1")),("TEXTCOLOR",(0,0),(-1,0),RC.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),7),
            ("ALIGN",(0,0),(-1,-1),"CENTER"),("ALIGN",(3,1),(3,-1),"LEFT"),("ALIGN",(6,1),(6,-1),"LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[RC.HexColor("#f8fafc"),RC.HexColor("#e2e8f0")]),
            ("GRID",(0,0),(-1,-1),.4,RC.HexColor("#94a3b8")),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("PADDING",(0,0),(-1,-1),4),
        ]))
        story.append(dt); story.append(Spacer(1,.3*cm))
        story.append(Paragraph("Date From/To to be filled by Flying Squad at duty.",SML))
        story.append(PageBreak())
    doc.build(story)
    return buf.getvalue()

def generate_html_duties(panel_df,sf,submap):
    sd={}
    for _,row in panel_df.iterrows():
        sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
        ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if not sid: continue
            sd.setdefault(sid,[]).append({"ins":ins,"sc":sc,"sn":sn,"role":role,
                "cid":norm_id(row.get("EXTID" if role=="INT" else "INTID",""))})
    pages=[]
    for sid in sorted(sd.keys(),key=lambda s:get_name(sf,s)):
        duties=sd.get(sid,[])
        if not duties: continue
        name=get_name(sf,sid); phone=get_phone(sf,sid)
        m=sf[sf["Staff ID"].astype(str).str.upper()==sid]
        desig=str(m.iloc[0]["Designation"]) if not m.empty else ""
        dept =str(m.iloc[0]["Department"])  if not m.empty else ""
        instt=str(m.iloc[0]["INSTT"])       if not m.empty else ""
        rows=""
        for sno,d in enumerate(duties,1):
            pid=d["cid"]; pn=get_name(sf,pid) if pid else "-"; pp=get_phone(sf,pid) if pid else "-"
            rows+=f"<tr><td>{sno}</td><td>{d['ins']}</td><td>{d['sc']}</td><td style='text-align:left'>{d['sn'] or d['sc']}</td><td>{d['role']}</td><td>{pid or '-'}</td><td style='text-align:left'>{pn}</td><td>{pp}</td><td></td><td></td></tr>"
        pages.append(f"""<div class='page'>
<div class='title'>PRACTICAL EXAM DUTY ORDER</div><div class='creator'>{CREATOR}</div>
<table class='hdr'><tr><th>Staff ID</th><td>{sid}</td><th>Name</th><td>{name}</td></tr>
<tr><th>Institution</th><td>{instt}</td><th>Phone</th><td>{phone}</td></tr>
<tr><th>Department</th><td>{dept}</td><th>Designation</th><td>{desig}</td></tr></table>
<table class='duty'><thead><tr><th>S.No</th><th>Duty INSCODE</th><th>SubCode</th><th>Subject Name</th>
<th>Role</th><th>Partner ID</th><th>Partner Name</th><th>Partner Phone</th><th>Date From</th><th>Date To</th></tr></thead>
<tbody>{rows}</tbody></table><p class='note'>Date From/To to be filled by Flying Squad.</p></div>""")
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Duty Sheets</title>
<style>body{{font-family:Arial,sans-serif;font-size:10px;background:#fff;color:#000;}}
.page{{page-break-after:always;padding:18px 22px;border-bottom:2px dashed #ccc;}}
.title{{font-size:14px;font-weight:bold;text-align:center;margin-bottom:3px;}}
.creator{{font-size:8px;text-align:center;color:#555;margin-bottom:10px;}}
.note{{font-size:8px;color:#555;margin-top:6px;}}
table{{width:100%;border-collapse:collapse;margin-bottom:8px;}}
table.hdr th{{background:#1a1a2e;color:#fff;padding:5px 8px;text-align:left;width:90px;font-size:9px;}}
table.hdr td{{padding:5px 8px;border:1px solid #ccc;font-size:9px;}}
table.duty th{{background:#6366f1;color:#fff;padding:5px;text-align:center;font-size:8px;}}
table.duty td{{padding:4px 5px;border:1px solid #ccc;text-align:center;font-size:8.5px;}}
table.duty tr:nth-child(even) td{{background:#f5f7ff;}}
@media print{{.page{{page-break-after:always;border:none;}}body{{margin:0;}}}}</style>
</head><body>{"".join(pages)}</body></html>""".encode("utf-8")

# ═══════════════════════════════════════════════════════
# TOP BAR + STATS
# ═══════════════════════════════════════════════════════
pn=len(st.session_state.panel); pdn=len(st.session_state.pdate)
ef=st.session_state.panel["EXTID"].apply(lambda v:norm_id(v)!="").sum() if pn else 0
ep=pn-ef; sc2=len(st.session_state.staff); sm2c=len(st.session_state.ssmap); stg=len(st.session_state.staged)

st.markdown(f"""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:10px">
    <div class="tb-logo">🗂️</div>
    <div><div class="tb-title">DUTY MANAGER</div><div class="tb-sub">PRACTICAL EXAM PANEL</div></div>
  </div>
  <div class="tb-badge">👤 {CREATOR}</div>
</div>
<div class="statsbar">
  <div class="sc">📋 Panel <b>{pn}</b></div>
  <div class="sc">✅ EXTID Filled <b style="color:#22c55e !important">{ef}</b></div>
  <div class="sc">⏳ Pending <b style="color:#f59e0b !important">{ep}</b></div>
  <div class="sc">🗓️ Dated Panel <b style="color:#8b5cf6 !important">{pdn}</b></div>
  <div class="sc">🧑‍🏫 Staff <b style="color:#3b82f6 !important">{sc2}</b></div>
  <div class="sc">📘 SubjectMap <b style="color:#ec4899 !important">{sm2c}</b></div>
  <div class="sc">🔖 Staged <b style="color:#f59e0b !important">{stg}</b></div>
</div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# MAIN TABS  (4 tabs + Downloads)
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
    st.markdown('<div class="sec-hdr">📥 Upload Centre</div>',unsafe_allow_html=True)
    s1,s2,s3,s4=st.tabs([
        "  📋  Panel (No Dates)  ",
        "  🧑‍🏫  Staff Details  ",
        "  📘  Subject-Staff Mapping  ",
        "  🔤  SUBCODE → SUBNAME  ",
    ])

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
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
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
            st.markdown(f'<span class="sub-hdr">📋 Panel — {len(pv2)} rows <small style="color:#6e7681;font-weight:400">(editable)</small></span>',unsafe_allow_html=True)
            ep2=st.data_editor(pv2[show].fillna(""),key="p_ed",use_container_width=True,height=340,num_rows="dynamic")
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
            st.markdown('<div class="info-card">📌 Phone column required for PDF duty sheets</div>',unsafe_allow_html=True)
            sample_s=pd.DataFrame([{"Staff ID":"X123EEE1","INSTT":"123","Name of the Staff":"KUMAR S",
                "Department":"EEE","dep code":"1030","Designation":"Lecturer","Phone":"9876543210"}])
            st.download_button("📥 Sample Staff CSV",data=sample_s.to_csv(index=False).encode(),
                file_name="sample_staff.csv",mime="text/csv")
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
            st.markdown(f'<span class="sub-hdr">🧑‍🏫 Staff — {len(sv)} rows <small style="color:#6e7681;font-weight:400">(editable)</small></span>',unsafe_allow_html=True)
            es=st.data_editor(sv[dcols],key="s_ed",use_container_width=True,height=400,num_rows="dynamic")
            if st.button("💾 Save Staff",key="s_sv",use_container_width=True):
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
            c_t,c_s=st.columns(2)
            c_t.download_button("📥 Empty Template",data=pd.DataFrame(columns=SMAP_COLS).to_csv(index=False).encode(),
                file_name="ssmap_template.csv",mime="text/csv",use_container_width=True)
            c_s.download_button("📥 Sample CSV",data=sample_sm.to_csv(index=False).encode(),
                file_name="ssmap_sample.csv",mime="text/csv",use_container_width=True)
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
                        SS(); st.success(f"✅ {len(tmp)} rows loaded")
                except Exception as e: st.error(f"❌ {e}")
        with mr:
            ssv=st.session_state.ssmap.copy()
            sf3,sf4=st.columns(2)
            dm_f=sf3.selectbox("🏭 Dept",["All"]+sorted(set(ssv["Department"].astype(str))),key="ssm_d")
            sc_f=sf4.text_input("","",key="ssm_s",placeholder="🔍 Subject Code...",label_visibility="collapsed")
            if dm_f!="All": ssv=ssv[ssv["Department"]==dm_f]
            if sc_f.strip(): ssv=ssv[ssv["Subject_Code"].str.contains(sc_f.strip().upper(),na=False)]
            st.markdown(f'<span class="sub-hdr">📘 Mapping — {len(ssv)} rows <small style="color:#6e7681;font-weight:400">(editable)</small></span>',unsafe_allow_html=True)
            essm=st.data_editor(ssv.fillna(""),key="ssm_ed",use_container_width=True,height=400,num_rows="dynamic")
            if st.button("💾 Save Mapping",key="ssm_sv",use_container_width=True):
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

    with s4:
        tl,tr2=st.columns([1,1],gap="medium")
        with tl:
            st.markdown('<span class="sub-hdr">📂 Upload SUBCODE → SUBNAME</span>',unsafe_allow_html=True)
            st.code("SUBCODE  SUBNAME",language="")
            st.markdown('<div class="info-card">Upload CSV/XLSX with 2 columns. Used to display subject names everywhere.</div>',unsafe_allow_html=True)
            samp_sub=pd.DataFrame([{"SUBCODE":"P3401","SUBNAME":"Basic Electrical Lab"},
                                   {"SUBCODE":"P3402","SUBNAME":"Electrical Machines Lab"}])
            st.download_button("📥 Sample SUBNAME CSV",data=samp_sub.to_csv(index=False).encode(),
                file_name="subname_sample.csv",mime="text/csv")
            sf2=st.file_uploader("",type=["csv","xlsx"],key="sub_up",label_visibility="collapsed")
            if sf2:
                try:
                    sm2=(pd.read_csv(sf2,dtype=object) if sf2.name.lower().endswith(".csv")
                         else pd.read_excel(sf2,dtype=object,sheet_name=0)).fillna("")
                    if "SUBCODE" not in sm2.columns or "SUBNAME" not in sm2.columns:
                        if sm2.shape[1]>=2:
                            sm2=pd.DataFrame({"SUBCODE":sm2.iloc[:,0].astype(str),"SUBNAME":sm2.iloc[:,1].astype(str)})
                    st.session_state.submap=sm2[["SUBCODE","SUBNAME"]].copy(); SM()
                    st.success(f"✅ {len(sm2)} entries saved")
                except Exception as e: st.error(f"❌ {e}")
        with tr2:
            smv=st.session_state.submap.copy()
            sc_fi=st.text_input("","",key="sm_fi",placeholder="🔍 Filter SUBCODE...",label_visibility="collapsed")
            smv2=smv[smv["SUBCODE"].astype(str).str.contains(sc_fi.strip().upper(),na=False)] if sc_fi.strip() else smv
            st.markdown(f'<span class="sub-hdr">🔤 SUBCODE Mapping — {len(smv2)} rows <small style="color:#6e7681;font-weight:400">(editable)</small></span>',unsafe_allow_html=True)
            esm=st.data_editor(
                smv2[["SUBCODE","SUBNAME"]].fillna("") if not smv2.empty else pd.DataFrame(columns=["SUBCODE","SUBNAME"]),
                key="sm_ed",use_container_width=True,height=420,num_rows="dynamic")
            if st.button("💾 Save SUBNAME",key="sm_sv",use_container_width=True):
                st.session_state.submap=esm.copy(); SM()
                st.success("✅ SUBNAME mapping saved")

# ═══════════════════════════════════════════════════════
# TAB 2 — EXT ALLOCATE
# ═══════════════════════════════════════════════════════
with tab_ext:
    st.markdown('<div class="sec-hdr">🎯 EXT Allocate — Assign External Examiners</div>',unsafe_allow_html=True)

    # Priority legend
    st.markdown("""
<div style="display:flex;gap:10px;margin-bottom:8px;flex-wrap:wrap">
  <span class="badge-green">🟢 Best (0 duties)</span>
  <span class="badge-yellow">🟡 Good (1-2 duties)</span>
  <span class="badge-red">🔴 Busy (3+ duties)</span>
  <span style="color:#8b949e;font-size:.78rem;align-self:center">Dropdown shows eligible staff sorted by priority</span>
</div>""", unsafe_allow_html=True)

    with st.expander("ℹ️ Allocation Logic"):
        st.markdown("""
| # | Rule | Detail |
|---|------|--------|
| 1 | **Subject Match** | Staff mapped to panel SUBCODE via Subject-Staff Mapping |
| 2 | **External Rule** | Staff INSTT must differ from panel INSCODE |
| 3 | **Priority** | 🟢 0 duties (best) → 🟡 1-2 duties → 🔴 3+ duties |
| 4 | **Fallback** | No SubjectMap → matches `dep code` == panel `NCNO` |
        """)

    panel=st.session_state.panel.copy()
    sf=st.session_state.staff.copy()
    ssmap=st.session_state.ssmap.copy()
    submap=st.session_state.submap.copy()

    # ── FILTERS ──
    fc1,fc2,fc3=st.columns([2,2,2])
    ins_f=fc1.selectbox("🏫 Filter INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ea_i")
    nc_f =fc2.selectbox("🏭 Filter NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ea_n")
    show_f=fc3.selectbox("👁️ Show",["Pending Only","All Rows","Filled Only"],key="ea_sh")

    # ── FIX: needs_ext only checks EXTID is empty (no INTID requirement) ──
    def needs_ext(r): return norm_id(r.get("EXTID",""))==""
    def has_ext(r):   return norm_id(r.get("EXTID",""))!=""

    filt_panel=panel.copy()
    if ins_f!="All": filt_panel=filt_panel[filt_panel["INSCODE"].astype(str)==ins_f]
    if nc_f !="All": filt_panel=filt_panel[filt_panel["NCNO"].astype(str)==nc_f]

    candidates=filt_panel[filt_panel.apply(needs_ext,axis=1)].copy()

    if show_f=="Pending Only":  view_panel=candidates.copy()
    elif show_f=="Filled Only": view_panel=filt_panel[filt_panel.apply(has_ext,axis=1)].copy()
    else:                       view_panel=filt_panel.copy()

    m1,m2,m3,m4=st.columns(4)
    m1.metric("📋 Pending EXTID",len(candidates))
    m2.metric("🧑‍🏫 Staff Loaded",len(sf))
    m3.metric("📘 SubjectMap",len(ssmap))
    m4.metric("🔖 Staged",len(st.session_state.staged))

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ── PANEL PREVIEW (color-coded) ──
    st.markdown('<div class="sec-hdr">📊 Panel Preview</div>',unsafe_allow_html=True)
    if not view_panel.empty:
        pv=view_panel.copy()
        if not submap.empty:
            pv=pv.merge(submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
        pv["INT_NAME"]=pv["INTID"].apply(lambda x:get_name(sf,x))
        pv["EXT_NAME"]=pv["EXTID"].apply(lambda x:get_name(sf,x))
        pv["STATUS"]=pv.apply(lambda r:"✅ Filled" if has_ext(r) else "⏳ Pending",axis=1)
        show_cols=[c for c in ["STATUS","INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","INT_NAME","EXTID","EXT_NAME"] if c in pv.columns]
        def sty_status(v): return "background-color:#0d2218;color:#86efac" if v=="✅ Filled" else "background-color:#2d1515;color:#fca5a5"
        def sty_ext(v):
            v2=str(v).strip()
            return ("background-color:#0d2218;color:#86efac" if v2 and not is_zero(v2)
                    else "background-color:#2d1515;color:#fca5a5")
        styled=pv[show_cols].fillna("").style\
            .applymap(sty_status,subset=["STATUS"])\
            .applymap(sty_ext,subset=["EXTID"])
        st.dataframe(styled,use_container_width=True,height=250)
    else:
        st.markdown('<div class="info-card">ℹ️ No panel rows for current filters.</div>',unsafe_allow_html=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ── AUTO ALLOCATE ──
    st.markdown('<div class="sec-hdr">🤖 Auto-Allocate</div>',unsafe_allow_html=True)
    st.markdown('<div class="info-card">Picks 🟢 Best (0 duties) staff first → different INSTT → matches SUBCODE mapping → stages all pending rows</div>',unsafe_allow_html=True)
    if st.button("🤖 Auto-Allocate ALL Pending",type="primary"):
        if sf.empty: st.error("❌ Upload staff data first!")
        else:
            res,skip=auto_allocate(candidates,sf,ssmap if not ssmap.empty else None)
            for k,v in res.items(): st.session_state.staged[str(k)]=v
            st.success(f"✅ Auto-staged {len(res)} rows.")
            if skip:
                with st.expander(f"⚠️ {len(skip)} rows skipped"):
                    st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]),use_container_width=True)
        st.rerun()

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ── PER-ROW MANUAL ALLOCATION ──
    st.markdown('<div class="sec-hdr">📝 Per-Row Manual Allocation</div>',unsafe_allow_html=True)

    if candidates.empty:
        st.markdown('<div class="ok-card">🎉 All rows in current filter have EXTID assigned!</div>',unsafe_allow_html=True)
        st.markdown('<div class="info-card">💡 Change filter to "All Rows" or select a different INSCODE to see filled rows.</div>',unsafe_allow_html=True)
    else:
        for _,row in candidates.reset_index().iterrows():
            pidx=int(row["index"])
            sc=str(row.get("SUBCODE","")).strip()
            sn=get_subname(submap,sc)
            ins=str(row.get("INSCODE","")).strip()
            nc=str(row.get("NCNO","")).strip()
            noc=str(row.get("NOC","")).strip()
            intid=norm_id(row.get("INTID",""))
            intname=get_name(sf,intid)
            int_desig=get_desig(sf,intid)
            int_phone=get_phone(sf,intid)
            cur_ext=norm_id(row.get("EXTID",""))
            sv_val=st.session_state.staged.get(str(pidx),"")

            suggs=ext_suggestions(row,sf,ssmap if not ssmap.empty else None)
            s_labels=["— Select External Examiner —"]
            for s in suggs:
                s_labels.append(make_dropdown_label(s))

            with st.container():
                # Row header
                h1,h2,h3=st.columns([3,3,2])
                h1.markdown(
                    f'<div style="font-size:.87rem">'
                    f'🏫 <b style="color:#e6edf3">{ins}</b> &nbsp;·&nbsp; 🏭 <span style="color:#c9d1d9">{nc}</span>'
                    f' &nbsp;·&nbsp; 📚 <code style="background:#010409;padding:1px 6px;border-radius:4px;color:#79c0ff">{sc}</code>'
                    f'{"<br><small style=color:#8b949e>"+sn+"</small>" if sn else ""}</div>',
                    unsafe_allow_html=True)
                h2.markdown(
                    f'<div style="font-size:.82rem;color:#8b949e">'
                    f'👥 <b style="color:#c9d1d9">{noc}</b> students'
                    f'{"<br>🎓 INT: <code style=background:#010409;padding:1px 5px;border-radius:4px;color:#fbbf24>"+intid+"</code>" if intid else "<br><span style=color:#6e7681>No INT assigned</span>"}'
                    f'{"<span style=color:#c9d1d9> "+intname+"</span>" if intname else ""}'
                    f'{"<br><small style=color:#6e7681>"+int_desig+"</small>" if int_desig else ""}</div>',
                    unsafe_allow_html=True)

                # Status indicator
                if cur_ext:
                    ext_name=get_name(sf,cur_ext); ext_desig=get_desig(sf,cur_ext)
                    h3.markdown(f'<div class="ok-card" style="font-size:.79rem;padding:5px 8px">✅ <b>{cur_ext}</b><br>{ext_name}<br><small>{ext_desig}</small></div>',unsafe_allow_html=True)
                elif sv_val:
                    sv_id=extract_sid(sv_val)
                    h3.markdown(f'<div class="warn-card" style="font-size:.79rem;padding:5px 8px">🟡 Staged<br><b>{sv_id}</b><br><small>{get_name(sf,sv_id)}</small></div>',unsafe_allow_html=True)
                else:
                    h3.markdown('<div class="err-card" style="font-size:.79rem;padding:5px 8px">⏳ Not assigned</div>',unsafe_allow_html=True)

                # Dropdown
                r1,r2,r3=st.columns([5,3,1])
                cur_lbl=sv_val if sv_val in s_labels else s_labels[0]
                di=s_labels.index(cur_lbl) if cur_lbl in s_labels else 0
                sel=r1.selectbox(
                    f"💡 {len(suggs)} eligible staff (🟢best first)",
                    s_labels, index=di, key=f"sel_{pidx}",
                    help="Green=0 duties (best), Yellow=1-2 duties, Red=3+ duties")

                man=r2.text_input("",value="",key=f"man_{pidx}",
                                   placeholder="✏️ Manual Staff ID",
                                   label_visibility="collapsed",
                                   help="Type Staff ID directly if not in list")

                if sel and sel!=s_labels[0]:
                    st.session_state.staged[str(pidx)]=sel
                if man.strip():
                    st.session_state.staged[str(pidx)]=man.strip()

                # Apply button
                if r3.button("▶",key=f"app_{pidx}",help="Apply now"):
                    chosen=sv_val or (sel if sel!=s_labels[0] else "") or man.strip()
                    if not chosen:
                        st.warning("⚠️ Select or enter a Staff ID first")
                    else:
                        sid_c=extract_sid(chosen) if "|" in chosen else norm_id(chosen)
                        if sid_c:
                            st.session_state.panel.at[pidx,"EXTID"]=sid_c; P()
                            st.session_state.staged.pop(str(pidx),None)
                            ext_nm=get_name(sf,sid_c)
                            st.success(f"✅ EXTID {sid_c} — {ext_nm} applied!")
                            st.rerun()
                        else: st.error("❌ Invalid Staff ID")

                # Show selected staff card
                if sel and sel!=s_labels[0]:
                    parts=[p.strip() for p in re.sub(r'^[🟢🟡🔴]\s*','',sel).split("|")]
                    sid_s=norm_id(parts[0]) if parts else ""
                    name_s=parts[1] if len(parts)>1 else ""
                    desig_s=parts[2] if len(parts)>2 else ""
                    instt_s=parts[3].replace("🏫","").strip() if len(parts)>3 else ""
                    duties_s=parts[4].replace("Duties:","").strip() if len(parts)>4 else ""
                    cnt_v=int(duties_s) if duties_s.isdigit() else 0
                    badge=priority_class(cnt_v)
                    ph_s=get_phone(sf,sid_s)
                    st.markdown(
                        f'<div style="background:#0c1a2e;border-radius:6px;padding:7px 14px;margin:4px 0;font-size:.8rem;display:flex;gap:14px;flex-wrap:wrap;align-items:center">'
                        f'<b style="color:#93c5fd">{sid_s}</b>'
                        f'<span style="color:#c9d1d9">{name_s}</span>'
                        f'<span style="color:#8b949e">{desig_s}</span>'
                        f'<span style="color:#8b949e">🏫 {instt_s}</span>'
                        f'{"<span style=color:#8b949e>📞 "+ph_s+"</span>" if ph_s else ""}'
                        f'<span class="{badge}">Duties: {duties_s}</span>'
                        f'</div>',unsafe_allow_html=True)

                st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ── APPLY ALL STAGED ──
    staged_map=st.session_state.staged
    if staged_map:
        st.markdown('<hr class="thin">',unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🚀 Apply All Staged</div>',unsafe_allow_html=True)
        with st.expander(f"👁️ Preview {len(staged_map)} staged assignments"):
            rows=[]
            for k,v in list(staged_map.items())[:40]:
                try:
                    pi=int(k); r=st.session_state.panel.loc[pi] if pi in st.session_state.panel.index else {}
                    sid_v=extract_sid(v) if "|" in v else norm_id(v)
                    cnt_d=duty_stats(sf).get(sid_v,{}).get("count",0)
                    rows.append({"Row":k,"INSCODE":r.get("INSCODE","?"),"SUBCODE":r.get("SUBCODE","?"),
                                 "→ EXTID":sid_v,"Name":get_name(sf,sid_v),
                                 "Priority":f"{priority_icon(cnt_d)} {cnt_d} duties"})
                except: rows.append({"Row":k,"→ EXTID":v})
            st.dataframe(pd.DataFrame(rows),use_container_width=True,height=220)

        a1,a2=st.columns(2)
        if a1.button("✅ Apply ALL Staged",type="primary",use_container_width=True):
            ok_c,fc2b=[],[]
            for k,v in list(staged_map.items()):
                try: pi=int(k)
                except: fc2b.append(k); continue
                if pi not in st.session_state.panel.index: fc2b.append(k); continue
                sid_c=extract_sid(v) if "|" in v else norm_id(v)
                if sid_c:
                    st.session_state.panel.at[pi,"EXTID"]=sid_c
                    st.session_state.staged.pop(k,None); ok_c.append(k)
                else: fc2b.append(k)
            P(); st.success(f"✅ Applied {len(ok_c)} · ❌ Failed {len(fc2b)}")
            st.rerun()
        if a2.button("🗑️ Clear All Staged",use_container_width=True):
            st.session_state.staged={}; st.success("✅ Cleared"); st.rerun()

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
| 🔴 2 | **Date Clash** | Same staff · overlapping dates · different SUBCODE = ❌ |
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
    filt2=st.session_state.pdate.copy()
    if ins_g!="All": filt2=filt2[filt2["INSCODE"].astype(str)==ins_g]
    if nc_g !="All": filt2=filt2[filt2["NCNO"].astype(str)==nc_g]
    with gc3:
        st.markdown("<br>",unsafe_allow_html=True)
        if st.button("🔍 Run Error Check",type="primary",use_container_width=True):
            if st.session_state.pdate.empty:
                st.error("❌ Upload dated panel first!")
            else:
                with st.spinner("Running checks..."):
                    err_map=check_errors(filt2,st.session_state.staff)
                for idx in filt2.index:
                    if idx in st.session_state.pdate.index:
                        msgs=err_map.get(idx,[])
                        st.session_state.pdate.at[idx,"ERROR"]=" | ".join(msgs) if msgs else ""
                PD(); st.session_state.errors=err_map
                total=sum(len(v) for v in err_map.values())
                if total==0:
                    st.markdown('<div class="ok-card">✅ All checks passed! No clashes found.</div>',unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="err-card">🔴 {total} issue(s) in {len(err_map)} rows — see below.</div>',unsafe_allow_html=True)

    if st.session_state.errors:
        st.markdown('<hr class="thin">',unsafe_allow_html=True)
        st.markdown('<div class="sec-hdr">🔴 Error Report</div>',unsafe_allow_html=True)
        for idx,msgs in st.session_state.errors.items():
            r=st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
            with st.expander(f"🔴 Row {idx} · 🏫{r.get('INSCODE','?')} · 📚{r.get('SUBCODE','?')} · {len(msgs)} issue(s)"):
                for m in msgs:
                    st.markdown(f'<div class="err-card">{m}</div>',unsafe_allow_html=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">📊 Duty Count per Staff</div>',unsafe_allow_html=True)
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
            st.markdown('<div class="info-card">ℹ️ No staff in dated panel.</div>',unsafe_allow_html=True)
    else:
        st.markdown('<div class="info-card">ℹ️ Upload dated panel to see chart.</div>',unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# TAB 4 — DOWNLOADS (separate tab)
# ═══════════════════════════════════════════════════════
with tab_dl:
    st.markdown('<div class="sec-hdr">📦 Downloads — Panel CSVs, Dated Panel, PDF Duty Sheets</div>',unsafe_allow_html=True)

    all_p=st.session_state.panel.copy()
    all_d=st.session_state.pdate.copy()
    sf_dl=st.session_state.staff.copy()
    sub_dl=st.session_state.submap.copy()

    exp_p=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in all_p.columns]
    exp_d=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"] if c in all_d.columns]

    # ─── Section 1: Panel (No Dates) CSVs ───
    st.markdown('<span class="sub-hdr">📋 Panel (No Dates) — CSV Downloads</span>',unsafe_allow_html=True)
    if all_p.empty:
        st.markdown('<div class="info-card">ℹ️ No panel data. Upload in Upload Centre tab.</div>',unsafe_allow_html=True)
    else:
        inscodes_p=sorted(set(all_p["INSCODE"].astype(str)))
        # Full download first
        dl_full_cols=st.columns([2,2,2])
        with dl_full_cols[0]:
            st.download_button(
                label=f"📥 Full Panel CSV — {len(all_p)} rows",
                data=all_p[exp_p].to_csv(index=False).encode(),
                file_name="panel_full.csv", mime="text/csv",
                use_container_width=True)
        with dl_full_cols[1]:
            # With SUBNAME column
            pf2=all_p.copy()
            if not sub_dl.empty:
                pf2=pf2.merge(sub_dl[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            exp2=[c for c in exp_p+["SUBNAME"] if c in pf2.columns]
            st.download_button(
                label=f"📥 Panel CSV + SUBNAME — {len(pf2)} rows",
                data=pf2[exp2].to_csv(index=False).encode(),
                file_name="panel_full_subname.csv", mime="text/csv",
                use_container_width=True)
        with dl_full_cols[2]:
            # Pending only
            pend=all_p[all_p["EXTID"].apply(norm_id)==""]
            st.download_button(
                label=f"📥 Pending EXTID Only — {len(pend)} rows",
                data=pend[exp_p].to_csv(index=False).encode(),
                file_name="panel_pending_extid.csv", mime="text/csv",
                use_container_width=True)

        st.markdown('<span class="sub-hdr" style="font-size:.82rem">📊 Per Institution</span>',unsafe_allow_html=True)
        cols_per_row=4
        ins_chunks=[inscodes_p[i:i+cols_per_row] for i in range(0,len(inscodes_p),cols_per_row)]
        for chunk in ins_chunks:
            cols=st.columns(cols_per_row)
            for ci,ins in enumerate(chunk):
                df_i=all_p[all_p["INSCODE"].astype(str)==ins][exp_p]
                ef_i=df_i["EXTID"].apply(norm_id).ne("").sum()
                cols[ci].download_button(
                    label=f"📥 INSCODE {ins}\n({ef_i}/{len(df_i)} filled)",
                    data=df_i.to_csv(index=False).encode(),
                    file_name=f"panel_{ins}.csv", mime="text/csv",
                    key=f"dl_p_{ins}", use_container_width=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ─── Section 2: Dated Panel CSVs ───
    st.markdown('<span class="sub-hdr">🗓️ Dated Panel — CSV Downloads</span>',unsafe_allow_html=True)
    if all_d.empty:
        st.markdown('<div class="info-card">ℹ️ No dated panel. Upload in Duty Marking tab.</div>',unsafe_allow_html=True)
    else:
        inscodes_d=sorted(set(all_d["INSCODE"].astype(str)))
        dl2=st.columns([2,2,2])
        with dl2[0]:
            st.download_button(
                label=f"📥 Full Dated Panel — {len(all_d)} rows",
                data=all_d[exp_d].to_csv(index=False).encode(),
                file_name="dated_panel_full.csv", mime="text/csv",
                use_container_width=True)
        with dl2[1]:
            errd=all_d[all_d["ERROR"].astype(str).str.strip()!=""]
            st.download_button(
                label=f"📥 Errors Only — {len(errd)} rows",
                data=errd[exp_d].to_csv(index=False).encode(),
                file_name="dated_panel_errors.csv", mime="text/csv",
                use_container_width=True)

        st.markdown('<span class="sub-hdr" style="font-size:.82rem">📊 Per Institution</span>',unsafe_allow_html=True)
        ins_d_chunks=[inscodes_d[i:i+4] for i in range(0,len(inscodes_d),4)]
        for chunk in ins_d_chunks:
            cols=st.columns(4)
            for ci,ins in enumerate(chunk):
                df_i=all_d[all_d["INSCODE"].astype(str)==ins][exp_d]
                cols[ci].download_button(
                    label=f"📥 Dated {ins} ({len(df_i)} rows)",
                    data=df_i.to_csv(index=False).encode(),
                    file_name=f"dated_{ins}.csv", mime="text/csv",
                    key=f"dl_d_{ins}", use_container_width=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ─── Section 3: PDF Duty Sheets ───
    st.markdown('<span class="sub-hdr">🖨️ PDF Duty Sheets</span>',unsafe_allow_html=True)
    if all_p.empty:
        st.markdown('<div class="warn-card">⚠️ Upload panel data first.</div>',unsafe_allow_html=True)
    else:
        pdf_ins_f=st.selectbox("🏫 Filter by INSCODE for PDF",["All"]+sorted(set(all_p["INSCODE"].astype(str))),key="pdf_ins")
        pdf_data=all_p.copy()
        if pdf_ins_f!="All": pdf_data=pdf_data[pdf_data["INSCODE"].astype(str)==pdf_ins_f]
        st.markdown(f'<div class="info-card">📄 Will generate duty sheets for <b>{len(pdf_data)}</b> panel rows</div>',unsafe_allow_html=True)

        if RPDF:
            p1,p2=st.columns(2)
            with p1:
                if st.button("⚙️ Generate PDF Duty Sheets",type="primary",use_container_width=True):
                    with st.spinner("Building PDF... (may take a moment)"):
                        try:
                            pdf_b=generate_pdf_rl(pdf_data,sf_dl,sub_dl)
                            st.download_button(
                                "📄 Download PDF Duty Sheets",
                                data=pdf_b,
                                file_name=f"duty_sheets{'_'+pdf_ins_f if pdf_ins_f!='All' else ''}.pdf",
                                mime="application/pdf",
                                use_container_width=True)
                            st.success("✅ PDF ready!")
                        except Exception as e:
                            st.error(f"❌ PDF error: {e}")
            with p2:
                if st.button("🌐 Generate HTML (Print to PDF)",use_container_width=True):
                    with st.spinner("Building HTML..."):
                        html_b=generate_html_duties(pdf_data,sf_dl,sub_dl)
                    st.download_button(
                        "📄 Download HTML Duty Sheets",
                        data=html_b,
                        file_name=f"duty_sheets{'_'+pdf_ins_f if pdf_ins_f!='All' else ''}.html",
                        mime="text/html",
                        use_container_width=True)
                    st.markdown('<div class="info-card">💡 Open HTML in browser → Ctrl+P → Save as PDF for perfect formatting</div>',unsafe_allow_html=True)
        else:
            st.markdown('<div class="warn-card">⚠️ reportlab not found — add to requirements.txt</div>',unsafe_allow_html=True)
            if st.button("🌐 Generate HTML Duty Sheets (fallback)",use_container_width=True):
                with st.spinner("Building HTML..."):
                    html_b=generate_html_duties(pdf_data,sf_dl,sub_dl)
                st.download_button("📄 Download HTML",data=html_b,
                    file_name="duty_sheets.html",mime="text/html",use_container_width=True)
                st.markdown('<div class="info-card">💡 Open HTML in browser → Ctrl+P → Save as PDF</div>',unsafe_allow_html=True)

    st.markdown('<hr class="thin">',unsafe_allow_html=True)

    # ─── Section 4: Staff CSV ───
    st.markdown('<span class="sub-hdr">🧑‍🏫 Staff Data</span>',unsafe_allow_html=True)
    if not sf_dl.empty:
        sf_exp=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"] if c in sf_dl.columns]
        st.download_button(f"📥 Staff CSV — {len(sf_dl)} records",
            data=sf_dl[sf_exp].to_csv(index=False).encode(),
            file_name="staff_all.csv",mime="text/csv",use_container_width=False)
    else:
        st.markdown('<div class="info-card">ℹ️ No staff data loaded.</div>',unsafe_allow_html=True)

    st.markdown(
        f'<div style="text-align:center;margin-top:24px">'
        f'<span style="background:#161b22;border:1px solid #30363d;border-radius:20px;'
        f'padding:5px 18px;color:#8b949e;font-size:.75rem">✨ {CREATOR}</span></div>',
        unsafe_allow_html=True)
