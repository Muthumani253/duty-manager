#!/usr/bin/env python3
"""Duty Manager v5 — MUTHUMANI S, LECTURER-EEE, GPT KARUR | 9443100811"""
from __future__ import annotations
import os, uuid, subprocess, sys
from datetime import datetime, date
import re
from io import BytesIO

import streamlit as st
import pandas as pd

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

st.markdown("""
<style>
/* ── Reset & Base ── */
.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],
section[data-testid="stMain"],[data-testid="stVerticalBlock"],.main,body{background-color:#0d1117 !important;}
[data-testid="stSidebar"],[data-testid="collapsedControl"],
header[data-testid="stHeader"],#MainMenu,footer{display:none !important;}
.main .block-container{padding:0 1rem 1.5rem !important;max-width:100% !important;}
body,.stApp{color:#c9d1d9 !important;}
p,li,span,div,label,td,th,small,strong,b,i,em,
[data-testid="stMarkdownContainer"] *{color:#c9d1d9 !important;}
h1,h2,h3,h4,h5,h6{color:#e6edf3 !important;}
/* ── Tables ── */
[data-testid="stMarkdownContainer"] table{border-collapse:collapse;width:100%;}
[data-testid="stMarkdownContainer"] th{background:#1c2333 !important;color:#e6edf3 !important;padding:5px 10px !important;border:1px solid #30363d !important;font-weight:700;}
[data-testid="stMarkdownContainer"] td{background:#0d1117 !important;color:#c9d1d9 !important;padding:4px 10px !important;border:1px solid #21262d !important;}
code,pre{background:#010409 !important;color:#79c0ff !important;font-size:.78rem !important;border-radius:4px !important;}
/* ── TOP BAR ── */
.topbar{background:#010409;border-bottom:2px solid #6366f1;padding:0 16px;
    display:flex;align-items:center;justify-content:space-between;
    height:50px;margin:0 -1rem 0;position:sticky;top:0;z-index:100;}
.tb-logo{background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:8px;
    padding:6px 8px;font-size:1.1rem;line-height:1;margin-right:2px;}
.tb-title{color:#e6edf3 !important;font-weight:700;font-size:.95rem;line-height:1.2;}
.tb-sub{color:#6e7681 !important;font-size:.62rem;}
/* ── STATS CHIPS ── */
.sc{background:#161b22;border:1px solid #21262d;border-radius:5px;
    padding:2px 8px;font-size:.72rem;color:#8b949e !important;white-space:nowrap;}
.sc b{color:#e6edf3 !important;}
.progress-chip{position:relative;overflow:hidden;min-width:50px;text-align:center;}
.prog-bar{position:absolute;left:0;top:0;height:100%;background:linear-gradient(90deg,#22c55e33,#22c55e55);border-radius:5px;}
/* ── TABS ── */
.stTabs [data-baseweb="tab-list"]{background:#010409 !important;border-bottom:1px solid #21262d !important;
    gap:0 !important;padding:0 2px !important;margin:0 -1rem 0.8rem !important;overflow-x:auto !important;}
.stTabs [data-baseweb="tab"]{background:transparent !important;color:#8b949e !important;
    border:none !important;border-bottom:2px solid transparent !important;border-radius:0 !important;
    font-size:.84rem !important;font-weight:600 !important;padding:10px 18px !important;white-space:nowrap !important;}
.stTabs [data-baseweb="tab"]:hover{color:#e6edf3 !important;background:#161b22 !important;}
.stTabs [aria-selected="true"]{color:#6366f1 !important;border-bottom-color:#6366f1 !important;}
.stTabs [data-baseweb="tab"] p{color:inherit !important;font-size:inherit !important;font-weight:inherit !important;}
[data-testid="stTabsContent"]{padding:0 !important;border:none !important;background:transparent !important;}
/* ── SECTION HEADERS ── */
.sub-hdr{color:#e6edf3 !important;font-size:.84rem;font-weight:700;
    padding:0 0 4px;border-bottom:1px solid #21262d;margin:6px 0 5px;display:block;}
/* ── CARDS ── */
.err-card{background:#2d1515;border-left:3px solid #ef4444;border-radius:5px;padding:6px 10px;margin:3px 0;color:#fca5a5 !important;}
.err-card *{color:#fca5a5 !important;}
.ok-card{background:#0d2218;border-left:3px solid #22c55e;border-radius:5px;padding:6px 10px;margin:3px 0;color:#86efac !important;}
.ok-card *{color:#86efac !important;}
.warn-card{background:#2a1f0a;border-left:3px solid #f59e0b;border-radius:5px;padding:6px 10px;margin:3px 0;color:#fcd34d !important;}
.warn-card *{color:#fcd34d !important;}
.info-card{background:#0c1a2e;border-left:3px solid #3b82f6;border-radius:5px;padding:6px 10px;margin:3px 0;color:#93c5fd !important;}
.info-card *{color:#93c5fd !important;}
/* ── ALLOC CARD ── */
.alloc-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 14px;margin:6px 0;}
.alloc-card-pending{border-left:4px solid #ef4444 !important;}
.alloc-card-staged{border-left:4px solid #f59e0b !important;}
.alloc-card-done{border-left:4px solid #22c55e !important;}
/* ── CHIP ROW ── */
.chip-row{display:flex;gap:6px;flex-wrap:wrap;margin:5px 0 8px;}
.chip{background:#161b22;border:1px solid #21262d;border-radius:4px;padding:2px 8px;font-size:.73rem;color:#8b949e !important;}
.chip-ok{background:#0d2218;border:1px solid #22c55e;border-radius:4px;padding:2px 8px;font-size:.73rem;color:#86efac !important;}
.chip-warn{background:#2a1f0a;border:1px solid #f59e0b;border-radius:4px;padding:2px 8px;font-size:.73rem;color:#fcd34d !important;}
/* ── INPUTS ── */
[data-testid="stSelectbox"]>div>div{background:#161b22 !important;border:1px solid #30363d !important;border-radius:5px !important;color:#e6edf3 !important;}
[data-testid="stSelectbox"] span,[data-testid="stSelectbox"] label p{color:#8b949e !important;font-size:.8rem !important;}
[data-baseweb="popover"] ul,[data-baseweb="menu"]{background:#161b22 !important;border:1px solid #21262d !important;}
[data-baseweb="menu"] li{color:#c9d1d9 !important;background:#161b22 !important;}
[data-baseweb="menu"] li:hover{background:#21262d !important;}
[data-testid="stTextInput"] input{background:#161b22 !important;border:1px solid #30363d !important;color:#e6edf3 !important;border-radius:5px !important;}
[data-testid="stTextInput"] label p{color:#8b949e !important;font-size:.8rem !important;}
[data-testid="stCheckbox"] label p{color:#c9d1d9 !important;}
[data-testid="stFileUploader"]{background:#161b22 !important;border:1px solid #21262d !important;border-radius:7px !important;}
[data-testid="stFileUploaderDropzone"]{background:#0d1117 !important;border:1px dashed #30363d !important;border-radius:5px !important;}
[data-testid="stFileUploaderDropzone"] p,[data-testid="stFileUploaderDropzone"] span{color:#6e7681 !important;}
[data-testid="stFileUploaderDropzone"] button{background:#21262d !important;color:#c9d1d9 !important;border:1px solid #30363d !important;}
/* ── BUTTONS ── */
[data-testid="stDownloadButton"] button{background:linear-gradient(135deg,#6366f1,#8b5cf6) !important;color:#fff !important;border:none !important;border-radius:6px !important;font-weight:600 !important;font-size:.8rem !important;}
[data-testid="stDownloadButton"] button p{color:#fff !important;}
.stButton>button{border-radius:6px !important;font-weight:600 !important;font-size:.82rem !important;}
.stButton>button[kind="primary"]{background:linear-gradient(135deg,#6366f1,#8b5cf6) !important;border:none !important;color:#fff !important;}
.stButton>button[kind="secondary"]{background:#161b22 !important;border:1px solid #30363d !important;color:#e6edf3 !important;}
.stButton>button[kind="secondary"]:hover{border-color:#6366f1 !important;}
/* ── DATA TABLES ── */
div[data-testid="stDataFrame"],div[data-testid="stDataEditor"]{border-radius:7px !important;overflow:hidden !important;}
/* ── EXPANDER ── */
[data-testid="stExpander"]{background:#161b22 !important;border:1px solid #21262d !important;border-radius:7px !important;}
[data-testid="stExpander"] summary{background:#161b22 !important;padding:7px 14px !important;}
[data-testid="stExpander"] summary p{color:#e6edf3 !important;font-weight:600 !important;}
.streamlit-expanderContent{background:#0d1117 !important;padding:10px !important;}
/* ── BADGES ── */
.badge-green{background:#0d2218;color:#22c55e;border:1px solid #22c55e;border-radius:10px;padding:1px 7px;font-size:.7rem;font-weight:700;}
.badge-yellow{background:#2a1f0a;color:#f59e0b;border:1px solid #f59e0b;border-radius:10px;padding:1px 7px;font-size:.7rem;font-weight:700;}
.badge-red{background:#2d1515;color:#ef4444;border:1px solid #ef4444;border-radius:10px;padding:1px 7px;font-size:.7rem;font-weight:700;}
hr.thin{border:none;border-top:1px solid #21262d;margin:6px 0;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-track{background:#0d1117;}
::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px;}
::-webkit-scrollbar-thumb:hover{background:#6366f1;}
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
def get_depcode(sf,sid): return get_col(sf,sid,"dep code")

def get_subname(sm,code):
    if sm is None or sm.empty: return ""
    m=sm[sm["SUBCODE"].astype(str)==str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def priority_icon(count):
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

def P():
    df=st.session_state.panel.copy()
    key_c=[c for c in ["INSCODE","SUBCODE","REGL","INTID"] if c in df.columns]
    df=df.drop_duplicates(subset=key_c,keep="last").reset_index(drop=True)
    st.session_state.panel=rowid(df,"p")
    save_csv(st.session_state.panel,PANEL_PATH)
def PD(): st.session_state.pdate=rowid(st.session_state.pdate,"d"); save_csv(st.session_state.pdate,PANEL_DATED_PATH)
def S():  st.session_state.staff=rowid(st.session_state.staff,"s"); save_csv(st.session_state.staff,STAFF_PATH)
def SM(): save_csv(st.session_state.submap,SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap,SUBJMAP_PATH)

# ═══════════════════════════════════════════════════════
# LOGIC
# ═══════════════════════════════════════════════════════
def duty_stats(sf):
    """Count duties per staff — called ONCE and cached."""
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

def build_precomputed(sf, ssmap):
    """
    Pre-compute all lookups ONCE so per-row suggestions are O(1) dict lookups.
    Returns (staff_list, duty_stats_dict, ssmap_index)
    - staff_list: list of dicts, one per staff member
    - duty_stats_dict: {sid: count}
    - ssmap_index: {subject_code_upper: set(sid)}
    """
    stats = duty_stats(sf)

    staff_list = []
    for _,row in sf.iterrows():
        sid = norm_id(row.get("Staff ID"))
        if not sid: continue
        cnt = stats.get(sid,{}).get("count",0)
        staff_list.append({
            "sid":     sid,
            "name":    row.get("Name of the Staff",""),
            "desig":   row.get("Designation",""),
            "instt":   str(row.get("INSTT","")).strip(),
            "dep":     str(row.get("dep code","")).strip(),
            "depname": row.get("Department",""),
            "phone":   row.get("Phone",""),
            "count":   cnt,
            "icon":    priority_icon(cnt),
            "cls":     priority_class(cnt),
        })

    ssmap_index = {}
    if ssmap is not None and not ssmap.empty:
        for _,row in ssmap.iterrows():
            sc = str(row.get("Subject_Code","")).strip().upper()
            sid = norm_id(row.get("Staff_Last_Staff_ID",""))
            if sc and sid:
                ssmap_index.setdefault(sc, set()).add(sid)

    return staff_list, stats, ssmap_index

def ext_suggestions_fast(panel_row, staff_list, ssmap_index):
    """
    Fast version — uses pre-built staff_list and ssmap_index dicts.
    O(staff) single pass, no re-iterating sf or ssmap.
    Returns (willing, same_dept, others).
    """
    p_ins = str(panel_row.get("INSCODE","")).strip()
    sub   = str(panel_row.get("SUBCODE","")).strip().upper()
    p_dep = str(panel_row.get("NCNO","")).strip()

    willing_ids = ssmap_index.get(sub, set())

    willing, same_dept, others = [], [], []
    for s in staff_list:
        if s["instt"] == p_ins: continue   # must be external
        if s["sid"] in willing_ids:
            willing.append(s)
        elif s["dep"] == p_dep:
            same_dept.append(s)
        else:
            others.append(s)

    willing.sort(key=lambda x:x["count"])
    same_dept.sort(key=lambda x:x["count"])
    others.sort(key=lambda x:x["count"])
    return willing, same_dept, others

def ext_suggestions_v2(panel_row, sf, ssmap):
    """Wrapper for compatibility — builds precomputed on each call (slow path, for manual)."""
    staff_list, _, ssmap_index = build_precomputed(sf, ssmap)
    return ext_suggestions_fast(panel_row, staff_list, ssmap_index)

def make_ext_label(s, category):
    """
    Format: EMOJI INSTT-Dept | StaffID-Name | Designation | Duties:N
    category: 'willing' -> 🟢, 'same_dept' -> 🟡, 'other' -> ⚪
    """
    icon = {"willing":"🟢","same_dept":"🟡","other":"⚪"}.get(category,"⚪")
    dep_part  = f"{s['instt']}-{s['dep']}"   if s['dep']  else s['instt']
    id_name   = f"{s['sid']}-{s['name']}"    if s['name'] else s['sid']
    cnt = s['count']
    duty_icon = "🟢" if cnt==0 else ("🟡" if cnt<=2 else "🔴")
    return f"{icon} {dep_part} | {id_name} | {s['desig']} | {duty_icon}Duties:{cnt}"

def make_dropdown_label(s):
    return f"{s['icon']} {s['sid']} | {s['name']} | {s['desig']} | 🏫{s['instt']} | Duties:{s['count']}"

def extract_sid(label):
    """Extract StaffID from label: EMOJI INSTT-Dept | StaffID-Name | ... """
    l=str(label).strip()
    l=re.sub(r'^[🟢🟡🔴⚪]\s*','',l)
    parts=[p.strip() for p in l.split("|")]
    if len(parts)>=2:
        # part[1] is "StaffID-Name" — extract just the ID (before first hyphen-ish split or use norm_id)
        id_name_part = parts[1].strip()
        # StaffID contains letters+digits, typically format like X123EEE1
        # Split on first " - " or "-" that separates ID from name
        m = re.match(r'([A-Z0-9]+)',id_name_part.upper())
        if m:
            return norm_id(m.group(1))
        return norm_id(id_name_part.split("-")[0])
    return norm_id(parts[0])

def build_dropdown_options(willing, same_dept, others):
    """Build dropdown options list with section headers."""
    opts = ["— Select External Examiner —"]
    if willing:
        opts.append("── 🟢 WILLING STAFF (Mapped to Subject) ──")
        for s in willing:
            opts.append(make_ext_label(s,"willing"))
    if same_dept:
        opts.append("── 🟡 SAME DEPARTMENT (Not in Willing) ──")
        for s in same_dept:
            opts.append(make_ext_label(s,"same_dept"))
    if others:
        opts.append("── ⚪ OTHER STAFF (Less Duty Priority) ──")
        for s in others:
            opts.append(make_ext_label(s,"other"))
    return opts

def is_header_opt(lbl):
    return lbl.startswith("──") or lbl.startswith("— Select")

def auto_allocate(candidates, sf, ssmap, progress_cb=None):
    """
    Fast allocator with LIVE duty-count tracking.
    As each staff is allocated, their count is incremented in-memory so
    subsequent rows see accurate duty loads — prevents same-staff repeat allocation.
    """
    res, skip = {}, {}
    if sf.empty:
        return res, skip

    staff_list, _, ssmap_index = build_precomputed(sf, ssmap)

    # Live counts dict — key: sid, value: current duty count (mutable)
    live_counts = {s["sid"]: s["count"] for s in staff_list}

    total = len(candidates)
    for i, (pidx, row) in enumerate(candidates.iterrows()):
        # Update each entry in staff_list with current live count before sorting
        for s in staff_list:
            s["count"] = live_counts.get(s["sid"], s["count"])
            s["icon"]  = priority_icon(s["count"])
            s["cls"]   = priority_class(s["count"])

        willing, same_dept, others = ext_suggestions_fast(row, staff_list, ssmap_index)
        best = willing or same_dept or others
        if best:
            chosen = best[0]
            cat = "willing" if willing else ("same_dept" if same_dept else "other")
            res[pidx] = make_ext_label(chosen, cat)
            # Increment live count so this staff appears busier for next row
            live_counts[chosen["sid"]] = live_counts.get(chosen["sid"], 0) + 1
        else:
            skip[pidx] = f"No eligible external staff for SUBCODE {row.get('SUBCODE','?')}"

        if progress_cb:
            progress_cb(i+1, total)

    return res, skip

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
# PDF LAZY IMPORT — works on Streamlit Cloud
# ═══════════════════════════════════════════════════════
def _try_import_reportlab():
    """Lazy import reportlab, auto-install if missing. Returns True if available."""
    try:
        import reportlab  # noqa
        return True
    except ImportError:
        pass
    try:
        import subprocess, sys as _sys
        subprocess.check_call([_sys.executable,"-m","pip","install","reportlab","-q","--quiet"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import reportlab  # noqa
        return True
    except Exception:
        return False

def get_pdf_bytes(panel_df, sf, submap):
    """Generate PDF bytes. Returns (bytes, error_msg). Falls back to None on failure."""
    if not _try_import_reportlab():
        return None, "reportlab unavailable"
    try:
        from reportlab.lib import colors as _RC
        from reportlab.lib.pagesizes import A4 as _A4
        from reportlab.lib.styles import ParagraphStyle as _PS
        from reportlab.lib.units import cm as _cm
        from reportlab.platypus import (SimpleDocTemplate as _SDT, Table as _T,
                                         TableStyle as _TS, Paragraph as _P,
                                         Spacer as _SP, PageBreak as _PB)
        from reportlab.lib.enums import TA_CENTER as _TAC
        buf = BytesIO()
        doc = _SDT(buf, pagesize=_A4, leftMargin=1.5*_cm, rightMargin=1.5*_cm,
                   topMargin=1.5*_cm, bottomMargin=1.5*_cm)
        H1  = _PS("H1",  fontSize=12, fontName="Helvetica-Bold", spaceAfter=4,  alignment=_TAC)
        SML = _PS("SML", fontSize=7,  fontName="Helvetica",      textColor=_RC.grey, alignment=_TAC)
        story=[]; sd={}
        for _,row in panel_df.iterrows():
            sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
            ins=str(row.get("INSCODE","")).strip()
            for role,fld in [("INT","INTID"),("EXT","EXTID")]:
                sid=norm_id(row.get(fld,""))
                if not sid: continue
                sd.setdefault(sid,[]).append({"ins":ins,"sc":sc,"sn":sn,"role":role,
                    "cid":norm_id(row.get("EXTID" if role=="INT" else "INTID",""))})
        for sid in sorted(sd.keys(), key=lambda s: get_name(sf,s)):
            duties=sd.get(sid,[])
            if not duties: continue
            name=get_name(sf,sid); phone=get_phone(sf,sid)
            m=sf[sf["Staff ID"].astype(str).str.upper()==sid]
            desig=str(m.iloc[0]["Designation"]) if not m.empty else ""
            dept =str(m.iloc[0]["Department"])  if not m.empty else ""
            instt=str(m.iloc[0]["INSTT"])       if not m.empty else ""
            story.append(_P("PRACTICAL EXAM DUTY ORDER", H1))
            story.append(_P(CREATOR, SML)); story.append(_SP(1,.3*_cm))
            ht=_T([["Staff ID",sid,"Name",name],["Institution",instt,"Phone",phone],
                   ["Department",dept,"Designation",desig]],
                  colWidths=[2.5*_cm,4.5*_cm,2.5*_cm,7*_cm])
            ht.setStyle(_TS([
                ("BACKGROUND",(0,0),(-1,-1),_RC.HexColor("#161b22")),
                ("TEXTCOLOR",(0,0),(-1,-1),_RC.white),
                ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
                ("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
                ("FONTSIZE",(0,0),(-1,-1),8),
                ("GRID",(0,0),(-1,-1),.4,_RC.HexColor("#30363d")),
                ("PADDING",(0,0),(-1,-1),5),
            ]))
            story.append(ht); story.append(_SP(1,.4*_cm))
            tr=[["S.No","Exam Centre","SubCode","Subject","Role","Partner ID","Partner Name","Phone","Date From","Date To"]]
            for sno,d in enumerate(duties,1):
                pid=d["cid"]; pn=get_name(sf,pid) if pid else ""; pp=get_phone(sf,pid) if pid else ""
                tr.append([str(sno),d["ins"],d["sc"],d["sn"] or d["sc"],d["role"],
                           pid or "-",pn or "-",pp or "-","",""])
            dt=_T(tr,colWidths=[.8*_cm,2*_cm,2*_cm,4*_cm,1.2*_cm,2.2*_cm,3.5*_cm,2.2*_cm,2*_cm,2*_cm],repeatRows=1)
            dt.setStyle(_TS([
                ("BACKGROUND",(0,0),(-1,0),_RC.HexColor("#6366f1")),
                ("TEXTCOLOR",(0,0),(-1,0),_RC.white),
                ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
                ("FONTSIZE",(0,0),(-1,-1),7),
                ("ALIGN",(0,0),(-1,-1),"CENTER"),
                ("ALIGN",(3,1),(3,-1),"LEFT"),("ALIGN",(6,1),(6,-1),"LEFT"),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[_RC.HexColor("#f8fafc"),_RC.HexColor("#e8edf5")]),
                ("GRID",(0,0),(-1,-1),.4,_RC.HexColor("#94a3b8")),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("PADDING",(0,0),(-1,-1),4),
            ]))
            story.append(dt); story.append(_SP(1,.3*_cm))
            story.append(_P("Date From / To to be filled by Flying Squad at duty.", SML))
            story.append(_PB())
        doc.build(story)
        return buf.getvalue(), None
    except Exception as e:
        return None, str(e)

def generate_html_duties(panel_df, sf, submap, auto_print=False):
    """Generate print-ready HTML. If auto_print=True, injects window.print() on load."""
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
            rows+=f"<tr><td>{sno}</td><td>{d['ins']}</td><td>{d['sc']}</td><td style='text-align:left'>{d['sn'] or d['sc']}</td><td><b>{d['role']}</b></td><td>{pid or '-'}</td><td style='text-align:left'>{pn}</td><td>{pp}</td><td class='fill'></td><td class='fill'></td></tr>"
        pages.append(f"""<div class='page'>
<div class='title'>PRACTICAL EXAM DUTY ORDER</div>
<div class='creator'>{CREATOR}</div>
<table class='hdr'>
  <tr><th>Staff ID</th><td><b>{sid}</b></td><th>Name</th><td><b>{name}</b></td></tr>
  <tr><th>Institution</th><td>{instt}</td><th>Phone</th><td>{phone}</td></tr>
  <tr><th>Department</th><td>{dept}</td><th>Designation</th><td>{desig}</td></tr>
</table>
<table class='duty'>
  <thead><tr><th>#</th><th>Exam Centre</th><th>SubCode</th><th>Subject Name</th>
  <th>Role</th><th>Partner ID</th><th>Partner Name</th><th>Phone</th><th>Date From</th><th>Date To</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<p class='note'>★ Date From / To to be filled by the Flying Squad officer at the time of duty.</p>
</div>""")
    auto_js = "<script>window.onload=()=>{window.print();}</script>" if auto_print else ""
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Duty Sheets — {CREATOR}</title>
{auto_js}
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'Arial',sans-serif;font-size:10px;background:#fff;color:#000;}}
.page{{page-break-after:always;padding:16px 20px;max-width:100%;}}
.title{{font-size:13px;font-weight:bold;text-align:center;margin-bottom:2px;text-transform:uppercase;letter-spacing:1px;}}
.creator{{font-size:8px;text-align:center;color:#666;margin-bottom:8px;}}
.note{{font-size:8px;color:#555;margin-top:5px;font-style:italic;}}
table{{width:100%;border-collapse:collapse;margin-bottom:6px;}}
table.hdr th{{background:#1e293b;color:#fff;padding:4px 8px;text-align:left;width:100px;font-size:8.5px;white-space:nowrap;}}
table.hdr td{{padding:4px 8px;border:1px solid #cbd5e1;font-size:9px;}}
table.duty th{{background:#4f46e5;color:#fff;padding:4px 5px;text-align:center;font-size:8px;white-space:nowrap;}}
table.duty td{{padding:3px 5px;border:1px solid #e2e8f0;text-align:center;font-size:8.5px;}}
table.duty td.fill{{background:#fffbeb;min-width:60px;}}
table.duty tr:nth-child(even) td{{background:#f8fafc;}}
table.duty tr:nth-child(even) td.fill{{background:#fffde7;}}
@media print{{
  body{{margin:0;}}
  .page{{page-break-after:always;border:none;padding:12px 16px;}}
  @page{{margin:1cm;}}
}}
</style></head><body>{"".join(pages)}</body></html>""".encode("utf-8")

# ═══════════════════════════════════════════════════════
# REUSABLE PDF DOWNLOAD WIDGET
# ═══════════════════════════════════════════════════════
def pdf_download_section(panel_df, sf_dl, sub_dl, key_prefix="pdl"):
    """Compact PDF/HTML download section. HTML is primary (always works). PDF is bonus."""
    if panel_df.empty:
        st.markdown('<div class="warn-card">⚠️ No panel data to generate.</div>', unsafe_allow_html=True)
        return

    ins_list = ["All"] + sorted(set(panel_df["INSCODE"].astype(str)))
    sel_ins  = st.selectbox("🏫 Filter by INSCODE", ins_list, key=f"{key_prefix}_ins")
    pdf_df   = panel_df.copy()
    if sel_ins != "All":
        pdf_df = pdf_df[pdf_df["INSCODE"].astype(str)==sel_ins]

    n_tot = len(pdf_df)
    n_ok  = int(pdf_df["EXTID"].apply(norm_id).ne("").sum())
    n_pen = n_tot - n_ok

    st.markdown(
        f'<div class="chip-row">'
        f'<span class="chip">📄 {n_tot} rows</span>'
        f'<span class="chip-ok">✅ EXT filled: {n_ok}</span>'
        f'<span class="chip-warn">⏳ Pending: {n_pen}</span>'
        f'</div>', unsafe_allow_html=True)

    # ─── HTML is ALWAYS available ────────────────────────
    st.markdown('<div class="sub-hdr" style="font-size:.75rem;margin-top:6px">📄 Duty Sheets</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    # Column 1: HTML (primary — always works)
    with col1:
        html_b = generate_html_duties(pdf_df, sf_dl, sub_dl, auto_print=False)
        fname_h = f"duty_{sel_ins}.html" if sel_ins != "All" else "duty_all.html"
        st.download_button(
            label="🌐 Download HTML Duty Sheets",
            data=html_b, file_name=fname_h, mime="text/html",
            use_container_width=True, type="primary",
            key=f"{key_prefix}_html_dl",
            help="Open in browser → Ctrl+P → Save as PDF. Works on all devices.")
        st.markdown(
            '<div class="info-card" style="font-size:.72rem;margin-top:3px">'
            '💡 Open HTML → Ctrl+P → Save as PDF (recommended)</div>',
            unsafe_allow_html=True)

    # Column 2: PDF (try reportlab, graceful fallback)
    with col2:
        if st.button("📑 Generate PDF", use_container_width=True, key=f"{key_prefix}_pdf_btn",
                     help="Requires reportlab in requirements.txt"):
            with st.spinner("Generating PDF…"):
                pdf_bytes, err = get_pdf_bytes(pdf_df, sf_dl, sub_dl)
            if pdf_bytes:
                fname_p = f"duty_{sel_ins}.pdf" if sel_ins != "All" else "duty_all.pdf"
                st.download_button(
                    "⬇️ Download PDF", data=pdf_bytes,
                    file_name=fname_p, mime="application/pdf",
                    use_container_width=True, key=f"{key_prefix}_pdf_dl")
                st.success("✅ PDF ready!")
            else:
                st.markdown(
                    f'<div class="err-card" style="font-size:.72rem">'
                    f'❌ PDF failed: {err}<br>'
                    f'✅ Use the HTML option instead — same content, open & print as PDF.</div>',
                    unsafe_allow_html=True)

    # ─── Panel CSV ───────────────────────────────────────
    exp_p = [c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in pdf_df.columns]
    st.download_button(
        f"📥 Panel CSV ({n_tot} rows)",
        data=pdf_df[exp_p].to_csv(index=False).encode(),
        file_name=f"panel_{sel_ins}.csv", mime="text/csv",
        use_container_width=True, key=f"{key_prefix}_csv")
# ═══════════════════════════════════════════════════════
# TOP BAR + STATS  (compact single bar)
# ═══════════════════════════════════════════════════════
pn   = len(st.session_state.panel)
pdn  = len(st.session_state.pdate)
ef   = st.session_state.panel["EXTID"].apply(lambda v: norm_id(v)!="").sum() if pn else 0
ep   = pn - ef
sc2  = len(st.session_state.staff)
sm2c = len(st.session_state.ssmap)
stg  = len(st.session_state.staged)
pct  = int(ef/pn*100) if pn else 0

st.markdown(f"""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:8px">
    <div class="tb-logo">🗂️</div>
    <div>
      <div class="tb-title">DUTY MANAGER <span style="color:#8b5cf6;font-size:.8rem;font-weight:500">v5</span></div>
      <div class="tb-sub">Practical Exam Panel · {CREATOR}</div>
    </div>
  </div>
  <div style="display:flex;gap:5px;flex-wrap:wrap;align-items:center">
    <span class="sc">Panel <b>{pn}</b></span>
    <span class="sc" style="color:#22c55e">✅ <b>{ef}</b></span>
    <span class="sc" style="color:#f59e0b">⏳ <b>{ep}</b></span>
    <span class="sc" style="color:#3b82f6">Staff <b>{sc2}</b></span>
    <span class="sc" style="color:#8b5cf6">SubMap <b>{sm2c}</b></span>
    <span class="sc" style="color:#f59e0b">Staged <b>{stg}</b></span>
    <span class="sc progress-chip"><span class="prog-bar" style="width:{pct}%"></span><b style="position:relative;z-index:1">{pct}%</b></span>
  </div>
</div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════
# MAIN TABS
# ═══════════════════════════════════════════════════════
tab_up, tab_ext, tab_duty, tab_dl = st.tabs([
    "📥 Upload",
    "🎯 EXT Allocate",
    "▶️ Duty Marking",
    "📦 Downloads",
])

# ╔══════════════════════════════════════════════════════╗
# ║  TAB 1 — UPLOAD CENTRE                              ║
# ╚══════════════════════════════════════════════════════╝
with tab_up:
    s1,s2,s3,s4 = st.tabs(["📋 Panel","🧑‍🏫 Staff","📘 SubjectMap","🔤 SubName"])

    # ── Panel ──
    with s1:
        ul,ur = st.columns([1,1], gap="medium")
        with ul:
            st.markdown('<span class="sub-hdr">📂 Panel CSV / XLSX</span>', unsafe_allow_html=True)
            st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID", language="")
            uf = st.file_uploader("", type=["csv","xlsx"], key="p_up", label_visibility="collapsed")
            cl = st.checkbox("Clear existing before upload", key="p_cl")
            if uf:
                try:
                    tmp = (pd.read_csv(uf,dtype=object) if uf.name.lower().endswith(".csv")
                           else pd.read_excel(uf,dtype=object,sheet_name=0)).fillna("")
                    req = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                    miss = [c for c in req if c not in tmp.columns]
                    if miss:
                        st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp = tmp[req].copy(); tmp["ERROR"]=""; tmp=rowid(tmp,"p")
                        if cl:
                            st.session_state.panel = rowid(tmp.reset_index(drop=True),"p")
                        else:
                            ins_up = [str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                            bk = st.session_state.panel.copy()
                            bk = bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                            bk = pd.concat([bk,tmp],ignore_index=True)
                            kc = [c for c in ["INSCODE","SUBCODE","REGL","INTID"] if c in bk.columns]
                            bk = bk.drop_duplicates(subset=kc,keep="last").reset_index(drop=True)
                            st.session_state.panel = rowid(bk,"p")
                        P(); st.success(f"✅ {len(tmp)} rows uploaded")
                except Exception as e: st.error(f"❌ {e}")
        with ur:
            pv = st.session_state.panel.copy()
            if not st.session_state.submap.empty:
                pv = pv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            ec = [c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID","EXTID","ERROR","__rowid"] if c in pv.columns]
            fc1,fc2 = st.columns(2)
            pfi = fc1.selectbox("INSCODE",["All"]+sorted(set(pv["INSCODE"].astype(str))),key="pf_i",label_visibility="collapsed")
            pfd = fc2.selectbox("NCNO",   ["All"]+sorted(set(pv["NCNO"].astype(str))),   key="pf_n",label_visibility="collapsed")
            pv2 = pv.copy()
            if pfi!="All": pv2=pv2[pv2["INSCODE"].astype(str)==pfi]
            if pfd!="All": pv2=pv2[pv2["NCNO"].astype(str)==pfd]
            st.markdown(f'<span class="sub-hdr">Panel — {len(pv2)} rows</span>', unsafe_allow_html=True)
            ep2 = st.data_editor(pv2[ec].fillna(""), key="p_ed", use_container_width=True, height=300,
                                 num_rows="dynamic",
                                 column_config={"__rowid":st.column_config.Column(disabled=True,width="small")})
            sa,sb = st.columns(2)
            if sa.button("💾 Save Panel", key="p_sv", use_container_width=True):
                try:
                    bk=st.session_state.panel.copy()
                    ed=ep2.copy()
                    if "SUBNAME" in ed.columns: ed=ed.drop(columns=["SUBNAME"])
                    if "ERROR" not in ed.columns: ed["ERROR"]=""
                    ed_exist=ed[ed["__rowid"].astype(str).str.strip()!=""].copy()
                    ed_new=ed[ed["__rowid"].astype(str).str.strip()==""].copy()
                    ed_new=rowid(ed_new,"p")
                    bk_i=bk.set_index("__rowid",drop=False)
                    for _,er in ed_exist.iterrows():
                        rid=str(er["__rowid"]).strip()
                        if rid in bk_i.index:
                            for c in ed_exist.columns: bk_i.at[rid,c]=er[c]
                    if not ed_new.empty:
                        bk_i=pd.concat([bk_i.reset_index(drop=True),ed_new.reset_index(drop=True)],ignore_index=True)
                    result=bk_i.reset_index(drop=True)
                    kc=[c for c in ["INSCODE","SUBCODE","REGL","INTID"] if c in result.columns]
                    before=len(result)
                    result=result.drop_duplicates(subset=kc,keep="last").reset_index(drop=True)
                    dropped=before-len(result)
                    st.session_state.panel=rowid(result,"p"); P()
                    msg="✅ Saved"
                    if dropped: msg+=f" · 🧹 {dropped} dupes removed"
                    st.success(msg)
                except Exception as e: st.error(f"❌ {e}")
            if sb.button("🧹 Dedup", key="p_dedup", use_container_width=True,
                         help="Remove duplicate rows by INSCODE+SUBCODE+REGL+INTID"):
                before=len(st.session_state.panel)
                kc=[c for c in ["INSCODE","SUBCODE","REGL","INTID"] if c in st.session_state.panel.columns]
                st.session_state.panel=st.session_state.panel.drop_duplicates(subset=kc,keep="last").reset_index(drop=True)
                st.session_state.panel=rowid(st.session_state.panel,"p")
                save_csv(st.session_state.panel,PANEL_PATH)
                dropped=before-len(st.session_state.panel)
                (st.success(f"🧹 Removed {dropped} dupes.") if dropped else st.info("✅ No duplicates."))
                st.rerun()

    # ── Staff ──
    with s2:
        sl,sr = st.columns([1,1], gap="medium")
        with sl:
            st.markdown('<span class="sub-hdr">📂 Staff CSV / XLSX</span>', unsafe_allow_html=True)
            st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation  Phone", language="")
            samp_s=pd.DataFrame([{"Staff ID":"X123EEE1","INSTT":"123","Name of the Staff":"KUMAR S",
                "Department":"EEE","dep code":"1030","Designation":"Lecturer","Phone":"9876543210"}])
            st.download_button("📥 Sample CSV",data=samp_s.to_csv(index=False).encode(),
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
            f1,f2=st.columns(2)
            fi_s=f1.selectbox("INSTT",["All"]+sorted(set(sv["INSTT"].astype(str))),key="sf_i",label_visibility="collapsed")
            fd_s=f2.selectbox("Dept", ["All"]+sorted(set(sv["Department"].astype(str))),key="sf_d",label_visibility="collapsed")
            if fi_s!="All": sv=sv[sv["INSTT"].astype(str)==fi_s]
            if fd_s!="All": sv=sv[sv["Department"].astype(str)==fd_s]
            dc=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone","__rowid"] if c in sv.columns]
            st.markdown(f'<span class="sub-hdr">Staff — {len(sv)} rows</span>', unsafe_allow_html=True)
            es=st.data_editor(sv[dc],key="s_ed",use_container_width=True,height=340,num_rows="dynamic",
                column_config={"__rowid":st.column_config.Column(disabled=True,width="small")})
            if st.button("💾 Save Staff",key="s_sv",use_container_width=True):
                try:
                    bk=st.session_state.staff.copy()
                    ed=es.copy()
                    ed_exist=ed[ed["__rowid"].astype(str).str.strip()!=""].copy()
                    ed_new=ed[ed["__rowid"].astype(str).str.strip()==""].copy()
                    ed_new=rowid(ed_new,"s")
                    bk_i=bk.set_index("__rowid",drop=False)
                    for _,er in ed_exist.iterrows():
                        rid=str(er["__rowid"]).strip()
                        if rid in bk_i.index:
                            for c in ed_exist.columns: bk_i.at[rid,c]=er[c]
                    if not ed_new.empty:
                        bk_i=pd.concat([bk_i.reset_index(drop=True),ed_new.reset_index(drop=True)],ignore_index=True)
                    result=bk_i.reset_index(drop=True)
                    result=result.drop_duplicates(subset=["Staff ID"],keep="last").reset_index(drop=True)
                    st.session_state.staff=rowid(result,"s"); S(); st.success("✅ Staff saved")
                except Exception as e: st.error(f"❌ {e}")

    # ── SubjectMap ──
    with s3:
        ml,mr = st.columns([1,1], gap="medium")
        with ml:
            st.markdown('<span class="sub-hdr">📂 Subject-Staff Map CSV / XLSX</span>', unsafe_allow_html=True)
            st.code("\n".join(SMAP_COLS), language="")
            samp_sm=pd.DataFrame([{"Staff_Last_Staff_ID":"X123EEE1","Staff_Name":"KUMAR S",
                "Department":"EEE","Department_Code":"1030","Subject_Type":"Core",
                "Subject_Code":"P3401","Subject_Name":"Basic Electrical Lab","Subject_Remarks":""}])
            c_t,c_s=st.columns(2)
            c_t.download_button("📥 Template",data=pd.DataFrame(columns=SMAP_COLS).to_csv(index=False).encode(),
                file_name="ssmap_template.csv",mime="text/csv",use_container_width=True)
            c_s.download_button("📥 Sample",data=samp_sm.to_csv(index=False).encode(),
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
                        st.session_state.ssmap=tmp[SMAP_COLS].copy(); SS()
                        st.success(f"✅ {len(tmp)} rows loaded")
                except Exception as e: st.error(f"❌ {e}")
        with mr:
            ssv=st.session_state.ssmap.copy()
            sf3,sf4=st.columns(2)
            dm_f=sf3.selectbox("Dept",["All"]+sorted(set(ssv["Department"].astype(str))),key="ssm_d",label_visibility="collapsed")
            sc_f=sf4.text_input("",""  ,key="ssm_s",placeholder="🔍 Subject Code",label_visibility="collapsed")
            if dm_f!="All": ssv=ssv[ssv["Department"]==dm_f]
            if sc_f.strip(): ssv=ssv[ssv["Subject_Code"].str.contains(sc_f.strip().upper(),na=False)]
            st.markdown(f'<span class="sub-hdr">SubjectMap — {len(ssv)} rows</span>', unsafe_allow_html=True)
            essm=st.data_editor(ssv.fillna(""),key="ssm_ed",use_container_width=True,height=340,num_rows="dynamic")
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

    # ── SubName ──
    with s4:
        tl,tr2 = st.columns([1,1], gap="medium")
        with tl:
            st.markdown('<span class="sub-hdr">📂 SUBCODE → SUBNAME</span>', unsafe_allow_html=True)
            st.code("SUBCODE  SUBNAME", language="")
            samp_sub=pd.DataFrame([{"SUBCODE":"P3401","SUBNAME":"Basic Electrical Lab"},
                                   {"SUBCODE":"P3402","SUBNAME":"Electrical Machines Lab"}])
            st.download_button("📥 Sample",data=samp_sub.to_csv(index=False).encode(),
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
            sc_fi=st.text_input("","",key="sm_fi",placeholder="🔍 Filter SUBCODE",label_visibility="collapsed")
            smv2=smv[smv["SUBCODE"].astype(str).str.contains(sc_fi.strip().upper(),na=False)] if sc_fi.strip() else smv
            st.markdown(f'<span class="sub-hdr">SubName Map — {len(smv2)} rows</span>', unsafe_allow_html=True)
            esm=st.data_editor(
                smv2[["SUBCODE","SUBNAME"]].fillna("") if not smv2.empty else pd.DataFrame(columns=["SUBCODE","SUBNAME"]),
                key="sm_ed",use_container_width=True,height=360,num_rows="dynamic")
            if st.button("💾 Save SubName",key="sm_sv",use_container_width=True):
                st.session_state.submap=esm.copy(); SM()
                st.success("✅ SubName mapping saved")

# ╔══════════════════════════════════════════════════════╗
# ║  TAB 2 — EXT ALLOCATE                              ║
# ╚══════════════════════════════════════════════════════╝
with tab_ext:
    panel   = st.session_state.panel.copy()
    sf      = st.session_state.staff.copy()
    ssmap   = st.session_state.ssmap.copy()
    submap  = st.session_state.submap.copy()

    def needs_ext(r): return norm_id(r.get("EXTID",""))==""
    def has_ext(r):   return norm_id(r.get("EXTID",""))!=""

    etab_auto, etab_manual, etab_edl = st.tabs([
        "🤖 Auto Allocate",
        "📝 Manual Allocate",
        "📥 Download",
    ])

    # ── AUTO ──
    with etab_auto:
        # Inline filter row
        fc1,fc2,fc3,fc4 = st.columns([2,2,2,2])
        ins_f  = fc1.selectbox("INSCODE", ["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ea_i",label_visibility="collapsed")
        nc_f   = fc2.selectbox("NCNO",    ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ea_n",label_visibility="collapsed")
        show_f = fc3.selectbox("Show",    ["Pending","All","Filled"],key="ea_sh",label_visibility="collapsed")
        # metrics inline
        filt_panel=panel.copy()
        if ins_f!="All": filt_panel=filt_panel[filt_panel["INSCODE"].astype(str)==ins_f]
        if nc_f !="All": filt_panel=filt_panel[filt_panel["NCNO"].astype(str)==nc_f]
        candidates=filt_panel[filt_panel.apply(needs_ext,axis=1)].copy()

        if show_f=="Pending": view_panel=candidates.copy()
        elif show_f=="Filled": view_panel=filt_panel[filt_panel.apply(has_ext,axis=1)].copy()
        else: view_panel=filt_panel.copy()

        fc4.markdown(
            f'<div style="padding:6px 0;font-size:.82rem;color:#c9d1d9">'
            f'⏳ <b style="color:#f59e0b">{len(candidates)}</b> pending &nbsp;'
            f'✅ <b style="color:#22c55e">{len(filt_panel)-len(candidates)}</b> filled</div>',
            unsafe_allow_html=True)

        # Panel preview table — compact
        if not view_panel.empty:
            pv=view_panel.copy()
            if not submap.empty:
                pv=pv.merge(submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            pv["INTID_NAME"]=pv["INTID"].apply(lambda x:(x+" — "+get_name(sf,x)) if norm_id(x) else "—")
            pv["EXTID_NAME"]=pv["EXTID"].apply(lambda x:(x+" — "+get_name(sf,x)) if norm_id(x) else "—")
            pv["STATUS"]=pv.apply(lambda r:"✅" if has_ext(r) else "⏳",axis=1)
            if "SUBNAME" not in pv.columns: pv["SUBNAME"]=""
            sc_=[c for c in ["STATUS","INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID_NAME","EXTID_NAME"] if c in pv.columns]
            def sty_s(v): return "background-color:#0d2218;color:#86efac" if v=="✅" else "background-color:#2d1515;color:#fca5a5"
            def sty_e(v): return "background-color:#0d2218;color:#86efac" if (str(v).strip() and str(v).strip()!="—") else "background-color:#2d1515;color:#fca5a5"
            st.dataframe(pv[sc_].fillna("").style.applymap(sty_s,subset=["STATUS"]).applymap(sty_e,subset=["EXTID_NAME"]),
                         use_container_width=True, height=220)

        st.markdown('<hr class="thin">', unsafe_allow_html=True)

        # Auto-allocate button
        col_btn, col_info = st.columns([2,3])
        with col_btn:
            if st.button("🤖 Auto-Allocate All Pending", type="primary", use_container_width=True):
                if sf.empty:
                    st.error("❌ Upload staff data first!")
                elif candidates.empty:
                    st.warning("⚠️ No pending rows.")
                else:
                    total_c=len(candidates)
                    _status=st.empty(); _bar=st.progress(0)
                    _status.markdown(f'<div class="info-card">⚙️ Building lookups for {total_c} rows…</div>',unsafe_allow_html=True)
                    def _prog(done,total):
                        pct2=int(done/total*100); _bar.progress(pct2)
                        _status.markdown(f'<div class="info-card">🔄 {done}/{total} ({pct2}%)</div>',unsafe_allow_html=True)
                    res,skip=auto_allocate(candidates,sf,ssmap if not ssmap.empty else None,progress_cb=_prog)
                    _bar.progress(100)
                    for k,v in res.items(): st.session_state.staged[str(k)]=v
                    _status.markdown(
                        f'<div class="ok-card">✅ Staged <b>{len(res)}</b>'
                        f'{" · ⚠️ "+str(len(skip))+" skipped" if skip else ""}.</div>',
                        unsafe_allow_html=True)
                    if skip:
                        with st.expander(f"⚠️ {len(skip)} skipped"):
                            st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]),use_container_width=True)
                    if res: st.rerun()
        with col_info:
            st.markdown(
                '<div class="info-card" style="font-size:.78rem;padding:6px 10px">'
                '🟢 Willing (mapped to subject) → 🟡 Same dept → ⚪ Others · '
                'Sorted least-duties first · Live duty count updated each allocation</div>',
                unsafe_allow_html=True)

        # Apply staged
        staged_map=st.session_state.staged
        if staged_map:
            st.markdown('<hr class="thin">', unsafe_allow_html=True)
            with st.expander(f"🚀 Apply All Staged ({len(staged_map)} assignments)"):
                rows2=[]
                _ds_cache=duty_stats(sf)
                for k,v in list(staged_map.items())[:60]:
                    try:
                        pi=int(k); r=st.session_state.panel.loc[pi] if pi in st.session_state.panel.index else {}
                        sid_v=extract_sid(v)
                        cnt_d=_ds_cache.get(sid_v,{}).get("count",0)
                        sn2=get_subname(submap,str(r.get("SUBCODE","")).strip())
                        int_nm=get_name(sf,r.get("INTID",""))
                        int_disp=f"{r.get('INTID','')}{'— '+int_nm if int_nm else ''}"
                        rows2.append({"Row":k,"INSCODE":r.get("INSCODE","?"),
                                      "Subject":f"{r.get('SUBCODE','?')} {('— '+sn2) if sn2 else ''}",
                                      "INT":int_disp,"→ EXTID":sid_v,"EXT Name":get_name(sf,sid_v),
                                      "Duties":f"{priority_icon(cnt_d)}{cnt_d}"})
                    except: rows2.append({"Row":k,"→ EXTID":v})
                st.dataframe(pd.DataFrame(rows2),use_container_width=True,height=200)
                a1,a2=st.columns(2)
                if a1.button("✅ Apply ALL",type="primary",use_container_width=True):
                    ok_c,fc2b=[],[]
                    for k,v in list(staged_map.items()):
                        try: pi=int(k)
                        except: fc2b.append(k); continue
                        if pi not in st.session_state.panel.index: fc2b.append(k); continue
                        sid_c=extract_sid(v)
                        if sid_c:
                            st.session_state.panel.at[pi,"EXTID"]=sid_c
                            st.session_state.staged.pop(k,None); ok_c.append(k)
                        else: fc2b.append(k)
                    P(); st.success(f"✅ Applied {len(ok_c)} · ❌ Failed {len(fc2b)}"); st.rerun()
                if a2.button("🗑️ Clear Staged",use_container_width=True):
                    st.session_state.staged={}; st.success("✅ Cleared"); st.rerun()

    # ── MANUAL ──
    with etab_manual:
        # Pre-compute ONCE
        with st.spinner("⚙️ Loading staff lookup…"):
            _staff_list,_,_ssmap_index=build_precomputed(sf,ssmap if not ssmap.empty else pd.DataFrame())

        # Filter row
        mfc1,mfc2,mfc3=st.columns([2,2,2])
        m_ins_f=mfc1.selectbox("INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ma_i",label_visibility="collapsed")
        m_nc_f =mfc2.selectbox("NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ma_n",label_visibility="collapsed")
        m_show =mfc3.selectbox("Show",   ["Pending","All","Filled"],key="ma_sh",label_visibility="collapsed")

        m_filt=panel.copy()
        if m_ins_f!="All": m_filt=m_filt[m_filt["INSCODE"].astype(str)==m_ins_f]
        if m_nc_f !="All": m_filt=m_filt[m_filt["NCNO"].astype(str)==m_nc_f]
        if m_show=="Pending":  m_cands=m_filt[m_filt.apply(needs_ext,axis=1)].copy()
        elif m_show=="Filled": m_cands=m_filt[m_filt.apply(has_ext,axis=1)].copy()
        else:                  m_cands=m_filt.copy()

        st.markdown(
            f'<div class="chip-row">'
            f'<span class="chip">📋 {len(m_cands)} rows</span>'
            f'<span class="chip">🟢 Willing=mapped 🟡=same dept ⚪=other (least duties first)</span>'
            f'</div>', unsafe_allow_html=True)

        if m_cands.empty:
            st.markdown('<div class="ok-card">🎉 No rows for current filter!</div>', unsafe_allow_html=True)
        else:
            for _,row in m_cands.reset_index().iterrows():
                pidx    =int(row["index"])
                sc      =str(row.get("SUBCODE","")).strip()
                sn      =get_subname(submap,sc)
                ins     =str(row.get("INSCODE","")).strip()
                nc      =str(row.get("NCNO","")).strip()
                noc     =str(row.get("NOC","")).strip()
                intid   =norm_id(row.get("INTID",""))
                intname =get_name(sf,intid)
                int_desig=get_desig(sf,intid)
                int_dep  =get_dep(sf,intid)
                cur_ext  =norm_id(row.get("EXTID",""))
                sv_val   =st.session_state.staged.get(str(pidx),"")

                willing,same_dept,others=ext_suggestions_fast(row,_staff_list,_ssmap_index)
                opts=build_dropdown_options(willing,same_dept,others)
                total_suggs=len(willing)+len(same_dept)+len(others)

                # Compact card — 2 columns
                card_cls="alloc-card-done" if cur_ext else ("alloc-card-staged" if sv_val else "alloc-card-pending")
                st.markdown(f'<div class="alloc-card {card_cls}">', unsafe_allow_html=True)

                r1a,r1b=st.columns([4,3])
                with r1a:
                    sub_lbl=f"{sc}{' — '+sn if sn else ''}"
                    int_lbl=f"{intid} — {intname}" if intid else "No INT assigned"
                    st.markdown(
                        f'<div style="font-size:.85rem;line-height:1.8">'
                        f'<b style="color:#e6edf3">🏫 {ins}</b> &nbsp;·&nbsp; <span style="color:#93c5fd">NCNO:{nc}</span> &nbsp;·&nbsp; <span style="color:#8b949e">👥{noc}</span><br>'
                        f'📚 <code style="background:#010409;padding:1px 6px;border-radius:4px;color:#79c0ff">{sub_lbl}</code><br>'
                        f'🎓 <span style="color:#fbbf24">{int_lbl}</span>'
                        f'{"<span style=color:#6e7681;font-size:.75rem> · "+int_desig+"</span>" if int_desig else ""}'
                        f'</div>', unsafe_allow_html=True)
                with r1b:
                    if cur_ext:
                        en=get_name(sf,cur_ext); ed2=get_desig(sf,cur_ext)
                        st.markdown(f'<div class="ok-card" style="font-size:.78rem;padding:5px 8px">✅ <b>{cur_ext}</b> — {en}<br><small>{ed2}</small></div>',unsafe_allow_html=True)
                    elif sv_val:
                        sv_id=extract_sid(sv_val)
                        st.markdown(f'<div class="warn-card" style="font-size:.78rem;padding:5px 8px">🟡 Staged: <b>{sv_id}</b><br><small>{get_name(sf,sv_id)}</small></div>',unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="err-card" style="font-size:.78rem;padding:5px 8px">⏳ Not assigned<br><small>🟢{len(willing)} 🟡{len(same_dept)} ⚪{len(others)}</small></div>',unsafe_allow_html=True)

                # Dropdown + manual + apply on same row
                r3a,r3b,r3c=st.columns([5,2,1])
                cur_lbl=sv_val if sv_val in opts else opts[0]
                di=opts.index(cur_lbl) if cur_lbl in opts else 0
                sel=r3a.selectbox(
                    f"INSTT-Dept | StaffID-Name | Desig | Duties",
                    opts,index=di,key=f"sel_{pidx}",
                    help="🟢Willing 🟡SameDept ⚪Other — least duties first")
                man=r3b.text_input("",value="",key=f"man_{pidx}",
                    placeholder="Manual StaffID",label_visibility="collapsed")

                if sel and not is_header_opt(sel) and sel!=opts[0]:
                    st.session_state.staged[str(pidx)]=sel
                if man.strip():
                    st.session_state.staged[str(pidx)]=man.strip().upper()

                if r3c.button("▶",key=f"app_{pidx}",help="Apply now"):
                    chosen=sv_val or (sel if not is_header_opt(sel) and sel!=opts[0] else "") or man.strip()
                    if not chosen:
                        st.warning("⚠️ Select or enter Staff ID")
                    else:
                        sid_c=extract_sid(chosen) if "|" in chosen else norm_id(chosen)
                        if sid_c:
                            st.session_state.panel.at[pidx,"EXTID"]=sid_c; P()
                            st.session_state.staged.pop(str(pidx),None)
                            st.success(f"✅ {sid_c} — {get_name(sf,sid_c)}"); st.rerun()
                        else: st.error("❌ Invalid Staff ID")

                # Selected preview inline
                if sel and not is_header_opt(sel) and sel!=opts[0]:
                    raw=re.sub(r'^[🟢🟡🔴⚪]\s*','',sel)
                    parts=[p.strip() for p in raw.split("|")]
                    instt_dep=parts[0] if len(parts)>0 else ""
                    id_name_p=parts[1] if len(parts)>1 else ""
                    desig_s  =parts[2] if len(parts)>2 else ""
                    duties_raw=re.sub(r'[🟢🟡🔴]','',parts[3]).replace("Duties:","").strip() if len(parts)>3 else "0"
                    cnt_v=int(duties_raw) if duties_raw.isdigit() else 0
                    m_id=re.match(r'([A-Z0-9]+)',id_name_p.upper())
                    sid_s=norm_id(m_id.group(1)) if m_id else ""
                    name_s=id_name_p[len(sid_s)+1:].strip() if sid_s and len(id_name_p)>len(sid_s) else id_name_p
                    badge=priority_class(cnt_v); ph_s=get_phone(sf,sid_s)
                    w_ids={s["sid"] for s in willing}; yd_ids={s["sid"] for s in same_dept}
                    cat_lbl="🟢 Willing" if sid_s in w_ids else ("🟡 Same Dept" if sid_s in yd_ids else "⚪ Other")
                    st.markdown(
                        f'<div style="background:#0c1a2e;border-radius:6px;padding:7px 14px;margin:4px 0;font-size:.79rem;border:1px solid #1d3557;display:flex;gap:12px;flex-wrap:wrap;align-items:center">'
                        f'<b style="color:#93c5fd">{sid_s}</b><span style="color:#e6edf3">{name_s}</span>'
                        f'<span style="color:#8b949e">{desig_s}</span><span style="color:#8b949e">🏫{instt_dep}</span>'
                        f'{"<span style=color:#6e7681>📞"+ph_s+"</span>" if ph_s else ""}'
                        f'<span class="{badge}">Duties:{cnt_v}</span>'
                        f'<span style="color:#c9d1d9;font-size:.74rem">{cat_lbl}</span>'
                        f'</div>', unsafe_allow_html=True)

                st.markdown('</div>', unsafe_allow_html=True)

    # ── EXT DOWNLOAD ──
    with etab_edl:
        pdf_download_section(st.session_state.panel.copy(),
                             st.session_state.staff.copy(),
                             st.session_state.submap.copy(),
                             key_prefix="edl")

# ╔══════════════════════════════════════════════════════╗
# ║  TAB 3 — DUTY MARKING                              ║
# ╚══════════════════════════════════════════════════════╝
with tab_duty:
    d1c,d2c=st.columns([1,1],gap="medium")
    with d1c:
        st.markdown('<span class="sub-hdr">📂 Dated Panel CSV / XLSX</span>', unsafe_allow_html=True)
        st.code("INSCODE NCNO SUBCODE REGL NOC NOB INTID EXTID DATE_FROM DATE_TO", language="")
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
                        kc=[c for c in ["INSCODE","SUBCODE","REGL","INTID"] if c in bk.columns]
                        bk=bk.drop_duplicates(subset=kc,keep="last").reset_index(drop=True)
                        st.session_state.pdate=rowid(bk,"d")
                    PD(); st.success(f"✅ {len(tmp)} dated rows loaded")
            except Exception as e: st.error(f"❌ {e}")

    with d2c:
        pdv=st.session_state.pdate.copy()
        if not pdv.empty:
            pdv["_d"]=pdv["DATE_FROM"].apply(parse_date)
            pdv=pdv.sort_values("_d",na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdv=pdv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
        show=[c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"] if c in pdv.columns]
        st.markdown(f'<span class="sub-hdr">Dated Panel — {len(pdv)} rows</span>', unsafe_allow_html=True)
        st.dataframe(pdv[show].fillna(""),use_container_width=True,height=260)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)
    gc1,gc2,gc3=st.columns([2,2,2])
    ins_g=gc1.selectbox("INSCODE",["All"]+sorted(set(st.session_state.pdate["INSCODE"].astype(str))),key="dm_i",label_visibility="collapsed")
    nc_g =gc2.selectbox("NCNO",   ["All"]+sorted(set(st.session_state.pdate["NCNO"].astype(str))),   key="dm_n",label_visibility="collapsed")
    filt2=st.session_state.pdate.copy()
    if ins_g!="All": filt2=filt2[filt2["INSCODE"].astype(str)==ins_g]
    if nc_g !="All": filt2=filt2[filt2["NCNO"].astype(str)==nc_g]
    with gc3:
        if st.button("🔍 Run Error Check",type="primary",use_container_width=True):
            if st.session_state.pdate.empty:
                st.error("❌ Upload dated panel first!")
            else:
                with st.spinner("Running checks…"):
                    err_map=check_errors(filt2,st.session_state.staff)
                for idx in filt2.index:
                    if idx in st.session_state.pdate.index:
                        msgs=err_map.get(idx,[])
                        st.session_state.pdate.at[idx,"ERROR"]=" | ".join(msgs) if msgs else ""
                PD(); st.session_state.errors=err_map
                total2=sum(len(v) for v in err_map.values())
                if total2==0:
                    st.markdown('<div class="ok-card">✅ No clashes found.</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="err-card">🔴 {total2} issue(s) in {len(err_map)} rows.</div>', unsafe_allow_html=True)

    if st.session_state.errors:
        for idx,msgs in st.session_state.errors.items():
            r=st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
            with st.expander(f"🔴 Row {idx} · {r.get('INSCODE','?')} · {r.get('SUBCODE','?')} · {len(msgs)} issue(s)"):
                for m in msgs:
                    st.markdown(f'<div class="err-card">{m}</div>', unsafe_allow_html=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)
    st.markdown('<span class="sub-hdr">📊 Duty Count per Staff</span>', unsafe_allow_html=True)
    if not st.session_state.pdate.empty:
        dc_d={}
        for _,row in st.session_state.pdate.iterrows():
            for fld in ["INTID","EXTID"]:
                sid=norm_id(row.get(fld,"")); 
                if sid: dc_d[sid]=dc_d.get(sid,0)+1
        if dc_d:
            df_ch=pd.DataFrame(list(dc_d.items()),columns=["Staff ID","Duties"])
            df_ch["Name"]=df_ch["Staff ID"].apply(lambda s:get_name(st.session_state.staff,s))
            df_ch["Label"]=df_ch["Staff ID"]+" — "+df_ch["Name"]
            df_ch=df_ch.sort_values("Duties",ascending=False).head(30)
            st.bar_chart(df_ch.set_index("Label")["Duties"])

# ╔══════════════════════════════════════════════════════╗
# ║  TAB 4 — DOWNLOADS                                 ║
# ╚══════════════════════════════════════════════════════╝
with tab_dl:
    all_p=st.session_state.panel.copy()
    all_d=st.session_state.pdate.copy()
    sf_dl=st.session_state.staff.copy()
    sub_dl=st.session_state.submap.copy()
    exp_p=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in all_p.columns]
    exp_d=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"] if c in all_d.columns]

    # ── PDF section (primary) ──
    st.markdown('<span class="sub-hdr">🖨️ PDF / HTML Duty Sheets</span>', unsafe_allow_html=True)
    pdf_download_section(all_p, sf_dl, sub_dl, key_prefix="main")

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── Panel CSVs ──
    st.markdown('<span class="sub-hdr">📋 Panel CSV Downloads</span>', unsafe_allow_html=True)
    if all_p.empty:
        st.markdown('<div class="info-card">ℹ️ No panel data.</div>', unsafe_allow_html=True)
    else:
        c1,c2,c3=st.columns(3)
        c1.download_button(f"📥 Full Panel ({len(all_p)})",data=all_p[exp_p].to_csv(index=False).encode(),file_name="panel_full.csv",mime="text/csv",use_container_width=True)
        pend=all_p[all_p["EXTID"].apply(norm_id)==""]
        c2.download_button(f"📥 Pending ({len(pend)})",data=pend[exp_p].to_csv(index=False).encode(),file_name="panel_pending.csv",mime="text/csv",use_container_width=True)
        filled=all_p[all_p["EXTID"].apply(norm_id)!=""]
        c3.download_button(f"📥 Filled ({len(filled)})",data=filled[exp_p].to_csv(index=False).encode(),file_name="panel_filled.csv",mime="text/csv",use_container_width=True)

        # Per institution
        inscodes_p=sorted(set(all_p["INSCODE"].astype(str)))
        if inscodes_p:
            st.markdown('<span class="sub-hdr" style="font-size:.8rem">Per Institution</span>', unsafe_allow_html=True)
            chunks=[inscodes_p[i:i+4] for i in range(0,len(inscodes_p),4)]
            for chunk in chunks:
                cols=st.columns(4)
                for ci,ins in enumerate(chunk):
                    df_i=all_p[all_p["INSCODE"].astype(str)==ins][exp_p]
                    ef_i=df_i["EXTID"].apply(norm_id).ne("").sum()
                    cols[ci].download_button(f"📥 {ins} ({ef_i}/{len(df_i)})",data=df_i.to_csv(index=False).encode(),
                        file_name=f"panel_{ins}.csv",mime="text/csv",key=f"dl_p_{ins}",use_container_width=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── Dated Panel ──
    st.markdown('<span class="sub-hdr">🗓️ Dated Panel CSV</span>', unsafe_allow_html=True)
    if all_d.empty:
        st.markdown('<div class="info-card">ℹ️ No dated panel.</div>', unsafe_allow_html=True)
    else:
        dc1,dc2=st.columns(2)
        dc1.download_button(f"📥 Full Dated ({len(all_d)})",data=all_d[exp_d].to_csv(index=False).encode(),file_name="dated_full.csv",mime="text/csv",use_container_width=True)
        errd=all_d[all_d["ERROR"].astype(str).str.strip()!=""]
        dc2.download_button(f"📥 Errors Only ({len(errd)})",data=errd[exp_d].to_csv(index=False).encode(),file_name="dated_errors.csv",mime="text/csv",use_container_width=True)

    st.markdown('<hr class="thin">', unsafe_allow_html=True)

    # ── Staff ──
    st.markdown('<span class="sub-hdr">🧑‍🏫 Staff CSV</span>', unsafe_allow_html=True)
    if not sf_dl.empty:
        sf_exp=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"] if c in sf_dl.columns]
        st.download_button(f"📥 Staff ({len(sf_dl)} records)",data=sf_dl[sf_exp].to_csv(index=False).encode(),
            file_name="staff_all.csv",mime="text/csv")
    else:
        st.markdown('<div class="info-card">ℹ️ No staff loaded.</div>', unsafe_allow_html=True)
