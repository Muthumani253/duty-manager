#!/usr/bin/env python3
# streamlit_app.py  — Duty Manager v3
"""
Practical Exam Panel Scheduling System
Created by MUTHUMANI S, LECTURER-EEE, GPT KARUR | 9443100811
"""
from __future__ import annotations
import os, uuid, io, traceback, base64
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
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    RPDF = True
except ImportError:
    RPDF = False

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════
DATA_DIR          = "data"
PANEL_PATH        = os.path.join(DATA_DIR, "panel.csv")
PANEL_DATED_PATH  = os.path.join(DATA_DIR, "panel_dated.csv")
STAFF_PATH        = os.path.join(DATA_DIR, "staff.csv")
SUBMAP_PATH       = os.path.join(DATA_DIR, "submap.csv")
SUBJMAP_PATH      = os.path.join(DATA_DIR, "subjmap.csv")
os.makedirs(DATA_DIR, exist_ok=True)

CREATOR = "MUTHUMANI S | LECTURER-EEE | GPT KARUR | 9443100811"
PANEL_COLS        = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","ERROR","__rowid"]
PANEL_DATED_COLS  = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_COLS        = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone","__rowid"]
SUBJMAP_COLS      = ["Staff_Last_Staff_ID","Staff_Name","Department","Department_Code",
                     "Subject_Type","Subject_Code","Subject_Name","Subject_Remarks"]

st.set_page_config(page_title="Duty Manager", page_icon="🗂️", layout="wide",
                   initial_sidebar_state="expanded")

# ══════════════════════════════════════════════
# CSS — PROFESSIONAL DARK UI
# ══════════════════════════════════════════════
st.markdown("""
<style>
/* ── Sidebar overall ── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg,#0d0d1a 0%,#111827 50%,#0a0f1e 100%) !important;
    border-right: 1px solid #1e293b;
}
[data-testid="stSidebar"] * { color:#cbd5e1 !important; }

/* ── Nav buttons ── */
div[data-testid="stSidebar"] .stButton > button {
    width: 100% !important;
    background: transparent !important;
    border: none !important;
    border-radius: 10px !important;
    color: #94a3b8 !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    text-align: left !important;
    padding: 11px 16px !important;
    margin: 2px 0 !important;
    transition: all .2s ease !important;
    letter-spacing: .3px !important;
}
div[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(99,102,241,0.18) !important;
    color: #c7d2fe !important;
    padding-left: 22px !important;
}

/* Active nav item class applied via markdown */
.nav-active {
    background: linear-gradient(90deg,rgba(99,102,241,.45),rgba(139,92,246,.25)) !important;
    border-left: 3px solid #818cf8 !important;
    border-radius: 10px;
    padding: 11px 16px;
    color: #e0e7ff !important;
    font-weight: 700;
    font-size: .88rem;
    display: block;
    margin: 2px 0;
    cursor: default;
    letter-spacing: .3px;
}
.nav-inactive {
    border-left: 3px solid transparent;
    border-radius: 10px;
    padding: 11px 16px;
    color: #64748b;
    font-size: .88rem;
    display: block;
    margin: 2px 0;
    cursor: pointer;
}

/* ── Sidebar stat pills ── */
.stat-pill {
    display:flex; justify-content:space-between; align-items:center;
    background:rgba(255,255,255,.04);
    border-radius:8px; padding:6px 12px; margin:3px 0; font-size:.78rem;
    border:1px solid rgba(255,255,255,.06);
}
.stat-pill .sv { font-weight:700; font-size:.95rem; color:#a5b4fc; }

/* ── Section headers ── */
.section-hdr {
    background: linear-gradient(90deg,#4f46e5,#7c3aed);
    color:white !important; padding:9px 18px; border-radius:9px;
    font-weight:700; font-size:1rem; margin:12px 0 8px;
    letter-spacing:.3px;
}
.sub-hdr {
    background: rgba(99,102,241,.12);
    border-left:3px solid #6366f1;
    color:#c7d2fe !important; padding:7px 14px; border-radius:0 8px 8px 0;
    font-weight:600; font-size:.9rem; margin:8px 0 5px;
}

/* ── Cards ── */
.err-card  { background:#1f0f0f; border-left:4px solid #ef4444; border-radius:8px;
    padding:8px 14px; margin:4px 0; font-size:.83rem; color:#fca5a5 !important; }
.ok-card   { background:#0d1f0d; border-left:4px solid #22c55e; border-radius:8px;
    padding:8px 14px; margin:4px 0; font-size:.83rem; color:#86efac !important; }
.info-card { background:#0f172a; border-left:4px solid #38bdf8; border-radius:8px;
    padding:8px 14px; margin:4px 0; font-size:.83rem; color:#bae6fd !important; }
.row-card  { background:#1e293b; border-left:4px solid #6366f1; border-radius:8px;
    padding:9px 14px; margin:5px 0; }

/* ── Metric grid ── */
.metrics-row { display:flex; gap:10px; flex-wrap:wrap; margin:8px 0; }
.metric-box {
    flex:1; min-width:110px;
    background:linear-gradient(135deg,rgba(99,102,241,.2),rgba(139,92,246,.1));
    border:1px solid rgba(99,102,241,.3); border-radius:10px;
    padding:10px 14px; text-align:center;
}
.metric-box .mv { font-size:1.7rem; font-weight:700; color:#a5b4fc; }
.metric-box .ml { font-size:.72rem; color:#94a3b8; margin-top:2px; }

/* ── Download button links ── */
.dl-btn {
    display:inline-block; padding:7px 18px; border-radius:8px;
    text-decoration:none; font-size:.82rem; font-weight:600;
    margin:3px; transition:opacity .2s;
}
.dl-btn:hover { opacity:.8; }

/* ── Tab style ── */
[data-testid="stTab"] { font-weight:600 !important; }

/* ── Creator footer ── */
.creator-bar {
    background:linear-gradient(90deg,#0f172a,#1e293b);
    border-top:1px solid #334155; padding:8px 18px;
    text-align:center; font-size:.75rem; color:#64748b; border-radius:0 0 10px 10px;
    margin-top:16px;
}

/* ── Main bg ── */
.stApp { background:#0f172a; }
.block-container { padding-top:1rem !important; }
div[data-testid="stDataFrame"] { border-radius:10px; overflow:hidden; }

hr.thin { border:none; border-top:1px solid #1e293b; margin:6px 0; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════
def parse_date(s):
    if s is None: return None
    try:
        if pd.isna(s): return None
    except: pass
    if isinstance(s,(datetime,date,pd.Timestamp)):
        return s.date() if hasattr(s,"date") else None
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

def drange(s,e):
    d=s
    while d<=e: yield d; d+=timedelta(days=1)

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

def concat_r(df,d): return pd.concat([df,pd.DataFrame([d])],ignore_index=True)

SPLIT_RE=re.compile(r"[,\uFF0C;|\-/\\_\s]+")
def split_toks(v):
    if v is None: return []
    s=str(v).strip()
    return [p.strip() for p in SPLIT_RE.split(s) if p.strip()] if s else []

def is_busy(t):
    t2=str(t).strip().upper()
    return t2=="B" or bool(re.match(r"^B[\W_]*\d+$",t2))

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

def inscode_from_sid(sid):
    s=str(sid).strip()
    return s[1:4] if len(s)>=4 else ""

def get_name(sdf,sid):
    sid=norm_id(sid)
    if not sid: return ""
    m=sdf["Staff ID"].astype(str).str.upper()==sid
    if m.any():
        try: return str(sdf.loc[m,"Name of the Staff"].iloc[0])
        except: return ""
    return ""

def get_phone(sdf,sid):
    sid=norm_id(sid)
    if not sid: return ""
    m=sdf["Staff ID"].astype(str).str.upper()==sid
    if m.any():
        try: return str(sdf.loc[m,"Phone"].iloc[0])
        except: return ""
    return ""

def get_subname(submap,code):
    if submap is None or submap.empty: return ""
    m=submap[submap["SUBCODE"].astype(str)==str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def duty_stats(sdf):
    stats={}
    if sdf is None or sdf.empty: return stats
    dcols=[c for c in sdf.columns if c!="__rowid" and isinstance(c,str)
           and len(c.split("."))==3 and all(p.isdigit() for p in c.split("."))]
    for _,row in sdf.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        dm,cnt={},0
        for dc in dcols:
            toks=split_toks(row.get(dc,""))
            dm[dc]=toks; cnt+=sum(1 for t in toks if not is_busy(t))
        stats[sid]={"count":cnt,"dm":dm,
            "INSTT":row.get("INSTT",""),"dep":row.get("dep code",""),
            "name":row.get("Name of the Staff",""),"desig":row.get("Designation",""),
            "phone":row.get("Phone","")}
    return stats

# ══════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════
if "nav" not in st.session_state: st.session_state.nav="upload"
if "staged" not in st.session_state: st.session_state.staged={}
if "errors" not in st.session_state: st.session_state.errors={}

for key,path,cols,pre in [
    ("panel",  PANEL_PATH,       PANEL_COLS,       "p"),
    ("pdate",  PANEL_DATED_PATH, PANEL_DATED_COLS, "d"),
    ("staff",  STAFF_PATH,       STAFF_COLS,       "s"),
]:
    if key not in st.session_state:
        df=load_csv(path,cols); df=rowid(df,pre)
        for c in cols:
            if c not in df.columns: df[c]=""
        st.session_state[key]=df.copy()

if "submap" not in st.session_state:
    sm=load_csv(SUBMAP_PATH,["SUBCODE","SUBNAME"])
    st.session_state.submap=sm.copy()

if "ssmap" not in st.session_state:
    sm2=load_csv(SUBJMAP_PATH,SUBJMAP_COLS)
    for c in SUBJMAP_COLS:
        if c not in sm2.columns: sm2[c]=""
    st.session_state.ssmap=sm2.copy()

def P():  st.session_state.panel=rowid(st.session_state.panel,"p");  save_csv(st.session_state.panel,PANEL_PATH)
def PD(): st.session_state.pdate=rowid(st.session_state.pdate,"d");  save_csv(st.session_state.pdate,PANEL_DATED_PATH)
def S():  st.session_state.staff=rowid(st.session_state.staff,"s");  save_csv(st.session_state.staff,STAFF_PATH)
def SM(): save_csv(st.session_state.submap,SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap,SUBJMAP_PATH)

# ══════════════════════════════════════════════
# LOGIC — EXT SUGGESTIONS & AUTO-ALLOCATE
# ══════════════════════════════════════════════
def ext_suggs(panel_row, sdf, ssmap):
    panel_ins=str(panel_row.get("INSCODE","")).strip()
    subcode  =str(panel_row.get("SUBCODE","")).strip().upper()
    panel_dep=str(panel_row.get("NCNO","")).strip()
    stats    =duty_stats(sdf)
    if ssmap is not None and not ssmap.empty:
        mapped   =ssmap[ssmap["Subject_Code"].astype(str).str.strip().str.upper()==subcode]
        mapped_ids=set(mapped["Staff_Last_Staff_ID"].apply(norm_id).unique())
    else:
        mapped_ids=None
    results=[]
    for _,row in sdf.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        instt=str(row.get("INSTT","")).strip()
        if instt==panel_ins: continue
        dep=str(row.get("dep code","")).strip()
        if mapped_ids is not None:
            if sid not in mapped_ids: continue
        else:
            if dep!=panel_dep: continue
        se=stats.get(sid,{})
        results.append({"sid":sid,"name":row.get("Name of the Staff",""),
            "desig":row.get("Designation",""),"instt":instt,
            "dep":dep,"phone":row.get("Phone",""),"count":se.get("count",0)})
    results.sort(key=lambda x:x["count"])
    return results

def make_lbl(s): return f"🟢 {s['sid']} — {s['name']} — {s['desig']} — {s['instt']}"

def auto_allocate(candidates, sdf, ssmap):
    res,skip={},{}
    for pidx,row in candidates.iterrows():
        suggs=ext_suggs(row,sdf,ssmap)
        if suggs: res[pidx]=make_lbl(suggs[0])
        else: skip[pidx]=f"No eligible external for SUBCODE {str(row.get('SUBCODE','')).strip()}"
    return res,skip

# ══════════════════════════════════════════════
# ERROR CHECK (dated panel)
# ══════════════════════════════════════════════
def check_errors(panel_df, sdf):
    errs={i:[] for i in panel_df.index}
    staff_duties={}
    for idx,row in panel_df.iterrows():
        d1=parse_date(row.get("DATE_FROM")); d2=parse_date(row.get("DATE_TO"))
        sc=str(row.get("SUBCODE","")).strip(); ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if is_zero(row.get(fld,"")): sid=""
            if not sid: continue
            staff_ins=inscode_from_sid(sid)
            if role=="INT" and staff_ins and staff_ins!=ins:
                errs[idx].append(f"❌ INTID {sid}: home-inst {staff_ins} ≠ exam-inst {ins} (must be same)")
            if role=="EXT" and staff_ins and staff_ins==ins:
                errs[idx].append(f"❌ EXTID {sid}: home-inst {staff_ins} == exam-inst {ins} (must differ)")
            staff_duties.setdefault(sid,[]).append((idx,sc,d1,d2,role))
    for sid,duties in staff_duties.items():
        for i in range(len(duties)):
            idx_a,sc_a,d1_a,d2_a,_=duties[i]
            if d1_a is None or d2_a is None: continue
            for j in range(i+1,len(duties)):
                idx_b,sc_b,d1_b,d2_b,_=duties[j]
                if d1_b is None or d2_b is None: continue
                if max(d1_a,d1_b)<=min(d2_a,d2_b) and sc_a!=sc_b:
                    msg=(f"⚠️ CLASH {sid}: {sc_a}({d2s(d1_a)}→{d2s(d2_a)}) "
                         f"overlaps {sc_b}({d2s(d1_b)}→{d2s(d2_b)})")
                    errs[idx_a].append(msg); errs[idx_b].append(msg)
    return {k:v for k,v in errs.items() if v}

# ══════════════════════════════════════════════
# PDF
# ══════════════════════════════════════════════
def gen_pdf(panel_df, sdf, submap):
    buf=BytesIO()
    if not RPDF: buf.write(b"Install reportlab: pip install reportlab"); return buf.getvalue()
    doc=SimpleDocTemplate(buf,pagesize=A4,
        leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=1.5*cm,bottomMargin=1.5*cm)
    styles=getSampleStyleSheet()
    H1=ParagraphStyle("H1",fontSize=13,fontName="Helvetica-Bold",spaceAfter=4,alignment=TA_CENTER)
    SM_=ParagraphStyle("SM",fontSize=7.5,fontName="Helvetica",spaceAfter=2)
    FOOT=ParagraphStyle("FOOT",fontSize=7,fontName="Helvetica",textColor=RC.grey,alignment=TA_CENTER)
    story=[]
    all_ids=set()
    for _,row in panel_df.iterrows():
        for fld in ["INTID","EXTID"]:
            sid=norm_id(row.get(fld,"")); 
            if sid: all_ids.add(sid)
    staff_duties={}
    for _,row in panel_df.iterrows():
        sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
        ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if not sid: continue
            staff_duties.setdefault(sid,[]).append({
                "ins":ins,"sc":sc,"sn":sn,"role":role,
                "cp_id":norm_id(row.get("EXTID" if role=="INT" else "INTID",""))
            })
    sorted_staff=sorted(all_ids,key=lambda sid:(
        str(sdf[sdf["Staff ID"].astype(str).str.upper()==sid]["INSTT"].iloc[0])
        if not sdf[sdf["Staff ID"].astype(str).str.upper()==sid].empty else "",
        get_name(sdf,sid)))
    for sid in sorted_staff:
        duties=staff_duties.get(sid,[])
        if not duties: continue
        name=get_name(sdf,sid); phone=get_phone(sdf,sid)
        m=sdf[sdf["Staff ID"].astype(str).str.upper()==sid]
        desig=str(m.iloc[0]["Designation"]) if not m.empty else ""
        dept =str(m.iloc[0]["Department"])  if not m.empty else ""
        instt=str(m.iloc[0]["INSTT"])       if not m.empty else ""
        story.append(Paragraph("PRACTICAL EXAM DUTY ORDER", H1))
        story.append(Paragraph(CREATOR, FOOT))
        story.append(Spacer(1,0.3*cm))
        hd=[["Staff ID",sid,"Name",name],["Institution",instt,"Phone",phone],
            ["Department",dept,"Designation",desig]]
        ht=Table(hd,colWidths=[2.8*cm,4.5*cm,2.8*cm,6.5*cm])
        ht.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1),RC.HexColor("#1e293b")),
            ("TEXTCOLOR",(0,0),(-1,-1),RC.white),
            ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
            ("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("GRID",(0,0),(-1,-1),0.5,RC.HexColor("#334155")),
            ("PADDING",(0,0),(-1,-1),5),
        ]))
        story.append(ht); story.append(Spacer(1,0.4*cm))
        th=["S.No","Duty INSCODE","Sub Code","Subject Name","Role",
            "Partner ID","Partner Name","Partner Phone","Date From","Date To"]
        tr=[th]
        for sno,d in enumerate(duties,1):
            pid=d["cp_id"]; pname=get_name(sdf,pid) if pid else ""; pphone=get_phone(sdf,pid) if pid else ""
            tr.append([str(sno),d["ins"],d["sc"],d["sn"] or d["sc"],
                       d["role"],pid or "-",pname or "-",pphone or "-","",""])
        cw=[.9*cm,2*cm,2*cm,4.2*cm,1.2*cm,2.2*cm,3.5*cm,2.2*cm,2*cm,2*cm]
        dt=Table(tr,colWidths=cw,repeatRows=1)
        dt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),RC.HexColor("#4f46e5")),
            ("TEXTCOLOR",(0,0),(-1,0),RC.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),7.5),
            ("ALIGN",(0,0),(-1,-1),"CENTER"),
            ("ALIGN",(3,1),(3,-1),"LEFT"),("ALIGN",(6,1),(6,-1),"LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[RC.HexColor("#f8fafc"),RC.HexColor("#e2e8f0")]),
            ("GRID",(0,0),(-1,-1),0.4,RC.HexColor("#94a3b8")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("PADDING",(0,0),(-1,-1),4),
        ]))
        story.append(dt); story.append(Spacer(1,.3*cm))
        story.append(Paragraph("Date From / To — to be filled manually at time of duty.",FOOT))
        story.append(PageBreak())
    doc.build(story)
    return buf.getvalue()

def dl_link(df,fname,label,color="#4f46e5"):
    csv=df.to_csv(index=False).encode()
    b64=base64.b64encode(csv).decode()
    return (f'<a href="data:file/csv;base64,{b64}" download="{fname}" '
            f'class="dl-btn" style="background:{color};color:white;">{label}</a>')

def pdf_link(pdf_b,fname,label):
    b64=base64.b64encode(pdf_b).decode()
    return (f'<a href="data:application/pdf;base64,{b64}" download="{fname}" '
            f'class="dl-btn" style="background:#dc2626;color:white;">{label}</a>')

# ══════════════════════════════════════════════
# PROFESSIONAL SIDEBAR NAVIGATION
# ══════════════════════════════════════════════
NAV_ITEMS = [
    ("upload", "📥", "Upload Centre",  "Panel · Staff · Subject Map"),
    ("ext",    "🎯", "EXT Allocate",   "Assign External Examiners"),
    ("duty",   "▶️", "Duty Marking",   "Dates · Errors · Validation"),
]

with st.sidebar:
    # Logo / Brand
    st.markdown("""
    <div style="padding:20px 16px 12px;border-bottom:1px solid #1e293b;margin-bottom:10px">
      <div style="display:flex;align-items:center;gap:10px">
        <div style="background:linear-gradient(135deg,#4f46e5,#7c3aed);
                    width:40px;height:40px;border-radius:10px;display:flex;
                    align-items:center;justify-content:center;font-size:1.3rem">🗂️</div>
        <div>
          <div style="font-weight:800;font-size:1rem;color:#e2e8f0;letter-spacing:.5px">DUTY MANAGER</div>
          <div style="font-size:.68rem;color:#475569;letter-spacing:.3px">PRACTICAL EXAM PANEL</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Navigation
    st.markdown('<div style="padding:4px 8px 6px;font-size:.68rem;color:#475569;letter-spacing:1px;font-weight:700">NAVIGATION</div>',
                unsafe_allow_html=True)

    for nav_key, icon, title, subtitle in NAV_ITEMS:
        is_active = st.session_state.nav == nav_key
        if is_active:
            st.markdown(
                f'<div class="nav-active">{icon}&nbsp;&nbsp;<b>{title}</b>'
                f'<div style="font-size:.7rem;color:#818cf8;margin-top:2px">{subtitle}</div></div>',
                unsafe_allow_html=True)
        else:
            if st.button(f"{icon}  {title}", key=f"nav_{nav_key}", use_container_width=True,
                         help=subtitle):
                st.session_state.nav = nav_key
                st.rerun()

    # Stats
    st.markdown('<hr style="border:1px solid #1e293b;margin:12px 0">',unsafe_allow_html=True)
    st.markdown('<div style="padding:2px 8px 6px;font-size:.68rem;color:#475569;letter-spacing:1px;font-weight:700">QUICK STATS</div>',
                unsafe_allow_html=True)

    pn=len(st.session_state.panel); pdn=len(st.session_state.pdate)
    ef=st.session_state.panel["EXTID"].apply(lambda v:norm_id(v)!="").sum() if pn else 0
    ep=pn-ef; sc_=len(st.session_state.staff); sm2c=len(st.session_state.ssmap)
    sub_c=len(st.session_state.submap)

    for lbl,val,color in [
        ("📋 Panel (No Date)", pn,  "#6366f1"),
        ("✅ EXTID Filled",    ef,  "#22c55e"),
        ("⏳ EXTID Pending",   ep,  "#f59e0b"),
        ("🗓️ Panel (Dated)",  pdn, "#8b5cf6"),
        ("🧑‍🏫 Staff Loaded",  sc_, "#3b82f6"),
        ("📘 SubjectMap",     sm2c, "#ec4899"),
        ("📖 SubNameMap",    sub_c, "#06b6d4"),
    ]:
        st.markdown(
            f'<div class="stat-pill">'
            f'<span style="font-size:.78rem">{lbl}</span>'
            f'<span class="sv" style="color:{color}">{val}</span>'
            f'</div>', unsafe_allow_html=True)

    # Footer
    st.markdown(
        f'<div style="margin-top:16px;padding:10px 12px;background:rgba(255,255,255,.03);'
        f'border-radius:8px;border:1px solid #1e293b;font-size:.7rem;color:#475569;text-align:center">'
        f'✨ {CREATOR}</div>', unsafe_allow_html=True)

page = st.session_state.nav

# ══════════════════════════════════════════════
# PAGE 1 — UPLOAD CENTRE
# ══════════════════════════════════════════════
if page=="upload":
    st.markdown('<div class="section-hdr">📥 Upload Centre</div>',unsafe_allow_html=True)
    t1,t2,t3=st.tabs(["📋 Panel (No Dates)","🧑‍🏫 Staff Details","📘 Subject-Staff Mapping"])

    # ── TAB 1: Panel ──────────────────────────────────────
    with t1:
        st.markdown('<div class="sub-hdr">Upload & Edit Panel</div>',unsafe_allow_html=True)
        ca,cb=st.columns([5,7])
        with ca:
            st.markdown("**Required columns:**")
            st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID",language="")
            st.caption("💡 INTID should be filled. EXTID will be assigned on EXT Allocate page.")
            uf=st.file_uploader("📂 Upload Panel CSV/XLSX",type=["csv","xlsx"],key="p_up")
            cl=st.checkbox("⚠️ Clear ALL before upload",key="p_clear")
            if uf:
                try:
                    tmp=(pd.read_csv(uf,dtype=object) if uf.name.lower().endswith(".csv")
                         else pd.read_excel(uf,dtype=object,sheet_name=0)).fillna("")
                    req=["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                    miss=[c for c in req if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp=tmp[req].copy(); tmp["ERROR"]=""; tmp=rowid(tmp,"p")
                        bk=st.session_state.panel.copy()
                        if cl:
                            st.session_state.panel=rowid(tmp.reset_index(drop=True),"p")
                        else:
                            ins_up=[str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                            bk=bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                            bk=pd.concat([bk,tmp],ignore_index=True)
                            st.session_state.panel=rowid(bk.reset_index(drop=True),"p")
                        P(); st.success(f"✅ Uploaded {len(tmp)} rows")
                except Exception as e: st.error(f"❌ {e}")

            st.markdown("---")
            st.markdown("**📖 SUBCODE → SUBNAME Mapping**")
            sf2=st.file_uploader("Upload SUBCODE-SUBNAME CSV",type=["csv","xlsx"],key="sub_up2")
            if sf2:
                try:
                    sm2=(pd.read_csv(sf2,dtype=object) if sf2.name.lower().endswith(".csv")
                         else pd.read_excel(sf2,dtype=object,sheet_name=0)).fillna("")
                    if "SUBCODE" not in sm2.columns or "SUBNAME" not in sm2.columns:
                        if sm2.shape[1]>=2:
                            sm2=pd.DataFrame({"SUBCODE":sm2.iloc[:,0].astype(str),"SUBNAME":sm2.iloc[:,1].astype(str)})
                    st.session_state.submap=sm2[["SUBCODE","SUBNAME"]].copy(); SM()
                    st.success(f"✅ SUBNAME mapping: {len(sm2)} rows")
                except Exception as e: st.error(f"❌ {e}")

            # Submap editable preview
            if not st.session_state.submap.empty:
                st.markdown("**✏️ Edit SUBCODE → SUBNAME**")
                edited_sub=st.data_editor(
                    st.session_state.submap[["SUBCODE","SUBNAME"]],
                    use_container_width=True, num_rows="dynamic",
                    key="submap_editor", height=200)
                if st.button("💾 Save SUBNAME Map Changes", key="sub_save"):
                    st.session_state.submap=edited_sub.fillna("").copy()
                    SM(); st.success("✅ SUBNAME map saved")

        with cb:
            pv=st.session_state.panel.copy()
            if not st.session_state.submap.empty:
                pv=pv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            show_cols=[c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID","EXTID","ERROR"] if c in pv.columns]
            st.markdown(f"**Panel — {len(pv)} rows** *(inline editable)*")
            edited_panel=st.data_editor(pv[show_cols].fillna(""),
                use_container_width=True, height=420, key="panel_editor", num_rows="dynamic")
            if st.button("💾 Save Panel Changes", key="p_edit_save"):
                try:
                    ep2=edited_panel.copy()
                    if "SUBNAME" in ep2.columns: ep2=ep2.drop(columns=["SUBNAME"])
                    if "ERROR" not in ep2.columns: ep2["ERROR"]=""
                    ep2=rowid(ep2,"p")
                    st.session_state.panel=ep2.copy(); P()
                    st.success("✅ Panel changes saved")
                except Exception as e: st.error(f"❌ {e}")
            # download
            exp_c=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in st.session_state.panel.columns]
            st.markdown(dl_link(st.session_state.panel[exp_c],"panel.csv","📥 Download Panel CSV"),
                        unsafe_allow_html=True)

    # ── TAB 2: Staff ──────────────────────────────────────
    with t2:
        st.markdown('<div class="sub-hdr">Upload & Edit Staff Master</div>',unsafe_allow_html=True)
        sa,sb=st.columns([5,7])
        with sa:
            st.markdown("**Required columns:**")
            st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation  Phone",language="")
            st.caption("💡 Phone is used in PDF duty sheets.")
            usf=st.file_uploader("📂 Upload Staff CSV/XLSX",type=["csv","xlsx"],key="s_up")
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
            # sample
            sample_staff=pd.DataFrame([{"Staff ID":"X123EEE1","INSTT":"123",
                "Name of the Staff":"KUMAR S","Department":"Electrical & Electronics Engg",
                "dep code":"1030","Designation":"Lecturer","Phone":"9876543210"}])
            st.markdown(dl_link(sample_staff,"sample_staff.csv","📥 Sample Staff CSV","#0891b2"),
                        unsafe_allow_html=True)
        with sb:
            sv=st.session_state.staff.copy()
            fi,fd=st.columns(2)
            fi_s=fi.selectbox("Filter INSTT",["All"]+sorted(set(sv["INSTT"].astype(str))),key="sf_ins")
            fd_s=fd.selectbox("Filter Dept",["All"]+sorted(set(sv["Department"].astype(str))),key="sf_dep")
            if fi_s!="All": sv=sv[sv["INSTT"].astype(str)==fi_s]
            if fd_s!="All": sv=sv[sv["Department"].astype(str)==fd_s]
            disp=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"] if c in sv.columns]
            st.markdown(f"**Staff — {len(sv)} rows** *(inline editable)*")
            es2=st.data_editor(sv[disp],use_container_width=True,height=420,
                               key="staff_ed2",num_rows="dynamic")
            if st.button("💾 Save Staff Changes",key="s_save2"):
                try:
                    bk=st.session_state.staff.copy().set_index("__rowid",drop=False)
                    ed=es2.copy()
                    if "__rowid" not in ed.columns: ed["__rowid"]=""
                    ed=rowid(ed,"s").set_index("__rowid",drop=False)
                    for rid in bk.index.intersection(ed.index):
                        for c in ed.columns: bk.at[rid,c]=ed.at[rid,c]
                    nw=ed.index.difference(bk.index)
                    if len(nw):
                        bk=pd.concat([bk.reset_index(drop=True),ed.loc[nw].reset_index(drop=True)],ignore_index=True)
                    st.session_state.staff=rowid(bk.reset_index(drop=True),"s")
                    S(); st.success("✅ Staff saved")
                except Exception as e: st.error(f"❌ {e}")
            disp2=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"] if c in st.session_state.staff.columns]
            st.markdown(dl_link(st.session_state.staff[disp2],"staff.csv","📥 Download Staff CSV"),
                        unsafe_allow_html=True)

    # ── TAB 3: Subject-Staff Mapping ──────────────────────
    with t3:
        st.markdown('<div class="sub-hdr">Upload & Edit Subject-Staff Mapping</div>',unsafe_allow_html=True)
        ma,mb=st.columns([5,7])
        with ma:
            st.markdown("**Required columns:**")
            st.code("\n".join(SUBJMAP_COLS),language="")
            c_t,c_s=st.columns(2)
            c_t.markdown(dl_link(pd.DataFrame(columns=SUBJMAP_COLS),"ssmap_template.csv",
                         "📥 Empty Template","#0891b2"),unsafe_allow_html=True)
            sample_ssm=pd.DataFrame([{"Staff_Last_Staff_ID":"X123EEE1","Staff_Name":"KUMAR S",
                "Department":"EEE","Department_Code":"1030","Subject_Type":"Core",
                "Subject_Code":"P3401","Subject_Name":"Basic Electrical Lab","Subject_Remarks":""}])
            c_s.markdown(dl_link(sample_ssm,"ssmap_sample.csv","📥 Sample CSV","#7c3aed"),
                         unsafe_allow_html=True)
            st.markdown("")
            ussm=st.file_uploader("📂 Upload Subject-Staff Mapping CSV/XLSX",type=["csv","xlsx"],key="ssm_up2")
            if ussm:
                try:
                    tmp=(pd.read_csv(ussm,dtype=object) if ussm.name.lower().endswith(".csv")
                         else pd.read_excel(ussm,dtype=object,sheet_name=0)).fillna("")
                    miss=[c for c in SUBJMAP_COLS if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp["Staff_Last_Staff_ID"]=tmp["Staff_Last_Staff_ID"].apply(norm_id)
                        tmp["Subject_Code"]=tmp["Subject_Code"].astype(str).str.strip().str.upper()
                        st.session_state.ssmap=tmp[SUBJMAP_COLS].copy()
                        SS(); st.success(f"✅ {len(tmp)} mapping rows loaded")
                except Exception as e: st.error(f"❌ {e}")

        with mb:
            ssv=st.session_state.ssmap.copy()
            sf1_,sf2_=st.columns(2)
            dep_f_=sf1_.selectbox("Filter Dept",["All"]+sorted(set(ssv["Department"].astype(str))),key="sm_dep2")
            sub_f_=sf2_.text_input("Filter Subject Code","",key="sm_sub2",placeholder="e.g. P3401")
            if dep_f_!="All": ssv=ssv[ssv["Department"]==dep_f_]
            if sub_f_.strip(): ssv=ssv[ssv["Subject_Code"].str.contains(sub_f_.strip().upper())]
            st.markdown(f"**Subject-Staff Mapping — {len(ssv)} rows** *(inline editable)*")
            edited_ssm=st.data_editor(ssv,use_container_width=True,height=400,
                                      key="ssm_editor2",num_rows="dynamic")
            if st.button("💾 Save Mapping Changes",key="ssm_save2"):
                try:
                    # Merge edited rows back into full ssmap
                    full=st.session_state.ssmap.copy()
                    if "__rowid" in ssv.columns and "__rowid" in edited_ssm.columns:
                        # update rows present in filtered view
                        full_idx=full.reset_index().set_index("index")
                        for _,er in edited_ssm.iterrows():
                            mask=full["Subject_Code"].astype(str)==str(er.get("Subject_Code",""))
                            mask2=full["Staff_Last_Staff_ID"].astype(str)==str(er.get("Staff_Last_Staff_ID",""))
                            hit=full[mask & mask2].index
                            for c in SUBJMAP_COLS:
                                if c in er.index:
                                    for i in hit: full.at[i,c]=er[c]
                    else:
                        # simple: replace with edited (filtered view)
                        full=edited_ssm.copy()
                    for c in SUBJMAP_COLS:
                        if c not in full.columns: full[c]=""
                    st.session_state.ssmap=full[SUBJMAP_COLS].copy()
                    SS(); st.success("✅ Mapping changes saved")
                except Exception as e: st.error(f"❌ {e}")
            st.markdown(dl_link(st.session_state.ssmap,"ssmap.csv","📥 Download Mapping CSV"),
                        unsafe_allow_html=True)

# ══════════════════════════════════════════════
# PAGE 2 — EXT ALLOCATE
# ══════════════════════════════════════════════
elif page=="ext":
    st.markdown('<div class="section-hdr">🎯 EXT Allocate — Assign External Examiners</div>',
                unsafe_allow_html=True)
    with st.expander("ℹ️ Allocation Logic"):
        st.markdown("""
| Step | Rule |
|------|------|
| 1️⃣ | Match panel **SUBCODE** → Subject-Staff Mapping `Subject_Code` |
| 2️⃣ | Staff `INSTT` must be **different** from panel `INSCODE` (External rule) |
| 3️⃣ | If no subject map: fallback to `dep code == panel NCNO` |
| 4️⃣ | Sort by **least existing duty count** → pick best |
| ⚠️ | Date clash is **NOT** checked here — validate on Duty Marking page |
        """)

    panel=st.session_state.panel.copy()
    staff=st.session_state.staff.copy()
    ssmap=st.session_state.ssmap.copy()
    submap=st.session_state.submap.copy()

    def needs_ext2(r):
        return str(r.get("INTID","")).strip()!="" and (str(r.get("EXTID","")).strip()=="" or is_zero(r.get("EXTID","")))

    candidates=panel[panel.apply(needs_ext2,axis=1)].copy()

    fc1,fc2=st.columns(2)
    ins_f=fc1.selectbox("🏫 INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ea_ins2")
    nc_f =fc2.selectbox("🏭 NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ea_nc2")
    if ins_f!="All": candidates=candidates[candidates["INSCODE"].astype(str)==ins_f]
    if nc_f !="All": candidates=candidates[candidates["NCNO"].astype(str)==nc_f]

    # Metrics
    st.markdown(
        f'<div class="metrics-row">'
        f'<div class="metric-box"><div class="mv">📋 {len(candidates)}</div><div class="ml">Pending EXTID</div></div>'
        f'<div class="metric-box"><div class="mv">🧑‍🏫 {len(staff)}</div><div class="ml">Staff Loaded</div></div>'
        f'<div class="metric-box"><div class="mv">📘 {len(ssmap)}</div><div class="ml">SubjectMap</div></div>'
        f'<div class="metric-box"><div class="mv">🔖 {len(st.session_state.staged)}</div><div class="ml">Staged</div></div>'
        f'</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="sub-hdr">🤖 Auto-Allocate</div>',unsafe_allow_html=True)
    ac1,ac2=st.columns([4,1])
    ac1.caption("Matches SUBCODE → Subject map → Different INSTT → Least duty count → Auto-stages all visible rows")
    with ac2:
        if st.button("🤖 Auto-Allocate ALL",type="primary",use_container_width=True):
            if staff.empty: st.error("❌ Upload staff first!")
            else:
                res,skip=auto_allocate(candidates,staff,ssmap if not ssmap.empty else None)
                for k,v in res.items(): st.session_state.staged[str(k)]=v
                st.success(f"✅ Auto-staged {len(res)} rows.")
                if skip:
                    with st.expander(f"⚠️ {len(skip)} skipped"):
                        st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]),
                                     use_container_width=True)

    st.markdown('<div class="sub-hdr">📝 Per-Row Allocation</div>',unsafe_allow_html=True)
    if candidates.empty:
        st.markdown('<div class="ok-card">🎉 No pending EXTID rows!</div>',unsafe_allow_html=True)
    else:
        for _,row in candidates.reset_index().iterrows():
            pidx=int(row["index"])
            sc=str(row.get("SUBCODE","")).strip(); sn=get_subname(submap,sc)
            ins=str(row.get("INSCODE","")).strip(); nc=str(row.get("NCNO","")).strip()
            intid=str(row.get("INTID","")).strip(); intname=get_name(staff,intid)
            sv=st.session_state.staged.get(str(pidx),"")
            suggs=ext_suggs(row,staff,ssmap if not ssmap.empty else None)
            sugg_labels=["— Select —"]+[make_lbl(s) for s in suggs]
            with st.container():
                h1,h2,h3=st.columns([3,3,2])
                h1.markdown(f'<div style="font-size:.85rem"><b>🏫 {ins}</b> &nbsp;|&nbsp; 🏭 {nc} &nbsp;|&nbsp; 📚 {sc} {("— "+sn) if sn else ""}</div>',unsafe_allow_html=True)
                h2.markdown(f'<div style="font-size:.82rem">👥 {row.get("NOC","")} &nbsp;|&nbsp; INT: <code>{intid}</code> {intname}</div>',unsafe_allow_html=True)
                sv_d=(sv[:45]+"...") if len(sv)>45 else sv
                h3.markdown(f'<div style="font-size:.78rem">{"✅ "+sv_d if sv else "⬜ not staged"}</div>',unsafe_allow_html=True)
                r1,r2,r3=st.columns([4,3,1])
                di=sugg_labels.index(sv) if sv in sugg_labels else 0
                sel=r1.selectbox("💡",sugg_labels,index=di,key=f"sel2_{pidx}_{sv[:8]}",
                                 label_visibility="collapsed")
                man=r2.text_input("✏️",value="",key=f"man2_{pidx}",placeholder="Manual Staff ID",
                                  label_visibility="collapsed")
                if sel and sel!="— Select —": st.session_state.staged[str(pidx)]=sel
                if man.strip(): st.session_state.staged[str(pidx)]=man.strip()
                if r3.button("▶",key=f"app2_{pidx}"):
                    chosen=sv or (sel if sel!="— Select —" else "") or man.strip()
                    if not chosen: st.warning("⚠️ Select or type a staff ID")
                    else:
                        lc=str(chosen).replace("🟢 ","").split("—")
                        sid_c=norm_id(lc[0].strip()) if lc else ""
                        if sid_c:
                            st.session_state.panel.at[pidx,"EXTID"]=sid_c; P()
                            st.session_state.staged.pop(str(pidx),None)
                            st.success(f"✅ EXTID={sid_c} (Row {pidx})")
                        else: st.error("❌ Invalid staff ID")
                st.markdown('<hr class="thin">',unsafe_allow_html=True)

    st.markdown("---")
    staged_map=st.session_state.staged
    if staged_map:
        st.markdown('<div class="sub-hdr">🚀 Apply Staged</div>',unsafe_allow_html=True)
        with st.expander(f"👁️ Preview {len(staged_map)} staged"):
            rows=[]
            for k,v in list(staged_map.items())[:30]:
                try:
                    pi=int(k); r=st.session_state.panel.loc[pi] if pi in st.session_state.panel.index else {}
                    rows.append({"Idx":k,"INSCODE":r.get("INSCODE","?"),"SUBCODE":r.get("SUBCODE","?"),"Staged":v})
                except: rows.append({"Idx":k,"Staged":v})
            st.dataframe(pd.DataFrame(rows),use_container_width=True)
        a1,a2=st.columns(2)
        if a1.button("✅ Apply ALL Staged",type="primary",use_container_width=True):
            ok_c,fail_c=0,[]
            for k,v in list(staged_map.items()):
                try: pi=int(k)
                except: fail_c.append(k); continue
                if pi not in st.session_state.panel.index: fail_c.append(k); continue
                lc=str(v).replace("🟢 ","").split("—")
                sid_c=norm_id(lc[0].strip()) if lc else ""
                if sid_c:
                    st.session_state.panel.at[pi,"EXTID"]=sid_c
                    st.session_state.staged.pop(k,None); ok_c+=1
                else: fail_c.append(k)
            P(); st.success(f"✅ {ok_c} applied | ❌ {len(fail_c)} failed")
        if a2.button("🗑️ Clear Staged",use_container_width=True):
            st.session_state.staged={}; st.success("✅ Staged cleared")

    st.markdown("---")
    st.markdown('<div class="sub-hdr">📥 Downloads</div>',unsafe_allow_html=True)
    all_p=st.session_state.panel.copy()
    if not all_p.empty:
        exp_c=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"] if c in all_p.columns]
        inscodes=sorted(set(all_p["INSCODE"].astype(str)))
        dc1,dc2,dc3=st.columns(3)
        with dc1:
            st.markdown("**📋 CSV per Institution**")
            for ins in inscodes:
                df_ins=all_p[all_p["INSCODE"].astype(str)==ins][exp_c]
                st.markdown(dl_link(df_ins,f"panel_{ins}.csv",f"📥 INSCODE {ins}","#4f46e5"),
                            unsafe_allow_html=True)
        with dc2:
            st.markdown("**📋 Full Panel CSV**")
            st.markdown(dl_link(all_p[exp_c],"panel_all.csv","📥 Full Panel CSV"),unsafe_allow_html=True)
        with dc3:
            st.markdown("**🖨️ PDF Duty Sheets**")
            if RPDF:
                if st.button("🖨️ Generate PDF"):
                    with st.spinner("Building PDF..."):
                        pdf_b=gen_pdf(all_p,st.session_state.staff,submap)
                    st.markdown(pdf_link(pdf_b,"duty_sheets.pdf","📄 Download PDF"),unsafe_allow_html=True)
            else:
                st.markdown('<div class="err-card">Install reportlab:<br><code>pip install reportlab</code></div>',
                            unsafe_allow_html=True)

# ══════════════════════════════════════════════
# PAGE 3 — DUTY MARKING
# ══════════════════════════════════════════════
elif page=="duty":
    st.markdown('<div class="section-hdr">▶️ Duty Marking — Upload Dated Panel & Validate</div>',
                unsafe_allow_html=True)
    with st.expander("ℹ️ Error-Check Logic"):
        st.markdown("""
| # | Check | Rule |
|---|-------|------|
| 🔴 1 | **INT/EXT Institution Rule** | `INTID` chars[2-4] of Staff ID must **equal** panel INSCODE. `EXTID` must **differ**. |
| 🔴 2 | **Single-Day Clash** | Same staff on same date with **different SUBCODE** = CLASH. Same SUBCODE (batches) = ✅ OK. |
| 🔴 3 | **Multi-Day Overlap** | Overlapping DATE_FROM→DATE_TO ranges for same staff, different SUBCODE = CLASH. |
        """)

    dm1,dm2=st.columns([4,6])
    with dm1:
        st.markdown('<div class="sub-hdr">📂 Upload Dated Panel</div>',unsafe_allow_html=True)
        st.code("INSCODE NCNO SUBCODE REGL NOC NOB INTID EXTID DATE_FROM DATE_TO",language="")
        udp=st.file_uploader("Upload Panel with Dates",type=["csv","xlsx"],key="dp_up2")
        cl2=st.checkbox("⚠️ Clear existing dated panel",key="dp_clear2")
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
                    PD(); st.success(f"✅ {len(tmp)} rows loaded")
            except Exception as e: st.error(f"❌ {e}")

        # Filters + Run
        gc1,gc2=st.columns(2)
        ins_g=gc1.selectbox("INSCODE",["All"]+sorted(set(st.session_state.pdate["INSCODE"].astype(str))),key="dm_ins2")
        nc_g =gc2.selectbox("NCNO",   ["All"]+sorted(set(st.session_state.pdate["NCNO"].astype(str))),   key="dm_nc2")
        filt=st.session_state.pdate.copy()
        if ins_g!="All": filt=filt[filt["INSCODE"].astype(str)==ins_g]
        if nc_g !="All": filt=filt[filt["NCNO"].astype(str)==nc_g]

        st.markdown(f'<div class="info-card">📋 {len(filt)} rows selected for check</div>',unsafe_allow_html=True)

        if st.button("🔍 Run Error Check",type="primary",use_container_width=True):
            with st.spinner("Checking..."):
                em=check_errors(filt,st.session_state.staff)
            for idx in filt.index:
                if idx in st.session_state.pdate.index:
                    msgs=em.get(idx,[])
                    st.session_state.pdate.at[idx,"ERROR"]=" | ".join(msgs) if msgs else ""
            PD(); st.session_state.errors=em
            te=sum(len(v) for v in em.values())
            if te==0: st.markdown('<div class="ok-card">✅ No errors! All checks passed.</div>',unsafe_allow_html=True)
            else: st.markdown(f'<div class="err-card">🔴 {te} issues in {len(em)} rows.</div>',unsafe_allow_html=True)

        if st.session_state.errors:
            st.markdown('<div class="sub-hdr">🔴 Errors</div>',unsafe_allow_html=True)
            for idx,msgs in list(st.session_state.errors.items())[:20]:
                r=st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
                with st.expander(f"Row {idx} | {r.get('INSCODE','?')} | {r.get('SUBCODE','?')} | {r.get('DATE_FROM','?')}→{r.get('DATE_TO','?')}"):
                    for m in msgs:
                        st.markdown(f'<div class="err-card">{m}</div>',unsafe_allow_html=True)

    with dm2:
        pdv=st.session_state.pdate.copy()
        pdv["_d"]=pdv["DATE_FROM"].apply(parse_date)
        pdv=pdv.sort_values("_d",na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdv=pdv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
        show=["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"]
        show=[c for c in show if c in pdv.columns]
        st.markdown(f"**Dated Panel — {len(pdv)} rows**")
        st.dataframe(pdv[show].fillna(""),use_container_width=True,height=460)
        exp_dc=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"]
                if c in st.session_state.pdate.columns]
        st.markdown(dl_link(st.session_state.pdate[exp_dc],"panel_dated.csv","📥 Download Dated Panel CSV"),
                    unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sub-hdr">📊 Duty Count Chart</div>',unsafe_allow_html=True)
        dc_data={}
        for _,row in st.session_state.pdate.iterrows():
            for fld in ["INTID","EXTID"]:
                sid=norm_id(row.get(fld,""))
                if sid: dc_data[sid]=dc_data.get(sid,0)+1
        if dc_data:
            df_ch=pd.DataFrame(list(dc_data.items()),columns=["Staff ID","Duties"])
            df_ch["Name"]=df_ch["Staff ID"].apply(lambda s:get_name(st.session_state.staff,s))
            df_ch["Label"]=df_ch.apply(lambda r:r["Staff ID"]+(f" {r['Name']}" if r["Name"] else ""),axis=1)
            df_ch=df_ch.sort_values("Duties",ascending=False).head(25)
            st.bar_chart(df_ch.set_index("Label")["Duties"],height=280)
        else:
            st.markdown('<div class="info-card">📊 No duty data yet.</div>',unsafe_allow_html=True)

st.markdown(
    f'<div class="creator-bar">✨ {CREATOR}</div>',
    unsafe_allow_html=True)
