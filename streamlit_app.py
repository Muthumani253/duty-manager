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

st.set_page_config(page_title="🗂️ Duty Manager", page_icon="🗂️", layout="wide",
                   initial_sidebar_state="expanded")

# ══════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════
st.markdown("""
<style>
[data-testid="stSidebar"] { background: linear-gradient(180deg,#1a1a2e 0%,#16213e 60%,#0f3460 100%); }
[data-testid="stSidebar"] * { color: #e0e0e0 !important; }
.metric-card { background:linear-gradient(135deg,#667eea,#764ba2); border-radius:12px;
    padding:12px 16px; color:white; text-align:center; margin:4px; }
.metric-card .val { font-size:2rem; font-weight:700; }
.metric-card .lbl { font-size:.78rem; opacity:.85; }
.row-card { background:#1e293b; border-left:4px solid #6366f1; border-radius:8px;
    padding:10px 14px; margin:6px 0; }
.err-card { background:#2d1515; border-left:4px solid #ef4444; border-radius:8px;
    padding:8px 14px; margin:4px 0; font-size:.85rem; }
.ok-card  { background:#142514; border-left:4px solid #22c55e; border-radius:8px;
    padding:8px 14px; margin:4px 0; font-size:.85rem; }
.section-hdr { background:linear-gradient(90deg,#6366f1,#8b5cf6);
    color:white; padding:8px 18px; border-radius:8px; font-weight:700;
    font-size:1.05rem; margin:10px 0 6px; }
.creator-badge { background:#0f3460; color:#e0e0e0; border-radius:20px;
    padding:4px 14px; font-size:.78rem; display:inline-block; }
hr.thin { border:none; border-top:1px solid #334155; margin:8px 0; }
div[data-testid="stDataFrame"] { border-radius:10px; overflow:hidden; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════
def _now(): return datetime.now().isoformat(timespec="seconds")

def parse_date(s):
    if s is None: return None
    try:
        if pd.isna(s): return None
    except: pass
    if isinstance(s, (datetime, date, pd.Timestamp)):
        return s.date() if hasattr(s,"date") else None
    t = str(s).strip()
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

def rowid(df, pre="r"):
    df=df.copy()
    if "__rowid" not in df.columns:
        df["__rowid"]=[f"{pre}_{uuid.uuid4().hex}" for _ in range(len(df))]
    else:
        df["__rowid"]=df["__rowid"].astype(str)
        m=df["__rowid"].str.strip()==""
        if m.any(): df.loc[m,"__rowid"]=[f"{pre}_{uuid.uuid4().hex}" for _ in range(m.sum())]
    return df

def load_csv(path, cols):
    if os.path.exists(path):
        try:
            df=pd.read_csv(path,dtype=object).fillna("")
            for c in cols:
                if c not in df.columns: df[c]=""
            return df
        except: pass
    return pd.DataFrame(columns=cols)

def save_csv(df, path):
    try: df.to_csv(path,index=False); return True
    except Exception as e: st.error(f"Save failed: {e}"); return False

def concat_r(df, d): return pd.concat([df,pd.DataFrame([d])],ignore_index=True)

SPLIT_RE = re.compile(r"[,\uFF0C;|\-/\\_\s]+")

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
    if v is None: return []
    s=str(v).strip()
    if not s: return []
    return [p.strip() for p in SPLIT_RE.split(s) if p.strip()]

def is_busy(t):
    t2=str(t).strip().upper()
    return t2=="B" or bool(re.match(r"^B[\W_]*\d+$",t2))

def inscode_from_staffid(sid):
    """Extract INSCODE from staff ID: chars at positions 1,2,3 (0-indexed)"""
    s=str(sid).strip()
    if len(s)>=4: return s[1:4]
    return ""

def get_name(staff_df, sid):
    sid=norm_id(sid)
    if not sid: return ""
    m=staff_df["Staff ID"].astype(str).str.upper()==sid
    if m.any():
        try: return str(staff_df.loc[m,"Name of the Staff"].iloc[0])
        except: return ""
    return ""

def get_phone(staff_df, sid):
    sid=norm_id(sid)
    if not sid: return ""
    m=staff_df["Staff ID"].astype(str).str.upper()==sid
    if m.any():
        try: return str(staff_df.loc[m,"Phone"].iloc[0])
        except: return ""
    return ""

def get_subname(submap, code):
    if submap is None or submap.empty: return ""
    m=submap[submap["SUBCODE"].astype(str)==str(code).strip()]
    return m.iloc[0]["SUBNAME"] if not m.empty else ""

def remove_inscode_tokens(staff_df, inscode, d1, d2):
    s=staff_df.copy()
    for d in drange(d1,d2):
        dc=d2s(d)
        if dc not in s.columns: continue
        for i in s.index:
            cur=s.at[i,dc]
            if not cur or str(cur).strip()=="": continue
            toks=[t for t in split_toks(cur) if t!=str(inscode).strip()]
            s.at[i,dc]=",".join(toks) if toks else ""
    return s

# ══════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════
for key,path,cols,pre in [
    ("panel",   PANEL_PATH,       PANEL_COLS,       "p"),
    ("pdate",   PANEL_DATED_PATH, PANEL_DATED_COLS, "d"),
    ("staff",   STAFF_PATH,       STAFF_COLS,       "s"),
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

if "staged" not in st.session_state: st.session_state.staged={}
if "errors" not in st.session_state: st.session_state.errors={}

def P(): st.session_state.panel=rowid(st.session_state.panel,"p"); save_csv(st.session_state.panel,PANEL_PATH)
def PD(): st.session_state.pdate=rowid(st.session_state.pdate,"d"); save_csv(st.session_state.pdate,PANEL_DATED_PATH)
def S(): st.session_state.staff=rowid(st.session_state.staff,"s"); save_csv(st.session_state.staff,STAFF_PATH)
def SM(): save_csv(st.session_state.submap,SUBMAP_PATH)
def SS(): save_csv(st.session_state.ssmap,SUBJMAP_PATH)

# ══════════════════════════════════════════════
# DUTY STATS
# ══════════════════════════════════════════════
def duty_stats(staff_df):
    stats={}
    if staff_df is None or staff_df.empty: return stats
    dcols=[c for c in staff_df.columns if c!="__rowid" and isinstance(c,str)
           and len(c.split("."))==3 and all(p.isdigit() for p in c.split("."))]
    for _,row in staff_df.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        dm,cnt={},0
        for dc in dcols:
            toks=split_toks(row.get(dc,""))
            dm[dc]=toks
            cnt+=sum(1 for t in toks if not is_busy(t))
        stats[sid]={"count":cnt,"dm":dm,
            "INSTT":row.get("INSTT",""),"dep":row.get("dep code",""),
            "name":row.get("Name of the Staff",""),"desig":row.get("Designation",""),
            "phone":row.get("Phone","")}
    return stats

# ══════════════════════════════════════════════
# EXT SUGGESTIONS (no date check)
# ══════════════════════════════════════════════
def ext_suggestions_nodates(panel_row, staff_df, ssmap):
    """Return list of staff dicts eligible as external (no date check)."""
    panel_ins = str(panel_row.get("INSCODE","")).strip()
    subcode   = str(panel_row.get("SUBCODE","")).strip().upper()
    panel_dep = str(panel_row.get("NCNO","")).strip()
    stats     = duty_stats(staff_df)

    # If ssmap available, use subject mapping; else use dep code match
    if ssmap is not None and not ssmap.empty:
        mapped = ssmap[ssmap["Subject_Code"].astype(str).str.strip().str.upper()==subcode]
        mapped_ids = set(mapped["Staff_Last_Staff_ID"].apply(norm_id).unique())
    else:
        mapped_ids = None  # fallback to dep match

    results=[]
    for _,row in staff_df.iterrows():
        sid=norm_id(row.get("Staff ID"))
        if not sid: continue
        instt=str(row.get("INSTT","")).strip()
        if instt==panel_ins: continue   # same institution → skip (must be external)
        dep=str(row.get("dep code","")).strip()
        # filter: either mapped by subject or same dep code
        if mapped_ids is not None:
            if sid not in mapped_ids: continue
        else:
            if dep!=panel_dep: continue
        se=stats.get(sid,{})
        results.append({
            "sid":sid,"name":row.get("Name of the Staff",""),
            "desig":row.get("Designation",""),"instt":instt,
            "dep":dep,"phone":row.get("Phone",""),
            "count":se.get("count",0)
        })
    results.sort(key=lambda x:x["count"])
    return results

def make_lbl(s): return f"🟢 {s['sid']} — {s['name']} — {s['desig']} — {s['instt']}"

def auto_allocate(candidates, staff_df, ssmap):
    results,skipped={},{}
    for pidx,row in candidates.iterrows():
        suggs=ext_suggestions_nodates(row,staff_df,ssmap)
        if suggs:
            best=suggs[0]
            results[pidx]=make_lbl(best)
        else:
            subcode=str(row.get("SUBCODE","")).strip()
            skipped[pidx]=f"No eligible external staff for SUBCODE {subcode}"
    return results,skipped

# ══════════════════════════════════════════════
# ERROR CHECKING (for dated panel)
# ══════════════════════════════════════════════
def check_errors(panel_df, staff_df):
    """
    Returns dict {row_index: [error_strings]}
    Checks:
    1. INT/EXT institution rule
    2. Single-day clash (same staff, same date, different SUBCODE)
    3. Multi-day overlap (date range overlap same staff different row)
    """
    errs = {i: [] for i in panel_df.index}

    # Build: staff_id → [(row_idx, subcode, d1, d2, role)]
    staff_duties: dict[str, list] = {}
    for idx, row in panel_df.iterrows():
        d1=parse_date(row.get("DATE_FROM")); d2=parse_date(row.get("DATE_TO"))
        sc=str(row.get("SUBCODE","")).strip()
        ins=str(row.get("INSCODE","")).strip()
        for role, fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if is_zero(row.get(fld,"")): sid=""
            if not sid: continue
            # Rule 3: INT/EXT institution check
            staff_ins=inscode_from_staffid(sid)
            if role=="INT" and staff_ins and staff_ins!=ins:
                errs[idx].append(f"❌ INTID {sid}: home-inst {staff_ins} ≠ exam-inst {ins} (must be same)")
            if role=="EXT" and staff_ins and staff_ins==ins:
                errs[idx].append(f"❌ EXTID {sid}: home-inst {staff_ins} == exam-inst {ins} (must be different)")
            staff_duties.setdefault(sid,[]).append((idx,sc,d1,d2,role))

    # Rules 1 & 2: date clash
    for sid, duties in staff_duties.items():
        for i in range(len(duties)):
            idx_a,sc_a,d1_a,d2_a,role_a = duties[i]
            if d1_a is None or d2_a is None: continue
            for j in range(i+1,len(duties)):
                idx_b,sc_b,d1_b,d2_b,role_b = duties[j]
                if d1_b is None or d2_b is None: continue
                # check overlap
                overlap_start = max(d1_a,d1_b)
                overlap_end   = min(d2_a,d2_b)
                if overlap_start <= overlap_end:
                    if sc_a != sc_b:
                        msg=(f"⚠️ {sid} CLASH: {sc_a}({d2s(d1_a)}→{d2s(d2_a)}) "
                             f"overlaps {sc_b}({d2s(d1_b)}→{d2s(d2_b)})")
                        errs[idx_a].append(msg)
                        errs[idx_b].append(msg)

    # filter out empty
    return {k:v for k,v in errs.items() if v}

# ══════════════════════════════════════════════
# PDF GENERATION
# ══════════════════════════════════════════════
def generate_pdf_duties(panel_df, staff_df, submap):
    """Generate staff-wise duty PDF (no dates). Returns bytes."""
    buf=BytesIO()
    if not RPDF:
        buf.write(b"reportlab not installed. pip install reportlab")
        return buf.getvalue()

    doc=SimpleDocTemplate(buf,pagesize=A4,
        leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=1.5*cm,bottomMargin=1.5*cm)
    styles=getSampleStyleSheet()
    H1=ParagraphStyle("H1",fontSize=13,fontName="Helvetica-Bold",
                       spaceAfter=4,alignment=TA_CENTER)
    H2=ParagraphStyle("H2",fontSize=10,fontName="Helvetica-Bold",spaceAfter=3)
    SM=ParagraphStyle("SM",fontSize=8,fontName="Helvetica",spaceAfter=2)
    FOOT=ParagraphStyle("FOOT",fontSize=7,fontName="Helvetica",
                        textColor=RC.grey,alignment=TA_CENTER)

    story=[]
    # Collect all staff who appear in panel
    all_staff_ids=set()
    for _,row in panel_df.iterrows():
        for fld in ["INTID","EXTID"]:
            sid=norm_id(row.get(fld,""))
            if sid: all_staff_ids.add(sid)

    # Group duties per staff
    staff_duties: dict[str,list] = {}
    for _,row in panel_df.iterrows():
        sc=str(row.get("SUBCODE","")).strip()
        sn=get_subname(submap,sc)
        ins=str(row.get("INSCODE","")).strip()
        for role,fld in [("INT","INTID"),("EXT","EXTID")]:
            sid=norm_id(row.get(fld,""))
            if not sid: continue
            staff_duties.setdefault(sid,[]).append({
                "ins":ins,"sc":sc,"sn":sn,"role":role,
                "counterpart_role":"EXT" if role=="INT" else "INT",
                "counterpart_id":norm_id(row.get("EXTID" if role=="INT" else "INTID","")),
            })

    # Sort by INSTT then name
    sorted_staff=sorted(all_staff_ids,
        key=lambda sid:(str(staff_df[staff_df["Staff ID"].astype(str).str.upper()==sid]["INSTT"].iloc[0])
                        if not staff_df[staff_df["Staff ID"].astype(str).str.upper()==sid].empty else "",
                        get_name(staff_df,sid)))

    for sid in sorted_staff:
        duties=staff_duties.get(sid,[])
        if not duties: continue
        name=get_name(staff_df,sid)
        phone=get_phone(staff_df,sid)
        m=staff_df[staff_df["Staff ID"].astype(str).str.upper()==sid]
        desig=str(m.iloc[0]["Designation"]) if not m.empty else ""
        dept =str(m.iloc[0]["Department"])  if not m.empty else ""
        instt=str(m.iloc[0]["INSTT"])       if not m.empty else ""

        story.append(Paragraph("🗂️ PRACTICAL EXAM DUTY ORDER", H1))
        story.append(Paragraph(CREATOR, FOOT))
        story.append(Spacer(1,0.3*cm))

        # Staff header table
        hdr_data=[
            ["Staff ID", sid,           "Name", name],
            ["Institution", instt,      "Phone", phone],
            ["Department", dept,        "Designation", desig],
        ]
        ht=Table(hdr_data,colWidths=[2.8*cm,4.5*cm,2.8*cm,6.5*cm])
        ht.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1),RC.HexColor("#1e293b")),
            ("TEXTCOLOR",(0,0),(-1,-1),RC.white),
            ("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
            ("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),8),
            ("GRID",(0,0),(-1,-1),0.5,RC.HexColor("#334155")),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[RC.HexColor("#1e293b"),RC.HexColor("#0f172a")]),
            ("PADDING",(0,0),(-1,-1),5),
        ]))
        story.append(ht)
        story.append(Spacer(1,0.4*cm))

        # Duty table
        tbl_hdr=["S.No","Duty\nINSCODE","Sub\nCode","Subject Name",
                 "Role","Partner\nID","Partner Name","Partner\nPhone",
                 "Date From","Date To"]
        tbl_rows=[tbl_hdr]
        for sno,d in enumerate(duties,1):
            pid=d["counterpart_id"]
            pname=get_name(staff_df,pid) if pid else ""
            pphone=get_phone(staff_df,pid) if pid else ""
            tbl_rows.append([
                str(sno), d["ins"], d["sc"], d["sn"] or d["sc"],
                d["role"], pid or "-", pname or "-", pphone or "-",
                "", ""
            ])
        cw=[1*cm,2*cm,2.2*cm,4.5*cm,1.3*cm,2.2*cm,3.5*cm,2.2*cm,2*cm,2*cm]
        dt=Table(tbl_rows,colWidths=cw,repeatRows=1)
        dt.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),RC.HexColor("#6366f1")),
            ("TEXTCOLOR",(0,0),(-1,0),RC.white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),7.5),
            ("ALIGN",(0,0),(-1,-1),"CENTER"),
            ("ALIGN",(3,1),(3,-1),"LEFT"),
            ("ALIGN",(6,1),(6,-1),"LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[RC.HexColor("#f8fafc"),RC.HexColor("#e2e8f0")]),
            ("GRID",(0,0),(-1,-1),0.4,RC.HexColor("#94a3b8")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ("PADDING",(0,0),(-1,-1),4),
        ]))
        story.append(dt)
        story.append(Spacer(1,0.3*cm))
        story.append(Paragraph("Date From / Date To to be filled by Flying Squad at time of duty.", FOOT))
        story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()

def df_to_download_link(df, filename, label):
    csv=df.to_csv(index=False).encode()
    b64=base64.b64encode(csv).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="{filename}" style="background:#6366f1;color:white;padding:6px 16px;border-radius:6px;text-decoration:none;font-size:.85rem;">{label}</a>'

def pdf_download_link(pdf_bytes, filename, label):
    b64=base64.b64encode(pdf_bytes).decode()
    return f'<a href="data:application/pdf;base64,{b64}" download="{filename}" style="background:#ef4444;color:white;padding:6px 16px;border-radius:6px;text-decoration:none;font-size:.85rem;">{label}</a>'

def sample_ssmap_csv():
    sample=pd.DataFrame([{
        "Staff_Last_Staff_ID":"X123EEE1","Staff_Name":"KUMAR S",
        "Department":"Electrical & Electronics Engineering","Department_Code":"1030",
        "Subject_Type":"Core","Subject_Code":"P3401","Subject_Name":"Basic Electrical Lab",
        "Subject_Remarks":""}])
    return sample.to_csv(index=False).encode()

# ══════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════
with st.sidebar:
    st.markdown('<div style="text-align:center;padding:8px 0"><span style="font-size:2rem">🗂️</span><br><b style="font-size:1.1rem">Duty Manager</b></div>', unsafe_allow_html=True)
    st.markdown(f'<div style="text-align:center"><span class="creator-badge">{CREATOR}</span></div>', unsafe_allow_html=True)
    st.markdown("---")
    page=st.radio("",["📥 Upload Centre","🎯 EXT Allocate","▶️ Duty Marking"],
                  label_visibility="collapsed")
    st.markdown("---")
    pn=len(st.session_state.panel); pdn=len(st.session_state.pdate)
    ef=st.session_state.panel["EXTID"].apply(lambda v:norm_id(v)!="").sum() if pn else 0
    ep=pn-ef; sc=len(st.session_state.staff); sm2c=len(st.session_state.ssmap)
    for lbl,val,color in [("📋 Panel (no-date)",pn,"#6366f1"),("✅ EXTID Filled",ef,"#22c55e"),
                          ("⏳ EXTID Pending",ep,"#f59e0b"),("🗓️ Panel (dated)",pdn,"#8b5cf6"),
                          ("🧑‍🏫 Staff",sc,"#3b82f6"),("📘 SubjectMap",sm2c,"#ec4899")]:
        st.markdown(f'<div class="metric-card" style="background:{color}66;border-left:3px solid {color}">'
                    f'<div class="val">{val}</div><div class="lbl">{lbl}</div></div>',
                    unsafe_allow_html=True)
    st.markdown("---")
    st.caption(CREATOR)

# ══════════════════════════════════════════════
# PAGE 1 — UPLOAD CENTRE
# ══════════════════════════════════════════════
if page=="📥 Upload Centre":
    st.markdown('<div class="section-hdr">📥 Upload Centre</div>', unsafe_allow_html=True)
    t1,t2,t3=st.tabs(["📋 Panel (No Dates)","🧑‍🏫 Staff Details","📘 Subject-Staff Mapping"])

    # ── TAB 1: Panel without dates ──────────────────────────
    with t1:
        c1,c2=st.columns([1,1])
        with c1:
            st.markdown("**Required Headers:**")
            st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID",language="")
            st.caption("💡 Upload panel files institution-wise. INTID should be filled; EXTID will be allocated on EXT Allocate page.")
            uf=st.file_uploader("📂 Upload Panel CSV/XLSX",type=["csv","xlsx"],key="p_up")
            cl=st.checkbox("Clear ALL existing panel data before upload",key="p_clear")
            if uf:
                try:
                    tmp=(pd.read_csv(uf,dtype=object) if uf.name.lower().endswith(".csv")
                         else pd.read_excel(uf,dtype=object,sheet_name=0)).fillna("")
                    req=["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                    miss=[c for c in req if c not in tmp.columns]
                    if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                    else:
                        tmp=tmp[req].copy(); tmp["ERROR"]=""
                        tmp=rowid(tmp,"p")
                        bk=st.session_state.panel.copy()
                        if cl:
                            st.session_state.panel=rowid(tmp.reset_index(drop=True),"p")
                        else:
                            ins_up=[str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                            bk=bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                            bk=pd.concat([bk,tmp],ignore_index=True)
                            st.session_state.panel=rowid(bk.reset_index(drop=True),"p")
                        P(); st.success(f"✅ Panel uploaded ({len(tmp)} rows)")
                except Exception as e: st.error(f"❌ {e}")
        with c2:
            pv=st.session_state.panel.copy()
            if not st.session_state.submap.empty:
                pv=pv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
            show_cols=[c for c in ["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID","EXTID","ERROR"]
                       if c in pv.columns]
            st.markdown(f"**Panel Preview — {len(pv)} rows**")
            st.dataframe(pv[show_cols].fillna(""), use_container_width=True, height=380)
            if st.button("💾 Save panel edits",key="p_save"):
                P(); st.success("✅ Saved")

        st.markdown("**📂 SUBCODE → SUBNAME mapping** *(optional)*")
        sf2=st.file_uploader("Upload SUBCODE-SUBNAME CSV",type=["csv","xlsx"],key="sub_up2")
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

    # ── TAB 2: Staff ────────────────────────────────────────
    with t2:
        ca,cb=st.columns([1,1])
        with ca:
            st.markdown("**Required Headers:**")
            st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation  Phone",language="")
            st.caption("💡 Phone number column is new — fill mobile numbers for printing on duty sheets.")
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
                        S(); st.success(f"✅ Staff loaded ({len(tmp)} rows)")
                except Exception as e: st.error(f"❌ {e}")
            # Sample download
            sample_staff=pd.DataFrame([{
                "Staff ID":"X123EEE1","INSTT":"123",
                "Name of the Staff":"KUMAR S","Department":"Electrical & Electronics Engg",
                "dep code":"1030","Designation":"Lecturer","Phone":"9876543210"}])
            st.markdown(df_to_download_link(sample_staff,"sample_staff.csv","📥 Download Sample Staff CSV"),
                        unsafe_allow_html=True)
        with cb:
            sv=st.session_state.staff.copy()
            fi,fd=st.columns(2)
            fi_sel=fi.selectbox("Filter INSTT",["All"]+sorted(set(sv["INSTT"].astype(str))),key="sf_ins")
            fd_sel=fd.selectbox("Filter Dept",["All"]+sorted(set(sv["Department"].astype(str))),key="sf_dep")
            if fi_sel!="All": sv=sv[sv["INSTT"].astype(str)==fi_sel]
            if fd_sel!="All": sv=sv[sv["Department"].astype(str)==fd_sel]
            disp_cols=[c for c in ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","Phone"]
                       if c in sv.columns]
            st.markdown(f"**Staff Preview — {len(sv)} rows**")
            es=st.data_editor(sv[disp_cols],use_container_width=True,height=360,
                              key="staff_ed",num_rows="dynamic")
            if st.button("💾 Save staff edits",key="s_save"):
                try:
                    bk=st.session_state.staff.copy().set_index("__rowid",drop=False)
                    ed=es.copy()
                    if "__rowid" not in ed.columns: ed["__rowid"]=""
                    ed=rowid(ed,"s").set_index("__rowid",drop=False)
                    for rid in bk.index.intersection(ed.index):
                        for c in ed.columns: bk.at[rid,c]=ed.at[rid,c]
                    new=ed.index.difference(bk.index)
                    if len(new):
                        bk=pd.concat([bk.reset_index(drop=True),ed.loc[new].reset_index(drop=True)],ignore_index=True)
                    st.session_state.staff=rowid(bk.reset_index(drop=True),"s")
                    S(); st.success("✅ Staff saved")
                except Exception as e: st.error(f"❌ {e}")

    # ── TAB 3: Subject-Staff Mapping ────────────────────────
    with t3:
        cx,cy=st.columns([1,1])
        with cx:
            st.markdown("**Required Headers:**")
            st.code("\n".join(SUBJMAP_COLS),language="")
            st.markdown(df_to_download_link(pd.DataFrame(columns=SUBJMAP_COLS),"ssmap_template.csv",
                        "📥 Download Empty Template CSV"),unsafe_allow_html=True)
            st.markdown("&nbsp;",unsafe_allow_html=True)
            st.markdown(df_to_download_link(
                pd.DataFrame([{"Staff_Last_Staff_ID":"X123EEE1","Staff_Name":"KUMAR S",
                               "Department":"EEE","Department_Code":"1030",
                               "Subject_Type":"Core","Subject_Code":"P3401",
                               "Subject_Name":"Basic Electrical Lab","Subject_Remarks":""}]),
                "ssmap_sample.csv","📥 Download Sample CSV"),unsafe_allow_html=True)
            st.markdown("")
            ussm=st.file_uploader("📂 Upload Subject-Staff Mapping CSV/XLSX",type=["csv","xlsx"],key="ssm_up")
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
                        SS(); st.success(f"✅ Mapping loaded ({len(tmp)} rows)")
                except Exception as e: st.error(f"❌ {e}")
        with cy:
            ssv=st.session_state.ssmap.copy()
            sf1,sf2=st.columns(2)
            dep_f=sf1.selectbox("Filter Dept",["All"]+sorted(set(ssv["Department"].astype(str))),key="sm_dep")
            sub_f=sf2.text_input("Filter Subject Code","",key="sm_sub")
            if dep_f!="All": ssv=ssv[ssv["Department"]==dep_f]
            if sub_f.strip(): ssv=ssv[ssv["Subject_Code"].str.contains(sub_f.strip().upper())]
            st.markdown(f"**Mapping Preview — {len(ssv)} rows**")
            st.dataframe(ssv,use_container_width=True,height=400)

# ══════════════════════════════════════════════
# PAGE 2 — EXT ALLOCATE (no dates)
# ══════════════════════════════════════════════
elif page=="🎯 EXT Allocate":
    st.markdown('<div class="section-hdr">🎯 EXT Allocate — Assign External Examiners (No Date Check)</div>',
                unsafe_allow_html=True)

    # info about logic
    with st.expander("ℹ️ Logic Used in This Page"):
        st.markdown("""
| # | Rule | Detail |
|---|------|--------|
| 1 | **Subject Matching** | Staff must be mapped to the panel SUBCODE in Subject-Staff Mapping |
| 2 | **External Rule** | Staff `INSTT` must be **different** from panel `INSCODE` |
| 3 | **Least Duty First** | Among eligible staff, the one with **minimum existing duty count** is preferred |
| 4 | **Fallback** | If no subject mapping loaded, falls back to matching `dep code` == panel `NCNO` |
| ⚠️ | **No Date Check** | Date clash is NOT checked here — check on Duty Marking page after date assignment |
        """)

    panel=st.session_state.panel.copy()
    staff=st.session_state.staff.copy()
    ssmap=st.session_state.ssmap.copy()
    submap=st.session_state.submap.copy()

    def needs_ext2(r):
        intid=str(r.get("INTID","")).strip()
        extraw=r.get("EXTID","")
        return intid!="" and (str(extraw).strip()=="" or is_zero(extraw))

    candidates=panel[panel.apply(needs_ext2,axis=1)].copy()

    # Filters
    fc1,fc2=st.columns(2)
    ins_f=fc1.selectbox("🏫 Filter INSCODE",["All"]+sorted(set(panel["INSCODE"].astype(str))),key="ea_ins")
    nc_f =fc2.selectbox("🏭 Filter NCNO",   ["All"]+sorted(set(panel["NCNO"].astype(str))),   key="ea_nc")
    if ins_f!="All": candidates=candidates[candidates["INSCODE"].astype(str)==ins_f]
    if nc_f !="All": candidates=candidates[candidates["NCNO"].astype(str)==nc_f]

    # metrics
    m1,m2,m3,m4=st.columns(4)
    m1.metric("📋 Pending EXTID",len(candidates))
    m2.metric("🧑‍🏫 Staff Loaded",len(staff))
    m3.metric("📘 SubjectMap",len(ssmap))
    m4.metric("🔖 Staged",len(st.session_state.staged))

    st.markdown("---")

    # Auto-Allocate
    st.markdown('<div class="section-hdr">🤖 Auto-Allocate</div>', unsafe_allow_html=True)
    auto_col,_ = st.columns([3,1])
    with auto_col:
        st.caption("Matches SUBCODE → Subject-Staff Map → Different INSTT → Least Duty Count → Stages automatically")
    if st.button("🤖 Auto-Allocate ALL Visible Rows", type="primary", use_container_width=False):
        if staff.empty:
            st.error("❌ Upload staff data first!")
        else:
            res,skip=auto_allocate(candidates,staff,ssmap if not ssmap.empty else None)
            staged=st.session_state.staged
            for k,v in res.items(): staged[str(k)]=v
            st.session_state.staged=staged
            st.success(f"✅ Auto-staged {len(res)} rows.")
            if skip:
                with st.expander(f"⚠️ {len(skip)} rows skipped"):
                    st.dataframe(pd.DataFrame([{"idx":k,"reason":v} for k,v in skip.items()]),
                                 use_container_width=True)
    st.markdown("---")

    # Per-row allocation
    st.markdown('<div class="section-hdr">📝 Per-Row Manual / Suggestion</div>', unsafe_allow_html=True)
    if candidates.empty:
        st.markdown('<div class="ok-card">🎉 No pending EXTID rows for current filters!</div>',
                    unsafe_allow_html=True)
    else:
        for _,row in candidates.reset_index().iterrows():
            pidx=int(row["index"])
            sc=str(row.get("SUBCODE","")).strip()
            sn=get_subname(submap,sc)
            ins=str(row.get("INSCODE","")).strip()
            nc=str(row.get("NCNO","")).strip()
            intid=str(row.get("INTID","")).strip()
            intname=get_name(staff,intid)
            staged_v=st.session_state.staged.get(str(pidx),"")

            suggs=ext_suggestions_nodates(row,staff,ssmap if not ssmap.empty else None)
            sugg_labels=["— Select —"]+[make_lbl(s) for s in suggs]

            with st.container():
                h1,h2,h3=st.columns([3,3,2])
                h1.markdown(f"**🏫 {ins}** | 🏭 {nc} | 📚 {sc} {('— '+sn) if sn else ''}")
                h2.markdown(f"👥 {row.get('NOC','')} students | INT: `{intid}` {intname}")
                sv_display=staged_v[:40]+"..." if len(staged_v)>40 else staged_v
                h3.markdown(f"{'✅ '+sv_display if staged_v else '⬜ Not staged'}")

                r1,r2,r3=st.columns([4,3,1])
                di=sugg_labels.index(staged_v) if staged_v in sugg_labels else 0
                sel=r1.selectbox("💡 Suggestions",sugg_labels,index=di,key=f"sel_{pidx}_{staged_v[:10]}")
                man=r2.text_input("✏️ Manual Staff ID","",key=f"man_{pidx}",placeholder="Type Staff ID")
                if sel and sel!="— Select —":
                    st.session_state.staged[str(pidx)]=sel
                if man.strip():
                    st.session_state.staged[str(pidx)]=man.strip()

                if r3.button("▶",key=f"app_{pidx}",help="Apply now"):
                    chosen=staged_v or (sel if sel!="— Select —" else "") or man.strip()
                    if not chosen:
                        st.warning("⚠️ Select or type a staff ID")
                    else:
                        lbl_c=str(chosen).replace("🟢 ","").split("—")
                        sid_c=norm_id(lbl_c[0].strip()) if lbl_c else ""
                        if sid_c:
                            st.session_state.panel.at[pidx,"EXTID"]=sid_c
                            P()
                            st.session_state.staged.pop(str(pidx),None)
                            st.success(f"✅ EXTID {sid_c} set for row {pidx}")
                        else: st.error("❌ Invalid staff ID")
                st.markdown('<hr class="thin">',unsafe_allow_html=True)

    st.markdown("---")

    # Apply Staged
    staged_map=st.session_state.staged
    if staged_map:
        st.markdown('<div class="section-hdr">🚀 Apply Staged to ALL</div>',unsafe_allow_html=True)
        with st.expander(f"👁️ Preview {len(staged_map)} staged"):
            rows=[]
            for k,v in list(staged_map.items())[:25]:
                try:
                    pidx_i=int(k)
                    r=st.session_state.panel.loc[pidx_i] if pidx_i in st.session_state.panel.index else {}
                    rows.append({"Idx":k,"INSCODE":r.get("INSCODE","?"),"SUBCODE":r.get("SUBCODE","?"),"Staged":v})
                except: rows.append({"Idx":k,"Staged":v})
            st.dataframe(pd.DataFrame(rows),use_container_width=True)
        a1,a2=st.columns(2)
        if a1.button("✅ Apply ALL Staged",type="primary",use_container_width=True):
            ok_c,fail_c=0,[]
            for k,v in list(staged_map.items()):
                try: pidx_i=int(k)
                except: fail_c.append(k); continue
                if pidx_i not in st.session_state.panel.index:
                    fail_c.append(k); continue
                lbl_c=str(v).replace("🟢 ","").split("—")
                sid_c=norm_id(lbl_c[0].strip()) if lbl_c else ""
                if sid_c:
                    st.session_state.panel.at[pidx_i,"EXTID"]=sid_c
                    st.session_state.staged.pop(k,None); ok_c+=1
                else: fail_c.append(k)
            P()
            st.success(f"✅ Applied {ok_c} | ❌ Failed {len(fail_c)}")
        if a2.button("🗑️ Clear Staged",use_container_width=True):
            st.session_state.staged={}; st.success("✅ Cleared")

    st.markdown("---")

    # Downloads
    st.markdown('<div class="section-hdr">📥 Downloads</div>',unsafe_allow_html=True)
    all_panel=st.session_state.panel.copy()
    if not all_panel.empty:
        inscodes=sorted(set(all_panel["INSCODE"].astype(str)))
        dc1,dc2,dc3=st.columns(3)
        with dc1:
            st.markdown("**CSV — Institution Wise**")
            for ins in inscodes:
                df_ins=all_panel[all_panel["INSCODE"].astype(str)==ins].copy()
                exp_cols=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                          if c in df_ins.columns]
                st.markdown(df_to_download_link(df_ins[exp_cols],f"panel_{ins}.csv",
                            f"📥 CSV — INSCODE {ins}"),unsafe_allow_html=True)
                st.markdown("",unsafe_allow_html=True)
        with dc2:
            st.markdown("**CSV — Full Panel**")
            exp_cols=[c for c in ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID"]
                      if c in all_panel.columns]
            st.markdown(df_to_download_link(all_panel[exp_cols],"panel_all.csv","📥 Full Panel CSV"),
                        unsafe_allow_html=True)
        with dc3:
            st.markdown("**PDF — Duty Sheets (Per Staff)**")
            if RPDF:
                if st.button("🖨️ Generate PDF Duty Sheets"):
                    with st.spinner("Generating PDF..."):
                        pdf_b=generate_pdf_duties(all_panel,staff,submap)
                    st.markdown(pdf_download_link(pdf_b,"duty_sheets.pdf","📄 Download PDF"),
                                unsafe_allow_html=True)
            else:
                st.warning("Install reportlab: `pip install reportlab`")

# ══════════════════════════════════════════════
# PAGE 3 — DUTY MARKING
# ══════════════════════════════════════════════
elif page=="▶️ Duty Marking":
    st.markdown('<div class="section-hdr">▶️ Duty Marking — Upload Dated Panel, Generate & Validate</div>',
                unsafe_allow_html=True)

    with st.expander("ℹ️ Error-Check Logic Used"):
        st.markdown("""
| # | Check | Rule |
|---|-------|------|
| 🔴 1 | **INT/EXT Institution Rule** | `INTID` home-INSCODE (chars 2-4 of Staff ID) must **equal** panel INSCODE. `EXTID` must **differ**. |
| 🔴 2 | **Single-Day Clash** | Same staff cannot have duties on the same date under **different SUBCODE** panels. Same SUBCODE = allowed (multiple batches). |
| 🔴 3 | **Multi-Day Overlap** | If a staff's duty spans DATE_FROM→DATE_TO, any other duty that overlaps any date in that range = **CLASH**. |
        """)

    dm1,dm2=st.columns([1,1])

    # Upload dated panel
    with dm1:
        st.markdown("**📂 Upload Dated Panel CSV/XLSX**")
        st.code("INSCODE NCNO SUBCODE REGL NOC NOB INTID EXTID DATE_FROM DATE_TO",language="")
        udp=st.file_uploader("Upload Panel with Dates",type=["csv","xlsx"],key="dp_up")
        cl2=st.checkbox("Clear existing dated panel",key="dp_clear")
        if udp:
            try:
                tmp=(pd.read_csv(udp,dtype=object) if udp.name.lower().endswith(".csv")
                     else pd.read_excel(udp,dtype=object,sheet_name=0)).fillna("")
                req=["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
                miss=[c for c in req if c not in tmp.columns]
                if miss: st.error(f"❌ Missing: {', '.join(miss)}")
                else:
                    tmp=tmp[req].copy(); tmp["ERROR"]=""
                    tmp=rowid(tmp,"d")
                    if cl2:
                        st.session_state.pdate=rowid(tmp.reset_index(drop=True),"d")
                    else:
                        ins_up=[str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                        bk=st.session_state.pdate.copy()
                        bk=bk[~bk["INSCODE"].astype(str).str.strip().isin(ins_up)]
                        bk=pd.concat([bk,tmp],ignore_index=True)
                        st.session_state.pdate=rowid(bk.reset_index(drop=True),"d")
                    PD(); st.success(f"✅ Dated panel loaded ({len(tmp)} rows)")
            except Exception as e: st.error(f"❌ {e}")

    with dm2:
        pdv=st.session_state.pdate.copy()
        pdv["_d"]=pdv["DATE_FROM"].apply(parse_date)
        pdv=pdv.sort_values("_d",na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdv=pdv.merge(st.session_state.submap[["SUBCODE","SUBNAME"]],how="left",on="SUBCODE")
        show=["INSCODE","NCNO","SUBCODE","SUBNAME","NOC","INTID","EXTID","DATE_FROM","DATE_TO","ERROR"]
        show=[c for c in show if c in pdv.columns]
        st.markdown(f"**Dated Panel — {len(pdv)} rows**")

        def style_error(v):
            if v and str(v).strip(): return "background-color:#4a1515;color:#fca5a5"
            return ""

        styled=pdv[show].fillna("")
        st.dataframe(styled, use_container_width=True, height=320)

    st.markdown("---")

    # Filters + Generate
    gc1,gc2,gc3=st.columns([2,2,2])
    ins_g=gc1.selectbox("INSCODE",["All"]+sorted(set(st.session_state.pdate["INSCODE"].astype(str))),key="dm_ins")
    nc_g =gc2.selectbox("NCNO",   ["All"]+sorted(set(st.session_state.pdate["NCNO"].astype(str))),   key="dm_nc")

    filt=st.session_state.pdate.copy()
    if ins_g!="All": filt=filt[filt["INSCODE"].astype(str)==ins_g]
    if nc_g !="All": filt=filt[filt["NCNO"].astype(str)==nc_g]

    with gc3:
        st.markdown("<br>",unsafe_allow_html=True)
        run_check=st.button("🔍 Run Error Check",type="primary",use_container_width=True)

    if run_check:
        if st.session_state.pdate.empty:
            st.error("❌ Upload dated panel first!")
        else:
            with st.spinner("Checking for errors..."):
                err_map=check_errors(filt,st.session_state.staff)
            # write errors back to pdate
            for idx in filt.index:
                if idx in st.session_state.pdate.index:
                    msgs=err_map.get(idx,[])
                    st.session_state.pdate.at[idx,"ERROR"]=" | ".join(msgs) if msgs else ""
            PD()
            total_errs=sum(len(v) for v in err_map.values())
            if total_errs==0:
                st.markdown('<div class="ok-card">✅ No errors found! All clash checks passed.</div>',
                            unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="err-card">🔴 Found {total_errs} issues in {len(err_map)} rows — see details below.</div>',
                            unsafe_allow_html=True)
            st.session_state.errors=err_map

    st.markdown("---")

    # Error display
    if st.session_state.errors:
        st.markdown('<div class="section-hdr">🔴 Error Report</div>',unsafe_allow_html=True)
        for idx,msgs in st.session_state.errors.items():
            r=st.session_state.pdate.loc[idx] if idx in st.session_state.pdate.index else {}
            ins_r=r.get("INSCODE","?"); sc_r=r.get("SUBCODE","?")
            d1_r=r.get("DATE_FROM","?"); d2_r=r.get("DATE_TO","?")
            with st.expander(f"Row {idx} | 🏫 {ins_r} | 📚 {sc_r} | 📅 {d1_r}→{d2_r} — {len(msgs)} issue(s)"):
                for m in msgs:
                    st.markdown(f'<div class="err-card">{m}</div>',unsafe_allow_html=True)

    st.markdown("---")

    # Duty Chart
    st.markdown('<div class="section-hdr">📊 Duty Count Overview</div>',unsafe_allow_html=True)
    if not st.session_state.pdate.empty:
        dc_data={}
        for _,row in st.session_state.pdate.iterrows():
            for fld in ["INTID","EXTID"]:
                sid=norm_id(row.get(fld,""))
                if sid: dc_data[sid]=dc_data.get(sid,0)+1
        if dc_data:
            df_chart=pd.DataFrame(list(dc_data.items()),columns=["Staff ID","Duties"])
            df_chart["Name"]=df_chart["Staff ID"].apply(lambda s:get_name(st.session_state.staff,s))
            df_chart["Label"]=df_chart["Staff ID"]+" "+df_chart["Name"]
            df_chart=df_chart.sort_values("Duties",ascending=False).head(30)
            st.bar_chart(df_chart.set_index("Label")["Duties"])

    st.markdown("---")
    st.markdown(f'<div style="text-align:center;padding:10px"><span class="creator-badge">✨ {CREATOR}</span></div>',
                unsafe_allow_html=True)
