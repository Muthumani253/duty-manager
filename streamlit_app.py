#!/usr/bin/env python3
# streamlit_app.py
"""
Duty Manager — Full Application
- Panel authoritative; live Duty Mark view
- EXTID Auto-Allocation via Staff-Subject Mapping
- Advanced UI with emojis and clean layout
Created by MUTHUMANI S, LECTURER-EEE, GPT KARUR
"""
from __future__ import annotations
import os, uuid, traceback
from datetime import datetime, timedelta, date
import re
import streamlit as st
import pandas as pd

# ═══════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════
DATA_DIR        = "data"
PANEL_PATH      = os.path.join(DATA_DIR, "panel.csv")
STAFF_PATH      = os.path.join(DATA_DIR, "staff.csv")
SUBMAP_PATH     = os.path.join(DATA_DIR, "submap.csv")
BUSY_PATH       = os.path.join(DATA_DIR, "busy.csv")
SUBJMAP_PATH    = os.path.join(DATA_DIR, "subjmap.csv")   # NEW: staff-subject mapping
EXPORT_MONTH_TAG = "oct2025"

os.makedirs(DATA_DIR, exist_ok=True)
st.set_page_config(page_title="🗂️ Duty Manager", layout="wide", initial_sidebar_state="expanded")

# ═══════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════
def _now():
    return datetime.now().isoformat(timespec="seconds")

def parse_date_flexible(s):
    if s is None: return None
    try:
        if pd.isna(s): return None
    except Exception: pass
    if isinstance(s, (datetime, date, pd.Timestamp)):
        try: return s.date() if hasattr(s, "date") else None
        except Exception: pass
    s_str = str(s).strip()
    if not s_str: return None
    for f in ["%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try: return datetime.strptime(s_str, f).date()
        except Exception: pass
    try: return pd.to_datetime(s_str, dayfirst=True).date()
    except Exception: return None

def date_to_str(d):
    if d is None: return ""
    if isinstance(d, (datetime, pd.Timestamp)): d = d.date()
    return d.strftime("%d.%m.%Y")

def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def ensure_rowid(df, prefix="r"):
    df = df.copy()
    if "__rowid" not in df.columns:
        df["__rowid"] = [f"{prefix}_{uuid.uuid4().hex}" for _ in range(len(df))]
    else:
        df["__rowid"] = df["__rowid"].astype(str)
        missing = df["__rowid"].str.strip() == ""
        if missing.any():
            df.loc[missing, "__rowid"] = [f"{prefix}_{uuid.uuid4().hex}" for _ in range(missing.sum())]
    return df

def load_or_empty(path, columns):
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, dtype=object).fillna("")
            return df
        except Exception:
            return pd.DataFrame(columns=columns)
    return pd.DataFrame(columns=columns)

def save_csv(df, path):
    try:
        df.to_csv(path, index=False)
        return True
    except Exception as e:
        st.error(f"Failed to write {path}: {e}")
        return False

def concat_row(df, rowdict):
    return pd.concat([df, pd.DataFrame([rowdict])], ignore_index=True)

# ═══════════════════════════════════════════════
# STAFF ID NORMALIZATION
# ═══════════════════════════════════════════════
def normalize_staff_id(v) -> str:
    if v is None: return ""
    try:
        if isinstance(v, float) and (v != v): return ""
    except Exception: pass
    s = str(v).strip()
    if s == "" or s in ("0","0.0","0.00"): return ""
    if re.fullmatch(r"-?\d+\.\d+", s):
        try:
            fv = float(s)
            if abs(fv - int(fv)) < 1e-9: s = str(int(fv))
        except Exception: pass
    return "" if s == "0" else s.upper()

def is_zero_like(v) -> bool:
    return str(v).strip() in ("0","0.0","0.00") if v is not None else False

def get_staff_name_by_id(staff_df: pd.DataFrame, staff_id) -> str:
    sid = normalize_staff_id(staff_id)
    if not sid: return ""
    try:
        mask = staff_df["Staff ID"].astype(str).str.upper() == sid
        if mask.any(): return str(staff_df.loc[mask, "Name of the Staff"].iloc[0])
    except Exception: pass
    return ""

SPLIT_RE = re.compile(r"[,\uFF0C\u3001;|\-/\\_\s]+")

def split_tokens(cell_value):
    if cell_value is None: return []
    try:
        if isinstance(cell_value, float) and (cell_value != cell_value): return []
    except Exception: pass
    s = str(cell_value).strip()
    if s == "": return []
    parts = [p.strip() for p in SPLIT_RE.split(s) if p and p.strip()]
    new = []
    for p in parts:
        if re.fullmatch(r"\d{6,}", p) and len(p) % 3 == 0:
            for i in range(0, len(p), 3): new.append(p[i:i+3])
        else:
            if re.fullmatch(r"-?\d+\.\d+", p):
                fv = float(p)
                if abs(fv - int(fv)) < 1e-9:
                    new.append(str(int(fv))); continue
            new.append(p)
    return new

def is_busy_token(tok):
    if not tok: return False
    t = str(tok).strip().upper()
    return t == "B" or bool(re.match(r"^B[\W_]*\d+$", t))

# ═══════════════════════════════════════════════
# INSCODE UTILITIES
# ═══════════════════════════════════════════════
def remove_inscode_from_staff_cells(staff_df, inscode, dfrom, dto):
    if not inscode: return staff_df
    staff = staff_df.copy()
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff.columns: continue
        for ridx in staff.index:
            cur = staff.at[ridx, dc]
            if cur is None or str(cur).strip() == "": continue
            toks = [t for t in split_tokens(cur) if t != str(inscode).strip()]
            staff.at[ridx, dc] = ",".join(toks) if toks else ""
    return staff

def clear_all_inscode_tokens_keep_busy(staff_df):
    staff = staff_df.copy()
    date_cols = [c for c in staff.columns if c != "__rowid" and isinstance(c, str)
                 and len(c.split(".")) == 3 and all(p.isdigit() for p in c.split("."))]
    for dc in date_cols:
        for ridx in staff.index:
            cur = staff.at[ridx, dc]
            if cur is None or str(cur).strip() == "": continue
            kept = [t for t in split_tokens(cur) if is_busy_token(t)]
            staff.at[ridx, dc] = ",".join(kept) if kept else ""
    return staff

# ═══════════════════════════════════════════════
# DATA & SESSION STATE
# ═══════════════════════════════════════════════
PANEL_COLS     = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_CORE_COLS = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","__rowid"]
BUSY_COLS      = ["Staff ID","DATE_FROM","DATE_TO","NOTE","__rowid"]
SUBJMAP_COLS   = ["Staff_Last_Staff_ID","Staff_Name","Department","Department_Code",
                  "Subject_Type","Subject_Code","Subject_Name","Subject_Remarks"]

for key, path, cols, prefix in [
    ("panel_df",        PANEL_PATH,  PANEL_COLS,      "p"),
    ("staff_df",        STAFF_PATH,  STAFF_CORE_COLS, "s"),
    ("busy_df",         BUSY_PATH,   BUSY_COLS,       "b"),
]:
    if key not in st.session_state:
        df = load_or_empty(path, cols)
        df = ensure_rowid(df, prefix=prefix)
        for c in cols:
            if c not in df.columns: df[c] = ""
        st.session_state[key] = df[cols].copy() if key != "busy_df" else df.copy()

if "submap" not in st.session_state:
    sm = load_or_empty(SUBMAP_PATH, ["SUBCODE","SUBNAME"])
    sm["SUBCODE"] = sm.get("SUBCODE", "")
    sm["SUBNAME"] = sm.get("SUBNAME", "")
    st.session_state.submap = sm.copy()

if "subject_staff_map" not in st.session_state:
    sm2 = load_or_empty(SUBJMAP_PATH, SUBJMAP_COLS)
    for c in SUBJMAP_COLS:
        if c not in sm2.columns: sm2[c] = ""
    st.session_state.subject_staff_map = sm2.copy()

if "audit" not in st.session_state:       st.session_state.audit = []
if "staged_ext" not in st.session_state:  st.session_state["staged_ext"] = {}

# ═══════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════
def persist_panel():
    st.session_state.panel_df = ensure_rowid(st.session_state.panel_df, prefix="p")
    return save_csv(st.session_state.panel_df, PANEL_PATH)

def persist_staff():
    st.session_state.staff_df = ensure_rowid(st.session_state.staff_df, prefix="s")
    return save_csv(st.session_state.staff_df, STAFF_PATH)

def persist_submap():
    st.session_state.submap = st.session_state.submap.fillna("")
    return save_csv(st.session_state.submap, SUBMAP_PATH)

def persist_busy():
    st.session_state.busy_df = ensure_rowid(st.session_state.busy_df, prefix="b")
    return save_csv(st.session_state.busy_df, BUSY_PATH)

def persist_subjmap():
    return save_csv(st.session_state.subject_staff_map, SUBJMAP_PATH)

# ═══════════════════════════════════════════════
# BUSY / AVAILABILITY HELPERS
# ═══════════════════════════════════════════════
def apply_busy_to_staff_cells(staff_df, staff_id, dfrom, dto, busy_token="B"):
    staff_df = staff_df.copy()
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff_df.columns: staff_df[dc] = ""
    mask = staff_df["Staff ID"].astype(str).str.upper() == str(staff_id).strip().upper()
    if not mask.any():
        new = {c: "" for c in staff_df.columns}
        new["Staff ID"] = staff_id
        staff_df = concat_row(staff_df, new)
        mask = staff_df["Staff ID"].astype(str).str.upper() == str(staff_id).strip().upper()
    sidx = staff_df[mask].index[0]
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        cur = staff_df.at[sidx, dc] if dc in staff_df.columns else ""
        toks = split_tokens(cur)
        if any(is_busy_token(t) for t in toks): continue
        staff_df.at[sidx, dc] = busy_token if (cur is None or str(cur).strip() == "") else busy_token + "," + str(cur).strip()
    return staff_df

def remove_busy_from_staff_cells(staff_df, staff_id, dfrom, dto):
    staff_df = staff_df.copy()
    mask = staff_df["Staff ID"].astype(str).str.upper() == str(staff_id).strip().upper()
    if not mask.any(): return staff_df
    sidx = staff_df[mask].index[0]
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff_df.columns: continue
        cur = staff_df.at[sidx, dc]
        toks = [t for t in split_tokens(cur) if not is_busy_token(t)]
        staff_df.at[sidx, dc] = ",".join(toks) if toks else ""
    return staff_df

def compute_staff_duty_stats(staff_df):
    stats = {}
    if staff_df is None or staff_df.empty: return stats
    date_cols = [c for c in staff_df.columns if c != "__rowid" and isinstance(c, str)
                 and len(c.split(".")) == 3 and all(p.isdigit() for p in c.split("."))]
    for _, row in staff_df.iterrows():
        sid = normalize_staff_id(row.get("Staff ID"))
        if not sid: continue
        date_map, duty_count = {}, 0
        for dc in date_cols:
            toks = split_tokens(row.get(dc, ""))
            date_map[dc] = toks
            duty_count += sum(1 for t in toks if not is_busy_token(t))
        stats[sid] = {
            "duty_count": duty_count, "date_tokens": date_map,
            "INSTT": row.get("INSTT",""), "dep_code": row.get("dep code",""),
            "name": row.get("Name of the Staff",""), "designation": row.get("Designation","")
        }
    return stats

def availability_for_req_dates(stats_entry, req_dates, busy_records=None):
    date_tokens = (stats_entry.get("date_tokens", {}) if stats_entry else {})
    conflicts = []
    for dc in req_dates:
        toks = date_tokens.get(dc, [])
        for t in toks:
            if t and t not in conflicts: conflicts.append(t)
    busy_overlaps = []
    if busy_records:
        for br in busy_records:
            bfrom = parse_date_flexible(br.get("DATE_FROM"))
            bto   = parse_date_flexible(br.get("DATE_TO"))
            if bfrom is None or bto is None: continue
            for dc in req_dates:
                d = parse_date_flexible(dc)
                if d and (bfrom <= d <= bto):
                    busy_overlaps.append(f"{date_to_str(bfrom)}->{date_to_str(bto)}")
                    break
    is_free = (len(conflicts) == 0) and (len(busy_overlaps) == 0)
    return (is_free, sorted(conflicts), sorted(set(busy_overlaps)))

def build_preview_staff_df():
    base = st.session_state.staff_df.copy()
    staged = st.session_state.get("staged_ext", {}) or {}
    for pidx_str, label in staged.items():
        try: pidx = int(pidx_str)
        except Exception: continue
        if pidx not in st.session_state.panel_df.index: continue
        prow = st.session_state.panel_df.loc[pidx]
        ins = str(prow.get("INSCODE","")).strip()
        d1  = parse_date_flexible(prow.get("DATE_FROM"))
        d2  = parse_date_flexible(prow.get("DATE_TO"))
        if not ins or d1 is None or d2 is None or d1 > d2: continue
        lbl = str(label).replace("🟢 ","").replace("🟡 ","").replace("🔴 ","")
        parts = lbl.split("—")
        if not parts or not parts[0].strip(): continue
        sid = normalize_staff_id(parts[0].strip())
        if not sid: continue
        for d in daterange(d1, d2):
            dc = date_to_str(d)
            if dc not in base.columns: base[dc] = ""
        mask = base["Staff ID"].astype(str).str.upper() == sid.upper()
        if not mask.any():
            new = {c:"" for c in base.columns}; new["Staff ID"] = sid
            base = concat_row(base, new)
            mask = base["Staff ID"].astype(str).str.upper() == sid.upper()
        sidx = base[mask].index[0]
        for d in daterange(d1, d2):
            dc = date_to_str(d)
            cur = str(base.at[sidx, dc] if dc in base.columns else "").strip()
            toks = split_tokens(cur)
            if ins not in toks:
                base.at[sidx, dc] = (ins if cur == "" else cur + "," + ins)
    return base

# ═══════════════════════════════════════════════
# SUGGESTIONS HELPER
# ═══════════════════════════════════════════════
def suggestions_for_row_with_stats(row, preview_staff_df, stats, busy_records):
    """Return list of dicts: {sid, name, desig, instt, dep_code, duty_count, is_free, conflicts}"""
    panel_inscode = str(row.get("INSCODE","")).strip()
    panel_dep     = str(row.get("NCNO","")).strip()
    d1 = parse_date_flexible(row.get("DATE_FROM"))
    d2 = parse_date_flexible(row.get("DATE_TO"))
    if d1 is None or d2 is None or d1 > d2: return []
    req_dates = [date_to_str(d) for d in daterange(d1, d2)]
    results = []
    for sid, se in stats.items():
        if str(se.get("INSTT","")).strip() == panel_inscode: continue
        if str(se.get("dep_code","")).strip() != panel_dep: continue
        busy_for = [br for br in busy_records if normalize_staff_id(br.get("Staff ID","")) == sid]
        is_free, conflicts, _ = availability_for_req_dates(se, req_dates, busy_for)
        results.append({
            "sid": sid, "name": se.get("name",""), "desig": se.get("designation",""),
            "instt": se.get("INSTT",""), "dep_code": se.get("dep_code",""),
            "duty_count": se.get("duty_count", 0), "is_free": is_free, "conflicts": conflicts
        })
    results.sort(key=lambda x: (not x["is_free"], x["duty_count"]))
    return results[:10]

def make_label(s):
    emoji = "🟢" if s["is_free"] else ("🔴" if any(is_busy_token(t) for t in s["conflicts"]) else "🟡")
    return f"{emoji} {s['sid']} — {s['name']} — {s['desig']} — {s['instt']}"

# ═══════════════════════════════════════════════
# AUTO-ALLOCATE EXTID FROM SUBJECT-STAFF MAP  (NEW)
# ═══════════════════════════════════════════════
def auto_allocate_extid_from_subjmap(candidates_df, staff_df, subject_staff_map, busy_df):
    """
    For each candidate panel row (empty EXTID):
      1. Match SUBCODE with Subject_Code in subject_staff_map
      2. Filter those staff with different INSTT from panel INSCODE
      3. Check availability on required dates
      4. Pick the one with minimum duty count
    Returns dict { pidx: label_string }
    """
    results   = {}
    skipped   = {}
    stats     = compute_staff_duty_stats(staff_df)
    busy_recs = busy_df.to_dict("records") if (busy_df is not None and not busy_df.empty) else []

    if subject_staff_map is None or subject_staff_map.empty:
        return results, skipped

    for pidx, row in candidates_df.iterrows():
        subcode       = str(row.get("SUBCODE","")).strip().upper()
        panel_inscode = str(row.get("INSCODE","")).strip()
        d1 = parse_date_flexible(row.get("DATE_FROM"))
        d2 = parse_date_flexible(row.get("DATE_TO"))
        if not subcode or not panel_inscode or d1 is None or d2 is None or d1 > d2:
            skipped[pidx] = "Missing SUBCODE / INSCODE / Dates"; continue

        req_dates = [date_to_str(d) for d in daterange(d1, d2)]

        # Step 1: staff mapped to this SUBCODE
        mapped = subject_staff_map[
            subject_staff_map["Subject_Code"].astype(str).str.strip().str.upper() == subcode
        ]
        if mapped.empty:
            skipped[pidx] = f"No staff mapped for SUBCODE {subcode}"; continue

        # Step 2 & 3: filter different INSCODE & check availability
        eligible = []
        for _, srow in mapped.iterrows():
            sid = normalize_staff_id(srow.get("Staff_Last_Staff_ID",""))
            if not sid: continue
            mask = staff_df["Staff ID"].astype(str).str.upper() == sid.upper()
            if not mask.any(): continue
            staff_row   = staff_df[mask].iloc[0]
            staff_instt = str(staff_row.get("INSTT","")).strip()
            if staff_instt == panel_inscode: continue          # same institution → skip
            se         = stats.get(sid, None)
            busy_for   = [br for br in busy_recs if normalize_staff_id(br.get("Staff ID","")) == sid]
            is_free, _, _ = availability_for_req_dates(se, req_dates, busy_for)
            if not is_free: continue
            duty_count = se["duty_count"] if se else 0
            eligible.append({
                "sid": sid,
                "name":      str(staff_row.get("Name of the Staff","")).strip(),
                "desig":     str(staff_row.get("Designation","")).strip(),
                "instt":     staff_instt,
                "duty_count": duty_count,
            })

        if not eligible:
            skipped[pidx] = f"No free external staff found for SUBCODE {subcode}"; continue

        # Step 4: pick least duty
        eligible.sort(key=lambda x: x["duty_count"])
        best    = eligible[0]
        label   = f"🟢 {best['sid']} — {best['name']} — {best['desig']} — {best['instt']}"
        results[pidx] = label

    return results, skipped

# ═══════════════════════════════════════════════
# APPLY EXTID FOR PANEL ROW
# ═══════════════════════════════════════════════
def apply_ext_for_panel_row(pidx, chosen_label):
    lbl = str(chosen_label).strip()
    lbl_clean = lbl.replace("🟢 ","").replace("🟡 ","").replace("🔴 ","")
    parts = lbl_clean.split("—")
    if not parts or not parts[0].strip():
        return False, "Cannot parse staff ID from label"
    sid = normalize_staff_id(parts[0].strip())
    if not sid: return False, "Empty staff ID"

    panel = st.session_state.panel_df
    if pidx not in panel.index: return False, "Panel row not found"
    prow = panel.loc[pidx]
    ins  = str(prow.get("INSCODE","")).strip()
    d1   = parse_date_flexible(prow.get("DATE_FROM"))
    d2   = parse_date_flexible(prow.get("DATE_TO"))
    if not ins or d1 is None or d2 is None or d1 > d2:
        return False, "Invalid INSCODE/dates on panel row"

    staff = st.session_state.staff_df.copy()
    for d in daterange(d1, d2):
        dc = date_to_str(d)
        if dc not in staff.columns: staff[dc] = ""
    mask = staff["Staff ID"].astype(str).str.upper() == sid.upper()
    if not mask.any():
        new = {c: "" for c in staff.columns}; new["Staff ID"] = sid
        staff = concat_row(staff, new)
        mask  = staff["Staff ID"].astype(str).str.upper() == sid.upper()
    sidx = staff[mask].index[0]
    for d in daterange(d1, d2):
        dc  = date_to_str(d)
        cur = str(staff.at[sidx, dc] if dc in staff.columns else "").strip()
        toks = split_tokens(cur)
        if ins not in toks:
            staff.at[sidx, dc] = ins if cur == "" else cur + "," + ins

    st.session_state.staff_df = staff
    persist_staff()

    st.session_state.panel_df.at[pidx, "EXTID"] = sid
    prev_err = str(st.session_state.panel_df.at[pidx, "ERROR"]).strip()
    if prev_err and "EXT apply failed" in prev_err:
        cleaned = " | ".join(p.strip() for p in prev_err.split("|") if "EXT apply failed" not in p and p.strip())
        st.session_state.panel_df.at[pidx, "ERROR"] = cleaned
    persist_panel()
    return True, f"✅ EXTID {sid} applied for panel row {pidx}"

# ═══════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Jupyter_logo.svg/50px-Jupyter_logo.svg.png",
                 width=40) if False else None   # placeholder

with st.sidebar:
    st.markdown("## 🗂️ Duty Manager")
    st.caption("MUTHUMANI S | LECTURER-EEE | GPT KARUR")
    st.markdown("---")
    page = st.radio("📌 Navigation", ["📥 Panel Upload", "▶️ Duty Mark", "🎯 EXTID Allocate"],
                    label_visibility="collapsed")
    st.markdown("---")
    panel_count = len(st.session_state.panel_df)
    ext_filled  = st.session_state.panel_df["EXTID"].apply(lambda v: normalize_staff_id(v) != "").sum() if panel_count else 0
    ext_pending = panel_count - ext_filled
    st.metric("📋 Panel Rows",  panel_count)
    st.metric("✅ EXTID Filled", ext_filled)
    st.metric("⏳ EXTID Pending", ext_pending)
    st.markdown("---")
    staff_count   = len(st.session_state.staff_df)
    subjmap_count = len(st.session_state.subject_staff_map)
    st.metric("🧑‍🏫 Staff Loaded",       staff_count)
    st.metric("📘 SubjectMap Rows",  subjmap_count)

# ═══════════════════════════════════════════════
# PAGE 1 — PANEL UPLOAD
# ═══════════════════════════════════════════════
if page == "📥 Panel Upload":
    st.title("📥 Panel Upload")
    st.info("Upload panel CSV/XLSX and staff data. Panel is the authoritative source — changes here reflect live on Duty Mark and EXTID Allocate pages.")

    colA, colB = st.columns(2)

    # ─── Panel ───────────────────────────────────
    with colA:
        st.subheader("📋 Panel Data")
        st.code("INSCODE  NCNO  SUBCODE  REGL  NOC  NOB  INTID  EXTID  DATE_FROM  DATE_TO", language="")
        uploaded = st.file_uploader("Upload Panel CSV/XLSX", type=["csv","xlsx"], key="panel_upload")
        clear_all = st.checkbox("☑️ Clear ALL existing panel data before upload", value=False)

        if uploaded is not None:
            try:
                tmp = (pd.read_csv(uploaded, dtype=object) if uploaded.name.lower().endswith(".csv")
                       else pd.read_excel(uploaded, dtype=object, sheet_name=0)).fillna("")
                required = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
                missing  = [c for c in required if c not in tmp.columns]
                if missing:
                    st.error(f"❌ Missing headers: {', '.join(missing)}")
                else:
                    tmp = tmp[required].copy(); tmp["ERROR"] = ""
                    tmp = ensure_rowid(tmp, prefix="p")
                    backend = st.session_state.panel_df.copy()
                    staff   = st.session_state.staff_df.copy()
                    if clear_all:
                        for _, r in backend.iterrows():
                            ins = str(r.get("INSCODE","")).strip()
                            d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                            if ins and d1 and d2 and d1 <= d2:
                                staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                        st.session_state.staff_df = staff; persist_staff()
                        st.session_state.panel_df = ensure_rowid(tmp.reset_index(drop=True), prefix="p")
                        persist_panel() and st.success("✅ Panel replaced (all cleared).")
                    else:
                        ins_in_upload = [str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip()]
                        for ins in ins_in_upload:
                            for _, exr in backend[backend["INSCODE"].astype(str).str.strip() == ins].iterrows():
                                d1 = parse_date_flexible(exr.get("DATE_FROM")); d2 = parse_date_flexible(exr.get("DATE_TO"))
                                if d1 and d2 and d1 <= d2:
                                    staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                            backend = backend[backend["INSCODE"].astype(str).str.strip() != ins]
                        backend = pd.concat([backend, tmp], ignore_index=True)
                        backend = ensure_rowid(backend.reset_index(drop=True), prefix="p")
                        st.session_state.staff_df = staff; persist_staff()
                        st.session_state.panel_df = backend; persist_panel()
                        st.success(f"✅ Uploaded for INSCODE(s): {', '.join(ins_in_upload)}")
            except Exception as e:
                st.error(f"❌ {e}")

        st.markdown("**📂 SUBCODE → SUBNAME Mapping** *(optional)*")
        subfile = st.file_uploader("Upload SUBCODE-SUBNAME CSV/XLSX", type=["csv","xlsx"], key="sub_upload")
        if subfile is not None:
            try:
                sm = (pd.read_csv(subfile, dtype=object) if subfile.name.lower().endswith(".csv")
                      else pd.read_excel(subfile, dtype=object, sheet_name=0)).fillna("")
                if "SUBCODE" not in sm.columns or "SUBNAME" not in sm.columns:
                    if sm.shape[1] >= 2:
                        sm = pd.DataFrame({"SUBCODE": sm.iloc[:,0].astype(str), "SUBNAME": sm.iloc[:,1].astype(str)})
                    else:
                        st.error("❌ Need SUBCODE and SUBNAME columns"); sm = None
                if sm is not None:
                    st.session_state.submap = sm[["SUBCODE","SUBNAME"]].copy()
                    persist_submap(); st.success("✅ SUBCODE mapping saved.")
            except Exception as e: st.error(f"❌ {e}")

        st.markdown("---")
        st.markdown("**📝 Panel Inline Editor**")
        pdf = st.session_state.panel_df.copy()
        if "ERROR" not in pdf.columns: pdf["ERROR"] = ""
        pdf["_d"] = pdf["DATE_FROM"].apply(parse_date_flexible)
        pdf = pdf.sort_values("_d", na_position="last").drop(columns=["_d"])
        if not st.session_state.submap.empty:
            pdf = pdf.merge(st.session_state.submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        else: pdf["SUBNAME"] = ""
        edited = st.data_editor(pdf, key="panel_data_editor", use_container_width=True, num_rows="dynamic")
        if st.button("💾 Save edited panel"):
            try:
                ts = edited.copy()
                if "SUBNAME" in ts.columns: ts = ts.drop(columns=["SUBNAME"])
                if "ERROR" not in ts.columns: ts["ERROR"] = ""
                backend  = st.session_state.panel_df.copy()
                bk_idx   = backend.set_index("__rowid", drop=False)
                ed_idx   = ts.set_index("__rowid", drop=False)
                to_drop  = [r for r in bk_idx.index if r not in ed_idx.index]
                if to_drop:
                    staff = st.session_state.staff_df.copy()
                    for _, dr in bk_idx.loc[to_drop].iterrows():
                        ins = str(dr.get("INSCODE","")).strip()
                        d1  = parse_date_flexible(dr.get("DATE_FROM")); d2 = parse_date_flexible(dr.get("DATE_TO"))
                        if ins and d1 and d2 and d1 <= d2:
                            staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                    st.session_state.staff_df = staff; persist_staff()
                    bk_idx = bk_idx.drop(index=to_drop, errors="ignore")
                for rid in bk_idx.index.intersection(ed_idx.index):
                    for c in ed_idx.columns: bk_idx.at[rid, c] = ed_idx.at[rid, c]
                new_ids = [r for r in ed_idx.index if r not in bk_idx.index]
                if new_ids:
                    bk_idx = pd.concat([bk_idx.reset_index(drop=True), ed_idx.loc[new_ids].reset_index(drop=True)], ignore_index=True)
                final = ensure_rowid(bk_idx.reset_index(drop=True), prefix="p")
                if "ERROR" not in final.columns: final["ERROR"] = ""
                st.session_state.panel_df = final; persist_panel()
                st.success("✅ Panel saved.")
            except Exception as e: st.error(f"❌ {e}")

        st.markdown("---")
        with st.expander("🗑️ Danger Zone — Clear ALL Panel Data"):
            if st.checkbox("I confirm: clear ALL panel data", key="confirm_clear_panel"):
                if st.button("🗑️ Clear all panel data now"):
                    staff = st.session_state.staff_df.copy()
                    for _, r in st.session_state.panel_df.iterrows():
                        ins = str(r.get("INSCODE","")).strip()
                        d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                        if ins and d1 and d2 and d1 <= d2:
                            staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                    st.session_state.staff_df = staff; persist_staff()
                    st.session_state.panel_df = ensure_rowid(pd.DataFrame(columns=PANEL_COLS), prefix="p")
                    persist_panel(); st.success("✅ All panel data cleared.")

    # ─── Staff ───────────────────────────────────
    with colB:
        st.subheader("🧑‍🏫 Staff Data")
        st.code("Staff ID  INSTT  Name of the Staff  Department  dep code  Designation", language="")
        ups = st.file_uploader("Upload Staff CSV/XLSX", type=["csv","xlsx"], key="staff_upload")
        if ups is not None:
            try:
                tmp = (pd.read_csv(ups, dtype=object) if ups.name.lower().endswith(".csv")
                       else pd.read_excel(ups, dtype=object, sheet_name=0)).fillna("")
                req_s   = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation"]
                miss_s  = [c for c in req_s if c not in tmp.columns]
                if miss_s: st.error(f"❌ Missing: {', '.join(miss_s)}")
                else:
                    tmp["Staff ID"] = tmp["Staff ID"].apply(normalize_staff_id)
                    for c in STAFF_CORE_COLS:
                        if c not in tmp.columns: tmp[c] = ""
                    st.session_state.staff_df = ensure_rowid(tmp, prefix="s")[STAFF_CORE_COLS].copy()
                    persist_staff(); st.success("✅ Staff data loaded.")
            except Exception as e: st.error(f"❌ {e}")

        sf = st.session_state.staff_df.copy()
        i_opts = ["All"] + sorted(set(sf["INSTT"].astype(str)))
        d_opts = ["All"] + sorted(set(sf["Department"].astype(str)))
        ci, cd = st.columns(2)
        i_sel = ci.selectbox("INSTT", i_opts)
        d_sel = cd.selectbox("Dept",  d_opts)
        flt_s = sf.copy()
        if i_sel != "All": flt_s = flt_s[flt_s["INSTT"].astype(str) == i_sel]
        if d_sel != "All": flt_s = flt_s[flt_s["Department"].astype(str) == d_sel]
        es = st.data_editor(flt_s, key="staff_editor", use_container_width=True, num_rows="dynamic")
        if st.button("💾 Save staff edits"):
            try:
                bk  = st.session_state.staff_df.copy().set_index("__rowid", drop=False)
                ed  = es.copy().set_index("__rowid", drop=False)
                for rid in bk.index.intersection(ed.index):
                    for c in ed.columns: bk.at[rid, c] = ed.at[rid, c]
                new = ed.index.difference(bk.index)
                if len(new):
                    bk = pd.concat([bk.reset_index(drop=True), ed.loc[new].reset_index(drop=True)], ignore_index=True)
                st.session_state.staff_df = ensure_rowid(bk.reset_index(drop=True), prefix="s")
                persist_staff(); st.success("✅ Staff saved.")
            except Exception as e: st.error(f"❌ {e}")

        st.markdown("---")
        with st.expander("🧹 Clear INSCODE tokens (keep Busy B)"):
            if st.checkbox("Confirm clear INSCODE tokens", key="confirm_clear_ins"):
                if st.button("🧹 Clear tokens"):
                    try:
                        st.session_state.staff_df = clear_all_inscode_tokens_keep_busy(st.session_state.staff_df)
                        persist_staff(); st.success("✅ Cleared.")
                    except Exception as e: st.error(f"❌ {e}")

# ═══════════════════════════════════════════════
# PAGE 2 — DUTY MARK
# ═══════════════════════════════════════════════
elif page == "▶️ Duty Mark":
    st.title("▶️ Duty Mark")
    st.info("Live view from Panel Upload. Click Generate to stamp staff calendars with INSCODE duties.")

    panel   = st.session_state.panel_df.copy()
    staff   = st.session_state.staff_df.copy()
    busy_df = st.session_state.busy_df.copy()
    submap  = st.session_state.submap.copy()

    if panel.empty:
        st.warning("⚠️ No panel rows. Upload on Panel Upload page.")
    else:
        c1, c2 = st.columns(2)
        ins_sel = c1.selectbox("INSCODE", ["All"] + sorted(set(panel["INSCODE"].astype(str))))
        nc_sel  = c2.selectbox("NCNO",    ["All"] + sorted(set(panel["NCNO"].astype(str))))
        filt = panel.copy()
        if ins_sel != "All": filt = filt[filt["INSCODE"].astype(str) == ins_sel]
        if nc_sel  != "All": filt = filt[filt["NCNO"].astype(str)    == nc_sel]
        filt["_d"] = filt["DATE_FROM"].apply(parse_date_flexible)
        filt = filt.sort_values("_d", na_position="last").drop(columns=["_d"])

        dp = filt.copy()
        dp["INTID_NORM"] = dp["INTID"].apply(normalize_staff_id)
        dp["INTNAME"]    = dp["INTID_NORM"].apply(lambda s: get_staff_name_by_id(st.session_state.staff_df, s) if s else "")
        dp["INTID_display"] = dp.apply(lambda r: r["INTID"].strip() + (" — " + r["INTNAME"] if r["INTNAME"] else ""), axis=1)
        if not submap.empty: dp = dp.merge(submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        else: dp["SUBNAME"] = ""
        st.dataframe(dp[["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID_display","EXTID","DATE_FROM","DATE_TO","ERROR"]].fillna(""), use_container_width=True, height=240)

        st.markdown("### 🚀 Generate Duty")
        if st.button("⚡ Generate Duty (clean re-run)"):
            try:
                for idx in filt.index:
                    if idx in st.session_state.panel_df.index:
                        st.session_state.panel_df.at[idx, "ERROR"] = ""
                persist_panel()
                staff = st.session_state.staff_df.copy()
                for _, r in filt.iterrows():
                    ins = str(r.get("INSCODE","")).strip()
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if ins and d1 and d2 and d1 <= d2:
                        staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                st.session_state.staff_df = staff; persist_staff()

                # ensure date columns
                all_dates = set()
                for _, r in filt.iterrows():
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if d1 and d2 and d1 <= d2:
                        for d in daterange(d1, d2): all_dates.add(date_to_str(d))
                for dc in sorted(all_dates, key=lambda s: datetime.strptime(s,"%d.%m.%Y")):
                    if dc not in st.session_state.staff_df.columns:
                        st.session_state.staff_df[dc] = ""
                staff = st.session_state.staff_df.copy()
                staff_map = {normalize_staff_id(r.get("Staff ID")): idx_s
                             for idx_s, r in staff.iterrows() if normalize_staff_id(r.get("Staff ID"))}

                errs, appends, errors = {}, 0, 0
                for idx, r in filt.iterrows():
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    ins = str(r.get("INSCODE","")).strip()
                    if d1 is None or d2 is None or d1 > d2 or not ins:
                        errors += 1; errs.setdefault(idx, set()).add("Invalid dates/INSCODE"); continue
                    for d in daterange(d1, d2):
                        dc = date_to_str(d)
                        for role, fld in [("I","INTID"),("E","EXTID")]:
                            raw  = r.get(fld,""); norm = normalize_staff_id(raw)
                            if fld == "EXTID" and is_zero_like(raw): norm = ""
                            if norm:
                                if norm not in staff_map:
                                    errors += 1; errs.setdefault(idx, set()).add(f"{fld} {norm} not found")
                                else:
                                    sidx = staff_map[norm]
                                    cur  = str(staff.at[sidx, dc] if dc in staff.columns else "").strip()
                                    staff.at[sidx, dc] = ins if cur == "" else cur + "," + ins
                                    appends += 1
                            elif fld == "INTID":
                                errors += 1; errs.setdefault(idx, set()).add("INTID empty")

                for idx, msgs in errs.items():
                    if idx in st.session_state.panel_df.index:
                        st.session_state.panel_df.at[idx, "ERROR"] = " | ".join(sorted(msgs))
                st.session_state.staff_df = staff; persist_staff(); persist_panel()
                st.success(f"✅ Done — {appends} stamps applied, {errors} issues.")
                if errs: st.warning(f"⚠️ {len(errs)} rows had errors.")
            except Exception as e: st.error(f"❌ {e}\n{traceback.format_exc()}")

        # ── Busy Management ──
        st.markdown("---")
        st.subheader("📵 Busy / Leave Management")
        cba, cbb = st.columns(2)
        with cba:
            st.markdown("**Mark staff as Busy**")
            all_sids = sorted([normalize_staff_id(r.get("Staff ID")) for _, r in st.session_state.staff_df.iterrows()
                               if normalize_staff_id(r.get("Staff ID"))])
            sid_busy = st.selectbox("Staff ID", [""] + all_sids, key="busy_sid")
            bf, bt   = st.columns(2)
            bfrom    = bf.date_input("From", key="busy_from")
            bto      = bt.date_input("To",   key="busy_to")
            bnote    = st.text_input("Note (optional)", key="busy_note")
            if st.button("➕ Add Busy"):
                if sid_busy and bfrom and bto and bfrom <= bto:
                    st.session_state.staff_df = apply_busy_to_staff_cells(st.session_state.staff_df, sid_busy, bfrom, bto)
                    newb = {"Staff ID": sid_busy, "DATE_FROM": date_to_str(bfrom),
                            "DATE_TO": date_to_str(bto), "NOTE": bnote, "__rowid": f"b_{uuid.uuid4().hex}"}
                    st.session_state.busy_df = concat_row(st.session_state.busy_df, newb)
                    persist_staff(); persist_busy(); st.success(f"✅ Busy marked for {sid_busy}.")
                else: st.warning("⚠️ Fill all fields correctly.")
        with cbb:
            st.markdown("**Current Busy Records**")
            bdf = st.session_state.busy_df.copy()
            if bdf.empty: st.info("No busy records.")
            else:
                st.dataframe(bdf[["Staff ID","DATE_FROM","DATE_TO","NOTE"]].fillna(""), use_container_width=True)
                if st.button("🗑️ Clear ALL busy records"):
                    st.session_state.busy_df = ensure_rowid(pd.DataFrame(columns=BUSY_COLS), prefix="b")
                    persist_busy(); st.success("✅ All busy records cleared.")

# ═══════════════════════════════════════════════
# PAGE 3 — EXTID ALLOCATE  (FULLY REDESIGNED + AUTO-ALLOCATE)
# ═══════════════════════════════════════════════
elif page == "🎯 EXTID Allocate":
    st.title("🎯 EXTID Allocate")
    st.markdown("""
    > Assign **External Examiners** to pending panel rows.  
    > Upload the **Staff-Subject Mapping** to enable 🤖 **Auto-Allocate** by subject expertise.
    """)

    # ──────────────────────────────────────────
    # Section A: Staff-Subject Mapping Upload
    # ──────────────────────────────────────────
    with st.expander("📘 Staff-Subject Mapping — Upload / View", expanded=len(st.session_state.subject_staff_map) == 0):
        st.markdown("**Required columns:**")
        st.code("Staff_Last_Staff_ID | Staff_Name | Department | Department_Code | Subject_Type | Subject_Code | Subject_Name | Subject_Remarks", language="")
        ssm_file = st.file_uploader("📂 Upload Staff-Subject Mapping CSV/XLSX", type=["csv","xlsx"], key="ssm_upload")
        if ssm_file is not None:
            try:
                ssm = (pd.read_csv(ssm_file, dtype=object) if ssm_file.name.lower().endswith(".csv")
                       else pd.read_excel(ssm_file, dtype=object, sheet_name=0)).fillna("")
                missing_ssm = [c for c in SUBJMAP_COLS if c not in ssm.columns]
                if missing_ssm:
                    st.error(f"❌ Missing columns: {', '.join(missing_ssm)}")
                else:
                    ssm["Staff_Last_Staff_ID"] = ssm["Staff_Last_Staff_ID"].apply(normalize_staff_id)
                    ssm["Subject_Code"] = ssm["Subject_Code"].astype(str).str.strip().str.upper()
                    st.session_state.subject_staff_map = ssm[SUBJMAP_COLS].copy()
                    persist_subjmap()
                    st.success(f"✅ Staff-Subject Mapping loaded — {len(ssm)} rows.")
            except Exception as e: st.error(f"❌ {e}")

        if not st.session_state.subject_staff_map.empty:
            ssm_view = st.session_state.subject_staff_map.copy()
            col_f1, col_f2 = st.columns(2)
            dep_filter = col_f1.selectbox("Filter Dept", ["All"] + sorted(set(ssm_view["Department"].astype(str))))
            sub_filter = col_f2.text_input("Filter Subject Code", "")
            if dep_filter != "All": ssm_view = ssm_view[ssm_view["Department"] == dep_filter]
            if sub_filter.strip(): ssm_view = ssm_view[ssm_view["Subject_Code"].str.contains(sub_filter.strip().upper())]
            st.dataframe(ssm_view, use_container_width=True, height=220)
            st.caption(f"Showing {len(ssm_view)} of {len(st.session_state.subject_staff_map)} mapping rows.")

    st.markdown("---")

    # ──────────────────────────────────────────
    # Section B: Filters
    # ──────────────────────────────────────────
    st.subheader("🔍 Filters")
    panel = st.session_state.panel_df.copy()

    cf1, cf2, cf3, cf4 = st.columns(4)
    ins_sel3 = cf1.selectbox("🏫 INSCODE", ["All"] + sorted(set(panel["INSCODE"].astype(str))), key="ext_ins")
    dep_sel3 = cf2.selectbox("🏭 NCNO",    ["All"] + sorted(set(panel["NCNO"].astype(str))),    key="ext_dep")
    dfrom_f  = cf3.date_input("📅 Date From (optional)", value=None, key="ext_dfrom")
    dto_f    = cf4.date_input("📅 Date To (optional)",   value=None, key="ext_dto")

    def intersects_filter(d1, d2, f1, f2):
        if f1 is None and f2 is None: return True
        if d1 is None or d2 is None:  return False
        f1 = f1 or d1; f2 = f2 or d2
        return max(d1, f1) <= min(d2, f2)

    def needs_ext(r):
        intid    = str(r.get("INTID","")).strip()
        extid    = r.get("EXTID","")
        ext_empty = str(extid).strip() == "" or is_zero_like(extid)
        d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
        return intid and ext_empty and d1 and d2 and d1 <= d2

    candidates = panel[panel.apply(needs_ext, axis=1)].copy()
    if ins_sel3 != "All": candidates = candidates[candidates["INSCODE"].astype(str) == ins_sel3]
    if dep_sel3 != "All": candidates = candidates[candidates["NCNO"].astype(str)    == dep_sel3]
    if dfrom_f or dto_f:
        keep = []
        for idx, r in candidates.iterrows():
            d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
            if intersects_filter(d1, d2, dfrom_f, dto_f): keep.append(idx)
        candidates = candidates.loc[keep].copy()
    candidates["_d"] = candidates["DATE_FROM"].apply(parse_date_flexible)
    candidates = candidates.sort_values("_d", na_position="last").drop(columns=["_d"])

    # metrics bar
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📋 Visible Pending", len(candidates))
    m2.metric("🧑‍🏫 Staff Loaded",   len(st.session_state.staff_df))
    m3.metric("📘 SubjectMap",       len(st.session_state.subject_staff_map))
    m4.metric("🔖 Staged",           len(st.session_state.get("staged_ext",{})))

    st.markdown("---")

    if candidates.empty:
        st.success("🎉 No pending EXTID rows for selected filters!")
    else:
        # ──────────────────────────────────────────
        # Section C: AUTO-ALLOCATE (NEW)
        # ──────────────────────────────────────────
        st.subheader("🤖 Auto-Allocate EXTID")

        auto_col1, auto_col2 = st.columns([3,1])
        with auto_col1:
            st.markdown("""
            **How it works:**  
            ① Matches each panel row's **SUBCODE** with mapped staff in Subject Mapping  
            ② Picks staff from a **different institution** (INSTT ≠ INSCODE)  
            ③ Checks **date availability** (no clash with existing duties / busy)  
            ④ Assigns the staff with **least total duty count** (INT + EXT combined)
            """)
        with auto_col2:
            st.markdown("<br>", unsafe_allow_html=True)
            run_auto = st.button("🤖 Auto-Allocate ALL Visible Rows", type="primary", use_container_width=True)

        if run_auto:
            if st.session_state.subject_staff_map.empty:
                st.error("❌ Upload Staff-Subject Mapping first to use Auto-Allocate!")
            else:
                with st.spinner("⏳ Running auto-allocation..."):
                    alloc_results, alloc_skipped = auto_allocate_extid_from_subjmap(
                        candidates,
                        st.session_state.staff_df,
                        st.session_state.subject_staff_map,
                        st.session_state.busy_df
                    )
                staged = st.session_state.get("staged_ext", {})
                for pidx, label in alloc_results.items():
                    staged[str(pidx)] = label
                st.session_state["staged_ext"] = staged

                st.success(f"✅ Auto-staged **{len(alloc_results)}** rows.")
                if alloc_skipped:
                    with st.expander(f"⚠️ Skipped {len(alloc_skipped)} rows — click to view"):
                        skip_df = pd.DataFrame([
                            {"Panel Index": k, "Panel Row": f"{candidates.loc[k,'INSCODE']} / {candidates.loc[k,'SUBCODE']}" if k in candidates.index else "?", "Reason": v}
                            for k, v in alloc_skipped.items()
                        ])
                        st.dataframe(skip_df, use_container_width=True)
                st.info("👇 Review staged allocations below, then click **Apply Staged to ALL**.")

        st.markdown("---")

        # ──────────────────────────────────────────
        # Section D: Per-row Manual / Suggestion
        # ──────────────────────────────────────────
        st.subheader("📝 Per-Row Allocation")

        preview_staff = build_preview_staff_df()
        stats         = compute_staff_duty_stats(preview_staff)
        busy_recs     = st.session_state.busy_df.to_dict("records") if not st.session_state.busy_df.empty else []
        submap        = st.session_state.submap.copy()

        def get_subname(code):
            if submap.empty: return ""
            m = submap[submap["SUBCODE"].astype(str) == str(code)]
            return m.iloc[0]["SUBNAME"] if not m.empty else ""

        for _, row in candidates.reset_index().iterrows():
            pidx    = int(row["index"])
            subcode = str(row.get("SUBCODE","")).strip()
            subname = get_subname(subcode)
            noc     = row.get("NOC","")
            intid   = str(row.get("INTID","")).strip()
            intname = get_staff_name_by_id(st.session_state.staff_df, intid) if intid else ""
            int_lbl = intid + (f" — {intname}" if intname else "")

            with st.container():
                # Row header
                header_cols = st.columns([3,3,2,2])
                header_cols[0].markdown(f"**🏫 {row.get('INSCODE','')}** | 🏭 {row.get('NCNO','')}")
                header_cols[1].markdown(f"**📚 {subcode}** {('— ' + subname) if subname else ''} | 👥 {noc}")
                header_cols[2].markdown(f"📅 {row.get('DATE_FROM','')} → {row.get('DATE_TO','')}")
                header_cols[3].markdown(f"👤 INT: `{int_lbl}`")

                # suggestion + manual + apply
                suggs    = suggestions_for_row_with_stats(row, preview_staff, stats, busy_recs)
                sugg_opts = ["— Select —"] + [make_label(s) for s in suggs]

                staged_val  = st.session_state.get("staged_ext",{}).get(str(pidx),"")
                row_cols    = st.columns([4,3,1,1])

                # suggestions dropdown
                default_idx = 0
                if staged_val and staged_val in sugg_opts:
                    default_idx = sugg_opts.index(staged_val)
                sel = row_cols[0].selectbox(
                    "💡 Suggestions", sugg_opts, index=default_idx,
                    key=f"sugg_{pidx}_{normalize_staff_id(staged_val)}"
                )
                if sel and sel != "— Select —":
                    st.session_state.setdefault("staged_ext",{})[str(pidx)] = sel

                # manual entry
                man = row_cols[1].text_input("✏️ Manual Staff ID", value="", key=f"man_{pidx}", placeholder="Enter Staff ID")
                if man.strip():
                    st.session_state.setdefault("staged_ext",{})[str(pidx)] = man.strip()

                # staged badge
                sv = st.session_state.get("staged_ext",{}).get(str(pidx),"")
                if sv and str(sv).strip():
                    row_cols[2].markdown(f"<br>✅ Staged", unsafe_allow_html=True)
                else:
                    row_cols[2].markdown(f"<br>⬜ -", unsafe_allow_html=True)

                # single Apply button
                if row_cols[3].button("▶ Apply", key=f"apply_{pidx}"):
                    chosen = sv or (sel if sel != "— Select —" else "") or man.strip()
                    if not chosen:
                        st.warning("⚠️ Choose or type a staff ID first.")
                    else:
                        ok, msg = apply_ext_for_panel_row(pidx, chosen)
                        if ok:
                            st.session_state.get("staged_ext",{}).pop(str(pidx), None)
                            st.success(msg)
                        else:
                            st.error(msg)

                st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

        st.markdown("---")

        # ──────────────────────────────────────────
        # Section E: Apply Staged (Bulk)
        # ──────────────────────────────────────────
        st.subheader("🚀 Apply Staged Allocations")
        staged_map = st.session_state.get("staged_ext",{})

        if not staged_map:
            st.info("ℹ️ No staged rows. Use per-row selectors above or Auto-Allocate to stage.")
        else:
            preview_lines = []
            for k, v in list(staged_map.items())[:20]:
                try:
                    pidx_int = int(k)
                    ins  = st.session_state.panel_df.at[pidx_int,"INSCODE"] if pidx_int in st.session_state.panel_df.index else "?"
                    sub  = st.session_state.panel_df.at[pidx_int,"SUBCODE"] if pidx_int in st.session_state.panel_df.index else "?"
                    preview_lines.append(f"Row {k} | {ins} / {sub} → {v}")
                except Exception: preview_lines.append(f"Row {k} → {v}")

            with st.expander(f"👁️ Preview {len(staged_map)} staged rows"):
                st.text("\n".join(preview_lines))

            apply_col1, apply_col2 = st.columns(2)
            if apply_col1.button("✅ Apply Staged to ALL Visible Rows", type="primary", use_container_width=True):
                success_count, fail_list = 0, []
                for pidx_str, chosen_label in list(staged_map.items()):
                    try: pidx = int(pidx_str)
                    except Exception: fail_list.append({"row": pidx_str, "reason": "invalid index"}); continue
                    if pidx not in candidates.index:
                        fail_list.append({"row": pidx, "reason": "not visible in current filter"}); continue
                    ok, msg = apply_ext_for_panel_row(pidx, chosen_label)
                    if ok:
                        success_count += 1
                        st.session_state.get("staged_ext",{}).pop(pidx_str, None)
                    else:
                        fail_list.append({"row": pidx, "reason": msg})
                if success_count: st.success(f"✅ Applied EXTID for {success_count} rows.")
                if fail_list: st.error(f"❌ Failed for {len(fail_list)} rows."); st.dataframe(pd.DataFrame(fail_list))

            if apply_col2.button("🗑️ Clear All Staged", use_container_width=True):
                st.session_state["staged_ext"] = {}
                st.success("✅ Staged cleared.")

        st.markdown("---")

        # ──────────────────────────────────────────
        # Section F: Duty Chart
        # ──────────────────────────────────────────
        st.subheader("📊 Staff Duty Count Chart")
        stats_all = compute_staff_duty_stats(st.session_state.staff_df)
        if stats_all:
            chart_data = pd.DataFrame([
                {"Staff ID": sid, "Name": v.get("name",""), "INSTT": v.get("INSTT",""),
                 "Department": v.get("dep_code",""), "Duty Count": v.get("duty_count",0)}
                for sid, v in stats_all.items() if v.get("duty_count",0) > 0
            ]).sort_values("Duty Count", ascending=False).head(30)
            if not chart_data.empty:
                st.bar_chart(chart_data.set_index("Staff ID")["Duty Count"])
            else: st.info("No duty assignments found.")
        else: st.info("No staff data loaded.")
