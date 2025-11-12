#!/usr/bin/env python3
# streamlit_app.py
"""
Duty Manager - Full application
- Panel authoritative; live Duty Mark view
- EXTID suggestions & manual dropdowns now include inline status emojis:
    🟢 FREE, 🔴 BUSY/DUTY, 🟡 WORKSHOP
- By default blocked candidates (🔴 or 🟡) are not applied unless override is enabled.
Created by MUTHUMANI S, LECTURER-EEE, GPT KARUR
"""
from __future__ import annotations
import os
import uuid
import traceback
from datetime import datetime, timedelta, date
import re

import streamlit as st
import pandas as pd

# ---------- CONFIG ----------
DATA_DIR = "data"
PANEL_PATH = os.path.join(DATA_DIR, "panel.csv")
STAFF_PATH = os.path.join(DATA_DIR, "staff.csv")
SUBMAP_PATH = os.path.join(DATA_DIR, "submap.csv")
BUSY_PATH = os.path.join(DATA_DIR, "busy.csv")
CHECK_ERRORS_XLSX = os.path.join(DATA_DIR, "check_errors.xlsx")
EXPORT_MONTH_TAG = "oct2025"

os.makedirs(DATA_DIR, exist_ok=True)
st.set_page_config(page_title="Duty Manager", layout="wide")

# ---------- HELPERS ----------
def _now():
    return datetime.now().isoformat(timespec="seconds")

def parse_date_flexible(s):
    if s is None:
        return None
    try:
        if pd.isna(s):
            return None
    except Exception:
        pass
    if isinstance(s, (datetime, date, pd.Timestamp)):
        try:
            return s.date() if hasattr(s, "date") else None
        except Exception:
            pass
    s_str = str(s).strip()
    if not s_str:
        return None
    fmts = ["%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"]
    for f in fmts:
        try:
            return datetime.strptime(s_str, f).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s_str, dayfirst=True).date()
    except Exception:
        return None

def date_to_str(d):
    if d is None:
        return ""
    if isinstance(d, (datetime, pd.Timestamp)):
        d = d.date()
    return d.strftime("%d.%m.%Y")

def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)

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
            df = pd.read_csv(path, dtype=object)
            df = df.fillna("")
            return df
        except Exception:
            return pd.DataFrame(columns=columns)
    else:
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

# ---------- STAFF ID NORMALIZATION ----------
def normalize_staff_id(v) -> str:
    if v is None:
        return ""
    try:
        if isinstance(v, float) and (v != v):  # NaN
            return ""
    except Exception:
        pass
    s = str(v).strip()
    if s == "":
        return ""
    if s in ("0", "0.0", "0.00"):
        return ""
    if re.fullmatch(r"-?\d+\.\d+", s):
        try:
            fv = float(s)
            if abs(fv - int(fv)) < 1e-9:
                s = str(int(fv))
        except Exception:
            pass
    if s == "0":
        return ""
    return s.upper()

def is_zero_like(v) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    return s in ("0", "0.0", "0.00")

def get_staff_name_by_id(staff_df: pd.DataFrame, staff_id) -> str:
    sid = normalize_staff_id(staff_id)
    if not sid:
        return ""
    try:
        mask = staff_df["Staff ID"].astype(str).str.upper() == sid
    except Exception:
        return ""
    if mask.any():
        try:
            return str(staff_df.loc[mask, "Name of the Staff"].iloc[0])
        except Exception:
            return ""
    return ""

# token splitting and busy detection
SPLIT_RE = re.compile(r"[,\uFF0C\u3001;|\-/\\_\s]+")

def split_tokens(cell_value):
    if cell_value is None:
        return []
    try:
        if isinstance(cell_value, float) and (cell_value != cell_value):
            return []
    except Exception:
        pass
    s = str(cell_value).strip()
    if s == "":
        return []
    parts = [p.strip() for p in SPLIT_RE.split(s) if p is not None and p.strip() != ""]
    new = []
    for p in parts:
        if re.fullmatch(r"\d{6,}", p) and (len(p) % 3 == 0):
            for i in range(0, len(p), 3):
                new.append(p[i:i+3])
        else:
            if re.fullmatch(r"-?\d+\.\d+", p):
                fv = float(p)
                if abs(fv - int(fv)) < 1e-9:
                    new.append(str(int(fv)))
                    continue
            new.append(p)
    return new

def is_busy_token(tok):
    if not tok:
        return False
    t = str(tok).strip().upper()
    if t == "B":
        return True
    if re.match(r"^B[\W_]*\d+$", t):
        return True
    return False

# ---------- INSCODE removal/utilities ----------
def remove_inscode_from_staff_cells(staff_df: pd.DataFrame, inscode: str, dfrom: date, dto: date):
    if not inscode:
        return staff_df
    staff = staff_df.copy()
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff.columns:
            continue
        for ridx in staff.index:
            cur = staff.at[ridx, dc]
            if cur is None or str(cur).strip() == "":
                continue
            toks = [t for t in split_tokens(cur) if t != str(inscode).strip()]
            staff.at[ridx, dc] = ",".join(toks) if toks else ""
    return staff

def clear_all_inscode_tokens_keep_busy(staff_df: pd.DataFrame):
    staff = staff_df.copy()
    cols = [c for c in staff.columns if c != "__rowid"]
    date_cols = [c for c in cols if isinstance(c, str) and len(c.split(".")) == 3 and all(part.isdigit() for part in c.split("."))]
    for dc in date_cols:
        for ridx in staff.index:
            cur = staff.at[ridx, dc]
            if cur is None or str(cur).strip() == "":
                continue
            toks = split_tokens(cur)
            kept = [t for t in toks if is_busy_token(t)]
            staff.at[ridx, dc] = ",".join(kept) if kept else ""
    return staff

# ---------- DATA & SESSION INITIALIZATION ----------
PANEL_COLS = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO","ERROR","__rowid"]
STAFF_CORE_COLS = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation","__rowid"]
BUSY_COLS = ["Staff ID","DATE_FROM","DATE_TO","NOTE","__rowid"]

if "panel_df" not in st.session_state:
    pf = load_or_empty(PANEL_PATH, PANEL_COLS)
    pf = ensure_rowid(pf, prefix="p")
    for c in PANEL_COLS:
        if c not in pf.columns:
            pf[c] = ""
    st.session_state.panel_df = pf[PANEL_COLS].copy()

if "staff_df" not in st.session_state:
    sf = load_or_empty(STAFF_PATH, STAFF_CORE_COLS)
    sf = ensure_rowid(sf, prefix="s")
    for c in STAFF_CORE_COLS:
        if c not in sf.columns:
            sf[c] = ""
    st.session_state.staff_df = sf.copy()

if "submap" not in st.session_state:
    sm = load_or_empty(SUBMAP_PATH, ["SUBCODE","SUBNAME"])
    if "SUBCODE" not in sm.columns:
        sm["SUBCODE"] = ""
    if "SUBNAME" not in sm.columns:
        sm["SUBNAME"] = ""
    st.session_state.submap = sm.copy()

if "busy_df" not in st.session_state:
    bf = load_or_empty(BUSY_PATH, BUSY_COLS)
    bf = ensure_rowid(bf, prefix="b")
    for c in BUSY_COLS:
        if c not in bf.columns:
            bf[c] = ""
    st.session_state.busy_df = bf.copy()

if "audit" not in st.session_state:
    st.session_state.audit = []

# ---------- PERSISTENCE ----------
def persist_panel():
    st.session_state.panel_df = ensure_rowid(st.session_state.panel_df, prefix="p")
    ok = save_csv(st.session_state.panel_df, PANEL_PATH)
    return ok

def persist_staff():
    st.session_state.staff_df = ensure_rowid(st.session_state.staff_df, prefix="s")
    ok = save_csv(st.session_state.staff_df, STAFF_PATH)
    return ok

def persist_submap():
    st.session_state.submap = st.session_state.submap.fillna("")
    ok = save_csv(st.session_state.submap, SUBMAP_PATH)
    return ok

def persist_busy():
    st.session_state.busy_df = ensure_rowid(st.session_state.busy_df, prefix="b")
    ok = save_csv(st.session_state.busy_df, BUSY_PATH)
    return ok

def apply_busy_to_staff_cells(staff_df, staff_id, dfrom, dto, busy_token="B"):
    staff_df = staff_df.copy()
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff_df.columns:
            staff_df[dc] = ""
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
        if any(is_busy_token(t) for t in toks):
            continue
        if cur is None or str(cur).strip() == "":
            staff_df.at[sidx, dc] = busy_token
        else:
            staff_df.at[sidx, dc] = busy_token + "," + str(cur).strip()
    return staff_df

def remove_busy_from_staff_cells(staff_df, staff_id, dfrom, dto):
    staff_df = staff_df.copy()
    mask = staff_df["Staff ID"].astype(str).str.upper() == str(staff_id).strip().upper()
    if not mask.any():
        return staff_df
    sidx = staff_df[mask].index[0]
    for d in daterange(dfrom, dto):
        dc = date_to_str(d)
        if dc not in staff_df.columns:
            continue
        cur = staff_df.at[sidx, dc]
        toks = [t for t in split_tokens(cur) if not is_busy_token(t)]
        staff_df.at[sidx, dc] = ",".join(toks) if toks else ""
    return staff_df

# ---------- Staff stats ----------
def compute_staff_duty_stats(staff_df: pd.DataFrame):
    stats = {}
    if staff_df is None or staff_df.empty:
        return stats
    cols = [c for c in staff_df.columns if c != "__rowid"]
    date_cols = [c for c in cols if isinstance(c, str) and len(c.split(".")) == 3 and all(part.isdigit() for part in c.split("."))]
    for _, row in staff_df.iterrows():
        sid = normalize_staff_id(row.get("Staff ID"))
        if not sid:
            continue
        date_map = {}
        duty_count = 0
        for dc in date_cols:
            cell = row.get(dc, "")
            toks = split_tokens(cell)
            date_map[dc] = toks
            for t in toks:
                if not is_busy_token(t):
                    duty_count += 1
        stats[sid] = {"duty_count": duty_count, "date_tokens": date_map, "INSTT": row.get("INSTT",""), "dep_code": row.get("dep code",""), "name": row.get("Name of the Staff",""), "designation": row.get("Designation","")}
    return stats

def availability_for_req_dates(stats_entry, req_dates, busy_records=None):
    """
    Return (is_free_bool, conflicts_list, busy_overlap_list)
    conflicts_list: list of INSCODE tokens that are non-B on those dates
    busy_overlap_list: list of busy intervals that overlap
    """
    if stats_entry is None:
        return (True, [], [])
    date_tokens = stats_entry.get("date_tokens", {})
    conflicts = []
    for dc in req_dates:
        toks = date_tokens.get(dc, []) if date_tokens is not None else []
        for t in toks:
            if not is_busy_token(t):
                if t and t not in conflicts:
                    conflicts.append(t)
    busy_overlaps = []
    if busy_records is not None:
        for br in busy_records:
            bfrom = parse_date_flexible(br.get("DATE_FROM"))
            bto = parse_date_flexible(br.get("DATE_TO"))
            if bfrom is None or bto is None:
                continue
            for dc in req_dates:
                d = parse_date_flexible(dc)
                if d and (bfrom <= d <= bto):
                    busy_overlaps.append(f"{date_to_str(bfrom)}->{date_to_str(bto)}")
                    break
    return (len(conflicts) == 0 and len(busy_overlaps) == 0, sorted(conflicts), sorted(set(busy_overlaps)))

def is_workshop_designation(designation: str) -> bool:
    if designation is None:
        return False
    s = str(designation).lower()
    keywords = ["workshop", "instructor", "lab instructor", "lab-instructor", "trainer", "work shop", "work-shop", "tech instr", "technician"]
    for k in keywords:
        if k in s:
            return True
    return False

# ---------- UI ----------
st.title("🗂️ Duty Manager")
st.caption("Created by MUTHUMANI S, LECTURER-EEE, GPT KARUR")

page = st.sidebar.radio("Pages", ["Panel Upload", "Duty Mark", "EXTID Allocate"])

# ------------------- Panel Upload -------------------
if page == "Panel Upload":
    st.header("📥 Panel Upload — upload & edit allocations")
    st.info("Panel (allocations) is the authoritative dataset. Any upload/save here persists to data/panel.csv and is shown live on Duty Mark page.")

    colA, colB = st.columns(2)

    # Panel upload/editor
    with colA:
        st.subheader("Panel (allocations) — upload & inline edit")
        st.markdown("**Required panel headers (exact):**")
        st.code("INSCODE\tNCNO\tSUBCODE\tREGL\tNOC\tNOB\tINTID\tEXTID\tDATE_FROM\tDATE_TO")

        uploaded = st.file_uploader("Upload Panel CSV/XLSX (institute-wise). Each upload replaces existing rows for the INSCODE(s) in file.", type=["csv","xlsx"], key="panel_upload")

        clear_all_checkbox = st.checkbox("Clear all existing panel data before upload", value=False)

        if uploaded is not None:
            try:
                if str(uploaded.name).lower().endswith(".csv"):
                    tmp = pd.read_csv(uploaded, dtype=object).fillna("")
                else:
                    tmp = pd.read_excel(uploaded, dtype=object, sheet_name=0).fillna("")
                required = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
                missing = [c for c in required if c not in tmp.columns]
                if missing:
                    st.error("Uploaded file missing required headers: " + ", ".join(missing))
                else:
                    tmp = tmp[required].copy()
                    tmp["ERROR"] = ""
                    tmp = ensure_rowid(tmp, prefix="p")
                    backend = st.session_state.panel_df.copy()

                    if clear_all_checkbox:
                        existing = backend.copy()
                        staff = st.session_state.staff_df.copy()
                        for _, r in existing.iterrows():
                            ins = str(r.get("INSCODE","")).strip()
                            d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                            if ins and d1 and d2 and d1 <= d2:
                                staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                        st.session_state.staff_df = staff.copy()
                        persist_staff()

                        backend = tmp.reset_index(drop=True)
                        backend = ensure_rowid(backend, prefix="p")
                        st.session_state.panel_df = backend.copy()
                        if persist_panel():
                            st.success("Cleared old panel data and saved uploaded panel as backend (previous marks removed).")
                        else:
                            st.error("Failed to persist panel.csv")
                    else:
                        ins_in_upload = sorted([str(x).strip() for x in tmp["INSCODE"].unique() if str(x).strip() != ""])
                        staff = st.session_state.staff_df.copy()
                        for ins in ins_in_upload:
                            existing_rows = backend[backend["INSCODE"].astype(str).str.strip() == ins]
                            for _, exr in existing_rows.iterrows():
                                d1 = parse_date_flexible(exr.get("DATE_FROM")); d2 = parse_date_flexible(exr.get("DATE_TO"))
                                if d1 and d2 and d1 <= d2:
                                    staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                        st.session_state.staff_df = staff.copy()
                        persist_staff()

                        for ins in ins_in_upload:
                            backend = backend[backend["INSCODE"].astype(str).str.strip() != str(ins)]
                        backend = pd.concat([backend.reset_index(drop=True), tmp.reset_index(drop=True)], ignore_index=True)
                        backend = ensure_rowid(backend.reset_index(drop=True), prefix="p")
                        st.session_state.panel_df = backend.copy()
                        if persist_panel():
                            st.success(f"Uploaded and replaced backend rows for INSCODE(s): {', '.join(ins_in_upload)}")
                        else:
                            st.error("Failed to persist panel.csv")
            except Exception as e:
                st.error("Failed to load panel upload: " + str(e))

        st.markdown("**SUBCODE -> SUBNAME (optional)**")
        st.markdown("Required headers: SUBCODE, SUBNAME")
        subfile = st.file_uploader("Upload SUBCODE->SUBNAME CSV/XLSX (replaces mapping).", type=["csv","xlsx"], key="sub_upload")
        if subfile is not None:
            try:
                if str(subfile.name).lower().endswith(".csv"):
                    sm = pd.read_csv(subfile, dtype=object).fillna("")
                else:
                    sm = pd.read_excel(subfile, dtype=object, sheet_name=0).fillna("")
                if "SUBCODE" not in sm.columns or "SUBNAME" not in sm.columns:
                    if sm.shape[1] >= 2:
                        sm2 = pd.DataFrame({"SUBCODE": sm.iloc[:,0].astype(str), "SUBNAME": sm.iloc[:,1].astype(str)})
                    else:
                        st.error("Uploaded SUB mapping must contain SUBCODE and SUBNAME.")
                        sm2 = None
                else:
                    sm2 = sm[["SUBCODE","SUBNAME"]].copy()
                if sm2 is not None:
                    st.session_state.submap = sm2.copy()
                    if persist_submap():
                        st.success("SUBCODE -> SUBNAME mapping uploaded and saved.")
                    else:
                        st.error("Failed to persist submap")
            except Exception as e:
                st.error("Failed to load submap upload: " + str(e))

        st.markdown("**Panel — inline editor (rows sorted by DATE_FROM ascending)**")
        panel_df = st.session_state.panel_df.copy()
        if "ERROR" not in panel_df.columns:
            panel_df["ERROR"] = ""
        panel_df["_parsed_date_from"] = panel_df["DATE_FROM"].apply(parse_date_flexible)
        panel_df = panel_df.sort_values(by="_parsed_date_from", na_position="last").drop(columns=["_parsed_date_from"])
        if not st.session_state.submap.empty:
            editor_panel = panel_df.merge(st.session_state.submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        else:
            editor_panel = panel_df.copy()
            editor_panel["SUBNAME"] = ""
        edited = st.data_editor(editor_panel, key="panel_data_editor", use_container_width=True, num_rows="dynamic")

        # Save edited logic (deletions persist and clear previous marks)
        if st.button("Save edited panel to backend (deletions persist)"):
            try:
                to_save = edited.copy()
                if "SUBNAME" in to_save.columns:
                    to_save = to_save.drop(columns=["SUBNAME"])
                if "ERROR" not in to_save.columns:
                    to_save["ERROR"] = ""
                backend = st.session_state.panel_df.copy()
                backend_idx = backend.set_index("__rowid", drop=False)
                edited_idx = to_save.set_index("__rowid", drop=False)

                # detect deletions -> remove corresponding INSCODE tokens from staff
                to_drop = [rid for rid in backend_idx.index if rid not in edited_idx.index]
                if to_drop:
                    staff = st.session_state.staff_df.copy()
                    dropped_rows = backend_idx.loc[to_drop]
                    for _, dr in dropped_rows.iterrows():
                        ins = str(dr.get("INSCODE","")).strip()
                        d1 = parse_date_flexible(dr.get("DATE_FROM")); d2 = parse_date_flexible(dr.get("DATE_TO"))
                        if ins and d1 and d2 and d1 <= d2:
                            staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                    st.session_state.staff_df = staff.copy()
                    persist_staff()
                    backend_idx = backend_idx.drop(index=to_drop, errors="ignore")

                # update existing rows
                common = backend_idx.index.intersection(edited_idx.index)
                for rid in common:
                    for c in edited_idx.columns:
                        backend_idx.at[rid, c] = edited_idx.at[rid, c]

                # append new rows
                new_ids = [rid for rid in edited_idx.index if rid not in backend_idx.index]
                if new_ids:
                    to_append = edited_idx.loc[new_ids].reset_index(drop=True)
                    backend_idx = pd.concat([backend_idx.reset_index(drop=True), to_append.reset_index(drop=True)], ignore_index=True)

                backend_final = ensure_rowid(backend_idx.reset_index(drop=True), prefix="p")
                if "ERROR" not in backend_final.columns:
                    backend_final["ERROR"] = ""
                st.session_state.panel_df = backend_final.copy()
                if persist_panel():
                    st.success("Saved edited panel rows into backend (deletions persisted).")
                else:
                    st.error("Failed to persist panel.csv")
            except Exception as e:
                st.error("Failed to save edits: " + str(e))

        st.markdown("---")
        st.markdown("**Clear ALL panel data (removes previous marks)**")
        confirm_clear = st.checkbox("I confirm: clear ALL panel data (this will remove every row from backend).", key="confirm_clear_panel")
        if st.button("Clear all panel data now"):
            if not confirm_clear:
                st.warning("Tick confirmation to clear ALL panel data.")
            else:
                existing = st.session_state.panel_df.copy()
                staff = st.session_state.staff_df.copy()
                for _, r in existing.iterrows():
                    ins = str(r.get("INSCODE","")).strip()
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if ins and d1 and d2 and d1 <= d2:
                        staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                st.session_state.staff_df = staff.copy()
                persist_staff()

                st.session_state.panel_df = ensure_rowid(pd.DataFrame(columns=PANEL_COLS), prefix="p")
                if persist_panel():
                    st.success("All panel data cleared and previous staff marks removed.")
                else:
                    st.error("Failed to persist panel.csv")

    # Staffdata upload/edit/clear INSCODE tokens
    with colB:
        st.subheader("🧑‍🏫 Staffdata — upload, edit & clear INSCODE tokens")
        st.markdown("**Required staff headers:**")
        st.code("Staff ID\tINSTT\tName of the Staff\tDepartment\tdep code\tDesignation")
        uploaded_s = st.file_uploader("Upload Staffdata CSV/XLSX (single upload). Replace backend staff.", type=["csv","xlsx"], key="staff_upload")
        if uploaded_s is not None:
            try:
                if str(uploaded_s.name).lower().endswith(".csv"):
                    tmp = pd.read_csv(uploaded_s, dtype=object).fillna("")
                else:
                    tmp = pd.read_excel(uploaded_s, dtype=object, sheet_name=0).fillna("")
                required_s = ["Staff ID","INSTT","Name of the Staff","Department","dep code","Designation"]
                missing_s = [c for c in required_s if c not in tmp.columns]
                if missing_s:
                    st.error("Staff upload missing required headers: " + ", ".join(missing_s))
                else:
                    tmp = tmp.copy()
                    tmp["Staff ID"] = tmp["Staff ID"].apply(lambda v: normalize_staff_id(v))
                    for c in STAFF_CORE_COLS:
                        if c not in tmp.columns:
                            tmp[c] = ""
                    tmp = ensure_rowid(tmp, prefix="s")
                    st.session_state.staff_df = tmp[STAFF_CORE_COLS].copy()
                    if persist_staff():
                        st.success("Staffdata uploaded and replaced backend staff table.")
                    else:
                        st.error("Failed to persist staff.csv")
            except Exception as e:
                st.error("Failed to load staff upload: " + str(e))

        st.markdown("**Staff view & filters**")
        staff_df = st.session_state.staff_df.copy()
        inst_opts = ["All"] + sorted([x for x in staff_df["INSTT"].unique() if str(x).strip()!=""])
        dept_opts = ["All"] + sorted([x for x in staff_df["Department"].unique() if str(x).strip()!=""])
        inst_sel = st.selectbox("INSTT (filter)", inst_opts, index=0)
        dept_sel = st.selectbox("Department (filter)", dept_opts, index=0)
        flt = staff_df.copy()
        if inst_sel != "All":
            flt = flt[flt["INSTT"].astype(str) == str(inst_sel)]
        if dept_sel != "All":
            flt = flt[flt["Department"].astype(str) == str(dept_sel)]

        edited_staff = st.data_editor(flt, key="staff_data_editor", use_container_width=True, num_rows="dynamic")
        if st.button("Save edited staff to backend (merge)"):
            try:
                backend = st.session_state.staff_df.copy()
                edited_df = edited_staff.copy()
                backend_idx = backend.set_index("__rowid", drop=False)
                edited_idx = edited_df.set_index("__rowid", drop=False)
                common = backend_idx.index.intersection(edited_idx.index)
                for rid in common:
                    for col in edited_idx.columns:
                        backend_idx.at[rid, col] = edited_idx.at[rid, col]
                new_ids = edited_idx.index.difference(backend_idx.index)
                if len(new_ids) > 0:
                    to_append = edited_idx.loc[new_ids].reset_index(drop=True)
                    backend_idx = pd.concat([backend_idx.reset_index(drop=True), to_append.reset_index(drop=True)], ignore_index=True)
                backend_final = ensure_rowid(backend_idx.reset_index(drop=True), prefix="s")
                st.session_state.staff_df = backend_final.copy()
                if persist_staff():
                    st.success("Staff edits merged to backend.")
                else:
                    st.error("Failed to persist staff.csv")
            except Exception as e:
                st.error("Save failed: " + str(e))

        st.markdown("---")
        st.markdown("**Clear INSCODE tokens from staff date columns (keeps Busy 'B')**")
        st.caption("Use this to prepare a fresh Generate run. Busy tokens (B) are preserved.")
        confirm_clear_ins = st.checkbox("I confirm: clear all INSCODE tokens (keep B tokens).", key="confirm_clear_ins")
        if st.button("Clear INSCODE tokens from staff grid"):
            if not confirm_clear_ins:
                st.warning("Tick confirmation before clearing.")
            else:
                try:
                    staff_cleaned = clear_all_inscode_tokens_keep_busy(st.session_state.staff_df)
                    st.session_state.staff_df = staff_cleaned.copy()
                    if persist_staff():
                        st.success("Cleared INSCODE tokens (busy tokens retained).")
                    else:
                        st.error("Failed to persist staff.csv")
                except Exception as e:
                    st.error("Failed to clear INSCODE tokens: " + str(e))

# ------------------- Duty Mark -------------------
elif page == "Duty Mark":
    st.header("▶️ Duty Mark — generate duties & busy management")
    st.info("This page shows the authoritative Panel data live (from Panel Upload). Any change you make on Panel Upload is shown here immediately.")

    # ALWAYS read the authoritative panel from session state (live)
    panel = st.session_state.panel_df.copy()
    staff = st.session_state.staff_df.copy()
    busy_df = st.session_state.busy_df.copy()
    submap = st.session_state.submap.copy()

    if panel.empty:
        st.info("No panel rows found. Upload / edit on Panel Upload page.")
    else:
        ins_opts = ["All"] + sorted([x for x in panel["INSCODE"].unique() if str(x).strip()!=""])
        nc_opts = ["All"] + sorted([x for x in panel["NCNO"].unique() if str(x).strip()!=""])
        ins_sel = st.selectbox("INSCODE (filter)", ins_opts, index=0)
        nc_sel = st.selectbox("NCNO (filter)", nc_opts, index=0)

        filt = panel.copy()
        if ins_sel != "All":
            filt = filt[filt["INSCODE"].astype(str) == str(ins_sel)]
        if nc_sel != "All":
            filt = filt[filt["NCNO"].astype(str) == str(nc_sel)]

        filt["_parsed_date_from"] = filt["DATE_FROM"].apply(parse_date_flexible)
        filt = filt.sort_values(by="_parsed_date_from", na_position="last").drop(columns=["_parsed_date_from"])

        display_panel = filt.copy()
        display_panel["INTID_NORM"] = display_panel["INTID"].apply(lambda v: normalize_staff_id(v))
        display_panel["INTNAME"] = display_panel["INTID_NORM"].apply(lambda sid: get_staff_name_by_id(st.session_state.staff_df, sid) if sid else "")
        if not submap.empty:
            display_panel = display_panel.merge(submap[["SUBCODE","SUBNAME"]], how="left", on="SUBCODE")
        else:
            display_panel["SUBNAME"] = ""
        display_panel["INTID_with_name"] = display_panel.apply(
            lambda r: (str(r["INTID"]).strip() + (" — " + str(r["INTNAME"]).strip() if str(r["INTNAME"]).strip() else "")),
            axis=1
        )
        # show the live authoritative panel table (will reflect changes made on Panel Upload)
        st.dataframe(display_panel[["INSCODE","NCNO","SUBCODE","SUBNAME","REGL","NOC","NOB","INTID_with_name","EXTID","DATE_FROM","DATE_TO","ERROR"]].fillna(""), height=260)

        st.markdown("### Generate Duty (clean re-run)")
        if st.button("Generate Duty (clean re-run)"):
            try:
                # Clear ERROR for processed rows in authoritative panel
                for idx in filt.index:
                    if idx in st.session_state.panel_df.index:
                        st.session_state.panel_df.at[idx, "ERROR"] = ""
                persist_panel()

                # remove previous markings for those panel rows from staff grid
                staff = st.session_state.staff_df.copy()
                for _, r in filt.iterrows():
                    ins = str(r.get("INSCODE","")).strip()
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if ins and d1 and d2 and d1 <= d2:
                        staff = remove_inscode_from_staff_cells(staff, ins, d1, d2)
                st.session_state.staff_df = staff.copy()
                persist_staff()

                # ensure date columns exist on staff
                dates = set()
                for _, r in filt.iterrows():
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if d1 and d2 and d1 <= d2:
                        for d in daterange(d1,d2):
                            dates.add(date_to_str(d))
                date_cols_needed = sorted(list(dates), key=lambda s: datetime.strptime(s, "%d.%m.%Y")) if dates else []
                for dc in date_cols_needed:
                    if dc not in st.session_state.staff_df.columns:
                        st.session_state.staff_df[dc] = ""
                staff = st.session_state.staff_df.copy()

                # build staff_map
                staff_map = {}
                for idx_s, r in staff.iterrows():
                    sid_norm = normalize_staff_id(r.get("Staff ID"))
                    if sid_norm:
                        staff_map[sid_norm] = idx_s

                audit = []
                error_panel_rows = {}
                total_attempts = total_appends = total_errors = 0

                for idx, r in filt.iterrows():
                    d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                    if d1 is None or d2 is None or d1 > d2:
                        total_errors += 1
                        error_panel_rows.setdefault(idx, set()).add(f"Invalid dates: {r.get('DATE_FROM')} -> {r.get('DATE_TO')}")
                        continue
                    ins = str(r.get("INSCODE","")).strip()
                    if ins == "":
                        total_errors += 1
                        error_panel_rows.setdefault(idx, set()).add("Empty INSCODE")
                        continue
                    for d in daterange(d1, d2):
                        dc = date_to_str(d)
                        total_attempts += 1
                        intid = normalize_staff_id(r.get("INTID"))
                        if intid:
                            if intid not in staff_map:
                                total_errors += 1
                                error_panel_rows.setdefault(idx, set()).add(f"INTID {intid} not found")
                                audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"I","staff_id": intid, "applied":False, "sheet2_before":None, "sheet2_after":None, "timestamp":_now(), "error":"INTID not found"})
                            else:
                                sidx = staff_map[intid]
                                before = staff.at[sidx, dc] if dc in staff.columns else ""
                                after = ("" if before is None or str(before).strip()=="" else str(before).strip() + ",") + ins
                                staff.at[sidx, dc] = after
                                total_appends += 1
                                audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"I","staff_id": intid, "applied":True, "sheet2_before": before, "sheet2_after": after, "timestamp":_now()})
                        else:
                            total_errors += 1
                            error_panel_rows.setdefault(idx, set()).add("INTID empty")
                            audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"I","staff_id": "", "applied":False, "sheet2_before":None, "sheet2_after":None, "timestamp":_now(), "error":"INTID empty"})

                        extid_raw = r.get("EXTID","")
                        extid_norm = normalize_staff_id(extid_raw)
                        if is_zero_like(extid_raw):
                            extid_norm = ""
                        if extid_norm:
                            if extid_norm not in staff_map:
                                total_errors += 1
                                error_panel_rows.setdefault(idx, set()).add(f"EXTID {extid_norm} not found")
                                audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"E","staff_id": extid_norm, "applied":False, "sheet2_before":None, "sheet2_after":None, "timestamp":_now(), "error":"EXTID not found"})
                            else:
                                sidx = staff_map[extid_norm]
                                before = staff.at[sidx, dc] if dc in staff.columns else ""
                                after = ("" if before is None or str(before).strip()=="" else str(before).strip() + ",") + ins
                                staff.at[sidx, dc] = after
                                total_appends += 1
                                audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"E","staff_id": extid_norm, "applied":True, "sheet2_before": before, "sheet2_after": after, "timestamp":_now()})
                        else:
                            audit.append({"allocation_row_index": idx, "date_iso": dc, "role":"E","staff_id": "", "applied":False, "sheet2_before":None, "sheet2_after":None, "timestamp":_now()})

                # write error flags into panel ERROR column and persist panel
                if error_panel_rows:
                    for pidx, reasons in error_panel_rows.items():
                        val = "; ".join(sorted(reasons))
                        if pidx in st.session_state.panel_df.index:
                            st.session_state.panel_df.at[pidx, "ERROR"] = val
                    persist_panel()

                st.session_state.staff_df = staff.copy()
                st.session_state.audit = audit.copy()
                persist_staff()
                st.success("Generate pass completed.")
                st.write(f"Attempts: {total_attempts}  |  Appends: {total_appends}  |  Errors: {total_errors}")

                if error_panel_rows:
                    err_list = []
                    for pidx, reasons in error_panel_rows.items():
                        row = panel.loc[pidx, ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]].to_dict()
                        row["panel_index"] = pidx
                        row["ERROR"] = "; ".join(sorted(reasons))
                        err_list.append(row)
                    st.markdown("### Panel rows with errors (also flagged in panel ERROR column)")
                    st.dataframe(pd.DataFrame(err_list).fillna(""), height=300)
                else:
                    st.info("No panel-level errors detected.")

                if audit:
                    st.markdown("### Audit (recent events)")
                    st.dataframe(pd.DataFrame(audit).fillna("").head(500))
            except Exception as e:
                st.error("Generate failed: " + traceback.format_exc())

        st.markdown("---")
        st.subheader("Busy management (date-range)")
        with st.form("add_busy_form", clear_on_submit=False):
            col1, col2, col3 = st.columns([3,3,2])
            with col1:
                # Build labeled staff dropdown for busy management: "StaffID — Name"
                staff_df_local = st.session_state.staff_df.copy()
                staff_labels = []
                for _, s in staff_df_local.iterrows():
                    sid = normalize_staff_id(s.get("Staff ID"))
                    if not sid:
                        continue
                    name = str(s.get("Name of the Staff","")).strip()
                    staff_labels.append(f"{sid} — {name}")
                staff_options = [""] + sorted(staff_labels)
                busy_staff_label = st.selectbox("Staff (ID — Name)", staff_options, key="busy_staff")
            with col2:
                busy_from = st.date_input("Date From", value=date.today(), key="busy_from")
                busy_to = st.date_input("Date To", value=date.today(), key="busy_to")
            with col3:
                note = st.text_input("NOTE (optional)", key="busy_note")
            submitted = st.form_submit_button("Add Busy record (save & apply to staff cells)")
            if submitted:
                if not busy_staff_label:
                    st.warning("Choose a Staff.")
                else:
                    # extract staff id from label
                    staff_id_selected = busy_staff_label.split("—")[0].strip()
                    staff_id_selected = normalize_staff_id(staff_id_selected)
                    if not staff_id_selected:
                        st.error("Invalid staff selection.")
                    else:
                        if busy_from > busy_to:
                            st.error("DATE_FROM must be <= DATE_TO.")
                        else:
                            new = {"Staff ID": staff_id_selected, "DATE_FROM": date_to_str(busy_from), "DATE_TO": date_to_str(busy_to), "NOTE": note, "__rowid": ""}
                            st.session_state.busy_df = concat_row(st.session_state.busy_df, new)
                            persist_busy()
                            st.session_state.staff_df = apply_busy_to_staff_cells(st.session_state.staff_df, staff_id_selected, busy_from, busy_to, busy_token="B")
                            persist_staff()
                            st.success(f"Busy added for {staff_id_selected} from {date_to_str(busy_from)} to {date_to_str(busy_to)} and applied to staff cells.")

        st.markdown("### Existing busy records (edit / delete)")
        busy_df = st.session_state.busy_df.copy()
        if busy_df.empty:
            st.info("No busy records.")
        else:
            st.dataframe(busy_df[["Staff ID","DATE_FROM","DATE_TO","NOTE"]].fillna(""), height=220)
            st.write("To delete a busy record: enter its Row index (0-based for displayed table) and click Delete.")
            del_idx = st.number_input("Busy row index to delete (0-based)", min_value=0, max_value=max(0, len(busy_df)-1), step=1)
            if st.button("Delete Busy record"):
                try:
                    rec = busy_df.iloc[int(del_idx)]
                    sd = parse_date_flexible(rec["DATE_FROM"]); ed = parse_date_flexible(rec["DATE_TO"])
                    sid = rec["Staff ID"]
                    st.session_state.busy_df = busy_df.drop(busy_df.index[int(del_idx)]).reset_index(drop=True)
                    persist_busy()
                    st.session_state.staff_df = remove_busy_from_staff_cells(st.session_state.staff_df, sid, sd, ed)
                    persist_staff()
                    st.success(f"Deleted busy record for {sid} {date_to_str(sd)}->{date_to_str(ed)} and removed B tokens from staff cells.")
                except Exception as e:
                    st.error("Delete failed: " + str(e))

        st.markdown("---")
        st.subheader("Staff-date view (date columns show INSCODE tokens and B)")
        staff_cols = [c for c in st.session_state.staff_df.columns if c != "__rowid"]
        date_cols = [c for c in staff_cols if isinstance(c, str) and len(c.split("."))==3 and all(part.isdigit() for part in c.split("."))]
        try:
            date_cols_sorted = sorted(date_cols, key=lambda s: datetime.strptime(s, "%d.%m.%Y"))
        except:
            date_cols_sorted = date_cols
        non_date_cols = [c for c in staff_cols if c not in date_cols_sorted]
        show_cols = non_date_cols + date_cols_sorted
        st.dataframe(st.session_state.staff_df[show_cols].fillna(""), height=420)

        st.markdown("---")
        st.subheader("Run Checks")
        if st.button("Run Checks"):
            try:
                staff2 = st.session_state.staff_df.copy()
                staff_cols_all = [c for c in staff2.columns if c != "__rowid"]
                date_cols = [c for c in staff_cols_all if isinstance(c, str) and len(c.split("."))==3 and all(part.isdigit() for part in c.split("."))]
                errors = []
                totals = {"checked_cells":0, "TOO_MANY_ENTRIES":0, "MULTIPLE_UNIQUE_INSCODES":0, "BUSY_WITH_DUTY":0, "total_errors":0}
                for ridx, row in staff2.iterrows():
                    staff_id = str(row.get("Staff ID","")).strip()
                    name = row.get("Name of the Staff","")
                    for dc in date_cols:
                        val = row.get(dc,"")
                        toks = split_tokens(val)
                        if not toks:
                            continue
                        totals["checked_cells"] += 1
                        busy_tokens = [t for t in toks if is_busy_token(t)]
                        unique_ins = []
                        seen = set()
                        for t in toks:
                            if is_busy_token(t):
                                continue
                            v = t.strip()
                            if v not in seen:
                                seen.add(v); unique_ins.append(v)
                        entries_count = len(toks)
                        error_codes = []
                        msgs = []
                        if entries_count >= 3:
                            error_codes.append("TOO_MANY_ENTRIES"); msgs.append(f"{entries_count} entries")
                            totals["TOO_MANY_ENTRIES"] += 1
                        if len(unique_ins) > 1:
                            error_codes.append("MULTIPLE_UNIQUE_INSCODES"); msgs.append("Multiple INSCODEs: " + ",".join(unique_ins))
                            totals["MULTIPLE_UNIQUE_INSCODES"] += 1
                        if busy_tokens and len(unique_ins) > 0:
                            error_codes.append("BUSY_WITH_DUTY"); msgs.append("Busy marker(s) and duty present: " + ",".join(busy_tokens))
                            totals["BUSY_WITH_DUTY"] += 1
                        if error_codes:
                            totals["total_errors"] += 1
                            errors.append({
                                "ExcelRow": ridx + 2,
                                "Staff ID": staff_id,
                                "Name": name,
                                "DateColumn": dc,
                                "CellValue": "" if val is None else str(val),
                                "EntriesCount": entries_count,
                                "UniqueINSCODEs": ",".join(unique_ins),
                                "BusyTokens": ",".join(busy_tokens),
                                "ErrorCodes": ",".join(error_codes),
                                "ErrorMessage": "; ".join(msgs)
                            })
                if errors:
                    df_err = pd.DataFrame(errors)
                    st.error(f"Found {len(df_err)} problematic staff/date cells.")
                    st.dataframe(df_err, height=400)
                    try:
                        with pd.ExcelWriter(CHECK_ERRORS_XLSX, engine="openpyxl") as writer:
                            df_err.to_excel(writer, sheet_name="__CHECK_ERRORS__", index=False)
                        st.success(f"Wrote errors to {CHECK_ERRORS_XLSX}")
                    except Exception as e:
                        st.warning("Failed to write check errors xlsx: " + str(e))
                    st.write("Summary:", {k: v for k, v in totals.items() if k != "total_errors"})
                else:
                    st.success("No errors found on staff/date grid.")
                    st.write("Checked cells:", totals["checked_cells"])
            except Exception as e:
                st.error("Run Checks failed: " + traceback.format_exc())

        st.markdown("---")
        st.subheader("Export per-INSCODE CSVs (separate files)")
        st.write("Each CSV contains only columns: INSCODE,NCNO,SUBCODE,REGL,NOC,NOB,INTID,EXTID,DATE_FROM,DATE_TO")
        all_ins = sorted([x for x in panel["INSCODE"].unique() if str(x).strip()!=""])
        cols_for_export = ["INSCODE","NCNO","SUBCODE","REGL","NOC","NOB","INTID","EXTID","DATE_FROM","DATE_TO"]
        for ins in all_ins:
            out_df = panel[panel["INSCODE"].astype(str) == str(ins)].copy()
            for c in cols_for_export:
                if c not in out_df.columns:
                    out_df[c] = ""
            out_df = out_df[cols_for_export]
            csv_bytes = out_df.to_csv(index=False).encode("utf-8")
            fname = f"panel_{ins}_{EXPORT_MONTH_TAG}.csv"
            st.download_button(f"Download {fname}", data=csv_bytes, file_name=fname, mime="text/csv", key=f"dl_{ins}")

# ------------------- EXTID Allocate -------------------
elif page == "EXTID Allocate":
    st.header("🧾 EXTID Allocate — assign externals")
    st.info("Filter by INSCODE and Department. Suggestions & manual dropdowns include inline status emoji. By default blocked candidates are not applied unless override is enabled.")

    panel = st.session_state.panel_df.copy()
    staff = st.session_state.staff_df.copy()
    submap = st.session_state.submap.copy()

    ins_opts3 = ["All"] + sorted([x for x in panel["INSCODE"].unique() if str(x).strip()!=""])
    ins_sel3 = st.selectbox("INSCODE (All)", ins_opts3, index=0)
    dept_opts3 = ["All"] + sorted([x for x in panel["NCNO"].unique() if str(x).strip()!=""])
    dept_sel3 = st.selectbox("Department / NCNO (All)", dept_opts3, index=0)

    override_allow = st.checkbox("Allow override (enable selecting blocked candidates)", value=False, help="When checked, EXTID assignment may forcibly assign a busy/duty/workshop staff. Use with caution.")

    def get_subname(subcode):
        if submap is None or submap.empty:
            return ""
        m = submap[submap["SUBCODE"].astype(str) == str(subcode)]
        if not m.empty:
            return m.iloc[0]["SUBNAME"]
        return ""

    def needs_ext(r):
        intid = str(r.get("INTID","")).strip()
        extid_raw = r.get("EXTID","")
        ext_empty = (str(extid_raw).strip() == "") or is_zero_like(extid_raw)
        d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
        return intid != "" and ext_empty and (d1 is not None and d2 is not None and d1 <= d2)

    candidates = panel[panel.apply(needs_ext, axis=1)].copy()
    if ins_sel3 != "All":
        candidates = candidates[candidates["INSCODE"].astype(str) == str(ins_sel3)]
    if dept_sel3 != "All":
        candidates = candidates[candidates["NCNO"].astype(str) == str(dept_sel3)]

    candidates["_parsed_date_from"] = candidates["DATE_FROM"].apply(parse_date_flexible)
    candidates = candidates.sort_values(by="_parsed_date_from", na_position="last").drop(columns=["_parsed_date_from"])

    st.metric("Rows needing EXTID (visible)", len(candidates))
    st.metric("Staff rows", len(st.session_state.staff_df))

    # prepare staff rows & stats
    staff_rows = []
    for _, s in st.session_state.staff_df.iterrows():
        sid_norm = normalize_staff_id(s.get("Staff ID"))
        if not sid_norm:
            continue
        staff_rows.append({
            "Staff ID": sid_norm,
            "INSTT": s.get("INSTT",""),
            "dep code": s.get("dep code",""),
            "name": s.get("Name of the Staff",""),
            "designation": s.get("Designation","")
        })
    staff_stats = compute_staff_duty_stats(st.session_state.staff_df)
    busy_list = []
    for _, b in st.session_state.busy_df.iterrows():
        busy_list.append({"Staff ID": normalize_staff_id(b.get("Staff ID")), "DATE_FROM": b.get("DATE_FROM"), "DATE_TO": b.get("DATE_TO"), "NOTE": b.get("NOTE","")})

    def build_candidate_entries_for_row(row):
        ins = str(row.get("INSCODE","")).strip()
        dept = str(row.get("NCNO","")).strip()
        d1 = parse_date_flexible(row.get("DATE_FROM")); d2 = parse_date_flexible(row.get("DATE_TO"))
        if not (d1 and d2):
            return []
        req_dates = [date_to_str(d) for d in daterange(d1, d2)]
        out = []
        for s in staff_rows:
            if s["INSTT"] == ins:
                continue
            if dept and str(s["dep code"]).strip() and str(s["dep code"]).strip() != str(dept).strip():
                continue
            sid = s["Staff ID"]
            stats_entry = staff_stats.get(sid, {"duty_count":0, "date_tokens":{}, "INSTT": s["INSTT"], "dep_code": s["dep code"], "name": s.get("name",""), "designation": s.get("designation","")})
            duty_count = stats_entry.get("duty_count", 0)
            busy_for_staff = [br for br in busy_list if br.get("Staff ID") == sid]
            is_free, conflicts, busy_overlaps = availability_for_req_dates(stats_entry, req_dates, busy_records=busy_for_staff)
            workshop_flag = is_workshop_designation(s.get("designation",""))
            blocked = False
            status_text = "FREE"
            emoji = "🟢"
            reason = ""
            if busy_overlaps:
                blocked = True
                status_text = "BUSY"
                emoji = "🔴"
                reason = ",".join(busy_overlaps)
            elif conflicts:
                blocked = True
                status_text = "DUTY"
                emoji = "🔴"
                reason = ",".join(conflicts)
            elif workshop_flag:
                blocked = True
                status_text = "WORKSHOP"
                emoji = "🟡"
                reason = "workshop"
            label = f"{emoji} {sid} — {s.get('name','')} — {s.get('designation','')} — INST:{s.get('INSTT','')} — DEP:{s.get('dep code','')} — duties:{duty_count} — {status_text}"
            out.append({
                "staff_id": sid,
                "label": label,
                "blocked": blocked,
                "status": status_text,
                "reason": reason,
                "duty_count": duty_count,
                "name": s.get("name",""),
                "designation": s.get("designation",""),
                "INSTT": s.get("INSTT",""),
                "dep_code": s.get("dep code","")
            })
        # sort by duty_count then id
        return sorted(out, key=lambda x: (x["duty_count"], x["staff_id"]))

    if candidates.empty:
        st.info("No rows require EXTID (for selected filters).")
    else:
        for _, row in candidates.reset_index().iterrows():
            pidx = int(row["index"])
            subcode = row.get("SUBCODE","")
            subname = get_subname(subcode) if 'get_subname' in locals() else ""
            cols = st.columns([3,5,3,1,1])
            with cols[0]:
                display_sub = f" — Subname: {subname}" if subname else ""
                noc = row.get("NOC","")
                intid_raw = row.get("INTID","")
                intid_norm = normalize_staff_id(intid_raw)
                intname = get_staff_name_by_id(st.session_state.staff_df, intid_norm) if intid_norm else ""
                int_display = str(intid_raw).strip()
                if intname:
                    int_display = int_display + " — " + intname
                st.markdown(
                    f"**Row {pidx}** • INSCODE **{row.get('INSCODE')}** • NCNO **{row.get('NCNO')}** • SUBCODE **{row.get('SUBCODE')}**{display_sub} • NOC **{noc}** • {row.get('DATE_FROM')} → {row.get('DATE_TO')}  \n"
                    f"**INT:** {int_display}"
                )
            with cols[1]:
                entries = build_candidate_entries_for_row(row)
                if entries:
                    # Build suggestion dropdown options with inline emoji/status
                    sugg_options = [""] + [e["label"] for e in entries]
                    sel_key = f"sugg_{pidx}_select"
                    sel = st.selectbox(f"Suggestions — Row {pidx}", options=sugg_options, key=sel_key)
                else:
                    sel = ""
                    st.caption("No suggestions (no eligible staff found for filters).")
            with cols[2]:
                entries = build_candidate_entries_for_row(row)
                if entries:
                    manual_options = [""] + [e["label"] for e in entries]
                    man_key = f"man_{pidx}_select"
                    manual_sel = st.selectbox(f"Manual — Row {pidx}", options=manual_options, key=man_key)
                else:
                    manual_sel = ""
            with cols[3]:
                staged = st.session_state.panel_df.at[pidx,"EXTID"] if pidx in st.session_state.panel_df.index else ""
                if staged and str(staged).strip() != "" and not is_zero_like(staged):
                    st.success("✅")
                else:
                    st.write("◻️")
            with cols[4]:
                if st.button("Apply", key=f"apply_{pidx}"):
                    chosen_label = ""
                    # preference: suggestion selectbox then manual
                    if 'sel' in locals() and sel and str(sel).strip() != "":
                        chosen_label = sel
                    elif 'manual_sel' in locals() and manual_sel and str(manual_sel).strip() != "":
                        chosen_label = manual_sel
                    else:
                        st.warning("Choose suggestion or manual staff.")
                        continue

                    # extract staff id
                    parts = chosen_label.split("—")
                    if len(parts) == 0 or not parts[0].strip():
                        st.error("Selected label does not contain staff id.")
                        continue
                    # first token includes emoji and id: e.g. "🟢 42218301 "
                    first_token = parts[0].strip()
                    # remove emoji prefix if present
                    sid_candidate = first_token
                    # remove any leading emoji and space
                    sid_candidate = re.sub(r"^[^\w\d]+", "", sid_candidate).strip()
                    staff_id_only_norm = normalize_staff_id(sid_candidate)
                    if not staff_id_only_norm:
                        st.error("Invalid staff id selected.")
                        continue

                    # find chosen entry to inspect blocked status
                    entries = build_candidate_entries_for_row(row)
                    chosen_entry = next((e for e in entries if e["staff_id"] == staff_id_only_norm), None)

                    if chosen_entry is not None and chosen_entry["blocked"] and (not override_allow):
                        st.error(f"Selected staff {staff_id_only_norm} is blocked ({chosen_entry['status']}). Enable override to force assign.")
                        if pidx in st.session_state.panel_df.index:
                            prev = st.session_state.panel_df.at[pidx, "ERROR"]
                            newerr = (str(prev) + "; " if str(prev).strip() else "") + f"EXT apply blocked:{chosen_entry['status']}"
                            st.session_state.panel_df.at[pidx, "ERROR"] = newerr
                            persist_panel()
                        continue

                    ins = str(row.get("INSCODE","")).strip()
                    d1 = parse_date_flexible(row.get("DATE_FROM")); d2 = parse_date_flexible(row.get("DATE_TO"))
                    if not ins or d1 is None or d2 is None or d1 > d2:
                        st.error("Invalid panel row dates or INSCODE — cannot apply.")
                        continue

                    # prepare staff2 and ensure date cols
                    staff2 = st.session_state.staff_df.copy()
                    for d in daterange(d1, d2):
                        dc = date_to_str(d)
                        if dc not in staff2.columns:
                            staff2[dc] = ""

                    # remove previous INSCODE tokens for this panel row across all staff
                    staff2 = remove_inscode_from_staff_cells(staff2, ins, d1, d2)

                    # find or add staff row
                    mask = staff2["Staff ID"].astype(str).str.upper() == staff_id_only_norm.upper()
                    if not mask.any():
                        new = {c:"" for c in staff2.columns}
                        new["Staff ID"] = staff_id_only_norm
                        staff2 = concat_row(staff2, new)
                        mask = staff2["Staff ID"].astype(str).str.upper() == staff_id_only_norm.upper()
                    sidx = staff2[mask].index[0]

                    # check explicit busy records
                    busy_for_this = [br for br in busy_list if br["Staff ID"] == staff_id_only_norm]
                    busy_conflicts = []
                    for br in busy_for_this:
                        bfrom = parse_date_flexible(br["DATE_FROM"]); bto = parse_date_flexible(br["DATE_TO"])
                        if bfrom and bto:
                            for d in daterange(d1, d2):
                                if bfrom <= d <= bto:
                                    busy_conflicts.append(f"{date_to_str(bfrom)}->{date_to_str(bto)}")
                                    break
                    if busy_conflicts and (not override_allow):
                        st.error(f"Cannot apply EXTID {staff_id_only_norm}: busy on {', '.join(busy_conflicts)}. Enable override to force.")
                        if pidx in st.session_state.panel_df.index:
                            prev = st.session_state.panel_df.at[pidx, "ERROR"]
                            newerr = (str(prev) + "; " if str(prev).strip() else "") + f"EXT apply failed busy_rec:{','.join(busy_conflicts)}"
                            st.session_state.panel_df.at[pidx, "ERROR"] = newerr
                            persist_panel()
                        continue

                    # check tokens (non-B)
                    busy_found = []
                    for d in daterange(d1, d2):
                        dc = date_to_str(d)
                        val = staff2.at[sidx, dc] if dc in staff2.columns else ""
                        toks = split_tokens(val)
                        if any(not is_busy_token(t) for t in toks):
                            busy_found.append(dc)
                    if busy_found and (not override_allow):
                        st.error(f"Cannot apply EXTID {staff_id_only_norm}: already has duty token(s) on {', '.join(busy_found)}. Enable override to force.")
                        if pidx in st.session_state.panel_df.index:
                            prev = st.session_state.panel_df.at[pidx, "ERROR"]
                            newerr = (str(prev) + "; " if str(prev).strip() else "") + f"EXT apply failed busy_tok:{','.join(busy_found)}"
                            st.session_state.panel_df.at[pidx, "ERROR"] = newerr
                            persist_panel()
                        continue

                    # Append INSCODE for each date to chosen EXT staff
                    for d in daterange(d1, d2):
                        dc = date_to_str(d)
                        cur = staff2.at[sidx, dc] if dc in staff2.columns else ""
                        cur_s = "" if cur is None else str(cur).strip()
                        if cur_s == "":
                            staff2.at[sidx, dc] = ins
                        else:
                            staff2.at[sidx, dc] = cur_s + "," + ins

                    # Also append to INTID if present
                    intid = normalize_staff_id(row.get("INTID"))
                    if intid:
                        mask_i = staff2["Staff ID"].astype(str).str.upper() == intid.upper()
                        if not mask_i.any():
                            new = {c:"" for c in staff2.columns}
                            new["Staff ID"] = intid
                            staff2 = concat_row(staff2, new)
                            mask_i = staff2["Staff ID"].astype(str).str.upper() == intid.upper()
                        iidx = staff2[mask_i].index[0]
                        for d in daterange(d1, d2):
                            dc = date_to_str(d)
                            cur = staff2.at[iidx, dc] if dc in staff2.columns else ""
                            cur_s = "" if cur is None else str(cur).strip()
                            if cur_s == "":
                                staff2.at[iidx, dc] = ins
                            else:
                                staff2.at[iidx, dc] = cur_s + "," + ins

                    # persist EXTID in panel row
                    if pidx in st.session_state.panel_df.index:
                        st.session_state.panel_df.at[pidx, "EXTID"] = staff_id_only_norm
                        prev_err = st.session_state.panel_df.at[pidx, "ERROR"]
                        if prev_err and "EXT apply failed" in prev_err:
                            parts = [pt for pt in str(prev_err).split(";") if "EXT apply failed" not in pt]
                            st.session_state.panel_df.at[pidx, "ERROR"] = ";".join([p.strip() for p in parts if p.strip()!=""])
                        persist_panel()

                    # persist staff
                    st.session_state.staff_df = staff2.copy()
                    persist_staff()

                    st.success(f"✅ Applied EXTID {staff_id_only_norm} and saved. INSCODE {ins} marked for {date_to_str(d1)} → {date_to_str(d2)}")

        st.markdown("---")
        if st.button("Commit staged EXTIDs to Staffdata"):
            panel2 = st.session_state.panel_df.copy()
            staff2 = st.session_state.staff_df.copy()
            staff_map = {}
            for idx_s, r in staff2.iterrows():
                sid_norm = normalize_staff_id(r.get("Staff ID"))
                if sid_norm:
                    staff_map[sid_norm] = idx_s

            fails = []
            commits = 0
            for idx, r in panel2.iterrows():
                ext_raw = str(r.get("EXTID","")).strip()
                ext_norm = normalize_staff_id(ext_raw)
                if not ext_norm:
                    fails.append({"panel_index": idx, "staff": ext_raw, "reason": "invalid_staff_id"})
                    continue
                ins = str(r.get("INSCODE","")).strip()
                d1 = parse_date_flexible(r.get("DATE_FROM")); d2 = parse_date_flexible(r.get("DATE_TO"))
                for d in daterange(d1, d2):
                    dc = date_to_str(d)
                    if dc not in staff2.columns:
                        staff2[dc] = ""
                    if ext_norm not in staff_map:
                        new = {c:"" for c in staff2.columns}
                        new["Staff ID"] = ext_norm
                        staff2 = concat_row(staff2, new)
                        staff_map[ext_norm] = staff2.index.max()
                    sidx = staff_map[ext_norm]
                    cur = staff2.at[sidx, dc] if dc in staff2.columns else ""
                    if split_tokens(cur):
                        fails.append({"panel_index": idx, "staff": ext_norm, "date": dc, "reason":"busy"})
                    else:
                        if cur is None or str(cur).strip()=="":
                            staff2.at[sidx, dc] = ins
                        else:
                            staff2.at[sidx, dc] = str(cur).strip() + "," + ins
                        commits += 1
            st.session_state.staff_df = staff2.copy()
            persist_staff()
            st.success(f"Committed {commits} appended duties.")
            if fails:
                st.error(f"{len(fails)} commits failed (invalid ids or busy).")
                st.dataframe(pd.DataFrame(fails))

# ---------- END ----------
