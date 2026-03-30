"""
Flipkart (Client) Debit Master Sync
1. Creates a copy of source sheet: fixed title for replace-on-rerun
2. Analyzes the copied sheet
3. Pushes summary to destination (ODH Debit Master worksheet)
4. Sends to WhatsApp: Total Debit, Recovery Pending, Recovered (all hub statuses on sheet)

Source: https://docs.google.com/spreadsheets/d/1sUK3d2abb6EbGBU5O8rIKSp38fPlZMjkL9YM2vl4azA/edit
Destination: https://docs.google.com/spreadsheets/d/1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM/edit (worksheet: ODH Debit Master)

Usage:
    python flipkart_debit_master_sync.py              # Copy → Analyze → Push → WhatsApp
    python flipkart_debit_master_sync.py --raw        # Push raw data instead of summary
    python flipkart_debit_master_sync.py --input f.xlsx
    python flipkart_debit_master_sync.py --no-whatsapp   # Skip WhatsApp send
    python flipkart_debit_master_sync.py --no-email      # Skip Recovery Pending email
    python flipkart_debit_master_sync.py --pending-include-closed-hubs   # Recovery Pending raw: include closed hubs (default: active only)
"""

import argparse
import os
import warnings
from pathlib import Path

try:
    import pandas as pd
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import set_with_dataframe
except ImportError:
    print("Install: pip install pandas gspread google-auth gspread-dataframe")
    raise

from odh_hub_status_region_map import attachment_status_region, hub_is_active_for_report

SCRIPT_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = SCRIPT_DIR / "service_account_key.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Source: Flipkart Debit master (must be shared with service account as Viewer)
SOURCE_SHEET_ID = "1sUK3d2abb6EbGBU5O8rIKSp38fPlZMjkL9YM2vl4azA"

# Destination: ODH / Flipkart reports
_DEFAULT_DEST_ID = "1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM"
DEST_SHEET_ID = os.getenv("FLIPKART_ODH_SPREADSHEET_ID") or _DEFAULT_DEST_ID
DEST_WORKSHEET_NAME = "ODH Debit Master"

# WhatsApp: fallback row bands if layout is not the standard 3 pivot tables; primary bands come from push_to_destination
DEBIT_MASTER_WA_ROW_BANDS = ((1, 29), (33, 60), (64, 91))
DEBIT_MASTER_WA_MAX_COL_SCAN = 52
TABLE_VERTICAL_GAP = 3  # blank rows between stacked pivot tables on ODH Debit Master

# Fixed copy title - replaces existing copy instead of creating new each run
COPY_SHEET_TITLE = "Copy of Flipkart Debit Master - Analysis"

# Client-Model column must match this value (case-insensitive after strip)
# Base Data / ODH use "FK-ODH", not "FLIPKART"
_CLIENT_MODEL_FILTER = "FK-ODH"


def read_source_data(gc, source_id: str, worksheet_name: str | None = None):
    """Read all data from source sheet via gspread. Returns DataFrame or None."""
    sh = gc.open_by_key(source_id)
    if worksheet_name:
        ws = sh.worksheet(worksheet_name)
    else:
        ws = sh.sheet1  # First worksheet (gid=0)
    print(f"  Reading from: {sh.title} / {ws.title}")
    raw = ws.get_all_values()
    if not raw or len(raw) < 1:
        print("  [WARN] No data in source")
        return None
    headers = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=headers)
    df = df.dropna(how="all")
    print(f"  Extracted: {len(df):,} rows, {len(df.columns)} columns")
    return df


def read_sheet_via_drive_export(copy_id: str, creds) -> pd.DataFrame | None:
    """Read sheet data via Drive API export (no Sheets API needed)."""
    try:
        from googleapiclient.discovery import build
        from io import StringIO
    except ImportError:
        return None
    try:
        drive = build("drive", "v3", credentials=creds)
        data = drive.files().export(fileId=copy_id, mimeType="text/csv").execute()
        if not data:
            return None
        df = pd.read_csv(StringIO(data.decode("utf-8")), on_bad_lines="skip")
        df = df.dropna(how="all")
        print(f"  Extracted via Drive API: {len(df):,} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"  [ERROR] Drive export failed: {e}")
        return None


# Meesho-style codes treated as Active when not in FK-ODH attachment map
_ACTIVE_HUBS = frozenset({"MQR", "MQE", "MHK", "YLZ", "YLG"})
# ODH Debit Master pivots: include these Status values (Closed/Inactive hubs appear on the output sheet)
_DEBIT_MASTER_STATUS_INCLUDE = frozenset({"Active", "Closed", "Inactive"})
# Sort: Active rows first, then Closed, then Inactive; descending by Total within each group
_HUB_STATUS_SORT_ORDER = {"Active": 0, "Closed": 1, "Inactive": 2}
# Pivot Total column = sum of month columns from Apr-25 through Mar-26 only (2024-25 and other months excluded)
_TOTAL_PIVOT_SUM_FIRST_MONTH = pd.Timestamp("2025-04-01")
_TOTAL_PIVOT_SUM_LAST_MONTH = pd.Timestamp("2026-03-01")

# Recovery Pending sheet: include waybills with Addition Date on or after Apr 1 2025 (no upper limit)
_PENDING_RECOVERY_MIN_DATE = pd.Timestamp("2025-04-01")

# Hub name overrides: map these to the given code (merge hubs into one)
_HUB_CODE_OVERRIDES = {
    "MAR": "MQR",
    "MARATHALLI": "MQR",
    "BOM": "BMN",
    "ECY": "ECQ",
    "JPN": "JPW",
    "KOR": "LSK",
}
# Same physical hub, alternative spelling / legacy name in source → canonical pivot label
_HUB_FULL_NAME_MERGE = {
    "SAIDABADSPILTODH": "SAIDABADSPLITODH_HYD",
}


def _normalize_hub_code(name) -> str:
    """Extract 3-letter hub code from hub name. Handles BLS-BOM, S2/BLS/6/BMN, BMN, ylg."""
    s = str(name).strip()
    if not s:
        return ""
    # Split by / or - and take the last segment (e.g. BLS-BOM→BOM, S2/BLS/6/BMN→BMN)
    parts = s.replace("-", "/").split("/")
    last = parts[-1].strip().upper() if parts else ""
    code = last if last else s.upper()
    code = _HUB_FULL_NAME_MERGE.get(code, code)
    # Apply overrides (e.g. MAR, MARATHALLI -> MQR)
    return _HUB_CODE_OVERRIDES.get(code, code)


def _debit_master_total_sum_columns(date_cols: list) -> list[str]:
    """Period column labels included in the Total column: Apr-25 … Mar-26 only."""
    out: list[str] = []
    for c in date_cols:
        label = str(c).strip()
        if label == "2024-25":
            continue
        ts = None
        for fmt in ("%b-%y", "%d-%b-%y"):
            try:
                ts = pd.to_datetime(label, format=fmt)
                break
            except (ValueError, TypeError):
                continue
        if ts is None or pd.isna(ts):
            continue
        month_start = pd.Timestamp(ts.year, ts.month, 1)
        if _TOTAL_PIVOT_SUM_FIRST_MONTH <= month_start <= _TOTAL_PIVOT_SUM_LAST_MONTH:
            out.append(c)
    return out


def _omit_hub_pivot_row(val) -> bool:
    """True if this hub label should not appear as a row in Debit/Pending pivots."""
    s = str(val).strip()
    s_cmp = s.upper().replace("'", "").replace("’", "")
    if s_cmp == "DONT KNOW":
        return True
    if s.strip().upper() in ("ACTIVE HUBS", "CLOSED HUBS"):
        return True
    return False


def _pick_detail_region_column(df: pd.DataFrame) -> str | None:
    """Pick a per-hub geography column (e.g. city), not the sheet-wide 'Region' filter (often South)."""
    cols_lower = [(c, str(c).lower().strip()) for c in df.columns]
    for c, cl in cols_lower:
        if cl == "region":
            continue
        for key in (
            "sub region",
            "operating region",
            "hub region",
            "city",
            "location",
            "zone",
            "cluster",
            "geography",
            "state",
            "branch",
        ):
            if key in cl:
                return c
    for c, cl in cols_lower:
        if "region" in cl and cl != "region":
            return c
    return "Region" if "Region" in df.columns else None


def _first_non_empty_mode(s: pd.Series) -> str:
    s = s.dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return ""
    return str(s.value_counts().index[0])


def _hub_code_to_region_map(df: pd.DataFrame) -> dict:
    if "_hub_code" not in df.columns:
        return {}
    col = _pick_detail_region_column(df)
    if col is None or col not in df.columns:
        return {}
    return df.groupby("_hub_code", sort=False)[col].apply(_first_non_empty_mode).to_dict()


def _build_hub_pivot(
    df: pd.DataFrame,
    value_col: str,
    date_col_candidates: list[str],
    date_header: str,
) -> pd.DataFrame:
    """Shared pivot logic: hub (rows) x date (columns), values = value_col. Returns formatted pivot."""
    df = df.copy()
    if value_col not in df.columns:
        return pd.DataFrame(columns=[date_header])
    df[value_col] = pd.to_numeric(df[value_col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
    hub_col = next((c for c in df.columns if "hub" in c.lower() and "name" in c.lower()), None) or ("Hub Name" if "Hub Name" in df.columns else None)
    if not hub_col:
        hub_col = next((c for c in df.columns if "hub" in c.lower()), None)
    df["_hub_code"] = df[hub_col].astype(str).apply(_normalize_hub_code)
    df = df[df["_hub_code"].str.len() >= 2]
    if "Client-Model" in df.columns:
        df = df[df["Client-Model"].astype(str).str.strip().str.upper() == _CLIENT_MODEL_FILTER]
    if "Region" in df.columns:
        df = df[df["Region"].astype(str).str.strip().str.upper() == "SOUTH"]
    if df.empty:
        return pd.DataFrame(columns=[date_header])
    date_cols_found = []
    for candidate in date_col_candidates:
        c = next((col for col in df.columns if candidate in col.lower()), None)
        if c and c not in date_cols_found:
            date_cols_found.append(c)
    if not date_cols_found:
        return pd.DataFrame(columns=[date_header])
    primary = date_cols_found[0]

    def _parse_dates(ser):
        with warnings.catch_warnings(action="ignore", category=UserWarning):
            try:
                return pd.to_datetime(ser, errors="coerce", dayfirst=True, format="mixed")
            except TypeError:
                return pd.to_datetime(ser, errors="coerce", dayfirst=True)

    df["_dt"] = _parse_dates(df[primary])
    for fallback in date_cols_found[1:]:
        mask = df["_dt"].isna() & df[fallback].notna()
        df.loc[mask, "_dt"] = _parse_dates(df.loc[mask, fallback])
    df = df[df["_dt"].notna()]
    df = df[df["_dt"].dt.year >= 2024]
    range_start, range_end = pd.Timestamp("2024-04-01"), pd.Timestamp("2025-03-31")
    month_group_start = pd.Timestamp("2025-04-01")  # From Apr-25 onward, group by month

    def _to_date_label(dt):
        if range_start <= dt <= range_end:
            return "2024-25"
        if dt >= month_group_start:
            return dt.strftime("%b-%y")  # Apr-25, May-25, etc. (month-wise)
        return dt.strftime("%d-%b-%y")

    def _to_sort_key(dt):
        if range_start <= dt <= range_end:
            return range_start
        if dt >= month_group_start:
            return pd.Timestamp(dt.year, dt.month, 1)  # First of month for sorting
        return dt

    df["_date_label"] = df["_dt"].apply(_to_date_label)
    df["_sort_key"] = df["_dt"].apply(_to_sort_key)
    # Pivot: hub (rows) x date (columns)
    pivot = df.pivot_table(index="_hub_code", columns="_date_label", values=value_col, aggfunc="sum", fill_value=0)
    date_order = df.groupby("_date_label")["_sort_key"].min().sort_values()
    date_cols = date_order.index.tolist()
    pivot = pivot.reindex(columns=date_cols, fill_value=0)
    pivot = pivot.reset_index()
    hub_col_name = "_hub_code" if "_hub_code" in pivot.columns else pivot.columns[0]
    pivot = pivot.rename(columns={hub_col_name: date_header})
    row_label = date_header
    pivot = pivot[~pivot[row_label].map(_omit_hub_pivot_row)]
    if pivot.empty:
        return pd.DataFrame(columns=[row_label, "Status", "Region"] + date_cols + ["Total"])
    region_map = _hub_code_to_region_map(df)
    status_list: list[str] = []
    region_list: list[str] = []
    for h in pivot[row_label]:
        att = attachment_status_region(h)
        if att:
            status_list.append(att[0])
            region_list.append(att[1])
        else:
            status_list.append("Active" if str(h).strip() in _ACTIVE_HUBS else "Closed")
            region_list.append(str(region_map.get(h, "") or "").strip())
    pivot["Status"] = status_list
    pivot["Region"] = region_list
    pivot = pivot[pivot["Status"].isin(_DEBIT_MASTER_STATUS_INCLUDE)].copy()
    if pivot.empty:
        return pd.DataFrame(columns=[row_label, "Status", "Region"] + date_cols + ["Total"])
    total_sum_cols = _debit_master_total_sum_columns(date_cols)
    if total_sum_cols:
        pivot["Total"] = pivot[total_sum_cols].sum(axis=1)
    else:
        pivot["Total"] = 0
    pivot["_st_ord"] = pivot["Status"].map(lambda s: _HUB_STATUS_SORT_ORDER.get(str(s).strip(), 9))
    pivot = pivot.sort_values(by=["_st_ord", "Total"], ascending=[True, False]).drop(columns=["_st_ord"])
    pivot = pivot[[row_label, "Status", "Region"] + date_cols + ["Total"]]
    grand = float(pivot[total_sum_cols].to_numpy().sum()) if total_sum_cols else 0.0
    total_row = {
        row_label: "Total",
        "Status": "",
        "Region": "",
        **{c: float(pivot[c].sum()) for c in date_cols},
        "Total": grand,
    }
    pivot = pd.concat([pivot, pd.DataFrame([total_row])], ignore_index=True)
    return pivot


def analyze_debit_data(df: pd.DataFrame, analysis_date: str) -> pd.DataFrame:
    """Pivot: Total Debit (rows) x Hub (columns), values = Debit Value ₹."""
    debit_col = next((c for c in df.columns if "debit" in c.lower() and "value" in c.lower()), None) or ("Debit Value ₹" if "Debit Value ₹" in df.columns else None)
    if not debit_col:
        return pd.DataFrame(columns=["Total Debit", "Status", "Region", "Total"])
    return _build_hub_pivot(df, debit_col, ["addition date"], "Total Debit")


def analyze_recovered_data(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: Recovered Date (rows) x Hub (columns), values = Recovered Amount. Fallback to Addition Date if no Recovered Date.
    Removes rows where Total is 0."""
    recovered_col = next((c for c in df.columns if "recovered" in c.lower() and "amount" in c.lower()), None) or ("Recovered Amount" if "Recovered Amount" in df.columns else None)
    if not recovered_col:
        return pd.DataFrame(columns=["Recovered Date"])
    pivot = _build_hub_pivot(df, recovered_col, ["recovered date", "addition date"], "Recovered")
    if pivot.empty:
        return pivot
    # Remove rows with Total 0 (keep Total row only among specials)
    hub_col = "Recovered"
    data_rows = pivot[~pivot[hub_col].astype(str).str.strip().isin(["Total", "Active Hubs", "Closed Hubs"])]
    data_rows = data_rows[data_rows["Total"] != 0]
    data_rows = data_rows[data_rows["Status"].isin(_DEBIT_MASTER_STATUS_INCLUDE)]
    if data_rows.empty:
        return pd.DataFrame(columns=list(pivot.columns))
    date_cols = [c for c in pivot.columns if c not in (hub_col, "Status", "Region", "Total")]
    # Remove columns where Total (column sum) is 0
    col_sums = data_rows[date_cols].sum()
    date_cols = [c for c in date_cols if col_sums[c] != 0]
    if not date_cols:
        return pd.DataFrame(columns=list(pivot.columns))
    dr = data_rows[[hub_col, "Status", "Region"] + date_cols + ["Total"]].copy()
    dr["_st_ord"] = dr["Status"].map(lambda s: _HUB_STATUS_SORT_ORDER.get(str(s).strip(), 9))
    dr = dr.sort_values(by=["_st_ord", "Total"], ascending=[True, False]).drop(columns=["_st_ord"])
    total_row = {
        hub_col: "Total",
        "Status": "",
        "Region": "",
        **{c: dr[c].sum() for c in date_cols},
        "Total": float(dr["Total"].sum()),
    }
    return pd.concat([dr, pd.DataFrame([total_row])], ignore_index=True)


def analyze_pending_data(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: Recovery Pending (rows) x Hub (columns), values = Pending Amount. No date filter - includes all (2024-25, Apr-25, etc)."""
    pending_col = next((c for c in df.columns if "pending" in c.lower() and "amount" in c.lower()), None)
    if not pending_col:
        pending_col = next((c for c in df.columns if "recovery" in c.lower() and "pending" in c.lower()), None)
    if not pending_col:
        pending_col = "Pending Amount" if "Pending Amount" in df.columns else None
    if not pending_col:
        return pd.DataFrame(columns=["Recovery Pending"])
    return _build_hub_pivot(df, pending_col, ["addition date"], "Recovery Pending")


def _is_valid_cell(val) -> bool:
    """True if value is non-empty and not NaN/'nan'."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    s = str(val).strip().strip("'\"").lower()
    return s != "" and s not in ("nan", "nat", "none", "#n/a", "#na")


def get_recovery_pending_raw(df: pd.DataFrame, include_closed_hubs: bool = False, apply_date_cutoff: bool = True) -> pd.DataFrame:
    """Raw rows with Pending Amount > 0 and valid Addition Date + Tracking ID."""
    df = df.copy()
    if "Client-Model" in df.columns:
        df = df[df["Client-Model"].astype(str).str.strip().str.upper() == _CLIENT_MODEL_FILTER]
    if "Region" in df.columns:
        df = df[df["Region"].astype(str).str.strip().str.upper() == "SOUTH"]
    pending_col = next((c for c in df.columns if "pending" in c.lower() and "amount" in c.lower()), None)
    if not pending_col:
        pending_col = next((c for c in df.columns if "recovery" in c.lower() and "pending" in c.lower()), None)
    if not pending_col:
        pending_col = "Pending Amount" if "Pending Amount" in df.columns else None
    if not pending_col:
        print("  [WARN] No pending amount column found (tried: pending+amount, recovery+pending)")
        return pd.DataFrame(columns=["S. No", "Addition Date", "Tracking ID", "Hub Name", "Debit Value ₹", "Pending Amount"])
    # Strip currency symbols and commas before numeric conversion
    df[pending_col] = pd.to_numeric(
        df[pending_col].astype(str).str.replace(r"[,₹\s]", "", regex=True),
        errors="coerce"
    ).fillna(0)
    df = df[df[pending_col] > 0]
    if df.empty:
        print("  [INFO] Pending Recovery: no rows with Pending Amount > 0")
        return pd.DataFrame(columns=["S. No", "Addition Date", "Tracking ID", "Hub Name", "Debit Value ₹", "Pending Amount"])
    n_before = len(df)
    add_col = next((c for c in df.columns if "addition" in c.lower() and "date" in c.lower()), None)
    track_col = (
        next((c for c in df.columns if "tracking" in c.lower() and "id" in c.lower()), None)
        or next((c for c in df.columns if "tracking" in c.lower()), None)
        or next((c for c in df.columns if "waybill" in c.lower() and "no" in c.lower()), None)
        or next((c for c in df.columns if "waybill" in c.lower()), None)
    )
    hub_col = next((c for c in df.columns if "hub" in c.lower() and "name" in c.lower()), None) or next((c for c in df.columns if "hub" in c.lower()), None)
    debit_col = next((c for c in df.columns if "debit" in c.lower() and "value" in c.lower()), None)
    # Filter out rows with invalid/empty Addition Date; require Tracking ID OR Waybill No (whichever we use)
    if add_col:
        df = df[df[add_col].apply(_is_valid_cell)]
    if track_col:
        df = df[df[track_col].apply(_is_valid_cell)]
    n_after_valid = len(df)
    if add_col and track_col:
        print(f"  [INFO] Pending Recovery: {n_after_valid} rows with valid Addition Date + Tracking ID before date filter")
    # Include only Addition Date on or after Apr 1 2025 (Apr-25, May-25, etc.; no upper limit) unless --pending-include-all-dates
    if add_col and apply_date_cutoff:
        def _parse_add_dates(ser):
            with warnings.catch_warnings(action="ignore", category=UserWarning):
                try:
                    return pd.to_datetime(ser, errors="coerce", dayfirst=True, format="mixed")
                except TypeError:
                    return pd.to_datetime(ser, errors="coerce", dayfirst=True)
        df["_add_dt"] = _parse_add_dates(df[add_col])
        n_parse_fail = df["_add_dt"].isna().sum()
        df = df[df["_add_dt"].notna() & (df["_add_dt"] >= _PENDING_RECOVERY_MIN_DATE)]
        df = df.drop(columns=["_add_dt"], errors="ignore")
        n_after_date = len(df)
        if n_parse_fail > 0:
            print(f"  [INFO] Pending Recovery: {n_parse_fail} rows had unparseable Addition Date")
        if n_after_date < n_after_valid:
            print(f"  [INFO] Pending Recovery: {n_after_valid - n_after_date} rows excluded (Addition Date before Apr 1 2025)")
        if n_after_date > 0:
            print(f"  [INFO] Pending Recovery: {n_after_date} rows with Addition Date >= Apr 1 2025")
    # Exclude closed hubs (attachment Status + Meesho-style MQR/MQE/MHK/YLZ/YLG) unless include_closed_hubs
    if hub_col and not include_closed_hubs:
        df["_hub_code"] = df[hub_col].astype(str).apply(_normalize_hub_code)
        n_before_hub = len(df)
        df = df[df["_hub_code"].apply(hub_is_active_for_report)]
        df = df.drop(columns=["_hub_code"], errors="ignore")
        if len(df) < n_before_hub:
            print(f"  [INFO] Pending Recovery: {n_before_hub - len(df)} rows excluded (closed / inactive hubs)")
    if df.empty and n_before > 0:
        print(f"  [INFO] Pending Recovery: all {n_before} rows filtered out (date/hub filters)")
    if df.empty:
        return pd.DataFrame(columns=["S. No", "Addition Date", "Tracking ID", "Hub Name", "Debit Value ₹", "Pending Amount"])
    df = df.reset_index(drop=True)  # Avoid index misalignment when building out
    out = pd.DataFrame()
    out["S. No"] = range(1, len(df) + 1)
    out["Addition Date"] = df[add_col].astype(str).str.strip().values if add_col else ""
    out["Tracking ID"] = df[track_col].astype(str).str.strip().values if track_col else ""
    out["Hub Name"] = (
        df[hub_col].astype(str).str.strip().map(lambda x: _HUB_FULL_NAME_MERGE.get(str(x).strip().upper(), str(x).strip())).values
        if hub_col
        else ""
    )
    out["Debit Value ₹"] = (df[debit_col].astype(str).str.replace(r"[,₹\s]", "", regex=True).values if debit_col else [0] * len(df))
    out["Debit Value ₹"] = pd.to_numeric(out["Debit Value ₹"], errors="coerce").fillna(0)
    out["Pending Amount"] = df[pending_col].values
    # Exclude rows where Pending Amount is 0
    out = out[out["Pending Amount"] > 0].copy()
    # Final filter: exclude rows with invalid Addition Date or empty Tracking ID (safety net)
    out = out[out["Addition Date"].apply(_is_valid_cell) & out["Tracking ID"].apply(_is_valid_cell)]
    if out.empty:
        return pd.DataFrame(columns=["S. No", "Addition Date", "Tracking ID", "Hub Name", "Debit Value ₹", "Pending Amount"])
    out["S. No"] = range(1, len(out) + 1)
    return out


def _parse_numeric(val) -> float:
    """Parse numeric value, stripping ₹ commas etc. Returns 0 on failure."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return 0.0 if pd.isna(val) else float(val)
    s = str(val).strip().replace(",", "").replace("₹", "").replace(" ", "")
    return float(pd.to_numeric(s, errors="coerce") or 0)


def _col_to_letter(n: int) -> str:
    """Convert 1-based column index to letter (1=A, 26=Z, 27=AA)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _format_df_for_sheet(df: pd.DataFrame, date_col: str | None, exclude_currency: tuple = ()) -> pd.DataFrame:
    """Add ₹ prefix to all numeric values, no decimals (strictly integer). Preserve Recovery% row as percentages."""
    df_out = df.copy()
    # Rows to exclude from ₹ formatting (keep as % strings)
    pct_mask = None
    if date_col and date_col in df_out.columns:
        pct_mask = df_out[date_col].astype(str).str.strip() == "Recovery%"
        df_out[date_col] = df_out[date_col].astype(str).apply(
            lambda x: f"'{x}" if x and str(x).strip() not in ("Total", "Recovery%") else x
        )
    for c in df_out.columns:
        if c in exclude_currency:
            continue
        if pd.api.types.is_numeric_dtype(df_out[c]):
            s = df_out[c].fillna(0).apply(lambda x: f"₹{int(round(x)):,}")
            if pct_mask is not None and pct_mask.any():
                s = s.where(~pct_mask, df_out[c].astype(str))
            df_out[c] = s
        elif pd.api.types.is_object_dtype(df_out[c]):
            converted = pd.to_numeric(df_out[c].astype(str).str.replace(r"[,₹\s]", "", regex=True), errors="coerce")
            if converted.notna().any():
                s = converted.fillna(0).apply(lambda x: f"₹{int(round(x)):,}")
                if pct_mask is not None and pct_mask.any():
                    s = s.where(~pct_mask, df_out[c].astype(str))
                df_out[c] = s
    return df_out


def _clear_worksheet_format(ws, max_rows: int = 1500, max_cols: int = 26) -> None:
    """Reset all cell formatting (colour, bold) on the worksheet."""
    try:
        body = {
            "requests": [{
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": max_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": max(max_cols, 40),
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                }
            }]
        }
        ws.spreadsheet.batch_update(body)
    except Exception as e:
        print(f"  [WARN] Could not reset sheet format: {e}")


def _push_table(ws, df_out: pd.DataFrame, start_row: int, date_col: str) -> None:
    """Write one table to the worksheet."""
    nrows = len(df_out) + 1
    ncols = len(df_out.columns)
    set_with_dataframe(ws, df_out, row=start_row, include_column_header=True, resize=False)
    end_row = start_row + nrows - 1
    if "Status" in df_out.columns and "Region" in df_out.columns:
        ws.format(f"A{start_row}:C{end_row}", {"numberFormat": {"type": "TEXT"}})
    else:
        ws.format(f"A{start_row}:A{end_row}", {"numberFormat": {"type": "TEXT"}})
    end_cell = f"{_col_to_letter(ncols)}{end_row}"
    ws.format(f"A{start_row}:{end_cell}", {"horizontalAlignment": "RIGHT"})


# Push raw recovery pending to this sheet only
RECOVERY_PENDING_SHEET_NAMES = ("Recovery Pending",)

# Formatting: header = blue, Total/Recovery% = yellow
_HEADER_BG = {"red": 0.2, "green": 0.6, "blue": 0.9}
_TOTAL_BG = {"red": 1.0, "green": 1.0, "blue": 0.0}
_RECOVERY_PCT_BG = {"red": 0.9, "green": 0.9, "blue": 0.9}


def _apply_table_format(ws, start_row: int, nrows: int, ncols: int, date_col: str, df: pd.DataFrame) -> None:
    """Apply bold and colour to header, Total, and Recovery% rows."""
    if nrows < 1 or ncols < 1:
        return
    end_row = start_row + nrows
    end_col = _col_to_letter(ncols)
    # Header row
    ws.format(f"A{start_row}:{end_col}{start_row}", {"textFormat": {"bold": True}, "backgroundColor": _HEADER_BG})
    # Find Total and Recovery% row indices (1-based) in df (0-based)
    total_row_1based = None
    recovery_row_1based = None
    if date_col and date_col in df.columns:
        for i, val in enumerate(df[date_col]):
            s = str(val).strip()
            if s == "Total":
                total_row_1based = start_row + 1 + i  # header at start_row, data at start_row+1+i
            elif s == "Recovery%":
                recovery_row_1based = start_row + 1 + i
    if total_row_1based:
        ws.format(f"A{total_row_1based}:{end_col}{total_row_1based}", {"textFormat": {"bold": True}, "backgroundColor": _TOTAL_BG})
    if recovery_row_1based:
        ws.format(f"A{recovery_row_1based}:{end_col}{recovery_row_1based}", {"textFormat": {"bold": True}, "backgroundColor": _RECOVERY_PCT_BG})


def push_to_destination(
    gc, dest_id: str, df_debit, df_recovered=None, df_pending=None, df_recovery_pending_raw=None, worksheet_name: str = DEST_WORKSHEET_NAME
) -> tuple[bool, tuple[tuple[int, int], ...]]:
    """Push Debit, Pending (2nd), Recovered (3rd) tables to Debit Master. Push raw recovery pending to 'Recovery pending' sheet.

    Returns (success, whatsapp_row_bands): bands are (start_row, end_row) inclusive per table, for use when exactly three pivot tables are written.
    """
    sh = gc.open_by_key(dest_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        total_rows = len(df_debit) + 20
        for d in (df_recovered, df_pending):
            if d is not None and not d.empty:
                total_rows += 3 + len(d) + 20
        ws = sh.add_worksheet(title=worksheet_name, rows=max(1000, total_rows), cols=min(26, len(df_debit.columns) + 5))
    ws.clear()
    _clear_worksheet_format(ws)
    date_col_1 = "Total Debit" if "Total Debit" in df_debit.columns else (df_debit.columns[0] if len(df_debit.columns) else None)
    # Add Recovery% row after Total: (Total Debit - Recovery Pending) / Total Debit for every column
    if df_pending is not None and not df_pending.empty and not df_debit.empty:
        debit_total_row = df_debit[df_debit[date_col_1].astype(str).str.strip() == "Total"]
        pending_label = "Recovery Pending" if "Recovery Pending" in df_pending.columns else (df_pending.columns[0] if len(df_pending.columns) else None)
        pending_total_row = df_pending[df_pending[pending_label].astype(str).str.strip() == "Total"] if pending_label else pd.DataFrame()
        if not debit_total_row.empty:
            _skip_val = {date_col_1, "Status", "Region", "Total"}
            value_cols = [c for c in df_debit.columns if c not in _skip_val]
            fy_total_cols = _debit_master_total_sum_columns(value_cols)
            d_total = sum(_parse_numeric(debit_total_row[c].iloc[0]) if c in debit_total_row.columns else 0 for c in fy_total_cols)
            p_total = sum(
                _parse_numeric(pending_total_row[c].iloc[0]) if not pending_total_row.empty and c in pending_total_row.columns else 0
                for c in fy_total_cols
            )
            overall_pct = ((d_total - p_total) / d_total * 100) if d_total != 0 else 0
            recovery_pct_row = {date_col_1: "Recovery%"}
            for c in df_debit.columns:
                if c == date_col_1:
                    continue
                if c in ("Status", "Region"):
                    recovery_pct_row[c] = ""
                    continue
                try:
                    if c == "Total":
                        pct = overall_pct
                    else:
                        d_val = debit_total_row[c].iloc[0] if c in debit_total_row.columns else 0
                        p_val = pending_total_row[c].iloc[0] if not pending_total_row.empty and c in pending_total_row.columns else 0
                        d = _parse_numeric(d_val)
                        p = _parse_numeric(p_val)
                        pct = ((d - p) / d * 100) if d != 0 else 0
                    recovery_pct_row[c] = f"{int(round(pct))}%"
                except Exception:
                    recovery_pct_row[c] = f"{int(round(overall_pct))}%"
            df_debit = pd.concat([df_debit, pd.DataFrame([recovery_pct_row])], ignore_index=True)
    df1 = _format_df_for_sheet(df_debit, date_col_1)
    wa_bands: list[tuple[int, int]] = []
    cur_row = 1
    _push_table(ws, df1, start_row=cur_row, date_col=date_col_1 or "Total Debit")
    _apply_table_format(ws, cur_row, len(df1) + 1, len(df1.columns), date_col_1 or "Total Debit", df1)
    wa_bands.append((cur_row, cur_row + len(df1)))
    cur_row = cur_row + len(df1) + TABLE_VERTICAL_GAP

    if df_pending is not None and not df_pending.empty:
        date_col_p = (
            "Recovery Pending"
            if "Recovery Pending" in df_pending.columns
            else (df_pending.columns[0] if len(df_pending.columns) else "Recovery Pending")
        )
        df_p_fmt = _format_df_for_sheet(df_pending, date_col_p)
        _push_table(ws, df_p_fmt, start_row=cur_row, date_col=date_col_p)
        _apply_table_format(
            ws,
            cur_row,
            len(df_p_fmt) + 1,
            len(df_p_fmt.columns),
            date_col_p,
            df_p_fmt,
        )
        wa_bands.append((cur_row, cur_row + len(df_p_fmt)))
        cur_row = cur_row + len(df_p_fmt) + TABLE_VERTICAL_GAP

    if df_recovered is not None and not df_recovered.empty:
        fallback_r = "Recovered"
        date_col_r = (
            "Recovered" if "Recovered" in df_recovered.columns else (df_recovered.columns[0] if len(df_recovered.columns) else fallback_r)
        )
        df_r_fmt = _format_df_for_sheet(df_recovered, date_col_r)
        _push_table(ws, df_r_fmt, start_row=cur_row, date_col=date_col_r)
        _apply_table_format(
            ws,
            cur_row,
            len(df_r_fmt) + 1,
            len(df_r_fmt.columns),
            date_col_r,
            df_r_fmt,
        )
        wa_bands.append((cur_row, cur_row + len(df_r_fmt)))
        cur_row = cur_row + len(df_r_fmt) + TABLE_VERTICAL_GAP
    # Always push raw recovery pending sheet (create/update even when empty)
    df_rp = df_recovery_pending_raw if df_recovery_pending_raw is not None and not df_recovery_pending_raw.empty else None
    if df_rp is None or df_rp.empty:
        cols = ["S. No", "Addition Date", "Tracking ID", "Hub Name", "Debit Value ₹", "Pending Amount"]
        df_rp = pd.DataFrame(columns=cols)
        df_rp.loc[0] = ["", "No pending recoveries", "", "", "", ""]
    ws_rp = None
    for sheet_name in RECOVERY_PENDING_SHEET_NAMES:
        try:
            ws_rp = sh.worksheet(sheet_name)
            break
        except gspread.WorksheetNotFound:
            continue
    if ws_rp is None:
        try:
            ws_rp = sh.add_worksheet(title=RECOVERY_PENDING_SHEET_NAMES[0], rows=max(1000, len(df_rp) + 50), cols=10)
        except gspread.exceptions.APIError as e:
            if "already exists" in str(e).lower():
                for w in sh.worksheets():
                    if w.title == RECOVERY_PENDING_SHEET_NAMES[0]:
                        ws_rp = w
                        break
                if ws_rp is None:
                    ws_rp = sh.worksheet(RECOVERY_PENDING_SHEET_NAMES[0])
            else:
                raise
    ws_rp.clear()
    _clear_worksheet_format(ws_rp)
    df_rp_fmt = _format_df_for_sheet(df_rp, "Addition Date", exclude_currency=("S. No",))
    set_with_dataframe(ws_rp, df_rp_fmt, row=1, include_column_header=True, resize=False)
    ncols_rp = len(df_rp_fmt.columns)
    end_col_rp = _col_to_letter(ncols_rp)
    ws_rp.format(f"A1:{end_col_rp}1", {"textFormat": {"bold": True}, "backgroundColor": _HEADER_BG})
    n = 0 if df_recovery_pending_raw is None or df_recovery_pending_raw.empty else len(df_recovery_pending_raw)
    print(f"  Pushed raw to: {sh.title} / {ws_rp.title} ({n} rows)")
    print(f"  Pushed to: {sh.title} / {ws.title}")
    return True, tuple(wa_bands)


# Recovery Pending email config
_RP_EMAIL_TO = ["venkatesh.n@loadshare.net", "bharath.s@loadshare.net", "lokeshh@loadshare.net"]
_RP_EMAIL_CC = ["saicharan@loadshare.net", "rakshith.ar@loadshare.net"]


def _build_hub_month_summary(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Build hub-wise month-wise summary from recovery pending raw data."""
    if df_raw is None or df_raw.empty or "Hub Name" not in df_raw.columns or "Pending Amount" not in df_raw.columns:
        return pd.DataFrame()
    df = df_raw.copy()
    df["_hub_code"] = df["Hub Name"].astype(str).apply(_normalize_hub_code)
    add_col = "Addition Date"
    if add_col not in df.columns:
        return pd.DataFrame()
    with warnings.catch_warnings(action="ignore", category=UserWarning):
        df["_dt"] = pd.to_datetime(df[add_col], errors="coerce", dayfirst=True, format="mixed")
    df = df[df["_dt"].notna()]
    if df.empty:
        return pd.DataFrame()
    df["_month"] = df["_dt"].dt.strftime("%b-%y")
    df["Pending Amount"] = pd.to_numeric(df["Pending Amount"], errors="coerce").fillna(0)
    pivot = df.pivot_table(index="_hub_code", columns="_month", values="Pending Amount", aggfunc="sum", fill_value=0)
    pivot = pivot.reset_index().rename(columns={"_hub_code": "Hub"})
    month_order = df.drop_duplicates("_month").set_index("_month")["_dt"].sort_values()
    month_cols = month_order.index.tolist()
    pivot = pivot[["Hub"] + [c for c in month_cols if c in pivot.columns]]
    pivot["Total"] = pivot.select_dtypes(include="number").sum(axis=1)
    total_row = {"Hub": "Total", **{c: pivot[c].sum() for c in pivot.columns if c != "Hub"}, "Total": pivot["Total"].sum()}
    pivot = pd.concat([pivot, pd.DataFrame([total_row])], ignore_index=True)
    return pivot


def _send_recovery_pending_email(df_recovery_pending_raw: pd.DataFrame, date_str: str | None = None) -> None:
    """Send Recovery Pending email: subject, body with hub-wise month-wise summary, Recovery Pending sheet as attachment."""
    if df_recovery_pending_raw is None or df_recovery_pending_raw.empty:
        return
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
    except ImportError:
        print("  [WARN] Email skip: smtplib/email modules required")
        return

    sender_email = os.getenv("GMAIL_SENDER_EMAIL", "arunraj@loadshare.net")
    sender_password = os.getenv("GMAIL_APP_PASSWORD", "")
    if not sender_password:
        print("  [WARN] Recovery Pending email skip: GMAIL_APP_PASSWORD not set")
        return

    from datetime import date
    dt_str = date_str or date.today().strftime("%d-%b-%Y")
    subject = f"Recovery Pending (Flipkart ODH Debit Master) as on - {dt_str}"

    summary_df = _build_hub_month_summary(df_recovery_pending_raw)
    if summary_df.empty:
        body_html = "<p>No recovery pending data.</p>"
    else:
        body_html = "<h3>Hub-wise Month-wise Summary</h3><table border='1' cellpadding='5' cellspacing='0' style='border-collapse:collapse'>"
        body_html += "<tr><th>Hub</th>"
        for c in summary_df.columns:
            if c != "Hub":
                body_html += f"<th style='text-align:right'>{c}</th>"
        body_html += "</tr>"
        for _, row in summary_df.iterrows():
            body_html += "<tr>"
            body_html += f"<td><b>{row['Hub']}</b></td>" if row["Hub"] == "Total" else f"<td>{row['Hub']}</td>"
            for c in summary_df.columns:
                if c != "Hub":
                    val = row[c]
                    if isinstance(val, (int, float)):
                        body_html += f"<td style='text-align:right'>₹{float(val):,.0f}</td>"
                    else:
                        body_html += f"<td style='text-align:right'>{val}</td>"
            body_html += "</tr>"
        body_html += "</table><p><i>Detailed data attached.</i></p>"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(_RP_EMAIL_TO)
    msg["CC"] = ", ".join(_RP_EMAIL_CC)
    msg["Subject"] = subject
    msg.attach(MIMEText(body_html, "html"))

    # Attach Recovery Pending as Excel
    try:
        attachment_df = df_recovery_pending_raw.copy()
        with pd.ExcelWriter(SCRIPT_DIR / "_recovery_pending_attachment.xlsx", engine="openpyxl") as w:
            attachment_df.to_excel(w, sheet_name="Recovery Pending", index=False)
        with open(SCRIPT_DIR / "_recovery_pending_attachment.xlsx", "rb") as f:
            part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename=Recovery_Pending_{date.today().strftime('%Y%m%d')}.xlsx")
        msg.attach(part)
        try:
            (SCRIPT_DIR / "_recovery_pending_attachment.xlsx").unlink()
        except OSError:
            pass
    except Exception as e:
        print(f"  [WARN] Could not create Excel attachment: {e}")

    all_recipients = _RP_EMAIL_TO + _RP_EMAIL_CC
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587, timeout=30)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, all_recipients, msg.as_string())
        server.quit()
        print(f"  Recovery Pending email sent to {', '.join(_RP_EMAIL_TO)} (CC: {', '.join(_RP_EMAIL_CC)})")
    except Exception as e:
        print(f"  [WARN] Recovery Pending email failed: {e}")


def _send_debit_master_to_whatsapp(
    gc, dest_id: str, worksheet_name: str = DEST_WORKSHEET_NAME, row_bands: tuple[tuple[int, int], ...] | None = None
) -> None:
    """Send three Debit Master screenshots; column extent is auto-detected (new month columns included)."""
    try:
        from whatsapp_sheet_image import send_sheet_range_to_whatsapp, _get_last_col_with_data
    except ImportError:
        print("  [WARN] WhatsApp skip: install whatsapp_sheet_image deps")
        return
    try:
        sh = gc.open_by_key(dest_id)
        ws = sh.worksheet(worksheet_name)
        print("-" * 40)
        print("  Sending Debit Master to WhatsApp (3 tables, all hub statuses)...")
        caps = (
            "Flipkart ODH — Total Debit (all hubs)",
            "Flipkart ODH — Recovery Pending (all hubs)",
            "Flipkart ODH — Recovered (all hubs)",
        )
        bands = row_bands if row_bands is not None and len(row_bands) == 3 else DEBIT_MASTER_WA_ROW_BANDS
        for (r0, r1), cap in zip(bands, caps):
            end_col = _get_last_col_with_data(
                ws, start_row=r0, end_row=r1, max_cols=DEBIT_MASTER_WA_MAX_COL_SCAN
            )
            rng = f"A{r0}:{end_col}{r1}"
            print(f"    {rng} — {cap}")
            send_sheet_range_to_whatsapp(ws, range=rng, caption=cap)
        print("  WhatsApp done.")
    except Exception as e:
        print(f"  [WARN] WhatsApp send failed: {e}")


def read_from_file(path: Path) -> pd.DataFrame | None:
    """Read data from downloaded CSV or Excel file. No Google Sheet access needed."""
    path = Path(path)
    if not path.exists():
        print(f"  [ERROR] File not found: {path}")
        return None
    suffix = path.suffix.lower()
    print(f"  Reading from file: {path}")
    try:
        if suffix == ".csv":
            df = pd.read_csv(path, encoding="utf-8", on_bad_lines="skip")
        elif suffix in (".xlsx", ".xls"):
            df = pd.read_excel(path, engine="openpyxl" if suffix == ".xlsx" else None, sheet_name=0)
        else:
            print(f"  [ERROR] Unsupported format: {suffix}. Use .csv or .xlsx")
            return None
    except Exception as e:
        print(f"  [ERROR] Failed to read file: {e}")
        return None
    df = df.dropna(how="all")
    print(f"  Extracted: {len(df):,} rows, {len(df.columns)} columns")
    return df


def main():
    ap = argparse.ArgumentParser(description="Sync Flipkart (Client) Debit master - copies source every run for fresh data")
    ap.add_argument("--input", "-i", help="Path to CSV/Excel file (skips copy, uses file instead)")
    ap.add_argument("--raw", action="store_true", help="Push raw data instead of summary")
    ap.add_argument("--service-account", action="store_true", help="Use service account for copy (owner must share source)")
    ap.add_argument("--reauth", action="store_true", help="Clear saved OAuth tokens and sign in again")
    ap.add_argument("--source-id", default=SOURCE_SHEET_ID, help="Source sheet ID to copy from")
    ap.add_argument("--source-worksheet", default=None, help="Source worksheet name (default: first sheet)")
    ap.add_argument("--dest-id", default=None, help="Destination sheet ID")
    ap.add_argument("--dest-worksheet", default=DEST_WORKSHEET_NAME, help="Destination worksheet name")
    ap.add_argument(
        "--pending-include-closed-hubs",
        action="store_true",
        help="Recovery Pending raw sheet: include closed/inactive hubs (default: active only, FK-ODH map + MQR/MQE/MHK/YLZ/YLG)",
    )
    ap.add_argument("--pending-include-all-dates", action="store_true", help="Include all pending recoveries regardless of date (default: on or after Apr 1 2025)")
    ap.add_argument("--no-whatsapp", action="store_true", help="Skip WhatsApp send after pushing to sheet")
    ap.add_argument("--no-email", action="store_true", help="Skip Recovery Pending email")
    args = ap.parse_args()

    dest_id = args.dest_id or DEST_SHEET_ID

    if args.input:
        df = read_from_file(args.input)
        if df is None or df.empty:
            print("  [ERROR] No data to push")
            return
        if not SERVICE_ACCOUNT_FILE.exists():
            print(f"ERROR: {SERVICE_ACCOUNT_FILE} not found")
            return
        creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=SCOPES)
        gc = gspread.authorize(creds)
        if not args.raw:
            print("  Analyzing...")
            from datetime import date
            df_debit = analyze_debit_data(df, date.today().isoformat())
            df_recovered = analyze_recovered_data(df)
            df_pending = analyze_pending_data(df)
            df_recovery_pending_raw = get_recovery_pending_raw(df, include_closed_hubs=args.pending_include_closed_hubs, apply_date_cutoff=not args.pending_include_all_dates)
            ok, wa_bands = push_to_destination(gc, dest_id, df_debit, df_recovered, df_pending, df_recovery_pending_raw, args.dest_worksheet)
            if ok and not args.no_email and df_recovery_pending_raw is not None and not df_recovery_pending_raw.empty:
                _send_recovery_pending_email(df_recovery_pending_raw)
            if ok and not args.no_whatsapp:
                _send_debit_master_to_whatsapp(gc, dest_id, args.dest_worksheet, row_bands=wa_bands if len(wa_bands) == 3 else None)
        else:
            ok, wa_bands = push_to_destination(gc, dest_id, df, None, None, None, args.dest_worksheet)
            if ok and not args.no_whatsapp:
                _send_debit_master_to_whatsapp(gc, dest_id, args.dest_worksheet, row_bands=wa_bands if len(wa_bands) == 3 else None)
        print("Done.")
        return

    # Copy source every run - try service account first (if you've shared source with SA), else OAuth
    from datetime import date
    from sheet_copy_utils import copy_sheet_for_analysis, copy_sheet_with_user_oauth

    auth_file = SCRIPT_DIR / "gspread_authorized_user.json"
    if args.reauth and auth_file.exists():
        auth_file.unlink()
        print("  Cleared saved OAuth tokens. You will sign in again.")

    print("Copying source sheet (fresh data)...")
    copy_id = None
    gc = None

    # Try service account first (source shared with SA - Edit or View)
    if args.service_account or True:
        copy_id = copy_sheet_for_analysis(
            source_id=args.source_id,
            copy_title=COPY_SHEET_TITLE,
        )
        if copy_id:
            if not SERVICE_ACCOUNT_FILE.exists():
                print(f"ERROR: {SERVICE_ACCOUNT_FILE} not found")
                return
            creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=SCOPES)
            gc = gspread.authorize(creds)
            print("  Using service account (source shared with SA)")
        else:
            copy_id = None

    if not copy_id:
        print("  Service account copy failed (source not shared?). Trying OAuth...")
        # OAuth: use YOUR account (same as manual File → Make a copy)
        result = copy_sheet_with_user_oauth(
            source_id=args.source_id,
            copy_title=COPY_SHEET_TITLE,
            credentials_file=SCRIPT_DIR / "gspread_credentials.json",
            authorized_user_file=SCRIPT_DIR / "gspread_authorized_user.json",
        )
        if not result:
            return
        copy_id, oauth_creds = result
        # Read from copy
        df = read_sheet_via_drive_export(copy_id, oauth_creds)
        if df is None or df.empty:
            print("  File too large for Drive export, using Sheets API...")
            gc = gspread.authorize(oauth_creds)
            df = read_source_data(gc, copy_id, args.source_worksheet)
        if df is None or df.empty:
            print("  [ERROR] No data to push.")
            print("  Enable Google Sheets API: https://console.cloud.google.com/apis/library/sheets.googleapis.com")
            return
        # Analyze and push summary (or raw if --raw)
        # Use OAuth for destination too (same account has access to both source and dest)
        gc_dest = gspread.authorize(oauth_creds)
        if not args.raw:
            print("  Analyzing...")
            df_debit = analyze_debit_data(df, date.today().isoformat())
            df_recovered = analyze_recovered_data(df)
            df_pending = analyze_pending_data(df)
            df_recovery_pending_raw = get_recovery_pending_raw(df, include_closed_hubs=args.pending_include_closed_hubs, apply_date_cutoff=not args.pending_include_all_dates)
            ok, wa_bands = push_to_destination(gc_dest, dest_id, df_debit, df_recovered, df_pending, df_recovery_pending_raw, args.dest_worksheet)
            if ok and not args.no_email and df_recovery_pending_raw is not None and not df_recovery_pending_raw.empty:
                _send_recovery_pending_email(df_recovery_pending_raw)
            if ok and not args.no_whatsapp:
                _send_debit_master_to_whatsapp(gc_dest, dest_id, args.dest_worksheet, row_bands=wa_bands if len(wa_bands) == 3 else None)
        else:
            ok, wa_bands = push_to_destination(gc_dest, dest_id, df, None, None, None, args.dest_worksheet)
            if ok and not args.no_whatsapp:
                _send_debit_master_to_whatsapp(gc_dest, dest_id, args.dest_worksheet, row_bands=wa_bands if len(wa_bands) == 3 else None)
        print("Done.")
        return

    df = read_source_data(gc, copy_id, args.source_worksheet)
    if df is None or df.empty:
        print("  [ERROR] No data to push")
        return

    if not args.raw:
        print("  Analyzing...")
        df_debit = analyze_debit_data(df, date.today().isoformat())
        df_recovered = analyze_recovered_data(df)
        df_pending = analyze_pending_data(df)
        df_recovery_pending_raw = get_recovery_pending_raw(df, include_closed_hubs=args.pending_include_closed_hubs, apply_date_cutoff=not args.pending_include_all_dates)
    else:
        df_debit = df
        df_recovered = None
        df_pending = None
        df_recovery_pending_raw = None

    # Push to destination (always use service account - destination is shared with it)
    if not SERVICE_ACCOUNT_FILE.exists():
        print(f"ERROR: {SERVICE_ACCOUNT_FILE} not found")
        return
    sa_creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=SCOPES)
    gc_dest = gspread.authorize(sa_creds)
    ok, wa_bands = push_to_destination(gc_dest, dest_id, df_debit, df_recovered, df_pending, df_recovery_pending_raw, args.dest_worksheet)
    if ok and not args.no_email and df_recovery_pending_raw is not None and not df_recovery_pending_raw.empty:
        _send_recovery_pending_email(df_recovery_pending_raw)
    if ok and not args.no_whatsapp:
        _send_debit_master_to_whatsapp(gc_dest, dest_id, args.dest_worksheet, row_bands=wa_bands if len(wa_bands) == 3 else None)
    print("Done.")


if __name__ == "__main__":
    main()
