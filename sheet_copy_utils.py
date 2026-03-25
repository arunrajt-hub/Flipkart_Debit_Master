"""
Copy a view-only Google Sheet for analysis.
Creates a writable copy owned by the service account, optionally shares with a user.

Usage:
    from sheet_copy_utils import copy_sheet_for_analysis

    copy_id = copy_sheet_for_analysis(
        source_id="1qnqzVf-S41F4S6DN8CRtXVgk-BcsaW377aVVEyFrnzg",
        copy_title="Meesho Reports - Copy",
        share_with_email="user@example.com",  # optional
    )
"""

import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = SCRIPT_DIR / "service_account_key.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _oauth_client_config_error(exc: BaseException) -> bool:
    """True when GCP OAuth client was deleted, rotated, or client id/secret is invalid."""
    s = str(exc).lower()
    return "deleted_client" in s or "invalid_client" in s


def _print_oauth_client_dead_help(auth_file: Path, creds_file: Path) -> None:
    print()
    print("  OAuth fix (your error means the old OAuth 'Desktop client' was removed from Google Cloud,")
    print("  or the saved token still references that client):")
    print("  1. https://console.cloud.google.com/apis/credentials")
    print("  2. Create credentials → OAuth client ID → Desktop app → Download JSON")
    print(f"  3. Save/replace as: {creds_file.name}")
    print(f"  4. Delete: {auth_file.name}  (stale refresh token / old client_id)")
    print("  5. Run your sync script again; complete the browser sign-in.")
    print()


def _print_service_account_share_hint(sa_file: Path | None = None) -> None:
    """Print client_email from service account JSON so users know whom to share with."""
    p = sa_file or SERVICE_ACCOUNT_FILE
    try:
        import json

        with open(p, encoding="utf-8") as f:
            email = json.load(f).get("client_email")
        if email:
            print(f"  Share the source sheet with this address (Viewer is enough): {email}")
    except Exception:
        pass


def _delete_existing_copy_by_name(title: str, creds) -> None:
    """Find and delete any Drive file with the exact name. Replaces instead of creating duplicates."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return
    try:
        drive = build("drive", "v3", credentials=creds)
        result = drive.files().list(
            q=f"name = '{title.replace(chr(39), chr(92) + chr(39))}' and trashed = false",
            spaces="drive",
            fields="files(id, name)",
        ).execute()
        files = result.get("files", [])
        for f in files:
            if f.get("name") == title:
                drive.files().delete(fileId=f["id"]).execute()
                print(f"  Replaced existing copy: {title}")
    except Exception:
        pass


def copy_sheet_for_analysis(
    source_id: str,
    copy_title: str | None = None,
    share_with_email: str | None = None,
    service_account_file: Path | None = None,
) -> str | None:
    """
    Copy a view-only sheet for analysis. Returns the copy's spreadsheet ID, or None on failure.

    Requires: Service account must have at least VIEW access to the source sheet.
    The copy is owned by the service account (full edit access).
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("  [ERROR] Install: pip install gspread google-auth")
        return None

    sa_file = service_account_file or SERVICE_ACCOUNT_FILE
    if not sa_file.exists():
        print(f"  [ERROR] {sa_file} not found")
        return None

    try:
        creds = Credentials.from_service_account_file(str(sa_file), scopes=SCOPES)
    except Exception as e:
        if type(e).__name__ == "MalformedError" or "not in the expected format" in str(e):
            print(f"  [ERROR] {sa_file} is not a valid Google *service account key* JSON.")
            print('  Expected: IAM → Service accounts → Keys → Add key → JSON (contains "type": "service_account", "client_email", "private_key").')
            print("  Not: OAuth Desktop client JSON (that belongs in gspread_credentials.json only).")
            if os.environ.get("GITHUB_ACTIONS") == "true":
                print("  Fix: replace GitHub secret SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_JSON with the full service-account key file contents.")
            return None
        raise
    gc = gspread.authorize(creds)

    try:
        source = gc.open_by_key(source_id)
    except Exception as e:
        print(f"  [ERROR] Cannot access source sheet: {e}")
        print("  Ensure the sheet is shared with the service account (viewer is enough).")
        _print_service_account_share_hint(sa_file)
        return None

    title = copy_title or f"{source.title} - Copy for Analysis"
    if copy_title:
        _delete_existing_copy_by_name(title, creds)
    print(f"  Copying '{source.title}' -> '{title}'...")
    try:
        copied = source.copy(title=title)
    except Exception as e:
        print(f"  [ERROR] Copy failed: {e}")
        return None

    copy_id = copied.id
    print(f"  Copy created: {copied.url}")

    if share_with_email:
        _share_with_user(copy_id, share_with_email, creds)

    return copy_id


def _share_with_user(file_id: str, email: str, creds) -> None:
    """Share the file with a user (writer role) using Drive API."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("  [INFO] Install google-api-python-client to auto-share copy with user")
        return
    try:
        drive = build("drive", "v3", credentials=creds)
        drive.permissions().create(
            fileId=file_id,
            body={
                "type": "user",
                "role": "writer",
                "emailAddress": email.strip(),
            },
            fields="id",
        ).execute()
        print(f"  Shared copy with {email}")
    except Exception as e:
        print(f"  [WARN] Could not share with {email}: {e}")


# File to store last copy ID so rto_pendency_dashboard can use the same copy
LAST_COPY_ID_FILE = SCRIPT_DIR / ".meesho_sheet_copy_id"


def save_last_copy_id(copy_id: str) -> None:
    """Save copy ID for use by subsequent scripts (e.g. rto_pendency_dashboard)."""
    try:
        LAST_COPY_ID_FILE.write_text(copy_id.strip(), encoding="utf-8")
    except Exception:
        pass


def get_oauth_credentials_path() -> Path | None:
    """Return path to OAuth credentials file if it exists."""
    for name in ["gspread_credentials.json", "gspread_credentials.json.json", "credentials.json"]:
        p = SCRIPT_DIR / name
        if p.exists():
            return p
    for p in SCRIPT_DIR.glob("client_secret*.json"):
        return p
    return None


def get_last_copy_id() -> str | None:
    """Read last copy ID from file. Returns None if invalid or missing."""
    try:
        if LAST_COPY_ID_FILE.exists():
            return LAST_COPY_ID_FILE.read_text(encoding="utf-8").strip() or None
    except Exception:
        pass
    return None


def copy_sheet_with_user_oauth(
    source_id: str,
    copy_title: str | None = None,
    credentials_file: Path | None = None,
    authorized_user_file: Path | None = None,
) -> str | None:
    """
    Copy a sheet using YOUR Google account (OAuth). Use when you have copy access
    but the owner hasn't shared with the service account.

    First-time setup: Get OAuth credentials from Google Cloud Console:
      1. console.cloud.google.com → APIs & Services → Credentials
      2. Create credentials → OAuth client ID → Desktop app
      3. Download JSON, save as gspread_credentials.json in this folder

    First run opens a browser to log in. Tokens are saved for future runs.
    """
    try:
        import gspread
    except ImportError:
        print("  [ERROR] Install: pip install gspread google-auth")
        return None

    creds_file = credentials_file or get_oauth_credentials_path()
    if not creds_file or not creds_file.exists():
        creds_file = SCRIPT_DIR / "gspread_credentials.json"
    auth_file = authorized_user_file or SCRIPT_DIR / "gspread_authorized_user.json"

    if not creds_file.exists():
        print(f"  [ERROR] OAuth credentials not found: {creds_file}")
        print()
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print("  GitHub Actions: create BOTH repository secrets on THIS repo (Settings → Secrets):")
            print("    • GSPREAD_CREDENTIALS_JSON  — full JSON (Desktop OAuth client), same as local gspread_credentials.json")
            print("    • GSPREAD_AUTHORIZED_USER_JSON — full JSON after sign-in, same as gspread_authorized_user.json")
            print("  Or share the SOURCE spreadsheet with the service account (client_email in service_account_key.json).")
            print()
        print("  Local / manual copy with YOUR account (same as File → Make a copy):")
        print("  1. Go to: https://console.cloud.google.com/apis/credentials")
        print("  2. Create OAuth client ID (Desktop app)")
        print("  3. Download JSON, rename to gspread_credentials.json")
        print(f"  4. Place in: {SCRIPT_DIR}")
        print()
        return None

    # Load credentials and create gspread client (we need creds for Drive API fallback)
    creds = _load_oauth_creds(creds_file, auth_file)
    if not creds:
        return None

    try:
        gc = gspread.authorize(creds)
    except Exception as e:
        print(f"  [ERROR] OAuth failed: {e}")
        return None

    # Try Sheets API first, then Drive API (Drive copy can work when Sheets open fails)
    copied_id = None
    if copy_title:
        _delete_existing_copy_by_name(copy_title, creds)
    try:
        source = gc.open_by_key(source_id)
        title = copy_title or f"{source.title} - Copy"
        print(f"  Copying '{source.title}' -> '{title}' (using your account)...")
        copied = source.copy(title=title)
        copied_id = copied.id
        print(f"  Copy created: {copied.url}")
    except Exception as e:
        err = str(e) or type(e).__name__
        if _oauth_client_config_error(e):
            print(f"  Sheets API failed ({err})")
            _print_oauth_client_dead_help(auth_file, creds_file)
            return None
        print(f"  Sheets API failed ({err}), trying Drive API copy...")
        copied_id = _copy_via_drive_api(source_id, copy_title, creds, auth_file, creds_file)

    if not copied_id:
        print("  [ERROR] Cannot access or copy source.")
        print("  Option A — Service account (recommended for automation):")
        print("    Share the source spreadsheet with the service account (see client_email in service_account_key.json).")
        _print_service_account_share_hint()
        print("  Option B — OAuth with your Google account:")
        print("    Fix gspread_credentials.json + delete gspread_authorized_user.json if you see deleted_client (see messages above).")
        print("    Then run: python flipkart_debit_master_sync.py --reauth   (or meesho_debit_master_sync.py --reauth)")
        print("    Sign in with the Google account that can open the source sheet.")
        return None
    return (copied_id, creds)  # (copy_id, credentials) for caller to read from copy


def _load_oauth_creds(creds_file: Path, auth_file: Path):
    """Load OAuth credentials from files. Returns credentials object or None."""
    import json

    try:
        if auth_file.exists():
            with open(auth_file, "r", encoding="utf-8") as f:
                auth_data = json.load(f)
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials

            creds = Credentials(
                token=auth_data.get("token"),
                refresh_token=auth_data.get("refresh_token"),
                token_uri=auth_data.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=auth_data.get("client_id"),
                client_secret=auth_data.get("client_secret"),
                scopes=auth_data.get("scopes", SCOPES),
            )
            if creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    with open(auth_file, "w", encoding="utf-8") as f:
                        f.write(creds.to_json())
                except Exception as refresh_err:
                    if _oauth_client_config_error(refresh_err):
                        print(f"  [ERROR] OAuth token refresh failed: {refresh_err}")
                        _print_oauth_client_dead_help(auth_file, creds_file)
                        return None
                    raise
            return creds
        # No saved tokens - need to run OAuth flow
        from google_auth_oauthlib.flow import InstalledAppFlow

        flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), SCOPES)
        creds = flow.run_local_server(port=0)
        # Save for next time (gspread-compatible format)
        with open(auth_file, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
        return creds
    except Exception as e:
        if _oauth_client_config_error(e):
            _print_oauth_client_dead_help(auth_file, creds_file)
        else:
            print(f"  [ERROR] Loading OAuth credentials: {e}")
        return None


def _copy_via_drive_api(
    source_id: str,
    copy_title: str | None,
    creds,
    auth_file: Path | None = None,
    creds_file: Path | None = None,
) -> str | None:
    """Copy via Drive API using credentials."""
    try:
        from googleapiclient.discovery import build
    except ImportError:
        print("  Install: pip install google-api-python-client")
        return None
    try:
        drive = build("drive", "v3", credentials=creds)
        body = {"name": copy_title or "Meesho Debit Master - Copy"}
        result = drive.files().copy(fileId=source_id, body=body, fields="id,webViewLink").execute()
        copy_id = result.get("id")
        url = result.get("webViewLink", "")
        if copy_id:
            print(f"  Copy created via Drive API: {url}")
        return copy_id
    except Exception as e:
        err = str(e)
        if _oauth_client_config_error(e) and auth_file is not None and creds_file is not None:
            print(f"  Drive API also failed: {e}")
            _print_oauth_client_dead_help(auth_file, creds_file)
        elif "Drive API" in err and "disabled" in err.lower():
            print("  Enable Google Drive API: https://console.cloud.google.com/apis/library/drive.googleapis.com")
        else:
            print(f"  Drive API also failed: {e}")
        return None
