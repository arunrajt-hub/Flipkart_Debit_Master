# Flipkart Debit Master

Automates copy → analysis → push for the Flipkart ODH debit master Google Sheets pipeline.

## Repository layout

| File | Role |
|------|------|
| `flipkart_debit_master_sync.py` | Main entry: sync source sheet, hub pivots, `ODH Debit Master` + Recovery Pending |
| `odh_hub_status_region_map.py` | FK-ODH hub Status / Region map and active-hub helper |
| `sheet_copy_utils.py` | Service-account or OAuth sheet copy helpers |
| `whatsapp_sheet_image.py` | Optional: sheet range → image → WhatsApp (WHAPI) |

## Local run

```bash
pip install -r requirements.txt
# Place service_account_key.json in this folder (not committed)
python flipkart_debit_master_sync.py
```

## GitHub Actions (every ~48 hours)

Workflow: [.github/workflows/flipkart_debit_master.yml](.github/workflows/flipkart_debit_master.yml)

- **Schedule:** `0 7 * * *` (daily 07:00 UTC) with a **Unix-day parity gate** so **scheduled** runs execute on **alternating days** (~48h apart).
- **Manual:** *Actions* → *Flipkart Debit Master Sync* → *Run workflow* (always runs; gate bypassed).

### Required secret

| Secret | Description |
|--------|-------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full JSON of the Google Cloud service account that can read the source sheet and write the destination workbook |

### Optional secrets

| Secret | Description |
|--------|-------------|
| `FLIPKART_ODH_SPREADSHEET_ID` | Override destination spreadsheet ID (default is in the script) |
| `WHAPI_TOKEN`, `WHATSAPP_PHONE`, `HTML_TO_IMAGE_SERVICE_URL` | If set, you can remove `--no-whatsapp` from the workflow to send images |
| `GMAIL_SENDER_EMAIL`, `GMAIL_APP_PASSWORD` | If set, remove `--no-email` from the workflow for Recovery Pending mail |

Share the **source** and **destination** spreadsheets with the service account email from the JSON (`client_email`).

## Sheet URLs

Configured in `flipkart_debit_master_sync.py` (source, destination `ODH Debit Master` tab).
