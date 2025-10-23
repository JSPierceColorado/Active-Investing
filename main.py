import os, json, sys
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = os.getenv("SHEET_NAME", "Trading Log")
WORKSHEET  = os.getenv("WORKSHEET", "log")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_client():
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if not creds_json:
        print("ERROR: GOOGLE_CREDS_JSON env var is missing.", file=sys.stderr)
        sys.exit(1)
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def get_ws(gc):
    sh = gc.open(SHEET_NAME)
    if WORKSHEET:
        try:
            return sh.worksheet(WORKSHEET)
        except gspread.WorksheetNotFound:
            return sh.add_worksheet(title=WORKSHEET, rows=100, cols=20)
    return sh.sheet1

def main():
    gc = get_client()
    ws = get_ws(gc)
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row(
        ["Deployed on Railway 🎉", ts],
        value_input_option="RAW"
    )
    print("Wrote a row to the sheet.")

if __name__ == "__main__":
    main()
