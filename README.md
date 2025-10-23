# Google Sheets on Railway

- Set env vars on Railway:
  - GOOGLE_CREDS_JSON: (paste entire service account JSON content)
  - SHEET_NAME: Trading Log
  - WORKSHEET: log

On deploy, the app appends a timestamped row to the sheet.
