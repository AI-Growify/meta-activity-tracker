# Meta Activity Tracker

Automated Meta advertising activity tracker with Airtable integration and Google Sheets logging.

## Features

✅ Tracks Meta advertising activities every 12 hours
✅ Maps brands from Airtable
✅ Prevents duplicate entries
✅ Logs to Google Sheets
✅ Runs automatically via GitHub Actions

## Setup

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed instructions.

## Manual Run
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export META_ACCESS_TOKEN="your_token"
export AIRTABLE_TOKEN="your_token"
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="your_table_name"
export GOOGLE_SPREADSHEET_ID="your_sheet_id"
export GOOGLE_CREDENTIALS_PATH="./google_credentials.json"

# Run tracker
python fetch_active_brands.py 12
```

## GitHub Actions

Runs automatically every 12 hours. Manual trigger available in Actions tab.

## Google Sheet Structure

- **Meta_Activities_Log**: Main activity data
- **GitHub_Actions_Log**: Run history and status
