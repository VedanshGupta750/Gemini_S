from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import json
import logging
import google.generativeai as genai

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'your_db'),
    'user': os.getenv('DB_USER', 'your_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'host': os.getenv('DB_HOST', 'your_host'),
    'port': os.getenv('DB_PORT', '5432')
}

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDS_FILE = os.getenv('GOOGLE_CREDS', 'credentials.json')
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
sheets_service = build('sheets', 'v4', credentials=creds)

# Gemini 2.0 Flash setup
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
gemini_model = genai.GenerativeModel('gemini-2.0-flash')

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# Existing upload route (preserved)
@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        files = request.files.getlist('files')
        if not files:
            return jsonify({'error': 'No files uploaded'}), 400

        # Existing Gemini processing (mocked - replace with your original logic)
        data_entries = []
        for file in files:
            # Assume original Gemini API call (e.g., older version)
            gemini_result = {}  # Replace with your original Gemini call
            entry = {
                'DATE': datetime.now().strftime('%Y-%m-%d'),
                'PARTICULARS': gemini_result.get('description', 'Processed File'),
                'Voucher_BillNo': gemini_result.get('bill_no', 'N/A'),
                'RECEIPTS_Quantity': gemini_result.get('quantity', 0),
                'RECEIPTS_Amount': float(gemini_result.get('amount', 0.0)),
                'ISSUED_Quantity': 0,
                'ISSUED_Amount': 0.0,
                'BALANCE_Quantity': gemini_result.get('quantity', 0),
                'BALANCE_Amount': float(gemini_result.get('amount', 0.0))
            }
            data_entries.append(entry)

        # Insert into PostgreSQL
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for entry in data_entries:
            cur.execute("""
                INSERT INTO table_name (DATE, PARTICULARS, Voucher_BillNo, RECEIPTS_Quantity, RECEIPTS_Amount,
                                        ISSUED_Quantity, ISSUED_Amount, BALANCE_Quantity, BALANCE_Amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING Entry_ID;
            """, (
                entry['DATE'], entry['PARTICULARS'], entry['Voucher_BillNo'],
                entry['RECEIPTS_Quantity'], entry['RECEIPTS_Amount'],
                entry['ISSUED_Quantity'], entry['ISSUED_Amount'],
                entry['BALANCE_Quantity'], entry['BALANCE_Amount']
            ))
            entry['Entry_ID'] = cur.fetchone()['Entry_ID']
        conn.commit()

        # Sync to Google Sheet (existing)
        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[
            e['Entry_ID'], e['DATE'], e['PARTICULARS'], e['Voucher_BillNo'],
            e['RECEIPTS_Quantity'], e['RECEIPTS_Amount'], e['ISSUED_Quantity'],
            e['ISSUED_Amount'], e['BALANCE_Quantity'], e['BALANCE_Amount']
        ] for e in data_entries]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        return jsonify({'message': 'Files processed'}), 200  # Existing response

    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'error': str(e)}), 500

# New upload route with Gemini 2.0 Flash
@app.route('/upload-flash', methods=['POST'])
def upload_files_flash():
    try:
        files = request.files.getlist('files')
        if not files:
            return jsonify({'error': 'No files uploaded'}), 400

        # Process files with Gemini 2.0 Flash
        data_entries = []
        for file in files:
            # Read file content
            file_content = file.read()
            response = gemini_model.generate_content([
                {
                    "mime_type": file.mimetype,
                    "data": file_content
                },
                {
                    "text": "Extract financial data: description, bill number, quantity, amount."
                }
            ])
            gemini_result = response.text  # Assume JSON-like text output
            gemini_data = json.loads(gemini_result) if gemini_result.startswith('{') else {
                'description': gemini_result,
                'bill_no': 'N/A',
                'quantity': 0,
                'amount': 0.0
            }
            entry = {
                'DATE': datetime.now().strftime('%Y-%m-%d'),
                'PARTICULARS': gemini_data.get('description', 'Processed File'),
                'Voucher_BillNo': gemini_data.get('bill_no', 'N/A'),
                'RECEIPTS_Quantity': int(gemini_data.get('quantity', 0)),
                'RECEIPTS_Amount': float(gemini_data.get('amount', 0.0)),
                'ISSUED_Quantity': 0,
                'ISSUED_Amount': 0.0,
                'BALANCE_Quantity': int(gemini_data.get('quantity', 0)),
                'BALANCE_Amount': float(gemini_data.get('amount', 0.0))
            }
            data_entries.append(entry)

        # Insert into PostgreSQL
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for entry in data_entries:
            cur.execute("""
                INSERT INTO table_name (DATE, PARTICULARS, Voucher_BillNo, RECEIPTS_Quantity, RECEIPTS_Amount,
                                        ISSUED_Quantity, ISSUED_Amount, BALANCE_Quantity, BALANCE_Amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING Entry_ID;
            """, (
                entry['DATE'], entry['PARTICULARS'], entry['Voucher_BillNo'],
                entry['RECEIPTS_Quantity'], entry['RECEIPTS_Amount'],
                entry['ISSUED_Quantity'], entry['ISSUED_Amount'],
                entry['BALANCE_Quantity'], entry['BALANCE_Amount']
            ))
            entry['Entry_ID'] = cur.fetchone()['Entry_ID']
        conn.commit()

        # Push to Google Sheet
        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[
            e['Entry_ID'], e['DATE'], e['PARTICULARS'], e['Voucher_BillNo'],
            e['RECEIPTS_Quantity'], e['RECEIPTS_Amount'], e['ISSUED_Quantity'],
            e['ISSUED_Amount'], e['BALANCE_Quantity'], e['BALANCE_Amount']
        ] for e in data_entries]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        return jsonify({'message': 'Files processed and synced to Google Sheet'}), 200

    except Exception as e:
        logger.error(f"Upload flash error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/results', methods=['GET'])
def get_results():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM table_name ORDER BY Entry_ID")
        data = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(data), 200
    except Exception as e:
        logger.error(f"Results error: {e}")
        return jsonify({'error': 'Failed to load data'}), 500

@app.route('/update', methods=['POST'])
def update_data():
    try:
        updates = request.json
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for update in updates:
            cur.execute("""
                UPDATE table_name
                SET DATE = %s, PARTICULARS = %s, Voucher_BillNo = %s,
                    RECEIPTS_Quantity = %s, RECEIPTS_Amount = %s,
                    ISSUED_Quantity = %s, ISSUED_Amount = %s,
                    BALANCE_Quantity = %s, BALANCE_Amount = %s
                WHERE Entry_ID = %s
            """, (
                update['DATE'], update['PARTICULARS'], update['Voucher_BillNo'],
                update['RECEIPTS_Quantity'], update['RECEIPTS_Amount'],
                update['ISSUED_Quantity'], update['ISSUED_Amount'],
                update['BALANCE_Quantity'], update['BALANCE_Amount'],
                update['Entry_ID']
            ))
        conn.commit()

        # Update Google Sheet
        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[
            u['Entry_ID'], u['DATE'], u['PARTICULARS'], u['Voucher_BillNo'],
            u['RECEIPTS_Quantity'], u['RECEIPTS_Amount'], u['ISSUED_Quantity'],
            u['ISSUED_Amount'], u['BALANCE_Quantity'], u['BALANCE_Amount']
        ] for u in updates]
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range='A1:J'
        ).execute()
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        return jsonify({'message': 'Data updated'}), 200

    except Exception as e:
        logger.error(f"Update error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/export-to-sheet', methods=['POST'])
def export_to_sheet():
    try:
        data = request.json
        spreadsheet = sheets_service.spreadsheets().create(
            body={'properties': {'title': f'Exported_Results_{datetime.now().strftime("%Y%m%d_%H%M%S")}'}}
        ).execute()
        spreadsheet_id = spreadsheet['spreadsheetId']

        headers = [
            'Entry_ID', 'DATE', 'PARTICULARS', 'Voucher_BillNo',
            'RECEIPTS_Quantity', 'RECEIPTS_Amount', 'ISSUED_Quantity',
            'ISSUED_Amount', 'BALANCE_Quantity', 'BALANCE_Amount'
        ]
        values = [headers] + [[
            d['Entry_ID'], d['DATE'], d['PARTICULARS'], d['Voucher_BillNo'],
            d['RECEIPTS_Quantity'], d['RECEIPTS_Amount'], d['ISSUED_Quantity'],
            d['ISSUED_Amount'], d['BALANCE_Quantity'], d['BALANCE_Amount']
        ] for d in data]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        shareable_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        return jsonify({'message': 'Sheet created', 'link': shareable_link}), 200

    except Exception as e:
        logger.error(f"Export error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
    app.run(debug=True)