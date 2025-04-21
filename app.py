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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000", "https://your-deployed-frontend.com"],
                                 "methods": ["GET", "POST", "OPTIONS"],
                                 "allow_headers": ["Content-Type"]}})

# Configure logging
logging.basicConfig(level=logging.INFO, filename='app.log')
logger = logging.getLogger(__name__)

# Database connection
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'student_data_from_image'),  # Changed database name here
    'user': os.getenv('DB_USER', 'your_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'host': os.getenv('DB_HOST', 'your_host'),
    'port': os.getenv('DB_PORT', '5432')
}

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDS_FILE = os.getenv('GOOGLE_CREDS', 'credentials.json')
try:
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    sheets_service = build('sheets', 'v4', credentials=creds)
except Exception as e:
    logger.error(f"Google Sheets API setup failed: {e}")
    raise

# Gemini 2.0 Flash setup
try:
    genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
    gemini_model = genai.GenerativeModel('gemini-2.0-flash')
except Exception as e:
    logger.error(f"Gemini API setup failed: {e}")
    raise

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# Existing upload route (preserved - needs adaptation if used for student data)
@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload_files():
    if request.method == 'OPTIONS':
        return '', 200
    try:
        if 'files' not in request.files:
            logger.warning("No files in request")
            return jsonify({'error': 'No files uploaded'}), 400
        files = request.files.getlist('files')
        if not files or all(f.filename == '' for f in files):
            logger.warning("Empty file list or no valid files")
            return jsonify({'error': 'No valid files uploaded'}), 400

        data_entries = []
        for file in files:
            gemini_result = {}  # Replace with your original Gemini logic
            entry = {
                'बालकांचे नाव': gemini_result.get('name', 'Processed File'),
                'वजन (किलो)': gemini_result.get('weight'),
                'उंची (सेमी)': gemini_result.get('height'),
                'शेरा': gemini_result.get('remark')
            }
            data_entries.append(entry)

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for entry in data_entries:
            cur.execute(f"""
                INSERT INTO student_data_from_image ("बालकांचे नाव", "वजन (किलो)", "उंची (सेमी)", "शेरा")
                VALUES (%s, %s, %s, %s)
                RETURNING "अ.क्र.";
            """, (
                entry.get("बालकांचे नाव"),
                entry.get("वजन (किलो)"),
                entry.get("उंची (सेमी)"),
                entry.get("शेरा")
            ))
            entry['अ.क्र.'] = cur.fetchone()['अ.क्र.']
        conn.commit()

        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[e['अ.क्र.'], None, e.get("बालकांचे नाव"), e.get("वजन (किलो)"), e.get("उंची (सेमी)"), e.get("शेरा")]
                  for e in data_entries]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(files)} files successfully via /upload")
        return jsonify({'message': 'Files processed'}), 200

    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'error': str(e)}), 500

# Updated upload-flash with detailed error logging
@app.route('/upload-flash', methods=['POST', 'OPTIONS'])
def upload_files_flash():
    if request.method == 'OPTIONS':
        return '', 200
    try:
        if 'files' not in request.files:
            logger.warning("No files in request")
            return jsonify({'error': 'No files uploaded'}), 400
        files = request.files.getlist('files')
        if not files or all(f.filename == '' for f in files):
            logger.warning("Empty file list or no valid files")
            return jsonify({'error': 'No valid files uploaded'}), 400

        logger.info(f"Processing {len(files)} files with Gemini 2.0 Flash")
        data_entries = []
        for file in files:
            try:
                file_content = file.read()
                logger.debug(f"Processing file: {file.filename}, size: {len(file_content)} bytes")
                response = gemini_model.generate_content([
                    {"mime_type": file.mimetype, "data": file_content},
                    {"text": "Extract student data: name, weight (kg), height (cm), remark."}
                ])
                gemini_result = response.text
                logger.debug(f"Gemini result: {gemini_result}")
                try:
                    gemini_data = json.loads(gemini_result)
                    entry = {
                        "बालकांचे नाव": gemini_data.get('name'),
                        "वजन (किलो)": gemini_data.get('weight'),
                        "उंची (सेमी)": gemini_data.get('height'),
                        "शेरा": gemini_data.get('remark')
                    }
                except json.JSONDecodeError:
                    # Fallback if Gemini doesn't return JSON in the expected format
                    parts = gemini_result.split(',')
                    entry = {
                        "बालकांचे नाव": parts[0].strip() if len(parts) > 0 else None,
                        "वजन (किलो)": parts[1].strip() if len(parts) > 1 else None,
                        "उंची (सेमी)": parts[2].strip() if len(parts) > 2 else None,
                        "शेरा": parts[3].strip() if len(parts) > 3 else None,
                    }
                data_entries.append(entry)
            except Exception as e:
                logger.error(f"Gemini processing failed for {file.filename}: {e}")
                raise

        logger.info("Inserting into PostgreSQL")
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        for entry in data_entries:
            cur.execute(f"""
                INSERT INTO student_data_from_image ("बालकांचे नाव", "वजन (किलो)", "उंची (सेमी)", "शेरा")
                VALUES (%s, %s, %s, %s)
                RETURNING "अ.क्र.";
            """, (
                entry.get("बालकांचे नाव"),
                entry.get("वजन (किलो)"),
                entry.get("उंची (सेमी)"),
                entry.get("शेरा")
            ))
            entry['अ.क्र.'] = cur.fetchone()['अ.क्र.']
        conn.commit()

        logger.info("Syncing to Google Sheets")
        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[e['अ.क्र.'], None, e.get("बालकांचे नाव"), e.get("वजन (किलो)"), e.get("उंची (सेमी)"), e.get("शेरा")]
                  for e in data_entries]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/preview"
        logger.info(f"Upload successful, sheet URL: {sheet_url}")
        return jsonify({'message': 'Files processed and synced to Google Sheet', 'sheet_url': sheet_url}), 200

    except Exception as e:
        logger.error(f"Upload-flash error: {e}")
        return jsonify({'error': f"Failed to process files: {str(e)}"}), 500

@app.route('/results', methods=['GET'])
def get_results():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM student_data_from_image ORDER BY \"अ.क्र.\"")
        data = cur.fetchall()
        cur.close()
        conn.close()
        logger.info("Fetched results successfully")
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
                UPDATE student_data_from_image
                SET "वर्ग क्र." = %s, "बालकांचे नाव" = %s, "वजन (किलो)" = %s, "उंची (सेमी)" = %s, "शेरा" = %s
                WHERE "अ.क्र." = %s
            """, (
                update.get("वर्ग क्र."),
                update.get("बालकांचे नाव"),
                update.get("वजन (किलो)"),
                update.get("उंची (सेमी)"),
                update.get("शेरा"),
                update.get("अ.क्र.")
            ))
        conn.commit()

        spreadsheet_id = os.getenv('SPREADSHEET_ID', 'your_spreadsheet_id')
        values = [[u.get("अ.क्र."), u.get("वर्ग क्र."), u.get("बालकांचे नाव"), u.get("वजन (किलो)"), u.get("उंची (सेमी)"), u.get("शेरा")]
                  for u in updates]
        sheets_service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='A1:F').execute()
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range='A1', valueInputOption='RAW', body={'values': values}
        ).execute()

        cur.close()
        conn.close()
        logger.info("Data updated successfully")
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

        headers = ["अ.क्र.", "वर्ग क्र.", "बालकांचे नाव", "वजन (किलो)", "उंची (सेमी)", "शेरा"]
        values = [headers] + [[d.get("अ.क्र."), d.get("वर्ग क्र."), d.get("बालकांचे नाव"), d.get("वजन (किलो)"), d.get("उंची (सेमी)"), d.get("शेरा")]
                              for d in data]
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range='A1', valueInputOption='RAW', body={'values': values}
        ).execute()

        shareable_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        logger.info(f"Exported to new sheet: {shareable_link}")
        return jsonify({'message': 'Sheet created', 'link': shareable_link}), 200
    except Exception as e:
        logger.error(f"Export error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)