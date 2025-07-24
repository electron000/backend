import sqlite3
import os
import io
import math
import threading
import logging
import shutil
from io import BytesIO
from datetime import datetime, timezone, timedelta
from functools import wraps

# --- Flask and Web-related Imports ---
from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

# --- Security and JWT Imports ---
import jwt

# --- Data Handling Imports ---
import pandas as pd

# --- Document Generation Imports ---
from docx import Document
from docx.shared import Inches
from docx.enum.section import WD_ORIENT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import landscape, A4, A3, A2
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER

# --- Configuration and Global Variables ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://ongc-contracts.vercel.app"}})
app.config['SECRET_KEY'] = 'a-strong-and-very-secret-key-that-you-should-change'
DB_FILE = 'contracts.db'
EXCEL_FILE = 'Contract Details.xlsx'
TABLE_NAME = 'contracts'
UPLOAD_FOLDER = '.'
BACKUP_FOLDER = os.path.join(UPLOAD_FOLDER, 'backups')
MAX_BACKUPS = 5
ALLOWED_EXTENSIONS = {'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
file_lock = threading.Lock()
FIXED_START_COL = 'SL No'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Setup and Schema Management ---

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_sqlite_column_type_string(data_type):
    """Maps abstract data types to SQLite column types."""
    if data_type == 'numeric':
        return 'REAL'
    elif data_type == 'date':
        return 'TEXT'
    else:
        return 'TEXT'

def sanitize_column_name(col_name):
    """Sanitizes column names for use in SQL queries."""
    return ''.join(e for e in col_name if e.isalnum() or e.isspace()).strip().replace(' ', '_')

def get_current_schema():
    """Fetches the current schema (headers and field types) from the metadata table."""
    conn = get_db_connection()
    try:
        schema_rows = conn.execute("SELECT name, type FROM schema_metadata ORDER BY display_order").fetchall()
    except sqlite3.OperationalError:
        return [], {'numeric': [], 'date': [], 'text': []}
    finally:
        conn.close()
        
    headers = [row['name'] for row in schema_rows]
    field_types = {'numeric': [], 'date': [], 'text': []}
    for row in schema_rows:
        if row['type'] in field_types:
            field_types[row['type']].append(row['name'])
        else:
            field_types['text'].append(row['name'])
    return headers, field_types

def infer_column_types(df):
    """Infers column types from a DataFrame based on keywords and content."""
    inferred = {"numeric": [], "date": [], "text": []}
    keyword_map = {'numeric': ['(₹)', 'amount', 'value', 'duration', 'charges', '(yr)', 'years', 'year', '(inr)'], 'date': ['date']}
    for col in df.columns:
        if col == FIXED_START_COL:
            inferred['numeric'].append(col)
            continue
        found = False
        for type_name, keywords in keyword_map.items():
            if any(keyword in col.lower() for keyword in keywords):
                inferred[type_name].append(col)
                found = True
                break
        if found: continue
        try:
            if not pd.to_numeric(df[col].dropna()).empty:
                inferred["numeric"].append(col)
                continue
        except (ValueError, TypeError): pass
        try:
            if not pd.to_datetime(df[col].dropna(), errors='coerce').isnull().all():
                inferred["date"].append(col)
                continue
        except (ValueError, TypeError): pass
        inferred["text"].append(col)
    return inferred

def reindex_sl_no_in_db(cursor):
    """Re-calculates the 'SL No' column to be sequential."""
    sanitized_sl_no = sanitize_column_name(FIXED_START_COL)
    try:
        rows_to_reindex = cursor.execute(f"SELECT rowid FROM {TABLE_NAME} ORDER BY rowid").fetchall()
        for index, row in enumerate(rows_to_reindex):
            cursor.execute(f'UPDATE {TABLE_NAME} SET "{sanitized_sl_no}" = ? WHERE rowid = ?', (str(index + 1), row['rowid']))
    except sqlite3.OperationalError as e:
        logging.warning(f"Could not re-index '{FIXED_START_COL}'. Error: {e}")

def initialize_schema_from_excel(cursor):
    """Initializes the database schema and populates it with data from the Excel file."""
    df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
    if FIXED_START_COL not in df.columns:
        df.insert(0, FIXED_START_COL, "")
    inferred_types = infer_column_types(df)

    cursor.execute("DELETE FROM schema_metadata")
    for i, col_name in enumerate(df.columns):
        col_type = next((t for t, names in inferred_types.items() if col_name in names), 'text')
        cursor.execute("INSERT INTO schema_metadata (name, type, display_order) VALUES (?, ?, ?)", (col_name, col_type, i))
    
    schema_rows = cursor.execute("SELECT name, type FROM schema_metadata ORDER BY display_order").fetchall()
    create_cols_sql = [f'"{sanitize_column_name(row["name"])}" {get_sqlite_column_type_string(row["type"])}' for row in schema_rows]
    
    cursor.execute(f"CREATE TABLE {TABLE_NAME} ({', '.join(create_cols_sql)})")
    
    df_sanitized_cols = df.copy()
    df_sanitized_cols.columns = [sanitize_column_name(col) for col in df.columns]
    df_sanitized_cols.to_sql(TABLE_NAME, cursor.connection, if_exists='append', index=False)

    reindex_sl_no_in_db(cursor)

def setup_database_and_schema():
    """Main setup function to initialize the database and all necessary tables."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if cursor.fetchone() is None:
        cursor.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL, role TEXT NOT NULL)')
        hashed_admin_pass = generate_password_hash('223010007007')
        cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", ('Infocom-Admin', hashed_admin_pass, 'admin'))
        hashed_user_pass = generate_password_hash('100020003000')
        cursor.execute("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", ('Infocom-User', hashed_user_pass, 'user'))
        logging.info("Users table created and default users added.")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_metadata'")
    if cursor.fetchone() is None:
        cursor.execute('CREATE TABLE schema_metadata (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, type TEXT NOT NULL, display_order INTEGER NOT NULL)')
        logging.info("Schema metadata table created.")

    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
    if cursor.fetchone() is None:
        if os.path.exists(EXCEL_FILE):
            logging.info(f"Initializing database from {EXCEL_FILE}")
            initialize_schema_from_excel(cursor)
        else:
            logging.info("Excel file not found. Creating default empty schema.")
            default_cols = [(FIXED_START_COL, 'numeric'), ('Contract Name', 'text'), ('Value', 'numeric'), ('Start Date', 'date')]
            for i, (name, type) in enumerate(default_cols):
                cursor.execute("INSERT INTO schema_metadata (name, type, display_order) VALUES (?, ?, ?)", (name, type, i))
            
            schema = cursor.execute("SELECT name, type FROM schema_metadata ORDER BY display_order").fetchall()
            sanitized_cols_with_types = [f'"{sanitize_column_name(col["name"])}" {get_sqlite_column_type_string(col["type"])}' for col in schema]
            cursor.execute(f"CREATE TABLE {TABLE_NAME} ({', '.join(sanitized_cols_with_types)})")
    
    conn.commit()
    conn.close()

def export_db_to_excel():
    """Exports the entire database to the Excel file and manages backups."""
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)
    
    if os.path.exists(EXCEL_FILE):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_path = os.path.join(BACKUP_FOLDER, f"backup_{timestamp}_{os.path.basename(EXCEL_FILE)}")
        shutil.copy(EXCEL_FILE, backup_path)
        logging.info(f"Created backup: {backup_path}")

    backups = sorted([os.path.join(BACKUP_FOLDER, f) for f in os.listdir(BACKUP_FOLDER)], key=os.path.getmtime)
    if len(backups) > MAX_BACKUPS:
        for old_backup in backups[:-MAX_BACKUPS]:
            os.remove(old_backup)
            logging.info(f"Removed old backup: {old_backup}")

    conn = get_db_connection()
    try:
        headers, _ = get_current_schema()
        if not headers:
            logging.warning("No schema found for Excel export. Skipping.")
            return
        
        sanitized_headers = [sanitize_column_name(h) for h in headers]
        query_cols = ", ".join([f'"{h}"' for h in sanitized_headers])
        
        db_df = pd.read_sql_query(f"SELECT {query_cols} FROM {TABLE_NAME}", conn)
        
        rename_map = {s_h: o_h for s_h, o_h in zip(sanitized_headers, headers)}
        db_df.rename(columns=rename_map, inplace=True)
        
        if headers:
            db_df = db_df[headers]
        
        with file_lock:
            db_df.to_excel(EXCEL_FILE, index=False)
            logging.info(f"Data successfully exported to {EXCEL_FILE}")
    except Exception as e:
        logging.error(f"An error occurred during DB to Excel sync: {e}")
    finally:
        if conn:
            conn.close()

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if a file's extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Authentication Decorators ---

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('authorization', '').split(" ")[-1] if 'authorization' in request.headers else None
        if not token: return jsonify({'message': 'Token is missing!'}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            g.current_user, g.current_role = data['username'], data['role']
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return jsonify({'message': 'Token is invalid or expired!'}), 401
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('authorization', '').split(" ")[-1] if 'authorization' in request.headers else None
        if not token: return jsonify({'message': 'Token is missing!'}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            if data.get('role') != 'admin':
                return jsonify({'message': 'Admin privileges required!'}), 403
            g.current_user, g.current_role = data['username'], data['role']
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return jsonify({'message': 'Token is invalid or expired!'}), 401
        return f(*args, **kwargs)
    return decorated

# --- Data Fetching and Filtering Logic ---

def get_filtered_sorted_data(headers, field_types, params):
    if not headers: return []
    conn = get_db_connection()
    sort_field = params.get('sortField', FIXED_START_COL)
    sort_direction = params.get('sortDirection', 'asc').upper()
    where_clauses, query_params = [], []
    filter_field = params.get('filterField')
    
    if filter_field:
        sanitized_filter_field = sanitize_column_name(filter_field)
        if filter_field in field_types.get('numeric', []):
            min_range, max_range = params.get('minRange'), params.get('maxRange')
            if min_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) >= ?'); query_params.append(float(min_range))
            if max_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) <= ?'); query_params.append(float(max_range))
        elif filter_field in field_types.get('date', []):
            from_date, to_date = params.get('fromDate'), params.get('toDate')
            if from_date: where_clauses.append(f'date("{sanitized_filter_field}") >= date(?)'); query_params.append(from_date)
            if to_date: where_clauses.append(f'date("{sanitized_filter_field}") <= date(?)'); query_params.append(to_date)
        else:
            filter_value = params.get('filterValue')
            if filter_value: where_clauses.append(f'LOWER("{sanitized_filter_field}") LIKE ?'); query_params.append(f"%{filter_value.lower()}%")
    
    where_statement = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sanitized_sort_field = sanitize_column_name(sort_field)
    order_by_clause = f'ORDER BY CAST("{sanitize_column_name(FIXED_START_COL)}" AS REAL) ASC'
    if sort_field in field_types.get('numeric', []): order_by_clause = f'ORDER BY CAST("{sanitized_sort_field}" AS REAL) {sort_direction}'
    elif sort_field in field_types.get('date', []): order_by_clause = f'ORDER BY date("{sanitized_sort_field}") {sort_direction}'
    elif sort_field in field_types.get('text', []): order_by_clause = f'ORDER BY "{sanitized_sort_field}" {sort_direction}'

    sanitized_headers = [sanitize_column_name(h) for h in headers]
    select_clause = "SELECT rowid as id, " + ", ".join(f'"{col}"' for col in sanitized_headers)
    data_query = f"{select_clause} FROM {TABLE_NAME} {where_statement} {order_by_clause}"
    
    results = conn.execute(data_query, tuple(query_params)).fetchall()
    conn.close()
    
    sanitized_to_original_map = {sanitize_column_name(h): h for h in headers}
    contracts = []
    for row in results:
        row_dict = dict(row)
        formatted_row = {'id': row_dict.get('id')}
        for s_key, value in row_dict.items():
            if s_key != 'id':
                original_key = sanitized_to_original_map.get(s_key)
                if original_key:
                    formatted_row[original_key] = value
        contracts.append(formatted_row)
            
    return contracts

# --- API Endpoints ---

@app.route('/api/schema', methods=['GET'])
@admin_required
def get_schema_endpoint():
    headers, field_types = get_current_schema()
    schema = [{'name': h, 'type': next((t for t, names in field_types.items() if h in names), 'text')} for h in headers]
    return jsonify(schema)

@app.route('/api/schema/columns', methods=['POST'])
@admin_required
def add_column():
    data = request.get_json()
    new_col_name = data.get('name')
    new_col_type = data.get('type')
    if not new_col_name or not new_col_type:
        return jsonify({"error": "New column name and type are required."}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM schema_metadata WHERE LOWER(name) = LOWER(?)", (new_col_name,))
        if cursor.fetchone():
            return jsonify({"error": f"A column named '{new_col_name}' already exists."}), 409
        sqlite_type_str = get_sqlite_column_type_string(new_col_type)
        cursor.execute(f'ALTER TABLE {TABLE_NAME} ADD COLUMN "{sanitize_column_name(new_col_name)}" {sqlite_type_str}')
        max_order_result = cursor.execute("SELECT MAX(display_order) FROM schema_metadata").fetchone()
        max_order = max_order_result[0] if max_order_result and max_order_result[0] is not None else 0
        cursor.execute("INSERT INTO schema_metadata (name, type, display_order) VALUES (?, ?, ?)", (new_col_name, new_col_type, max_order + 1))
        conn.commit()
        logging.info(f"Column '{new_col_name}' added successfully.")
    except sqlite3.Error as e:
        conn.rollback()
        logging.error(f"Database error adding column '{new_col_name}': {e}")
        return jsonify({"error": f"Database error: {e}"}), 500
    finally:
        conn.close()
    export_db_to_excel()
    return jsonify({"message": f"Column '{new_col_name}' added successfully."}), 201

@app.route('/api/schema/columns/<path:column_name>', methods=['PUT'])
@admin_required
def update_column(column_name):
    data = request.get_json()
    new_name = data.get('name')
    new_type = data.get('type')

    if not new_name and not new_type:
        return jsonify({"error": "No new name or type provided for update."}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # --- PRE-FLIGHT CHECKS ---
        cursor.execute("SELECT type FROM schema_metadata WHERE name = ?", (column_name,))
        if not cursor.fetchone():
            return jsonify({"error": f"Column '{column_name}' not found."}), 404

        if new_name and new_name != column_name:
            cursor.execute("SELECT name FROM schema_metadata WHERE LOWER(name) = LOWER(?)", (new_name,))
            if cursor.fetchone():
                return jsonify({"error": f"A column named '{new_name}' already exists."}), 409

        # --- REBUILD THE TABLE (Safest method for rename and/or type change) ---
        
        # 1. Get the current schema before any changes
        current_schema = cursor.execute("SELECT name, type FROM schema_metadata ORDER BY display_order").fetchall()
        
        # 2. Define the new schema based on the requested changes
        target_schema = []
        for col in current_schema:
            if col['name'] == column_name:
                target_schema.append({
                    'name': new_name if new_name else column_name,
                    'type': new_type if new_type else col['type']
                })
            else:
                target_schema.append(dict(col))

        # 3. Create the temporary table with the new, target schema
        temp_table_name = f"{TABLE_NAME}_temp_update"
        new_col_definitions = [f'"{sanitize_column_name(c["name"])}" {get_sqlite_column_type_string(c["type"])}' for c in target_schema]
        cursor.execute(f"CREATE TABLE {temp_table_name} ({', '.join(new_col_definitions)})")

        # 4. Copy data from the old table to the new one
        old_sanitized_names = [f'"{sanitize_column_name(c["name"])}"' for c in current_schema]
        new_sanitized_names = [f'"{sanitize_column_name(c["name"])}"' for c in target_schema]
        cursor.execute(f"INSERT INTO {temp_table_name} ({', '.join(new_sanitized_names)}) SELECT {', '.join(old_sanitized_names)} FROM {TABLE_NAME}")

        # 5. Drop the old table and rename the new one
        cursor.execute(f"DROP TABLE {TABLE_NAME}")
        cursor.execute(f"ALTER TABLE {temp_table_name} RENAME TO {TABLE_NAME}")

        # 6. Update the metadata table to reflect the changes
        if new_name and new_name != column_name:
            cursor.execute("UPDATE schema_metadata SET name = ? WHERE name = ?", (new_name, column_name))
        if new_type:
            final_name = new_name if new_name else column_name
            cursor.execute("UPDATE schema_metadata SET type = ? WHERE name = ?", (new_type, final_name))
        
        conn.commit()
        logging.info(f"Column '{column_name}' updated successfully. Table rebuilt.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating column '{column_name}': {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
        
    export_db_to_excel()
    return jsonify({"message": "Column updated successfully."}), 200

@app.route('/api/schema/reorder', methods=['POST'])
@admin_required
def reorder_columns():
    ordered_columns = request.get_json()
    if not ordered_columns:
        return jsonify({"error": "A list of ordered columns is required."}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for i, col_name in enumerate(ordered_columns):
            cursor.execute("UPDATE schema_metadata SET display_order = ? WHERE name = ?", (i, col_name))
        conn.commit()
        logging.info("Column order updated successfully.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error reordering columns: {e}")
        return jsonify({"error": str(e)}), 500
    finally: conn.close()
    export_db_to_excel()
    return jsonify({"message": "Column order updated successfully."}), 200

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username, password, login_mode = data.get('username'), data.get('password'), data.get('loginMode')
    if not all([username, password, login_mode]): return jsonify({"error": "Missing credentials or login mode."}), 400
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    conn.close()
    if user and check_password_hash(user['password'], password):
        if user['role'] != login_mode: return jsonify({"error": f"Please use the '{user['role'].capitalize()}' login panel."}), 403
        token = jwt.encode({'username': user['username'], 'role': user['role'], 'exp': datetime.now(timezone.utc) + timedelta(hours=24)}, app.config['SECRET_KEY'], algorithm="HS256")
        return jsonify({'token': token}), 200
    return jsonify({"error": "Invalid credentials"}), 401

@app.route('/api/contracts', methods=['GET'])
@token_required 
def get_contracts():
    headers, field_types = get_current_schema()
    all_data = get_filtered_sorted_data(headers, field_types, request.args)
    if 'page' not in request.args:
        return jsonify(all_data)
    page, limit = request.args.get('page', 1, type=int), request.args.get('limit', 10, type=int)
    total_pages = math.ceil(len(all_data) / limit) if all_data else 1
    paginated_data = all_data[((page - 1) * limit):(page * limit)]
    return jsonify({
        "data": paginated_data, 
        "totalPages": total_pages, 
        "currentPage": page, 
        "headers": headers, 
        "fieldTypes": field_types
    })

@app.route('/api/contracts', methods=['POST'])
@admin_required
def add_contract():
    new_data = request.get_json()
    headers, field_types = get_current_schema()
    is_valid, error_message = validate_data(new_data, field_types)
    if not is_valid: return jsonify({"error": error_message}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        final_data = {h: new_data.get(h, '') for h in headers if h != FIXED_START_COL}
        sanitized_keys = [sanitize_column_name(k) for k in final_data.keys()]
        columns = ', '.join(f'"{k}"' for k in sanitized_keys)
        placeholders = ', '.join(['?'] * len(final_data))
        cursor.execute(f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})", list(final_data.values()))
        reindex_sl_no_in_db(cursor)
        conn.commit()
    finally:
        conn.close()
    export_db_to_excel()
    return jsonify({"message": "Contract added successfully"}), 201

@app.route('/api/contracts/<int:row_id>', methods=['PUT'])
@admin_required
def update_contract(row_id):
    updated_data = request.get_json()
    _, field_types = get_current_schema()
    is_valid, error_message = validate_data(updated_data, field_types)
    if not is_valid: return jsonify({"error": error_message}), 400
    conn = get_db_connection()
    try:
        set_clauses = [f'"{sanitize_column_name(k)}" = ?' for k in updated_data if k != FIXED_START_COL and k != 'id']
        params = [v for k, v in updated_data.items() if k != FIXED_START_COL and k != 'id']
        if not set_clauses: return jsonify({"error": "No valid fields to update"}), 400
        params.append(row_id)
        conn.execute(f"UPDATE {TABLE_NAME} SET {', '.join(set_clauses)} WHERE rowid = ?", tuple(params))
        conn.commit()
    finally:
        conn.close()
    export_db_to_excel()
    return jsonify({"message": f"Contract {row_id} updated successfully."}), 200

@app.route('/api/contracts/<int:row_id>', methods=['DELETE'])
@admin_required
def delete_contract(row_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", (row_id,))
        reindex_sl_no_in_db(cursor)
        conn.commit()
    finally:
        conn.close()
    export_db_to_excel()
    return jsonify({"message": f"Contract {row_id} deleted successfully."}), 200

@app.route('/api/upload', methods=['POST'])
@admin_required
def upload_file():
    if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"error": "Invalid or no selected file"}), 400
    try:
        with file_lock:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], EXCEL_FILE)
            file.save(filepath)
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            setup_database_and_schema()
        return jsonify({"message": "File uploaded and database re-initialized successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/export', methods=['GET'])
@token_required
def data_export():
    headers, field_types = get_current_schema()
    filtered_data = get_filtered_sorted_data(headers, field_types, request.args)

    if not filtered_data:
        return jsonify({"error": "No data to export for the current filters."}), 404

    db_df = pd.DataFrame(filtered_data)
    
    selected_fields_str = request.args.get('selectedFields', '')
    if selected_fields_str:
        selected_fields = selected_fields_str.split(',')
        columns_to_keep = [col for col in selected_fields if col in db_df.columns]
        if 'id' in db_df.columns and 'id' not in columns_to_keep:
             columns_to_keep.insert(0, 'id')
        db_df = db_df[columns_to_keep]

    if 'id' in db_df.columns:
        db_df = db_df.drop(columns=['id'])

    output = io.BytesIO()
    format_type = request.args.get('format', 'xlsx')
    file_name_from_req = request.args.get('fileName')
    download_name_str = (file_name_from_req or 'contracts_export').replace(' ', '_')
    
    if format_type == 'csv':
        db_df.to_csv(output, index=False)
        mimetype, file_extension = 'text/csv', 'csv'

    elif format_type == 'docx':
        document = Document()
        section = document.sections[0]
        section.orientation = WD_ORIENT.LANDSCAPE
        section.page_width, section.page_height = section.page_height, section.page_width
        if file_name_from_req: document.add_heading(file_name_from_req, 0)
        table = document.add_table(rows=1, cols=len(db_df.columns))
        table.style = 'Table Grid'
        hdr_cells = table.rows[0].cells
        for i, col_name in enumerate(db_df.columns):
            hdr_cells[i].paragraphs[0].add_run(str(col_name)).bold = True
        for _, row in db_df.iterrows():
            row_cells = table.add_row().cells
            for i, cell_value in enumerate(row):
                row_cells[i].text = str(cell_value)
        document.save(output)
        mimetype, file_extension = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx'

    elif format_type == 'pdf':
        num_cols = len(db_df.columns)
        pagesize = landscape(A2) if num_cols > 25 else landscape(A3) if num_cols > 15 else landscape(A4)
        doc = SimpleDocTemplate(output, pagesize=pagesize, rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        font_scale = max(0.5, 1 - (num_cols / 40.0))
        base_font_size = 8
        scaled_font_size = base_font_size * font_scale
        header_style = ParagraphStyle('Header', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=scaled_font_size, textColor=colors.white, alignment=TA_CENTER, leading=scaled_font_size * 1.2)
        body_style = ParagraphStyle('Body', parent=styles['Normal'], fontName='Helvetica', fontSize=scaled_font_size - 1, alignment=TA_CENTER, leading=scaled_font_size * 1.2)
        elements = []
        available_width = doc.width
        col_widths = []
        for col in db_df.columns:
            header_len = len(str(col))
            max_data_len = db_df[col].astype(str).str.len().max()
            if pd.isna(max_data_len): max_data_len = 0
            col_width = max(header_len, int(max_data_len)) * scaled_font_size * 0.6
            col_widths.append(max(40, min(col_width, available_width / num_cols * 2.0 if num_cols > 0 else available_width)))
        total_width = sum(col_widths)
        if total_width > 0:
            col_widths = [w * available_width / total_width for w in col_widths]
        header_row = [Paragraph(str(h).replace('(₹)', '(INR)'), header_style) for h in db_df.columns]
        data_rows = [header_row]
        for _, row in db_df.iterrows():
            data_rows.append([Paragraph(str(item), body_style) for item in row])
        table = Table(data_rows, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#9B1C1C')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.black),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,0), 8),
            ('TOPPADDING', (0,0), (-1,0), 8),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#F0F0F0'), colors.white])
        ]))
        elements.append(table)
        def header_footer(canvas, doc):
            canvas.saveState()
            if file_name_from_req:
                canvas.setFont('Helvetica-Bold', 12)
                canvas.drawCentredString(doc.width / 2.0 + doc.leftMargin, doc.height + doc.topMargin - 25, file_name_from_req)
            canvas.setFont('Helvetica', 8)
            canvas.setFillColor(colors.grey)
            generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            canvas.drawString(doc.leftMargin, doc.bottomMargin - 15, f"Generated on: {generation_time}")
            canvas.drawRightString(doc.width + doc.leftMargin, doc.bottomMargin - 15, f"Page {doc.page}")
            canvas.restoreState()
        doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)
        mimetype, file_extension = 'application/pdf', 'pdf'
    else:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            db_df.to_excel(writer, index=False, sheet_name='Contracts')
            workbook, worksheet = writer.book, writer.sheets['Contracts']
            header_format = workbook.add_format({'bold': True, 'fg_color': '#D7E4BC', 'border': 1})
            for col_num, value in enumerate(db_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                column_len = max((db_df[value].astype(str).str.len().max() or 0), len(str(value))) + 2
                worksheet.set_column(col_num, col_num, column_len)
            worksheet.freeze_panes(1, 0)
        mimetype, file_extension = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'xlsx'
    output.seek(0)
    return send_file(output, mimetype=mimetype, as_attachment=True, download_name=f'{download_name_str}.{file_extension}')

def validate_data(data_dict, field_types):
    """Validates incoming data against schema field types."""
    errors = {}
    for key, value in data_dict.items():
        if value is None or str(value).strip() == '': continue
        if key in field_types.get('numeric', []):
            try: float(value)
            except (ValueError, TypeError): errors[key] = "must be a valid number."
        if key in field_types.get('date', []):
            try: pd.to_datetime(value, errors='raise')
            except (ValueError, TypeError): errors[key] = "must be a valid date format."
    if errors:
        return False, "Invalid data format: " + ", ".join([f"'{k}' ({v})" for k, v in errors.items()])
    return True, None

if __name__ == '__main__':
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)
    setup_database_and_schema()
    app.run(host='0.0.0.0', port=5001, debug=True)
