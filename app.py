# Standard Library Imports
import sqlite3
import pandas as pd
import os
import math
import threading
import io
from datetime import datetime

# Flask and Web-related Imports
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename

# Document Generation Imports
# -- DOCX --
from docx import Document
from docx.shared import Inches
from docx.enum.section import WD_ORIENT

# -- ReportLab for PDF --
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import landscape, A4, A3, A2
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# --- Configuration and Global Variables ---
app = Flask(__name__)
# Using a specific CORS policy for production security. Change to "*" for local development.
CORS(app, resources={r"/api/*": {"origins": "https://ongc-contracts.vercel.app"}})

DB_FILE = 'contracts.db'
EXCEL_FILE = 'Contract Details.xlsx'
TABLE_NAME = 'contracts'
UPLOAD_FOLDER = '.'
ALLOWED_EXTENSIONS = {'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
file_lock = threading.Lock()
original_column_names = []
column_metadata = {
    "headers": [],
    "fieldTypes": { "numeric": [], "date": [], "text": [] }
}

# --- Helper Functions ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def sanitize_column_name(col_name):
    return ''.join(e for e in col_name if e.isalnum() or e.isspace()).strip().replace(' ', '_')

def infer_column_metadata(df):
    global column_metadata, original_column_names
    original_column_names = df.columns.tolist()
    inferred_field_types = {"numeric": [], "date": [], "text": []}
    keyword_map = {
        'numeric': ['(₹)', 'amount', 'value', 'duration', 'charges', '(yr)', 'year', 'number', 'no', '(inr)'],
        'date': ['date']
    }
    for col in original_column_names:
        col_lower = col.lower()
        found_by_keyword = False
        for type_name, keywords in keyword_map.items():
            if any(keyword in col_lower for keyword in keywords):
                inferred_field_types[type_name].append(col)
                found_by_keyword = True
                break
        if found_by_keyword: continue
        try:
            numeric_series = pd.to_numeric(df[col].dropna())
            if not numeric_series.empty:
                inferred_field_types["numeric"].append(col)
                continue
        except (ValueError, TypeError): pass
        try:
            date_series = pd.to_datetime(df[col].dropna(), errors='coerce')
            if not date_series.isnull().all():
                 inferred_field_types["date"].append(col)
                 continue
        except (ValueError, TypeError): pass
        inferred_field_types["text"].append(col)
    if 'SL No' in original_column_names and 'SL No' not in inferred_field_types['numeric']:
        for key in inferred_field_types:
            if 'SL No' in inferred_field_types[key]:
                inferred_field_types[key].remove('SL No')
        inferred_field_types['numeric'].append('SL No')
    column_metadata["headers"] = original_column_names
    column_metadata["fieldTypes"] = inferred_field_types
    print(f"--- Inferred Column Metadata: {column_metadata}")

def reindex_sl_no_in_db(cursor):
    sanitized_sl_no = sanitize_column_name('SL No')
    try:
        cursor.execute(f"SELECT rowid FROM {TABLE_NAME} ORDER BY rowid")
        rows_to_reindex = cursor.fetchall()
        for index, row in enumerate(rows_to_reindex):
            new_sl_no = index + 1
            cursor.execute(f'UPDATE {TABLE_NAME} SET "{sanitized_sl_no}" = ? WHERE rowid = ?', (str(new_sl_no), row['rowid']))
        print("Re-indexed all 'SL No' values in the database.")
    except sqlite3.OperationalError as e:
        print(f"Warning: Could not re-index 'SL No'. Error: {e}")

# --- Database Initialization and Synchronization ---
def export_db_to_excel():
    print("Attempting to sync database to Excel file...")
    conn = get_db_connection()
    try:
        db_df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        sl_no_col_sanitized = sanitize_column_name('SL No')
        if sl_no_col_sanitized in db_df.columns:
            db_df[sl_no_col_sanitized] = pd.to_numeric(db_df[sl_no_col_sanitized], errors='coerce')
            db_df = db_df.sort_values(by=sl_no_col_sanitized).reset_index(drop=True)
            db_df[sl_no_col_sanitized] = range(1, len(db_df) + 1)
        if not original_column_names: return
        rename_map = {sanitize_column_name(col): col for col in original_column_names}
        db_df_renamed = db_df.rename(columns=rename_map)
        final_columns = [col for col in original_column_names if col in db_df_renamed.columns]
        db_df_final = db_df_renamed[final_columns]
        with file_lock:
            db_df_final.to_excel(EXCEL_FILE, index=False)
        print("Database successfully synced to Excel file.")
    except Exception as e:
        print(f"An error occurred during DB to Excel sync: {e}")
    finally:
        if conn: conn.close()

def setup_database():
    global original_column_names
    print(f"--- Running setup_database for {EXCEL_FILE} ---")
    if not os.path.exists(EXCEL_FILE):
        print(f"'{EXCEL_FILE}' not found. Database setup skipped.")
        return

    df = pd.DataFrame()
    try:
        xls = pd.ExcelFile(EXCEL_FILE)
        for sheet_name in xls.sheet_names:
            temp_df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str).fillna('')
            if not temp_df.empty or not temp_df.columns.empty:
                df = temp_df
                print(f"Data or headers found and loaded from sheet: '{sheet_name}'")
                break
    except Exception as e:
        print(f"Could not read Excel file. It may be empty or corrupted. Error: {e}")

    if df.empty and os.path.exists(EXCEL_FILE):
        try: df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
        except Exception: pass

    temp_original_cols = df.columns.tolist()
    if 'SL No' not in temp_original_cols:
        print("Adding 'SL No' column as it was not found.")
        df.insert(0, 'SL No', range(1, len(df) + 1))
    else:
        df['SL No'] = range(1, len(df) + 1)
        if df.columns.get_loc('SL No') != 0:
            cols = df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('SL No')))
            df = df[cols]
        print("Existing 'SL No' column re-indexed to ensure sequential order.")

    infer_column_metadata(df)
    df.columns = [sanitize_column_name(col) for col in original_column_names]
    
    conn = get_db_connection()
    try:
        df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        print(f"Database '{DB_FILE}' created/replaced and populated successfully.")
    except Exception as e:
        print(f"An error occurred during database setup: {e}")
    finally:
        conn.close()

def do_setup():
    if not os.path.exists(UPLOAD_FOLDER): os.makedirs(UPLOAD_FOLDER)
    db_exists = os.path.exists(DB_FILE)
    excel_exists = os.path.exists(EXCEL_FILE)
    if excel_exists:
        if not db_exists or os.path.getmtime(EXCEL_FILE) > os.path.getmtime(DB_FILE):
            print(f"'{EXCEL_FILE}' is newer or DB doesn't exist. Re-initializing...")
            if db_exists:
                try:
                    conn_check = get_db_connection(); conn_check.close()
                except Exception: pass
                os.remove(DB_FILE)
            setup_database()
        else:
            print(f"Database '{DB_FILE}' is up-to-date. Loading metadata.")
            temp_df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
            infer_column_metadata(temp_df)
    elif not db_exists:
        print("Neither Excel file nor Database found. Ready for first upload.")

# --- API Endpoints ---
users = {"Infocom-Sivasagar": "223010007007"}
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data: return jsonify({"error": "Request must be JSON"}), 400
    username = data.get('username')
    password = data.get('password')
    if username in users and users[username] == password:
        return jsonify({"message": "Login successful"}), 200
    return jsonify({"error": "Invalid credentials"}), 401

def get_filtered_sorted_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
        allowed_columns = [row[1] for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []

    sort_field = request.args.get('sortField', 'SL No')
    sort_direction = request.args.get('sortDirection', 'asc').upper()
    filter_field = request.args.get('filterField')
    filter_value = request.args.get('filterValue')
    min_range = request.args.get('minRange')
    max_range = request.args.get('maxRange')
    from_date = request.args.get('fromDate')
    to_date = request.args.get('toDate')

    where_clauses = []
    params = []
    if filter_field:
        sanitized_filter_field = sanitize_column_name(filter_field)
        if sanitized_filter_field in allowed_columns:
            if min_range or max_range:
                if min_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) >= ?'); params.append(float(min_range))
                if max_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) <= ?'); params.append(float(max_range))
            elif from_date or to_date:
                if from_date: where_clauses.append(f'date("{sanitized_filter_field}") >= date(?)'); params.append(from_date)
                if to_date: where_clauses.append(f'date("{sanitized_filter_field}") <= date(?)'); params.append(to_date)
            elif filter_value:
                where_clauses.append(f'LOWER("{sanitized_filter_field}") LIKE ?')
                params.append(f"%{filter_value.lower()}%")
                
    where_statement = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    sanitized_sort_field = sanitize_column_name(sort_field)
    order_by_clause = f'ORDER BY CAST("{sanitize_column_name("SL No")}" AS REAL) ASC'
    if sort_field in column_metadata['fieldTypes']['numeric']:
        order_by_clause = f'ORDER BY CAST("{sanitized_sort_field}" AS REAL) {sort_direction}'
    elif sort_field in column_metadata['fieldTypes']['date']:
        order_by_clause = f'ORDER BY date("{sanitized_sort_field}") {sort_direction}'
    
    select_clause = "SELECT rowid as id, " + ", ".join(f'"{col}"' for col in allowed_columns)
    data_query = f"{select_clause} FROM {TABLE_NAME} {where_statement} {order_by_clause}"
    
    contracts_cursor = cursor.execute(data_query, tuple(params))
    
    contracts = []
    sanitized_to_original_map = {sanitize_column_name(col): col for col in original_column_names}
    for row in contracts_cursor.fetchall():
        row_dict = dict(row)
        formatted_row = {sanitized_to_original_map.get(key, key): value for key, value in row_dict.items()}
        contracts.append(formatted_row)
        
    return contracts

@app.route('/api/contracts', methods=['GET'])
def get_contracts():
    conn = get_db_connection()
    all_data = get_filtered_sorted_data(conn)
    conn.close()
    
    is_paginated = 'page' in request.args and 'limit' in request.args

    # This logic correctly handles returning all data for exports (like PDF)
    if not is_paginated:
        return jsonify(all_data)

    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 10, type=int)
    
    total_records = len(all_data)
    total_pages = math.ceil(total_records / limit) if total_records > 0 else 1
    
    start_index = (page - 1) * limit
    end_index = start_index + limit
    paginated_data = all_data[start_index:end_index]
    
    return jsonify({
        "data": paginated_data,
        "totalPages": total_pages,
        "currentPage": page,
        "headers": column_metadata["headers"],
        "fieldTypes": column_metadata["fieldTypes"]
    })

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({"error": "Invalid or no selected file"}), 400
    try:
        with file_lock:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], EXCEL_FILE)
            file.save(filepath)
            try:
                conn_check = get_db_connection(); conn_check.close()
            except Exception: pass
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            setup_database()
        return jsonify({"message": "File uploaded and database re-initialized successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/contracts', methods=['POST'])
def add_contract():
    new_data = request.get_json()
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
        table_columns = [row[1] for row in cursor.fetchall()]
        final_data = {col: '' for col in table_columns}
        for key, value in new_data.items():
            sanitized_key = sanitize_column_name(key)
            if sanitized_key in final_data: final_data[sanitized_key] = value
        final_data.pop(sanitize_column_name('SL No'), None)
        columns = ', '.join(f'"{k}"' for k in final_data.keys())
        placeholders = ', '.join(['?'] * len(final_data))
        cursor.execute(f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})", list(final_data.values()))
        reindex_sl_no_in_db(cursor)
        conn.commit()
        export_db_to_excel()
        return jsonify({"message": "Contract added"}), 201
    finally: conn.close()

@app.route('/api/contracts/<int:id>', methods=['PUT'])
def update_contract(id):
    """Updates an existing contract record identified by its rowid."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # First, ensure the contract exists
        cursor.execute(f"SELECT rowid FROM {TABLE_NAME} WHERE rowid = ?", (id,))
        if cursor.fetchone() is None:
            return jsonify({"error": "Contract not found"}), 404

        updated_data = request.get_json()
        if not updated_data:
            return jsonify({"error": "Invalid data provided for update"}), 400

        set_clauses = []
        params = []
        for key, value in updated_data.items():
            sanitized_key = sanitize_column_name(key)
            # Prevent direct update of SL No or internal ID
            if sanitized_key not in ['id', sanitize_column_name('SL No')]:
                set_clauses.append(f'"{sanitized_key}" = ?')
                params.append(value)
        
        if not set_clauses:
            return jsonify({"error": "No valid fields to update"}), 400

        params.append(id)
        query = f"UPDATE {TABLE_NAME} SET {', '.join(set_clauses)} WHERE rowid = ?"
        
        cursor.execute(query, tuple(params))
        conn.commit()
        
        # Sync changes back to the Excel file
        export_db_to_excel()
        return jsonify({"message": f"Contract {id} updated successfully."}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/contracts/<int:id>', methods=['DELETE'])
def delete_contract(id):
    """Deletes a contract record identified by its rowid."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # First, ensure the contract exists
        cursor.execute(f"SELECT rowid FROM {TABLE_NAME} WHERE rowid = ?", (id,))
        if cursor.fetchone() is None:
            return jsonify({"error": "Contract not found"}), 404

        # Delete the specified row
        cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", (id,))
        
        # Re-index the 'SL No' for all remaining rows to maintain sequence
        reindex_sl_no_in_db(cursor)
        
        conn.commit()
        
        # Sync changes back to the Excel file
        export_db_to_excel()
        return jsonify({"message": f"Contract {id} deleted successfully."}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route('/api/export', methods=['GET'])
def data_export():
    conn = get_db_connection()
    filtered_data = get_filtered_sorted_data(conn)
    conn.close()

    if not filtered_data:
        return jsonify({"error": "No data to export for the current filters."}), 404

    db_df = pd.DataFrame(filtered_data)
    
    selected_fields_str = request.args.get('selectedFields', '')
    if selected_fields_str:
        selected_fields = selected_fields_str.split(',')
        db_df = db_df[[col for col in selected_fields if col in db_df.columns]]

    output = io.BytesIO()
    format_type = request.args.get('format', 'xlsx')
    
    file_name_from_req = request.args.get('fileName')
    
    download_name_str = (file_name_from_req or 'contracts_export').replace(' ', '_')
    
    if format_type == 'csv':
        db_df.to_csv(output, index=False)
        mimetype = 'text/csv'
        file_extension = 'csv'

    elif format_type == 'docx':
        document = Document()
        section = document.sections[0]
        section.orientation = WD_ORIENT.LANDSCAPE
        new_width, new_height = section.page_height, section.page_width
        section.page_width = new_width
        section.page_height = new_height
        
        if file_name_from_req:
            document.add_heading(file_name_from_req, 0)
            
        table = document.add_table(rows=1, cols=len(db_df.columns))
        table.style = 'Table Grid'
        hdr_cells = table.rows[0].cells
        for i, col_name in enumerate(db_df.columns):
            cell_paragraph = hdr_cells[i].paragraphs[0]
            run = cell_paragraph.add_run(str(col_name))
            run.bold = True
        for index, row in db_df.iterrows():
            row_cells = table.add_row().cells
            for i, cell_value in enumerate(row):
                row_cells[i].text = str(cell_value)
        document.save(output)
        mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        file_extension = 'docx'

    elif format_type == 'pdf':
        num_cols = len(db_df.columns)
        
        if num_cols > 25:
            pagesize = landscape(A2)
        elif num_cols > 15:
            pagesize = landscape(A3)
        else:
            pagesize = landscape(A4)

        doc = SimpleDocTemplate(output, pagesize=pagesize, rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=40)
        styles = getSampleStyleSheet()
        
        font_scale = max(0.5, 1 - (num_cols / 40))
        base_font_size = 8
        scaled_font_size = base_font_size * font_scale

        header_style = ParagraphStyle('Header', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=scaled_font_size, textColor=colors.white, alignment=TA_CENTER, leading=scaled_font_size * 1.2)
        
        body_style = ParagraphStyle('Body', parent=styles['Normal'], fontName='Helvetica', fontSize=scaled_font_size - 1, alignment=TA_CENTER, leading=scaled_font_size * 1.2)
        
        elements = []
        
        page_width, page_height = pagesize
        available_width = page_width - doc.leftMargin - doc.rightMargin
        
        col_widths = []
        for col in db_df.columns:
            header_len = len(col)
            max_data_len = db_df[col].astype(str).str.len().max()
            col_width = max(header_len, max_data_len) * scaled_font_size * 0.6
            col_widths.append(max(40, min(col_width, available_width / num_cols * 2.0)))

        if sum(col_widths) > 0:
            col_widths = [w * available_width / sum(col_widths) for w in col_widths]

        header_row = [Paragraph(h.replace('(₹)', '(INR)'), header_style) for h in db_df.columns]
        data_rows = [header_row]
        for index, row in db_df.iterrows():
            data_rows.append([Paragraph(str(item), body_style) for item in row])
            
        table = Table(data_rows, colWidths=col_widths)
        
        table_style = TableStyle([
            # --- THIS IS THE CHANGE ---
            # The header background color is now an ONGC-style saffron/orange.
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#9B1C1C')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.black), # Changed text to black for better contrast
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,0), 8),
            ('TOPPADDING', (0,0), (-1,0), 8),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey)
        ])
        
        for i in range(1, len(data_rows)):
            if i % 2 == 0:
                table_style.add('BACKGROUND', (0,i), (-1,i), colors.HexColor('#F0F0F0'))
                
        table.setStyle(table_style)
        elements.append(table)
        
        def header_footer(canvas, doc):
            canvas.saveState()
            page_width, page_height = pagesize
            if file_name_from_req:
                canvas.setFont('Helvetica-Bold', 12)
                canvas.drawCentredString(page_width / 2.0, doc.height + doc.topMargin - 25, file_name_from_req)
            
            canvas.setFont('Helvetica', 8)
            canvas.setFillColor(colors.grey)
            generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            canvas.drawString(doc.leftMargin, doc.bottomMargin - 15, f"Generated on: {generation_time}")
            canvas.drawRightString(doc.width + doc.leftMargin, doc.bottomMargin - 15, f"Page {doc.page}")
            canvas.restoreState()
            
        doc.build(elements, onFirstPage=header_footer, onLaterPages=header_footer)
        mimetype = 'application/pdf'
        file_extension = 'pdf'

    else: # Default to Excel
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        db_df.to_excel(writer, index=False, sheet_name='Contracts')
        workbook = writer.book
        worksheet = writer.sheets['Contracts']
        header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(db_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        for i, col in enumerate(db_df.columns):
            column_len = max((db_df[col].astype(str).str.len().max() or 0), len(str(col))) + 2
            worksheet.set_column(i, i, column_len)
        worksheet.freeze_panes(1, 0)
        writer.close()
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        file_extension = 'xlsx'
        
    output.seek(0)
    return send_file(output, mimetype=mimetype, as_attachment=True, download_name=f'{download_name_str}.{file_extension}')


if __name__ == '__main__':
    do_setup()
    app.run(host='0.0.0.0', port=5001, debug=True)
