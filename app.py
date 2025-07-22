# import sqlite3
# import pandas as pd
# import os
# import math
# import threading
# from flask import Flask, request, jsonify, send_file
# from flask_cors import CORS
# from werkzeug.utils import secure_filename
# import io
# from docx import Document
# from docx.shared import Inches
# from docx.enum.section import WD_ORIENT

# app = Flask(__name__)
# CORS(app, resources={r"/api/*": {"origins": "https://ongc-contracts.vercel.app"}})

# # --- Configuration and Helper functions ---
# DB_FILE = 'contracts.db'
# EXCEL_FILE = 'Contract Details.xlsx'
# TABLE_NAME = 'contracts'
# UPLOAD_FOLDER = '.'
# ALLOWED_EXTENSIONS = {'xlsx'}
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# file_lock = threading.Lock()
# original_column_names = [] # Stores the original column names from the Excel file

# # NEW: Global variable to store inferred metadata (headers and fieldTypes)
# column_metadata = {
#     "headers": [],
#     "fieldTypes": {
#         "range": [],
#         "date": [],
#         "yesNo": [],
#         "number": [],
#         "yearDropdown": [],
#         "text": []
#     }
# }

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# def get_db_connection():
#     conn = sqlite3.connect(DB_FILE)
#     conn.row_factory = sqlite3.Row # This allows accessing columns by name
#     return conn

# def sanitize_column_name(col_name):
#     # Replace non-alphanumeric (except space) with nothing, then spaces with underscores
#     return ''.join(e for e in col_name if e.isalnum() or e.isspace()).strip().replace(' ', '_')

# # NEW: Function to infer column types and populate metadata
# def infer_column_metadata(columns):
#     global column_metadata
    
#     inferred_field_types = {
#         "range": [],
#         "date": [],
#         "yesNo": [],
#         "number": [],
#         "yearDropdown": [],
#         "text": []
#     }

#     # Define the exact headers for each type as provided
#     field_type_map = {
#         "range": ["Contract Value (₹)", "Invoice Submitted & Amount Claimed (₹)", "Amount Passed (₹)", "Deduction (₹)", "PBG Amount (₹)", "Security Deposit Amount (₹)", "AMC Charges for Entire Duration (₹)", "Yearly Outflow as per OLA (₹)"],
#         "date": ["Date of Commissioning", "Warranty End Date", "AMC Start Date", "AMC End Date"],
#         "yesNo": ["Quarterly AMC Payment Status", "Post Contract Issues"],
#         "number": ["SL No"],
#         "yearDropdown": ["Warranty Duration (Yr)", "AMC Duration (Yr)"]
#     }

#     # Flatten the map to easily check if a column has a predefined type
#     predefined_columns = {col for cat in field_type_map.values() for col in cat}

#     # Assign types based on the provided lists
#     for col in columns:
#         found = False
#         for type_name, header_list in field_type_map.items():
#             if col in header_list:
#                 inferred_field_types[type_name].append(col)
#                 found = True
#                 break
#         # Any column not in the predefined lists is considered 'text'
#         if not found:
#             inferred_field_types["text"].append(col)
            
#     column_metadata["headers"] = columns
#     column_metadata["fieldTypes"] = inferred_field_types
#     print("Inferred Column Metadata based on new rules:", column_metadata)


# # --- Database Setup, Sync ---
# def export_db_to_excel():
#     print("Attempting to sync database to Excel file...")
#     conn = get_db_connection()
#     try:
#         # Fetch all data, ordered by SL_No to maintain sequence
#         db_df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} ORDER BY CAST({sanitize_column_name('SL No')} AS INTEGER)", conn)
        
#         if not original_column_names:
#             print("Warning: original_column_names is empty. Cannot sync to Excel with original headers.")
#             return

#         # Create a rename map from sanitized to original names
#         rename_map = {sanitize_column_name(col): col for col in original_column_names}
        
#         # Rename columns in the DataFrame
#         db_df_renamed = db_df.rename(columns=rename_map).copy() 
        
#         # Ensure only columns that were originally present and are now in the dataframe are included
#         final_columns = [col for col in original_column_names if col in db_df_renamed.columns]
#         db_df_final = db_df_renamed[final_columns]

#         with file_lock:
#             db_df_final.to_excel(EXCEL_FILE, index=False)
#         print("Database successfully synced to Excel file.")
#     except Exception as e:
#         print(f"An error occurred during DB to Excel sync: {e}")
#     finally:
#         if conn:
#             conn.close()

# def setup_database():
#     global original_column_names
#     if not os.path.exists(EXCEL_FILE):
#         print(f"'{EXCEL_FILE}' not found. Database setup skipped.")
#         return

#     print(f"Reading data from '{EXCEL_FILE}' to initialize database...")
#     df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
#     original_column_names = df.columns.tolist()
    
#     # Infer and store metadata after reading original columns
#     infer_column_metadata(original_column_names)

#     df.columns = [sanitize_column_name(col) for col in original_column_names]
#     conn = get_db_connection()
#     try:
#         df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
#         print(f"Database '{DB_FILE}' created and populated successfully.")
#     except Exception as e:
#         print(f"An error occurred during database setup: {e}")
#     finally:
#         conn.close()

# def do_setup():
#     if os.path.exists(DB_FILE) and os.path.exists(EXCEL_FILE):
#         try:
#             db_mod_time = os.path.getmtime(DB_FILE)
#             excel_mod_time = os.path.getmtime(EXCEL_FILE)
#             if excel_mod_time > db_mod_time:
#                 print(f"'{EXCEL_FILE}' is newer. Re-initializing...")
#                 os.remove(DB_FILE)
#                 setup_database()
#                 return
#         except FileNotFoundError:
#             pass 
    
#     if not os.path.exists(DB_FILE):
#         setup_database()
#     else:
#         print(f"Database '{DB_FILE}' is up-to-date.")
#         if os.path.exists(EXCEL_FILE):
#             global original_column_names
#             temp_df = pd.read_excel(EXCEL_FILE)
#             original_column_names = temp_df.columns.tolist()
#             infer_column_metadata(original_column_names)

# users = {"Infocom-Sivasagar": "223010007007"}

# @app.route('/api/login', methods=['POST'])
# def login():
#     data = request.get_json()
#     if not data: return jsonify({"error": "Request must be JSON"}), 400
#     username = data.get('username')
#     password = data.get('password')
#     if username in users and users[username] == password:
#         return jsonify({"message": "Login successful"}), 200
#     else:
#         return jsonify({"error": "Invalid credentials"}), 401
        
# @app.route('/api/contracts', methods=['GET'])
# def get_contracts_serverside():
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#     allowed_columns = [row[1] for row in cursor.fetchall()]
    
#     is_paginated = 'page' in request.args and 'limit' in request.args
    
#     page = request.args.get('page', 1, type=int)
#     limit = request.args.get('limit', 10, type=int)
#     sort_field = request.args.get('sortField', 'SL No')
#     sort_direction = request.args.get('sortDirection', 'asc').upper()
#     filter_field = request.args.get('filterField')
#     filter_value = request.args.get('filterValue')
#     min_range = request.args.get('minRange')
#     max_range = request.args.get('maxRange')
#     from_date = request.args.get('fromDate')
#     to_date = request.args.get('toDate')
    
#     where_clauses = []
#     params = []
#     if filter_field:
#         sanitized_filter_field = sanitize_column_name(filter_field)
#         if sanitized_filter_field in allowed_columns:
#             if filter_value:
#                 where_clauses.append(f"LOWER({sanitized_filter_field}) LIKE ?")
#                 params.append(f"%{filter_value.lower()}%")
#             elif min_range or max_range:
#                 if min_range: where_clauses.append(f"CAST({sanitized_filter_field} AS REAL) >= ?"); params.append(float(min_range))
#                 if max_range: where_clauses.append(f"CAST({sanitized_filter_field} AS REAL) <= ?"); params.append(float(max_range))
#             elif from_date or to_date:
#                 if from_date: where_clauses.append(f"date({sanitized_filter_field}) >= date(?)"); params.append(from_date)
#                 if to_date: where_clauses.append(f"date({sanitized_filter_field}) <= date(?)"); params.append(to_date)
#     where_statement = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

#     # --- CORRECTED SORTING LOGIC ---
#     # Default order by clause. This is the fallback for any non-sortable field.
#     order_by_clause = f"ORDER BY CAST({sanitize_column_name('SL No')} AS REAL) ASC"

#     # Map sanitized names back to original names to check against metadata types.
#     sanitized_to_original_map = {sanitize_column_name(col): col for col in original_column_names}
    
#     sanitized_sort_field = sanitize_column_name(sort_field)
#     original_field_name = sanitized_to_original_map.get(sanitized_sort_field)

#     # Check if the field is valid and determine its type from metadata.
#     if sanitized_sort_field in allowed_columns and original_field_name:
#         field_types = column_metadata['fieldTypes']

#         # Rule 1: Numeric sort for 'range', 'number', and 'yearDropdown' (duration) types.
#         if (original_field_name in field_types.get('range', []) or
#             original_field_name in field_types.get('number', []) or
#             original_field_name in field_types.get('yearDropdown', [])):
#             order_by_clause = f"ORDER BY CAST({sanitized_sort_field} AS REAL) {sort_direction}"
#         # Rule 2: Date-aware sort for 'date' types.
#         elif original_field_name in field_types.get('date', []):
#             order_by_clause = f"ORDER BY date({sanitized_sort_field}) {sort_direction}"
#         # Rule 3: Alphabetical sort for 'yesNo' types.
#         elif original_field_name in field_types.get('yesNo', []):
#             order_by_clause = f"ORDER BY {sanitized_sort_field} COLLATE NOCASE {sort_direction}"
#         # Note: 'text' fields are intentionally unsortable.
#         # If requested, the default 'ORDER BY SL No' clause is used.
#     # --- END OF CORRECTION ---

#     select_clause = "SELECT rowid as id, " + ", ".join(allowed_columns)

#     pagination_clause = ""
#     final_params = tuple(params)
#     if is_paginated:
#         pagination_clause = "LIMIT ? OFFSET ?"
#         offset = (page - 1) * limit
#         final_params = tuple(params) + (limit, offset)

#     count_query = f"SELECT COUNT(*) FROM {TABLE_NAME} {where_statement}"
#     total_records = cursor.execute(count_query, tuple(params)).fetchone()[0]
#     total_pages = math.ceil(total_records / limit) if total_records > 0 else 1
    
#     data_query = f"{select_clause} FROM {TABLE_NAME} {where_statement} {order_by_clause} {pagination_clause}"
#     contracts_cursor = cursor.execute(data_query, final_params)
#     contracts = []
    
#     for row in contracts_cursor.fetchall():
#         row_dict = dict(row)
#         formatted_row = {sanitized_to_original_map.get(key, key): value for key, value in row_dict.items()}
#         contracts.append(formatted_row)
#     conn.close()
    
#     response_data = {
#         "data": contracts,
#         "totalPages": total_pages,
#         "currentPage": page,
#         "headers": column_metadata["headers"],
#         "fieldTypes": column_metadata["fieldTypes"]
#     }
    
#     if not is_paginated:
#         return jsonify(contracts)
    
#     return jsonify(response_data)

# @app.route('/api/export', methods=['GET'])
# def export_data():
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#     allowed_columns = [row[1] for row in cursor.fetchall()]

#     format_type = request.args.get('format', 'csv')
#     sort_field = request.args.get('sortField', 'SL No')
#     sort_direction = request.args.get('sortDirection', 'asc').upper()
#     filter_field = request.args.get('filterField')
#     filter_value = request.args.get('filterValue')
#     min_range = request.args.get('minRange')
#     max_range = request.args.get('maxRange')
#     from_date = request.args.get('fromDate')
#     to_date = request.args.get('toDate')
#     selected_fields_str = request.args.get('selectedFields', '')

#     where_clauses = []
#     params = []
#     if filter_field:
#         sanitized_filter_field = sanitize_column_name(filter_field)
#         if sanitized_filter_field in allowed_columns:
#             if filter_value:
#                 where_clauses.append(f"LOWER({sanitized_filter_field}) LIKE ?")
#                 params.append(f"%{filter_value.lower()}%")
#             elif min_range or max_range:
#                 if min_range: where_clauses.append(f"CAST({sanitized_filter_field} AS REAL) >= ?"); params.append(float(min_range))
#                 if max_range: where_clauses.append(f"CAST({sanitized_filter_field} AS REAL) <= ?"); params.append(float(max_range))
#             elif from_date or to_date:
#                 if from_date: where_clauses.append(f"date({sanitized_filter_field}) >= date(?)"); params.append(from_date)
#                 if to_date: where_clauses.append(f"date({sanitized_filter_field}) <= date(?)"); params.append(to_date)
#     where_statement = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

#     # --- CORRECTED SORTING LOGIC ---
#     order_by_clause = f"ORDER BY CAST({sanitize_column_name('SL No')} AS REAL) ASC"
#     sanitized_to_original_map = {sanitize_column_name(col): col for col in original_column_names}
#     sanitized_sort_field = sanitize_column_name(sort_field)
#     original_field_name = sanitized_to_original_map.get(sanitized_sort_field)

#     if sanitized_sort_field in allowed_columns and original_field_name:
#         field_types = column_metadata['fieldTypes']
#         if (original_field_name in field_types.get('range', []) or
#             original_field_name in field_types.get('number', []) or
#             original_field_name in field_types.get('yearDropdown', [])):
#             order_by_clause = f"ORDER BY CAST({sanitized_sort_field} AS REAL) {sort_direction}"
#         elif original_field_name in field_types.get('date', []):
#             order_by_clause = f"ORDER BY date({sanitized_sort_field}) {sort_direction}"
#         elif original_field_name in field_types.get('yesNo', []):
#             order_by_clause = f"ORDER BY {sanitized_sort_field} COLLATE NOCASE {sort_direction}"
#     # --- END OF CORRECTION ---

#     selected_fields = selected_fields_str.split(',')
#     sanitized_selected_fields = [sanitize_column_name(f) for f in selected_fields]
#     valid_selected_fields = [f for f in sanitized_selected_fields if f in allowed_columns]
#     if not valid_selected_fields: valid_selected_fields = ['*']
#     select_clause = "SELECT " + ", ".join(valid_selected_fields)

#     query = f"{select_clause} FROM {TABLE_NAME} {where_statement} {order_by_clause}"
#     cursor.execute(query, tuple(params))
    
#     db_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
#     conn.close()

#     sanitized_sl_no_col = sanitize_column_name('SL No')
#     if sanitized_sl_no_col in db_df.columns:
#         db_df[sanitized_sl_no_col] = range(1, len(db_df) + 1)
    
#     db_df.rename(columns=sanitized_to_original_map, inplace=True)
    
#     final_columns = [col for col in selected_fields if col in db_df.columns]
#     db_df = db_df[final_columns]

#     output = io.BytesIO()
    
#     if format_type == 'xlsx':
#         # (Excel export formatting remains the same)
#         writer = pd.ExcelWriter(output, engine='xlsxwriter')
#         db_df.to_excel(writer, index=False, sheet_name='Contracts')
#         workbook   = writer.book
#         worksheet = writer.sheets['Contracts']
#         header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
#         for col_num, value in enumerate(db_df.columns.values):
#             worksheet.write(0, col_num, value, header_format)
#         for i, col in enumerate(db_df.columns):
#             column_len = len(str(col))
#             max_len = db_df[col].astype(str).str.len().max()
#             if pd.isna(max_len): max_len = 0
#             width = max(column_len, max_len) + 2
#             worksheet.set_column(i, i, width)
#         worksheet.freeze_panes(1, 0)
#         writer.close()
#         mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
#         file_extension = 'xlsx'
#     elif format_type == 'docx':
#         # (Word export formatting remains the same)
#         document = Document()
#         section = document.sections[0]
#         section.orientation = WD_ORIENT.LANDSCAPE
#         new_width, new_height = section.page_height, section.page_width
#         section.page_width = new_width
#         section.page_height = new_height
#         document.add_heading('Contracts Details', 0)
#         table = document.add_table(rows=1, cols=len(db_df.columns))
#         table.style = 'Table Grid'
#         hdr_cells = table.rows[0].cells
#         for i, col_name in enumerate(db_df.columns):
#             cell_paragraph = hdr_cells[i].paragraphs[0]
#             run = cell_paragraph.add_run(str(col_name))
#             run.bold = True
#         for index, row in db_df.iterrows():
#             row_cells = table.add_row().cells
#             for i, cell_value in enumerate(row):
#                 row_cells[i].text = str(cell_value)
#         document.save(output)
#         mimetype = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
#         file_extension = 'docx'
#     else: # Default to CSV
#         db_df.to_csv(output, index=False)
#         mimetype = 'text/csv'
#         file_extension = 'csv'

#     output.seek(0)
#     return send_file(output, mimetype=mimetype, as_attachment=True, download_name=f'contracts_export.{file_extension}')

# @app.route('/api/upload', methods=['POST'])
# def upload_file():
#     if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
#     file = request.files['file']
#     if file.filename == '': return jsonify({"error": "No selected file"}), 400
#     if file and allowed_file(file.filename):
#         try:
#             filepath = os.path.join(app.config['UPLOAD_FOLDER'], EXCEL_FILE)
#             file.save(filepath)
#             with file_lock:
#                 if os.path.exists(DB_FILE): os.remove(DB_FILE)
#             setup_database()
#             return jsonify({"message": "File uploaded and database re-initialized successfully."}), 200
#         except Exception as e: return jsonify({"error": str(e)}), 500
#     return jsonify({"error": "File type not allowed"}), 400

# # (The rest of the CRUD operations remain the same)
# @app.route('/api/contracts', methods=['POST'])
# def add_contract():
#     new_data = request.get_json()
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"SELECT MAX(CAST({sanitize_column_name('SL No')} AS INTEGER)) FROM {TABLE_NAME}")
#         max_sl_no = cursor.fetchone()[0]
#         next_sl_no = (max_sl_no or 0) + 1
        
#         cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#         table_columns = [row[1] for row in cursor.fetchall()]
        
#         final_data = {col: '' for col in table_columns}
        
#         for key, value in new_data.items():
#             sanitized_key = sanitize_column_name(key)
#             if sanitized_key in final_data:
#                 final_data[sanitized_key] = value
        
#         final_data[sanitize_column_name('SL No')] = str(next_sl_no)
        
#         columns = ', '.join(final_data.keys())
#         placeholders = ', '.join(['?'] * len(final_data))
#         values = list(final_data.values())
        
#         query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
#         cursor.execute(query, values)
#         conn.commit()
#         new_id = cursor.lastrowid
#         export_db_to_excel()
#         return jsonify({"message": "Contract added", "id": new_id}), 201
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# @app.route('/api/contracts/<int:row_id>', methods=['PUT'])
# def update_contract(row_id):
#     updated_data = request.get_json()
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#         table_columns = [row[1] for row in cursor.fetchall()]
        
#         filtered_data = {sanitize_column_name(k): v for k, v in updated_data.items() 
#                          if sanitize_column_name(k) in table_columns and sanitize_column_name(k) != sanitize_column_name('SL No')}
        
#         if not filtered_data: return jsonify({"error": "No valid fields to update"}), 400
        
#         set_clause = ', '.join([f"{key} = ?" for key in filtered_data.keys()])
#         values = list(filtered_data.values())
#         values.append(row_id)
        
#         query = f"UPDATE {TABLE_NAME} SET {set_clause} WHERE rowid = ?"
#         cursor.execute(query, values)
#         conn.commit()
        
#         if cursor.rowcount == 0: return jsonify({"error": "Contract not found"}), 404
        
#         export_db_to_excel()
#         return jsonify({"message": "Contract updated"})
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# @app.route('/api/contracts/<int:row_id>', methods=['DELETE'])
# def delete_contract(row_id):
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", (row_id,))
#         if cursor.rowcount == 0:
#             conn.close()
#             return jsonify({"error": "Contract not found"}), 404
        
#         cursor.execute(f"SELECT rowid FROM {TABLE_NAME} ORDER BY CAST({sanitize_column_name('SL No')} AS INTEGER)")
#         rows_to_reindex = cursor.fetchall()
        
#         for index, row in enumerate(rows_to_reindex):
#             new_sl_no = index + 1
#             cursor.execute(f"UPDATE {TABLE_NAME} SET {sanitize_column_name('SL No')} = ? WHERE rowid = ?", (str(new_sl_no), row['rowid']))
        
#         conn.commit()
#         export_db_to_excel()
#         return jsonify({"message": "Contract deleted"})
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# if __name__ == '__main__':
#     do_setup()
#     app.run(host='0.0.0.0', port=5001, debug=True)


# import sqlite3
# import pandas as pd
# import os
# import math
# import threading
# from flask import Flask, request, jsonify, send_file
# from flask_cors import CORS
# from werkzeug.utils import secure_filename
# import io

# # --- Configuration and Global Variables ---
# app = Flask(__name__)
# # Allow all origins for flexible development. Change "*" to your frontend's domain for production.
# CORS(app, resources={r"/api/*": {"origins": "https://ongc-contracts.vercel.app"}})

# DB_FILE = 'contracts.db'
# EXCEL_FILE = 'Contract Details.xlsx'
# TABLE_NAME = 'contracts'
# UPLOAD_FOLDER = '.'
# ALLOWED_EXTENSIONS = {'xlsx'}
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# # A thread lock to prevent race conditions during file I/O
# file_lock = threading.Lock()

# # Global variables to hold the state of the Excel file's structure
# original_column_names = []
# column_metadata = {
#     "headers": [],
#     "fieldTypes": {
#         "numeric": [],
#         "date": [],
#         "text": []
#     }
# }

# # --- Helper Functions ---

# def allowed_file(filename):
#     """Checks if the uploaded file has an allowed extension."""
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# def get_db_connection():
#     """Establishes a connection to the SQLite database."""
#     conn = sqlite3.connect(DB_FILE)
#     conn.row_factory = sqlite3.Row  # Allows accessing columns by name
#     return conn

# def sanitize_column_name(col_name):
#     """Sanitizes a column name to be database-friendly."""
#     return ''.join(e for e in col_name if e.isalnum() or e.isspace()).strip().replace(' ', '_')

# def infer_column_metadata(df):
#     """
#     Dynamically and intelligently infers the data type of each column for smart sorting and filtering.
#     It first checks for keywords in headers, then analyzes the data itself.
#     """
#     global column_metadata, original_column_names
    
#     original_column_names = df.columns.tolist()
#     inferred_field_types = {"numeric": [], "date": [], "text": []}
#     keyword_map = {
#         'numeric': ['(₹)', 'amount', 'value', 'duration', 'charges', '(yr)', 'year', 'number', 'no'],
#         'date': ['date']
#     }

#     for col in original_column_names:
#         # 1. Check for keywords in the header
#         col_lower = col.lower()
#         found_by_keyword = False
#         for type_name, keywords in keyword_map.items():
#             if any(keyword in col_lower for keyword in keywords):
#                 inferred_field_types[type_name].append(col)
#                 found_by_keyword = True
#                 break
        
#         if found_by_keyword:
#             continue

#         # 2. If no keyword matches, analyze the column's data content
#         try:
#             # Attempt to convert to numeric. If it works for most, it's numeric.
#             # We drop NA values to avoid errors with empty cells.
#             numeric_series = pd.to_numeric(df[col].dropna())
#             if not numeric_series.empty:
#                 inferred_field_types["numeric"].append(col)
#                 continue
#         except (ValueError, TypeError):
#             pass # Not numeric, proceed to check for dates

#         try:
#             # Attempt to convert to datetime. If it works for most, it's a date.
#             # `errors='coerce'` will turn unparseable values into NaT (Not a Time)
#             date_series = pd.to_datetime(df[col].dropna(), errors='coerce')
#             # Check if a significant portion of the non-null values are dates
#             if not date_series.isnull().all():
#                  inferred_field_types["date"].append(col)
#                  continue
#         except (ValueError, TypeError):
#             pass # Not a date

#         # 3. If neither keyword nor data analysis matches, it's text.
#         inferred_field_types["text"].append(col)

#     # Ensure 'SL No' is always treated as numeric if it exists
#     if 'SL No' in original_column_names and 'SL No' not in inferred_field_types['numeric']:
#         for key in inferred_field_types:
#             if 'SL No' in inferred_field_types[key]:
#                 inferred_field_types[key].remove('SL No')
#         inferred_field_types['numeric'].append('SL No')

#     column_metadata["headers"] = original_column_names
#     column_metadata["fieldTypes"] = inferred_field_types
#     print(f"--- Inferred Column Metadata: {column_metadata}")

# def reindex_sl_no_in_db(cursor):
#     """
#     Re-calculates and updates the 'SL No' for all rows in the database to ensure
#     it is sequential (1, 2, 3, ...). This is called after any addition or deletion.
#     """
#     sanitized_sl_no = sanitize_column_name('SL No')
#     try:
#         cursor.execute(f"SELECT rowid FROM {TABLE_NAME} ORDER BY rowid")
#         rows_to_reindex = cursor.fetchall()
        
#         for index, row in enumerate(rows_to_reindex):
#             new_sl_no = index + 1
#             cursor.execute(f'UPDATE {TABLE_NAME} SET "{sanitized_sl_no}" = ? WHERE rowid = ?', (str(new_sl_no), row['rowid']))
#         print("Re-indexed all 'SL No' values in the database.")
#     except sqlite3.OperationalError as e:
#         print(f"Warning: Could not re-index 'SL No'. The column '{sanitized_sl_no}' might not exist. Error: {e}")


# # --- Database Initialization and Synchronization ---

# def export_db_to_excel():
#     """Exports the current state of the database back to the Excel file (Two-Way Sync)."""
#     print("Attempting to sync database to Excel file...")
#     conn = get_db_connection()
#     try:
#         db_df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        
#         # Re-index SL No in the dataframe before exporting to ensure it's sequential
#         sl_no_col_sanitized = sanitize_column_name('SL No')
#         if sl_no_col_sanitized in db_df.columns:
#             db_df = db_df.sort_values(by=sl_no_col_sanitized).reset_index(drop=True)
#             db_df[sl_no_col_sanitized] = range(1, len(db_df) + 1)

#         if not original_column_names:
#             print("Warning: original_column_names is empty. Cannot sync to Excel with original headers.")
#             return

#         rename_map = {sanitize_column_name(col): col for col in original_column_names}
#         db_df_renamed = db_df.rename(columns=rename_map)
        
#         final_columns = [col for col in original_column_names if col in db_df_renamed.columns]
#         db_df_final = db_df_renamed[final_columns]

#         with file_lock:
#             db_df_final.to_excel(EXCEL_FILE, index=False)
#         print("Database successfully synced to Excel file.")
#     except Exception as e:
#         print(f"An error occurred during DB to Excel sync: {e}")
#     finally:
#         if conn:
#             conn.close()

# def setup_database():
#     """
#     Highly resilient function to set up the database from the Excel file.
#     Handles empty files, finds data on any sheet, and manages 'SL No'.
#     """
#     global original_column_names
#     print(f"--- Running setup_database for {EXCEL_FILE} ---")
#     if not os.path.exists(EXCEL_FILE):
#         print(f"'{EXCEL_FILE}' not found. Database setup skipped.")
#         return

#     df = pd.DataFrame()
#     try:
#         # Intelligently search all sheets for data
#         xls = pd.ExcelFile(EXCEL_FILE)
#         for sheet_name in xls.sheet_names:
#             temp_df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str).fillna('')
#             if not temp_df.empty:
#                 df = temp_df
#                 print(f"Data found and loaded from sheet: '{sheet_name}'")
#                 break
#     except Exception as e:
#         print(f"Could not read Excel file, it may be empty or corrupted. Error: {e}")

#     # If the dataframe is still empty, it might just have headers
#     if df.empty and os.path.exists(EXCEL_FILE):
#         try:
#             df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
#         except Exception:
#             pass # Keep df empty if it fails

#     # --- Automatic SL No. Management ---
#     temp_original_cols = df.columns.tolist()
#     if 'SL No' not in temp_original_cols:
#         print("Adding 'SL No' column as it was not found.")
#         df.insert(0, 'SL No', range(1, len(df) + 1))
#     else:
#         # Force re-indexing to ensure it's sequential and correct
#         df['SL No'] = range(1, len(df) + 1)
#         # Move SL No to the front if it's not already there
#         if df.columns.get_loc('SL No') != 0:
#             cols = df.columns.tolist()
#             cols.insert(0, cols.pop(cols.index('SL No')))
#             df = df[cols]
#         print("Existing 'SL No' column re-indexed to ensure sequential order.")

#     # Infer and store metadata after cleaning the data
#     infer_column_metadata(df)

#     df.columns = [sanitize_column_name(col) for col in original_column_names]
    
#     conn = get_db_connection()
#     try:
#         # Overwrite the database table completely with the fresh data
#         df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
#         print(f"Database '{DB_FILE}' created/replaced and populated successfully.")
#     except Exception as e:
#         print(f"An error occurred during database setup: {e}")
#     finally:
#         conn.close()

# def do_setup():
#     """
#     Orchestrates the database setup, including modification time checks.
#     """
#     if not os.path.exists(UPLOAD_FOLDER):
#         os.makedirs(UPLOAD_FOLDER)

#     db_exists = os.path.exists(DB_FILE)
#     excel_exists = os.path.exists(EXCEL_FILE)

#     if excel_exists:
#         # If Excel is newer than DB, or if DB doesn't exist, rebuild the DB
#         if not db_exists or os.path.getmtime(EXCEL_FILE) > os.path.getmtime(DB_FILE):
#             print(f"'{EXCEL_FILE}' is newer or DB doesn't exist. Re-initializing...")
#             if db_exists:
#                 # Critical fix: Ensure DB is closed before removing
#                 try:
#                     conn_check = get_db_connection()
#                     conn_check.close()
#                 except Exception: pass
#                 os.remove(DB_FILE)
#             setup_database()
#         else:
#             print(f"Database '{DB_FILE}' is up-to-date. Loading metadata.")
#             # Even if up-to-date, we need to load the column metadata for the API
#             temp_df = pd.read_excel(EXCEL_FILE, dtype=str).fillna('')
#             infer_column_metadata(temp_df)
#     elif not db_exists:
#         print("Neither Excel file nor Database found. Ready for first upload.")

# # --- API Endpoints ---

# users = {"Infocom-Sivasagar": "223010007007"} # Simple user authentication

# @app.route('/api/login', methods=['POST'])
# def login():
#     data = request.get_json()
#     if not data: return jsonify({"error": "Request must be JSON"}), 400
#     username = data.get('username')
#     password = data.get('password')
#     if username in users and users[username] == password:
#         return jsonify({"message": "Login successful"}), 200
#     else:
#         return jsonify({"error": "Invalid credentials"}), 401

# @app.route('/api/contracts', methods=['GET'])
# def get_contracts():
#     """Main endpoint to fetch, filter, sort, and paginate contracts."""
#     conn = get_db_connection()
#     cursor = conn.cursor()
    
#     try:
#         cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#         allowed_columns = [row[1] for row in cursor.fetchall()]
#     except sqlite3.OperationalError:
#         # This happens if the table doesn't exist yet
#         return jsonify({"data": [], "totalPages": 1, "currentPage": 1, "headers": [], "fieldTypes": {}})

#     # Pagination and Sorting parameters
#     page = request.args.get('page', 1, type=int)
#     limit = request.args.get('limit', 10, type=int)
#     sort_field = request.args.get('sortField', 'SL No')
#     sort_direction = request.args.get('sortDirection', 'asc').upper()
    
#     # Comprehensive Filtering parameters
#     filter_field = request.args.get('filterField')
#     filter_value = request.args.get('filterValue')
#     min_range = request.args.get('minRange')
#     max_range = request.args.get('maxRange')
#     from_date = request.args.get('fromDate')
#     to_date = request.args.get('toDate')
    
#     where_clauses = []
#     params = []
#     if filter_field:
#         sanitized_filter_field = sanitize_column_name(filter_field)
#         if sanitized_filter_field in allowed_columns:
#             # Logic for different filter types based on metadata
#             if filter_value:
#                 where_clauses.append(f'LOWER("{sanitized_filter_field}") LIKE ?')
#                 params.append(f"%{filter_value.lower()}%")
#             elif min_range or max_range:
#                 if min_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) >= ?'); params.append(float(min_range))
#                 if max_range: where_clauses.append(f'CAST("{sanitized_filter_field}" AS REAL) <= ?'); params.append(float(max_range))
#             elif from_date or to_date:
#                 if from_date: where_clauses.append(f'date("{sanitized_filter_field}") >= date(?)'); params.append(from_date)
#                 if to_date: where_clauses.append(f'date("{sanitized_filter_field}") <= date(?)'); params.append(to_date)
                
#     where_statement = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

#     # --- Smart Sorting Logic ---
#     sanitized_sort_field = sanitize_column_name(sort_field)
#     # Default sort order is by SL No
#     order_by_clause = f'ORDER BY CAST("{sanitize_column_name("SL No")}" AS REAL) ASC'

#     # Only apply special sorting if the field is numeric or a date
#     if sort_field in column_metadata['fieldTypes']['numeric']:
#         order_by_clause = f'ORDER BY CAST("{sanitized_sort_field}" AS REAL) {sort_direction}'
#     elif sort_field in column_metadata['fieldTypes']['date']:
#         order_by_clause = f'ORDER BY date("{sanitized_sort_field}") {sort_direction}'
    
#     # Count total records for pagination
#     count_query = f"SELECT COUNT(*) FROM {TABLE_NAME} {where_statement}"
#     total_records = cursor.execute(count_query, tuple(params)).fetchone()[0]
#     total_pages = math.ceil(total_records / limit) if total_records > 0 else 1
    
#     # Fetch the actual data for the current page
#     offset = (page - 1) * limit
#     select_clause = "SELECT rowid as id, " + ", ".join(f'"{col}"' for col in allowed_columns)
#     data_query = f"{select_clause} FROM {TABLE_NAME} {where_statement} {order_by_clause} LIMIT ? OFFSET ?"
    
#     contracts_cursor = cursor.execute(data_query, tuple(params) + (limit, offset))
    
#     contracts = []
#     sanitized_to_original_map = {sanitize_column_name(col): col for col in original_column_names}
#     for row in contracts_cursor.fetchall():
#         row_dict = dict(row)
#         formatted_row = {sanitized_to_original_map.get(key, key): value for key, value in row_dict.items()}
#         contracts.append(formatted_row)
#     conn.close()
    
#     return jsonify({
#         "data": contracts,
#         "totalPages": total_pages,
#         "currentPage": page,
#         "headers": column_metadata["headers"],
#         "fieldTypes": column_metadata["fieldTypes"]
#     })

# @app.route('/api/upload', methods=['POST'])
# def upload_file():
#     """Handles the upload of a new Excel file with thread-safe operations."""
#     if 'file' not in request.files: return jsonify({"error": "No file part"}), 400
#     file = request.files['file']
#     if file.filename == '' or not allowed_file(file.filename):
#         return jsonify({"error": "Invalid or no selected file"}), 400

#     try:
#         with file_lock:
#             filepath = os.path.join(app.config['UPLOAD_FOLDER'], EXCEL_FILE)
#             file.save(filepath)
            
#             # Critical fix: Ensure DB is closed before removing
#             try:
#                 conn_check = get_db_connection()
#                 conn_check.close()
#             except Exception: pass

#             if os.path.exists(DB_FILE):
#                 os.remove(DB_FILE)
            
#             setup_database() # Re-initialize the entire database
            
#         return jsonify({"message": "File uploaded and database re-initialized successfully."}), 200
#     except Exception as e:
#         print(f"Error during file upload: {e}")
#         return jsonify({"error": str(e)}), 500

# # --- CRUD Operations with Two-Way Sync ---

# @app.route('/api/contracts', methods=['POST'])
# def add_contract():
#     """Adds a new contract and syncs back to Excel."""
#     new_data = request.get_json()
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#         table_columns = [row[1] for row in cursor.fetchall()]
        
#         final_data = {col: '' for col in table_columns}
#         for key, value in new_data.items():
#             sanitized_key = sanitize_column_name(key)
#             if sanitized_key in final_data:
#                 final_data[sanitized_key] = value
        
#         # SL No will be re-indexed after insertion
#         final_data.pop(sanitize_column_name('SL No'), None)

#         columns = ', '.join(f'"{k}"' for k in final_data.keys())
#         placeholders = ', '.join(['?'] * len(final_data))
        
#         query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
#         cursor.execute(query, list(final_data.values()))
        
#         reindex_sl_no_in_db(cursor)
#         conn.commit()
#         export_db_to_excel() # Sync back to Excel
#         return jsonify({"message": "Contract added"}), 201
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# @app.route('/api/contracts/<int:row_id>', methods=['PUT'])
# def update_contract(row_id):
#     """Updates a contract and syncs back to Excel."""
#     updated_data = request.get_json()
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
#         table_columns = [row[1] for row in cursor.fetchall()]
        
#         filtered_data = {sanitize_column_name(k): v for k, v in updated_data.items() 
#                          if sanitize_column_name(k) in table_columns and k != 'SL No'}
        
#         if not filtered_data: return jsonify({"error": "No valid fields to update"}), 400
        
#         set_clause = ', '.join([f'"{key}" = ?' for key in filtered_data.keys()])
#         values = list(filtered_data.values()) + [row_id]
        
#         query = f"UPDATE {TABLE_NAME} SET {set_clause} WHERE rowid = ?"
#         cursor.execute(query, values)
#         conn.commit()
        
#         if cursor.rowcount == 0: return jsonify({"error": "Contract not found"}), 404
        
#         export_db_to_excel() # Sync back to Excel
#         return jsonify({"message": "Contract updated"})
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# @app.route('/api/contracts/<int:row_id>', methods=['DELETE'])
# def delete_contract(row_id):
#     """Deletes a contract, re-indexes, and syncs back to Excel."""
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", (row_id,))
#         if cursor.rowcount == 0:
#             return jsonify({"error": "Contract not found"}), 404
        
#         reindex_sl_no_in_db(cursor) # Re-index all SL numbers
#         conn.commit()
#         export_db_to_excel() # Sync back to Excel
#         return jsonify({"message": "Contract deleted"})
#     except sqlite3.Error as e:
#         conn.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         conn.close()

# @app.route('/api/export', methods=['GET'])
# def data_export():
#     """Exports data to Excel or CSV format."""
#     conn = get_db_connection()
#     # This export respects the current filters
#     # The logic is similar to get_contracts but without pagination
#     # For brevity, this is a simplified version.
#     db_df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
#     conn.close()

#     rename_map = {sanitize_column_name(col): col for col in original_column_names}
#     db_df.rename(columns=rename_map, inplace=True)
#     db_df = db_df[original_column_names]

#     output = io.BytesIO()
#     format_type = request.args.get('format', 'xlsx')
    
#     if format_type == 'csv':
#         db_df.to_csv(output, index=False)
#         mimetype = 'text/csv'
#         file_extension = 'csv'
#     else: # Default to Excel
#         db_df.to_excel(output, index=False)
#         mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
#         file_extension = 'xlsx'
        
#     output.seek(0)
#     return send_file(output, mimetype=mimetype, as_attachment=True, download_name=f'contracts_export.{file_extension}')


# if __name__ == '__main__':
#     do_setup() # Run initial setup check when the server starts
#     app.run(host='0.0.0.0', port=5001, debug=True)



import sqlite3
import pandas as pd
import os
import math
import threading
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import io
from docx import Document
from docx.shared import Inches
from docx.enum.section import WD_ORIENT

# --- Configuration and Global Variables ---
app = Flask(__name__)
# Using a specific CORS policy for production security
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
        'numeric': ['(₹)', 'amount', 'value', 'duration', 'charges', '(yr)', 'year', 'number', 'no'],
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
            # Handle files with headers but no data
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
        return [], []

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

@app.route('/api/contracts/<int:row_id>', methods=['PUT'])
def update_contract(row_id):
    updated_data = request.get_json()
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
        table_columns = [row[1] for row in cursor.fetchall()]
        filtered_data = {sanitize_column_name(k): v for k, v in updated_data.items() if sanitize_column_name(k) in table_columns and k != 'SL No'}
        if not filtered_data: return jsonify({"error": "No valid fields to update"}), 400
        set_clause = ', '.join([f'"{key}" = ?' for key in filtered_data.keys()])
        cursor.execute(f"UPDATE {TABLE_NAME} SET {set_clause} WHERE rowid = ?", list(filtered_data.values()) + [row_id])
        conn.commit()
        if cursor.rowcount == 0: return jsonify({"error": "Contract not found"}), 404
        export_db_to_excel()
        return jsonify({"message": "Contract updated"})
    finally: conn.close()

@app.route('/api/contracts/<int:row_id>', methods=['DELETE'])
def delete_contract(row_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rowid = ?", (row_id,))
        if cursor.rowcount == 0: return jsonify({"error": "Contract not found"}), 404
        reindex_sl_no_in_db(cursor)
        conn.commit()
        export_db_to_excel()
        return jsonify({"message": "Contract deleted"})
    finally: conn.close()

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
        document.add_heading('Contracts Details', 0)
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
    else: # Default to Excel
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        db_df.to_excel(writer, index=False, sheet_name='Contracts')
        workbook = writer.book
        worksheet = writer.sheets['Contracts']
        header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(db_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        for i, col in enumerate(db_df.columns):
            # Auto-adjust column width
            column_len = max((db_df[col].astype(str).str.len().max() or 0), len(str(col))) + 2
            worksheet.set_column(i, i, column_len)
        worksheet.freeze_panes(1, 0)
        writer.close()
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        file_extension = 'xlsx'
        
    output.seek(0)
    return send_file(output, mimetype=mimetype, as_attachment=True, download_name=f'contracts_export.{file_extension}')

if __name__ == '__main__':
    do_setup()
    app.run(host='0.0.0.0', port=5001, debug=True)
