import os
import sqlite3
import csv
from flask import Flask, request, jsonify
from flask_cors import CORS
from io import TextIOWrapper

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

def initialize_db():
    if not os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            authors TEXT,
            last_names TEXT,
            publication_date TEXT,
            doi TEXT,
            keywords TEXT,
            abstract TEXT
        )
        ''')
        conn.commit()
        conn.close()
        print("✅ Database initialized!")

def get_db_connection():
    initialize_db()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "API is running"})

@app.route("/api/papers", methods=["GET"])
def get_papers():
    lastNames = request.args.get("lastNames", "")
    startDate = request.args.get("startDate", "")
    endDate = request.args.get("endDate", "")
    keywords = request.args.get("keywords", "")

    last_name_list = [name.strip().lower() for name in lastNames.split(",") if name.strip()]
    query = "SELECT * FROM papers WHERE 1=1"
    params = []

    # Changed from OR to AND logic for last names
    if last_name_list:
        query += " AND " + " AND ".join(["LOWER(last_names) LIKE ?"] * len(last_name_list))
        params.extend([f"%{name}%" for name in last_name_list])

    if startDate and endDate:
        query += " AND publication_date BETWEEN ? AND ?"
        params.extend([startDate, endDate])

    if keywords:
        keyword_like = f"%{keywords.lower()}%"
        query += " AND (LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)"
        params.extend([keyword_like, keyword_like])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

@app.route("/api/search-csv", methods=["POST"])
def search_csv():
    if "file" not in request.files:
        return jsonify({"error": "CSV file required"}), 400

    file = request.files["file"]
    reader = csv.DictReader(TextIOWrapper(file, encoding="utf-8"))
    all_results = []

    conn = get_db_connection()
    cursor = conn.cursor()

    for row in reader:
        name1 = row.get("Last Name", "").strip().lower()
        name2 = row.get("Owner Last Name", "").strip().lower()

        # Skip if neither name is present
        if not name1 and not name2:
            continue

        query = "SELECT * FROM papers WHERE 1=1"
        params = []

        # Apply AND logic if both names are present
        if name1 and name2:
            query += " AND LOWER(last_names) LIKE ? AND LOWER(last_names) LIKE ?"
            params.extend([f"%{name1}%", f"%{name2}%"])
        elif name1:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name1}%")
        elif name2:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name2}%")

        cursor.execute(query, params)
        rows = cursor.fetchall()
        all_results.extend([dict(row) for row in rows])

    conn.close()

    # Optional: remove duplicates by DOI
    seen = set()
    unique_results = []
    for result in all_results:
        doi = result.get("doi", "")
        if doi not in seen:
            seen.add(doi)
            unique_results.append(result)

    return jsonify(unique_results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)