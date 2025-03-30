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

    if last_name_list:
        query += " AND (" + " OR ".join(["LOWER(last_names) LIKE ?"] * len(last_name_list)) + ")"
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
    last_names = {row.get("Last Name", "").strip().lower() for row in reader if row.get("Last Name", "").strip()}

    if not last_names:
        return jsonify([])

    query = "SELECT * FROM papers WHERE " + " OR ".join(["LOWER(last_names) LIKE ?"] * len(last_names))
    params = [f"%{name}%" for name in last_names]

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
