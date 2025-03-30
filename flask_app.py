import os
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

def initialize_db():
    """Create the database if it doesn't exist."""
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
    lastName = request.args.get("lastName", "")
    startDate = request.args.get("startDate", "")
    endDate = request.args.get("endDate", "")
    keywords = request.args.get("keywords", "")

    query = "SELECT * FROM papers WHERE 1=1"
    params = []

    if lastName:
        query += " AND last_names LIKE ?"
        params.append(f"%{lastName}%")

    if startDate and endDate:
        query += " AND publication_date BETWEEN ? AND ?"
        params.extend([startDate, endDate])

    if keywords:
        query += " AND (keywords LIKE ? OR abstract LIKE ?)"
        keyword_like = f"%{keywords}%"
        params.extend([keyword_like, keyword_like])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    papers = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    conn.close()

    return jsonify(papers)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
