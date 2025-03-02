import os
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

# Function to ensure database exists
def initialize_db():
    """Create the database with sample data if it doesn't exist."""
    if not os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id TEXT PRIMARY KEY,
            title TEXT,
            names TEXT,
            publication_date TEXT,
            keywords TEXT,
            abstract TEXT
        )
        ''')
        conn.commit()
        conn.close()
        print("Database initialized!")

# Function to get a database connection
def get_db_connection():
    initialize_db()  # Ensure the database exists before connecting
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "API is running"})

@app.route("/api/papers", methods=["GET"])
def get_papers():
    fullName = request.args.get("fullName", "")
    startDate = request.args.get("startDate", "")
    endDate = request.args.get("endDate", "")
    keywords = request.args.get("keywords", "")

    query = "SELECT * FROM papers WHERE 1=1"
    params = []

    if fullName:
        query += " AND names LIKE ?"
        params.append(f"%{fullName}%")

    if startDate and endDate:
        query += " AND publication_date BETWEEN ? AND ?"
        params.extend([startDate, endDate])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    papers = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    return jsonify(papers)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
