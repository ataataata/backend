from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import os

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests from React frontend

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

    conn = sqlite3.connect("papers.db")
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    papers = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    return jsonify(papers)

if __name__ == "__main__":
    app.run(debug=True)
