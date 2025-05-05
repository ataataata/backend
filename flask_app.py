#!/usr/bin/env python3
import os
import csv
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS
from io import TextIOWrapper

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

def initialize_db() -> None:
    if os.path.exists(DB_FILE):
        return
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            pmid             TEXT PRIMARY KEY,
            title            TEXT,
            journal          TEXT,
            year             INTEGER,
            authors          TEXT,
            last_names       TEXT,
            doi              TEXT UNIQUE,
            keywords         TEXT,
            abstract         TEXT,
            publication_date TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_year    ON papers(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal)")
    conn.commit()
    conn.close()

def get_db_connection() -> sqlite3.Connection:
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
    endDate   = request.args.get("endDate", "")
    keywords  = request.args.get("keywords", "")

    last_name_list = [n.strip().lower() for n in lastNames.split(",") if n.strip()]
    term_list      = [kw.strip().lower() for kw in keywords.split(",")  if kw.strip()]

    clauses = ["1=1"]
    params  = []

    if last_name_list:
        clauses.append(" AND ".join(["LOWER(p.last_names) LIKE ?"]*len(last_name_list)))
        params.extend([f"%{n}%" for n in last_name_list])

    if startDate and endDate:
        clauses.append("p.publication_date BETWEEN ? AND ?")
        params.extend([startDate, endDate])

    if term_list:
        or_parts = []
        for _ in term_list:
            pat = f"%{_}%"
            or_parts.append(
                "(LOWER(p.title) LIKE ? OR LOWER(p.keywords) LIKE ? OR LOWER(p.abstract) LIKE ?)"
            )
            params.extend([pat, pat, pat])
        clauses.append("(" + " OR ".join(or_parts) + ")")

    sql = f"""
        SELECT
            p.pmid               AS id,
            p.authors            AS names,
            p.title,
            p.journal,
            p.publication_date,
            p.doi,
            COALESCE(p.keywords, '')   AS keywords,
            COALESCE(p.abstract, '')   AS abstract
        FROM papers p
        WHERE {' AND '.join(clauses)}
        LIMIT 500;
    """

    try:
        conn = get_db_connection()
        rows = conn.execute(sql, params).fetchall()
        conn.close()

        results = [dict(r) for r in rows]

        if term_list:
            for r in results:
                tl = r["title"].lower()
                kl = r["keywords"].lower()
                al = r["abstract"].lower()
                matches = sum(1 for term in term_list if term in tl or term in kl or term in al)
                r["matchPercent"] = round(matches / len(term_list) * 100, 2)
        else:
            for r in results:
                r["matchPercent"] = 100.0

        # sort by percent, then date
        results.sort(key=lambda r: (r["matchPercent"], r["publication_date"]), reverse=True)
        return jsonify(results)

    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/search-csv", methods=["POST"])
def search_csv():
    if "file" not in request.files:
        return jsonify({"error": "CSV file required"}), 400

    start_date       = request.form.get("startDate", "")
    end_date         = request.form.get("endDate", "")
    form_lnames_list = [x for x in request.form.get("lastNames", "").split(",") if x.strip()]
    keywords_param   = request.form.get("keywords", "").strip().lower()

    file   = request.files["file"]
    reader = csv.DictReader(TextIOWrapper(file, encoding="utf-8"))

    conn = get_db_connection()
    cur  = conn.cursor()
    all_rows = []

    for row in reader:
        query, params = "SELECT * FROM papers WHERE 1=1", []

        name1 = row.get("Last Name", "").strip().lower()
        name2 = row.get("Owner Last Name", "").strip().lower()
        if name1 and name2:
            query += " AND LOWER(last_names) LIKE ? AND LOWER(last_names) LIKE ?"
            params += [f"%{name1}%", f"%{name2}%"]
        elif name1:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name1}%")
        elif name2:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name2}%")

        if form_lnames_list:
            query += " AND " + " AND ".join(["LOWER(last_names) LIKE ?"]*len(form_lnames_list))
            params += [f"%{ln}%" for ln in form_lnames_list]

        ord_raw    = row.get("Ordered At", "")
        ord_date   = ord_raw.split(" ")[0] if " " in ord_raw else ""
        if start_date and end_date and ord_date:
            query += " AND publication_date BETWEEN ? AND ?"
            params += [start_date, end_date]

        if keywords_param:
            kw_list = [kw for kw in keywords_param.split(",") if kw.strip()]
            for kw in kw_list:
                query += " AND (LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)"
                params += [f"%{kw}%", f"%{kw}%"]

        cur.execute(query, params)
        all_rows.extend(cur.fetchall())

    conn.close()

    seen, unique = set(), []
    for r in all_rows:
        doi = r["doi"]
        if doi not in seen:
            seen.add(doi)
            unique.append(dict(r))

    return jsonify(unique)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
