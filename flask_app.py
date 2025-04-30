#!/usr/bin/env python3
# api.py
import os, csv, sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS
from io import TextIOWrapper

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

def initialize_db() -> None:
    """Create the database and `papers` table if they don’t exist yet."""
    if os.path.exists(DB_FILE):
        return

    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            pmid             TEXT PRIMARY KEY,
            title            TEXT,
            journal          TEXT,
            year             INTEGER,
            authors          TEXT,      -- full names
            last_names       TEXT,
            doi              TEXT UNIQUE,
            keywords         TEXT,
            abstract         TEXT,
            publication_date TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_year    ON papers(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal)")
    conn.commit()
    conn.close()

def get_db_connection() -> sqlite3.Connection:
    initialize_db()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# ────────────────────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "API is running"})

@app.route("/api/papers", methods=["GET"])
def get_papers():
    lastNames = request.args.get("lastNames", "")
    startDate = request.args.get("startDate", "")
    endDate   = request.args.get("endDate", "")
    keywords  = request.args.get("keywords", "")

    last_name_list = [n.strip().lower()
                      for n in lastNames.split(",") if n.strip()]
    term_list = [kw.strip().lower()
                 for kw in keywords.split(",") if kw.strip()]

    score_parts = []
    params      = []

    for term in term_list:
        pat = f"%{term}%"
        score_parts.extend([
            f"(CASE WHEN LOWER(p.title)    LIKE ? THEN 2   ELSE 0 END)",
            f"(CASE WHEN LOWER(p.keywords) LIKE ? THEN 1   ELSE 0 END)",
            f"(CASE WHEN LOWER(p.abstract) LIKE ? THEN 0.5 ELSE 0 END)",
        ])
        params.extend([pat, pat, pat])

    score_expr = " + ".join(score_parts) if score_parts else "0"

    where_sql = ["1=1"]

    if last_name_list:
        where_sql.append(" AND ".join(["LOWER(p.last_names) LIKE ?"] *
                                       len(last_name_list)))
        params.extend([f"%{n}%" for n in last_name_list])

    if startDate and endDate:
        where_sql.append("p.publication_date BETWEEN ? AND ?")
        params.extend([startDate, endDate])

    if term_list:
        where_parts = []
        where_params = []
        for term in term_list:
            pat = f"%{term}%"
            where_parts.append("(LOWER(p.title) LIKE ? OR LOWER(p.keywords) LIKE ? OR LOWER(p.abstract) LIKE ?)")
            where_params.extend([pat, pat, pat])
        
        where_sql.append("(" + " OR ".join(where_parts) + ")")
        params.extend(where_params)

    sql = f"""
        SELECT
            p.authors AS names,
            p.title,
            p.journal,
            p.publication_date,
            p.doi,
            COALESCE(p.keywords, '') AS keywords,
            ({score_expr}) AS score
        FROM papers p
        WHERE {' AND '.join(where_sql)}
        ORDER BY score DESC, p.year DESC
        LIMIT 500;
    """

    try:
        conn = get_db_connection()
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        # Add proper error handling
        print(f"Database error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/search-csv", methods=["POST"])
def search_csv():
    if "file" not in request.files:
        return jsonify({"error": "CSV file required"}), 400

    start_date = request.form.get("startDate", "")
    end_date   = request.form.get("endDate", "")
    form_lnames = request.form.get("lastNames", "").strip().lower()
    form_lnames_list = [x.strip() for x in form_lnames.split(",") if x.strip()]
    keywords_param = request.form.get("keywords", "").strip().lower()

    file   = request.files["file"]
    reader = csv.DictReader(TextIOWrapper(file, encoding="utf-8"))

    all_results = []
    conn   = get_db_connection()
    cur    = conn.cursor()

    for row in reader:
        name1 = row.get("Last Name", "").strip().lower()
        name2 = row.get("Owner Last Name", "").strip().lower()

        ordered_at_raw = row.get("Ordered At", "")
        ordered_date   = ordered_at_raw.split(" ")[0] if " " in ordered_at_raw else ""

        if not name1 and not name2 and not form_lnames_list:
            continue

        query  = "SELECT * FROM papers WHERE 1=1"
        params = []

        if name1 and name2:
            query += " AND LOWER(last_names) LIKE ? AND LOWER(last_names) LIKE ?"
            params.extend([f"%{name1}%", f"%{name2}%"])
        elif name1:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name1}%")
        elif name2:
            query += " AND LOWER(last_names) LIKE ?"
            params.append(f"%{name2}%")

        if form_lnames_list:
            query += " AND " + " AND ".join(["LOWER(last_names) LIKE ?"] * len(form_lnames_list))
            params.extend([f"%{ln}%" for ln in form_lnames_list])

        if start_date and end_date and ordered_date:
            query += " AND publication_date BETWEEN ? AND ?"
            params.extend([start_date, end_date])

        if keywords_param:
            keyword_list = [kw.strip() for kw in keywords_param.split(",") if kw.strip()]
            for kw in keyword_list:
                query += " AND (LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)"
                params.extend([f"%{kw}%", f"%{kw}%"])

        cur.execute(query, params)
        rows = cur.fetchall()
        all_results.extend([dict(r) for r in rows])

    conn.close()

    seen, unique = set(), []
    for r in all_results:
        doi = r.get("doi", "")
        if doi not in seen:
            seen.add(doi)
            unique.append(r)

    return jsonify(unique)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
