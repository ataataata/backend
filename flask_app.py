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
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_last_names ON papers(last_names)")
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
        clauses.append(" AND ".join(["LOWER(last_names) LIKE ?"]*len(last_name_list)))
        params.extend([f"%{n}%" for n in last_name_list])

    if startDate and endDate:
        clauses.append("publication_date BETWEEN ? AND ?")
        params.extend([startDate, endDate])

    if term_list:
        or_parts = []
        for _ in term_list:
            pat = f"%{_}%"
            or_parts.append(
                "(LOWER(title) LIKE ? OR LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)"
            )
            params.extend([pat, pat, pat])
        clauses.append("(" + " OR ".join(or_parts) + ")")

    sql = f"""
        SELECT
            pmid   AS id,
            authors AS names,
            title,
            journal,
            publication_date AS date,
            doi,
            COALESCE(keywords, '')   AS keywords,
            COALESCE(abstract, '')   AS abstract
        FROM papers
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
                matches = sum(
                    1 for term in term_list
                    if term in r['title'].lower()
                    or term in r['keywords'].lower()
                    or term in r['abstract'].lower()
                )
                r['matchPercent'] = round(matches / len(term_list) * 100, 2)
        else:
            for r in results:
                r['matchPercent'] = 100.0

        results.sort(key=lambda r: (r['matchPercent'], r['date']), reverse=True)
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/search-csv", methods=["POST"])
def search_csv():
    if "file" not in request.files:
        return jsonify({"error": "CSV file required"}), 400

    start_date     = request.form.get("startDate", "")
    end_date       = request.form.get("endDate", "")
    form_lnames    = [x.strip().lower() for x in request.form.get("lastNames","").split(",") if x.strip()]
    keywords_param = [kw for kw in request.form.get("keywords","").lower().split(",") if kw.strip()]

    file   = request.files["file"]
    reader = csv.DictReader(TextIOWrapper(file, encoding="utf-8"))
    data_rows = []
    for r in reader:
        name1 = r.get("Last Name","").strip().lower()
        name2 = r.get("Owner Last Name","").strip().lower()
        ord_date = r.get("Ordered At","").split(" ")[0]
        data_rows.append((name1, name2, ord_date))

    conn = get_db_connection()
    cur  = conn.cursor()

    # Create temp table & bulk insert
    cur.execute("CREATE TEMP TABLE temp_csv (name1 TEXT, name2 TEXT, ord_date TEXT)")
    cur.executemany("INSERT INTO temp_csv VALUES (?,?,?)", data_rows)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_name1 ON temp_csv(name1)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_temp_name2 ON temp_csv(name2)")
    conn.commit()

    sql = '''
      SELECT DISTINCT p.*
      FROM papers p
      JOIN temp_csv t ON
        (t.name1 = '' OR LOWER(p.last_names) LIKE '%' || t.name1 || '%')
       AND (t.name2 = '' OR LOWER(p.last_names) LIKE '%' || t.name2 || '%')
      WHERE 1=1
    '''
    params = []

    for ln in form_lnames:
        sql += " AND LOWER(p.last_names) LIKE ?"
        params.append(f"%{ln}%")

    # date filter
    if start_date and end_date:
        sql += " AND p.publication_date BETWEEN ? AND ?"
        params += [start_date, end_date]

    # keywords filter
    for kw in keywords_param:
        sql += " AND (LOWER(p.keywords) LIKE ? OR LOWER(p.abstract) LIKE ?)"
        params += [f"%{kw}%", f"%{kw}%"]

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)