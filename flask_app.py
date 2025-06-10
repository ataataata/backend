import os
import csv
import re
import sqlite3
from datetime import datetime
from typing import List, Tuple, Optional
from io import TextIOWrapper

from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

DB_FILE = "papers.db"

SURNAME_EXPR = "LOWER(REPLACE(last_names,' ',''))"
DATE_FMT_SQL = "%Y-%m-%d"

def _last_name(full_name: str) -> str:
    """
    Extract the last token from 'First M. Last' or 'Last, First' variants,
    make it lowercase, and strip inner spaces so 'van der Waals' ➜ 'vanderwaals'.
    """
    full_name = full_name.strip()
    if not full_name:
        return ""
    surname = re.split(r"[;,]", full_name)[0].split()[-1].lower()
    return re.sub(r"\s+", "", surname)


def _detect_format(headers: List[str]) -> str:
    h = {h.lower().strip() for h in headers}
    if {"last name", "owner last name"} <= h:
        return "old" 
    if {"ordered for", "owner"} <= h:
        return "new"  
    raise ValueError("CSV does not match a supported format (old/new)")


def _to_iso(date_str: str) -> Optional[str]:
    """Return YYYY-MM-DD for common timestamp styles, else None."""
    if not date_str:
        return None
    date_part = date_str.split()[0]
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_part, fmt).strftime(DATE_FMT_SQL)
        except ValueError:
            continue
    return None

def initialize_db() -> None:
    if os.path.exists(DB_FILE):
        return
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE papers (
            pmid TEXT PRIMARY KEY,
            title TEXT,
            journal TEXT,
            year INTEGER,
            authors TEXT,
            last_names TEXT,              -- comma-separated list of surnames
            doi TEXT UNIQUE,
            keywords TEXT,
            abstract TEXT,
            publication_date TEXT
        );
    """)
    cur.execute("CREATE INDEX idx_ln ON papers(last_names)")
    cur.execute("CREATE INDEX idx_yr ON papers(year)")
    cur.execute("CREATE INDEX idx_jr ON papers(journal)")
    conn.commit(); conn.close()


def get_conn() -> sqlite3.Connection:
    initialize_db()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/")
def health():
    return jsonify({"status": "API is running"})

@app.route("/api/papers")
def get_papers():
    lnames = [s.lower().strip() for s in request.args.get("lastNames", "").split(",") if s.strip()]
    start  = request.args.get("startDate", "")
    end    = request.args.get("endDate", "")
    terms  = [s.lower().strip() for s in request.args.get("keywords", "").split(",") if s.strip()]

    clauses, params = ["1=1"], []

    if lnames:
        clauses.append(" AND ".join(
            [f"','||{SURNAME_EXPR}||',' LIKE '%,'||?||',%'" for _ in lnames]
        ))
        params += [ln.replace(" ", "") for ln in lnames]

    if start and end:
        clauses.append("publication_date BETWEEN ? AND ?"); params += [start, end]
    elif start:
        clauses.append("publication_date >= ?"); params.append(start)
    elif end:
        clauses.append("publication_date <= ?"); params.append(end)

    if terms:
        pattern_sets = []
        for kw in terms:
            like = f"%{kw}%"
            pattern_sets.append("(LOWER(title) LIKE ? OR LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)")
            params += [like, like, like]
        clauses.append("(" + " OR ".join(pattern_sets) + ")")

    sql = f"""
        SELECT pmid AS id,
               authors AS names,
               title,
               journal,
               publication_date AS date,
               doi,
               COALESCE(keywords,'')  AS keywords,
               COALESCE(abstract,'')  AS abstract
        FROM   papers
        WHERE  {' AND '.join(clauses)}
        LIMIT  500;
    """
    conn = get_conn()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    res = [dict(r) for r in rows]

    for r in res:
        if terms:
            m = sum(t in r["title"].lower() or
                    t in r["keywords"].lower() or
                    t in r["abstract"].lower()
                    for t in terms)
            r["matchPercent"] = round(m / len(terms) * 100, 2)
        else:
            r["matchPercent"] = 100.0

    res.sort(key=lambda r: (r["matchPercent"], r["date"]), reverse=True)
    return jsonify(res)

@app.route("/api/search-csv", methods=["POST"])
def search_csv():
    if "file" not in request.files:
        return jsonify({"error": "CSV file required"}), 400

    start = request.form.get("startDate", "")
    end   = request.form.get("endDate", "")
    ui_ln = [s.lower().strip() for s in request.form.get("lastNames", "").split(",") if s.strip()]
    kws   = [k.lower().strip() for k in request.form.get("keywords", "").split(",") if k.strip()]

    reader = csv.DictReader(TextIOWrapper(request.files["file"], encoding="utf-8"))
    try:
        style = _detect_format(reader.fieldnames or [])
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    rows: List[Tuple[str, str, str]] = []
    first_iso: Optional[str] = None

    for row in reader:
        if style == "old":
            l1_raw = row.get("Last Name", "").lower().strip()
            l2_raw = row.get("Owner Last Name", "").lower().strip()
            date_raw = row.get("Ordered At", "")
        else:  # "new" export
            l1_raw = _last_name(row.get("Ordered For", ""))
            l2_raw = _last_name(row.get("Owner", ""))
            date_raw = row.get("Fulfilled Date", "")

        l1 = re.sub(r"\s+", "", l1_raw)
        l2 = re.sub(r"\s+", "", l2_raw)

        if not l1 or not l2:
            continue

        iso = _to_iso(date_raw) or ""
        if first_iso is None:
            first_iso = iso
        rows.append((l1, l2, iso))

    if not rows:
        return jsonify([])

    conn = get_conn(); cur = conn.cursor()
    cur.execute("CREATE TEMP TABLE temp_csv(l1 TEXT, l2 TEXT, d TEXT)")
    cur.executemany("INSERT INTO temp_csv VALUES (?,?,?)", rows)
    conn.commit()

    sql = f"""
        SELECT DISTINCT p.*
        FROM   papers p
        JOIN   temp_csv t
               ON (','||{SURNAME_EXPR}||',') LIKE '%,'||t.l1||',%'
              AND (','||{SURNAME_EXPR}||',') LIKE '%,'||t.l2||',%'
    """
    params: List[str] = []

    for ln in ui_ln:
        sql += f" AND ','||{SURNAME_EXPR}||',' LIKE '%,'||?||',%'"
        params.append(ln.replace(" ", ""))

    eff_start = start or first_iso or ""
    if eff_start and end:
        sql += " AND p.publication_date BETWEEN ? AND ?"
        params += [eff_start, end]
    elif eff_start:
        sql += " AND p.publication_date >= ?"
        params.append(eff_start)
    elif end:
        sql += " AND p.publication_date <= ?"
        params.append(end)

    if kws:
        kw_clauses = []
        for kw in kws:
            like = f"%{kw}%"
            kw_clauses.append("(LOWER(p.keywords) LIKE ? OR LOWER(p.abstract) LIKE ?)")
            params += [like, like]
        sql += " AND (" + " OR ".join(kw_clauses) + ")"


    rows = cur.execute(sql, params).fetchall()
    conn.close()

    res = [dict(r) for r in rows]

    for r in res:
        if kws: 
            m = sum(
                kw in (r["title"] or "").lower()
             or kw in (r["keywords"] or "").lower()
             or kw in (r["abstract"] or "").lower()
                for kw in kws
            )
            r["matchPercent"] = round(m / len(kws) * 100, 2)
        else:
            r["matchPercent"] = 0

    res.sort(key=lambda r: (r["matchPercent"], r["publication_date"]), reverse=True)

    return jsonify(res)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)