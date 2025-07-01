#!/usr/bin/env python3
import os, csv, io, json, ssl, smtplib, sqlite3
from datetime import datetime
from email.message import EmailMessage

DB_FILE     = os.getenv("DB_FILE", "papers.db")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = int(os.getenv("SMTP_PORT", 465))
SMTP_USER   = "ialspublicationsearcherbot@gmail.com"
SMTP_PASS   = "qteswiijqsvcnvab" 
SITE_ROOT   = os.getenv("SITE_ROOT", "http://128.119.128.245:8000")

SURNAME_EXPR = "LOWER(REPLACE(last_names,' ',''))"
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn
def query_new_papers(filters: dict, since_iso: str):
    lnames = [x.strip().lower() for x in filters.get("lastNames", "").split(",") if x.strip()]
    start  = since_iso or filters.get("startDate") or "1900-01-01"
    end    = filters.get("endDate", "")
    kws    = [k.strip().lower() for k in filters.get("keywords", "").split(",") if k.strip()]

    clauses, params = ["1=1"], []

    if lnames:
        clauses.append(" AND ".join(
            [f"',|'||{SURNAME_EXPR}||',|' LIKE '%,|'||?||',|%'" for _ in lnames]
        ))
        params += [ln.replace(" ", "") for ln in lnames]

    if start and end:
        clauses.append("publication_date BETWEEN ? AND ?"); params += [start, end]
    elif start:
        clauses.append("publication_date >= ?"); params.append(start)
    elif end:
        clauses.append("publication_date <= ?"); params.append(end)
    if kws:
        or_sets = []
        for kw in kws:
            like = f"%{kw}%"
            or_sets.append("(LOWER(title) LIKE ? OR LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)")
            params += [like, like, like]
        clauses.append("(" + " OR ".join(or_sets) + ")")
    sql = f"""
        SELECT pmid AS id, authors AS names, title, journal, year,
               doi, publication_date AS date
        FROM   papers
        WHERE  {' AND '.join(clauses)}
    """
    conn = get_conn()
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows
def send_digest(to_addr: str, rows: list[dict], unsub_token: str):
    buf = io.StringIO(newline="")
    w   = csv.writer(buf)
    w.writerow(["Names", "Title", "Journal", "Year", "DOI", "Date"])
    for r in rows:
        w.writerow([r["names"], r["title"], r["journal"], r["year"], r["doi"], r["date"]])
    csv_bytes = buf.getvalue().encode()

    msg = EmailMessage()
    msg["Subject"] = "Your monthly IALS publication updates"
    msg["From"]    = SMTP_USER
    msg["To"]      = to_addr
    msg.set_content(
        f"{len(rows)} new paper(s) matched your saved search.\n\n"
        f"Unsubscribe: {SITE_ROOT}/api/unsubscribe/{unsub_token}\n\n"
        "Best,\nUMass IALS Core Facility Publication Searcher Bot"
    )
    msg.add_attachment(csv_bytes, maintype="text", subtype="csv",
                       filename=f"umass_updates_{datetime.utcnow():%Y%m%d}.csv")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=ctx) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def main():
    conn = get_conn(); cur = conn.cursor()
    subs = cur.execute("SELECT * FROM subscriptions").fetchall()

    for sub in subs:
        filters = json.loads(sub["json_filters"])
        since   = sub["last_sent"] or ""
        rows    = query_new_papers(filters, since)
        if not rows:
            continue

        send_digest(sub["email"], rows, sub["unsub_token"])

        newest = max(r["date"] for r in rows if r["date"])
        cur.execute("UPDATE subscriptions SET last_sent=? WHERE id=?",
                    (newest, sub["id"]))
        conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
