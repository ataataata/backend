import csv, io, json, os, re, smtplib, ssl, sqlite3, sys
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import List, Dict, Tuple

SUBS_DB  = os.getenv("SUBS_DB",  "papers.db")
PAPERS_DB = os.getenv("PAPERS_DB", "papers_delta.db")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = 465
SMTP_USER   = "ialspublicationsearcherbot@gmail.com"
SMTP_PASS   = "qteswiijqsvcnvab"
SITE_ROOT   = "https://128.119.128.180"

UPLOAD_DIR  = os.path.join(os.path.dirname(__file__), "csv_uploads")

def _last_name(full_name: str) -> str:
    full_name = full_name.strip()
    if not full_name:
        return ""
    surname = re.split(r"[;,]", full_name)[0].split()[-1].lower()
    return re.sub(r"\s+", "", surname)

def _detect_format(headers: List[str]) -> str:
    h = {h.lower().strip() for h in headers}
    if {"last name", "owner last name"} <= h: return "old"
    if {"ordered for", "owner"} <= h:        return "new"
    raise ValueError("CSV does not match a supported format (old/new)")

def conn_subs()   -> sqlite3.Connection:
    c = sqlite3.connect(SUBS_DB);  c.row_factory = sqlite3.Row;  return c

def conn_papers() -> sqlite3.Connection:
    c = sqlite3.connect(PAPERS_DB); c.row_factory = sqlite3.Row; return c

def search_papers(flt: Dict) -> List[Dict]:
    ln   = [s.strip().lower() for s in flt.get("lastNames","").split(",") if s.strip()]
    kws  = [k.strip().lower() for k in flt.get("keywords","").split(",")   if k.strip()]
    sdat = flt.get("startDate","")
    edat = flt.get("endDate","")

    clauses, params = ["1=1"], []
    if ln:
        clauses.append(" AND ".join(["LOWER(last_names) LIKE '%'||?||'%'" for _ in ln]))
        params += ln
    if sdat and edat:
        clauses.append("publication_date BETWEEN ? AND ?"); params += [sdat, edat]
    elif sdat:
        clauses.append("publication_date >= ?"); params.append(sdat)
    elif edat:
        clauses.append("publication_date <= ?"); params.append(edat)

    if kws:
        ors = []
        for kw in kws:
            like = f"%{kw}%"
            ors.append("(LOWER(title) LIKE ? OR LOWER(keywords) LIKE ? OR LOWER(abstract) LIKE ?)")
            params += [like, like, like]
        clauses.append("(" + " OR ".join(ors) + ")")

    sql = f"""SELECT pmid AS id, authors AS names, title, journal, year,
                     doi, publication_date AS date
              FROM   papers
              WHERE  {' AND '.join(clauses)}"""
    with conn_papers() as cp:
        return [dict(r) for r in cp.execute(sql, params)]

def pairs_from_csv(path: str) -> List[Tuple[str,str]]:
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        style = _detect_format(rdr.fieldnames or [])
        out: List[Tuple[str,str]] = []
        for row in rdr:
            if style == "old":
                l1 = _last_name(row.get("Last Name",""))
                l2 = _last_name(row.get("Owner Last Name",""))
            else:
                l1 = _last_name(row.get("Ordered For",""))
                l2 = _last_name(row.get("Owner",""))
            if l1 and l2:
                out.append((l1,l2))
        return out

def search_csv_cfg(cfg: Dict) -> List[Dict]:
    results: List[Dict] = []
    for l1,l2 in cfg["pairs"]:
        mini = {
            "lastNames": f"{l1},{l2}",
            "keywords" : cfg.get("keywords",""),
            "startDate": cfg.get("startDate",""),
            "endDate"  : cfg.get("endDate","")
        }
        results += search_papers(mini)
    return results

def send_digest(addr: str, rows: List[Dict], token: str) -> None:
    buf = io.StringIO(); w = csv.writer(buf, lineterminator="\n")
    w.writerow(["Names","Title","Journal","Year","DOI","Date"])
    for r in rows:
        w.writerow([r["names"], r["title"], r["journal"],
                    r["year"], r["doi"], r["date"]])

    msg = EmailMessage()
    msg["Subject"] = "Your monthly IALS publication updates"
    msg["From"] = SMTP_USER
    msg["To"]   = addr
    msg.set_content(
        f"{len(rows)} new paper(s) matched your saved search.\n\n"
        f"Unsubscribe: {SITE_ROOT}/unsubscribe/{token}\n\n"
        "Best,\nUMass IALS Publication Bot"
    )
    msg.add_attachment(buf.getvalue().encode(),
                       maintype="text", subtype="csv",
                       filename=f"ials_updates_{datetime.now(timezone.utc):%Y%m%d}.csv")

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=ctx) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def main() -> None:
    with conn_subs() as cs:
        subs      = cs.execute("SELECT * FROM subscriptions").fetchall()
        csv_rows  = cs.execute("SELECT * FROM csv_subscriptions").fetchall()

    buckets: Dict[str, List[Tuple[str,Dict,str]]] = {}
    for r in subs:
        buckets.setdefault(r["email"], []).append(("json", json.loads(r["json_filters"]), r["unsub_token"]))

    for r in csv_rows:
        try:
            pairs = pairs_from_csv(r["file_path"])
        except (ValueError, FileNotFoundError) as e:
            print(f"Skipping {r['file_path']}: {e}", file=sys.stderr)
            continue
        cfg = {
            "pairs": pairs,
            "keywords": r["keywords"],
            "startDate": r["start_date"],
            "endDate":   r["end_date"]
        }
        buckets.setdefault(r["email"], []).append(("csv", cfg, r["unsub_token"]))

    for email, items in buckets.items():
        pmid_map: Dict[str, Dict] = {}
        token = items[0][2] 

        for typ,cfg,_ in items:
            found = search_csv_cfg(cfg) if typ=="csv" else search_papers(cfg)
            for r in found:
                pmid_map.setdefault(r["id"], r)

        print(f"[DEBUG] {email}: {len(pmid_map)} match(es)")
        if pmid_map:
            send_digest(email, list(pmid_map.values()), token)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("‼️", e, file=sys.stderr)
        sys.exit(1)
