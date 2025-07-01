from __future__ import annotations
import os, re, sqlite3, sys, time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm
from pymed import PubMed
from requests.exceptions import HTTPError

EMAIL          = "chamb1@gmail.com"
DB_FILE        = "papers.db"
DELTA_DB_FILE  = "papers_delta.db"
START_BASE     = datetime(2015, 1, 1)
CHUNK_DAYS     = 60
TAG            = " (possible duplicate)"

AFF_VARIANTS = [
    '"University of Massachusetts Amherst"[AD]',
    '"University of Massachusetts, Amherst"[AD]',
    '"UMass Amherst"[AD]',
    '"UMass-Amherst"[AD]',
    '"University of Massachusetts"[AD] AND Amherst[AD]',
]

def ensure_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            pmid             TEXT PRIMARY KEY,
            title            TEXT,
            doi              TEXT UNIQUE,
            journal          TEXT,
            year             INTEGER,
            authors          TEXT,
            last_names       TEXT,
            keywords         TEXT,
            abstract         TEXT,
            publication_date TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_year  ON papers(year)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title ON papers(title)")
    return conn

def reset_delta_db(path: str) -> sqlite3.Connection:
    if os.path.exists(path):
        os.remove(path)
    return ensure_db(path)

def newest_date(conn: sqlite3.Connection) -> datetime:
    cur = conn.cursor()
    cur.execute("SELECT MAX(publication_date) FROM papers")
    val = cur.fetchone()[0]
    return datetime.fromisoformat(val) if val else START_BASE

pubmed = PubMed(tool="PubMedSearcher", email=EMAIL)

def search_once(term: str, max_results: int = 1_000) -> List[Dict[str, Any]]:
    try:
        out: List[Dict[str, Any]] = []
        for art in pubmed.query(term, max_results=max_results):
            try:
                d = art.toDict()
                if d.get("doi"):
                    out.append(d)
            except Exception as e:
                print(f"‼️  toDict error → {e}", file=sys.stderr)
        return out
    except HTTPError as e:
        if e.response.status_code == 429:
            print("429 - sleeping 120 s", file=sys.stderr)
            time.sleep(120)
            return search_once(term, max_results)
        raise

def scrape_new(upper: datetime, lower: datetime) -> List[Dict[str, Any]]:
    seen: set[str] = set(); out: List[Dict[str, Any]] = []
    win_end = upper
    while win_end >= lower:
        win_start = max(lower, win_end - timedelta(days=CHUNK_DAYS))
        date_rng  = f"{win_start:%Y/%m/%d}:{win_end:%Y/%m/%d}"

        for variant in AFF_VARIANTS:
            q = f"{variant} AND {date_rng}[DP]"
            print(f" {q}")
            arts = search_once(q)
            print(f"    → {len(arts)} articles")
            for art in arts:
                pmid = (art.get("pubmed_id", "") or "").split()[0]
                if pmid and pmid not in seen:
                    seen.add(pmid); out.append(art)
            time.sleep(3)
        win_end = win_start - timedelta(days=1)
        time.sleep(1)
    return out

def normalize(txt: str) -> str:
    return re.sub(r"\W+", " ", str(txt).lower()).strip()

def exact_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["nt"] = df["Title"].apply(normalize)
    df["nl"] = df["Last Names"].fillna("").apply(normalize)
    df = df.drop_duplicates(subset=["nt", "nl"], keep="first")
    return df.drop(columns=["nt", "nl"]).reset_index(drop=True)

def fuzzy_tag(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    df = df.copy().reset_index(drop=True)
    df["nt"] = df["Title"].apply(normalize)
    df["nl"] = df["Last Names"].fillna("").apply(normalize)
    changed = pd.Series(False, index=df.index)

    for i in tqdm(range(len(df)), desc="fuzzy duplicate pass"):
        ti, li = df.at[i, "nt"], df.at[i, "nl"]
        for j in range(i + 1, len(df)):
            if fuzz.ratio(ti, df.at[j, "nt"]) < 80:
                continue
            if fuzz.token_set_ratio(li, df.at[j, "nl"]) < 75:
                continue
            for idx in (i, j):
                if not df.at[idx, "Title"].endswith(TAG):
                    df.at[idx, "Title"] += TAG
                    changed.at[idx] = True

    df.drop(columns=["nt", "nl"], inplace=True)
    return df, changed

def main() -> None:
    conn   = ensure_db(DB_FILE)
    delta  = reset_delta_db(DELTA_DB_FILE)
    lower  = newest_date(conn) + timedelta(days=1)
    upper  = datetime.today()

    if upper.date() <= lower.date():
        print("✔️  DB already current")
        conn.close(); delta.close(); return

    print(f"Fetching {lower:%Y-%m-%d} → {upper:%Y-%m-%d}")
    raw = scrape_new(upper, lower)

    new_rows: List[Dict[str, Any]] = []
    for art in raw:
        pmid = (art.get("pubmed_id", "") or "").split()[0]
        pub  = art.get("publication_date")
        if hasattr(pub, "isoformat"):
            pub_iso, year_val = pub.isoformat(), pub.year
        elif isinstance(pub, str) and pub[:4].isdigit():
            pub_iso, year_val = pub, int(pub[:4])
        else:
            pub_iso, year_val = "Unknown", None

        full, last = [], []
        for au in art.get("authors", []):
            aff = (au.get("affiliation") or "").lower()
            if "amherst" not in aff:
                continue
            if any(x in aff for x in
                   ["umass-amherst", "umass amherst", "university of massachusetts"]):
                fn, ln = (au.get("firstname") or "").strip(), (au.get("lastname") or "").strip()
                if ln:
                    full.append(f"{fn} {ln}".strip()); last.append(ln)
        if not full:
            continue

        new_rows.append(
            dict(
                PMID=pmid,
                Title=art.get("title", "No Title"),
                DOI=f"https://doi.org/{art.get('doi').strip().splitlines()[0]}",
                Journal=art.get("journal", "No Journal"),
                Year=year_val,
                **{
                    "Full Names": ", ".join(full),
                    "Last Names": ", ".join(last),
                    "Keywords": ", ".join(art.get("keywords", [])) or "No Keywords",
                    "Abstract": (art.get("abstract") or "No Abstract").strip().replace("\n", " "),
                    "Publication Date": pub_iso,
                }
            )
        )

    if not new_rows:
        print("✔️  Nothing passed the author filter")
        conn.close(); delta.close(); return

    new_df      = exact_dedupe(pd.DataFrame(new_rows))
    exist_df    = pd.read_sql_query(
        "SELECT pmid AS PMID, title AS Title, "
        "last_names AS `Last Names` FROM papers", conn)
    all_df, chg = fuzzy_tag(pd.concat([exist_df, new_df], ignore_index=True))

    cur       = conn.cursor()
    delta_cur = delta.cursor()
    ins_all   = ("INSERT OR REPLACE INTO papers "
                 "(pmid,title,doi,journal,year,authors,last_names,keywords,abstract,publication_date)"
                 "VALUES (?,?,?,?,?,?,?,?,?,?)")
    ins_delta = ins_all
    added = 0
    for _, row in new_df.iterrows():
        rec = all_df[all_df["PMID"] == row["PMID"]].iloc[0]
        tup = (
            rec["PMID"], rec["Title"], row["DOI"], row["Journal"], row["Year"],
            row["Full Names"], rec["Last Names"], row["Keywords"],
            row["Abstract"], row["Publication Date"]
        )
        cur.execute(ins_all, tup)
        delta_cur.execute(ins_delta, tup)
        added += 1

    updated = 0
    for idx, flag in chg.items():
        if not flag:
            continue
        pmid = all_df.at[idx, "PMID"]
        if pmid in new_df["PMID"].values:
            continue
        cur.execute("UPDATE papers SET title=? WHERE pmid=?",
                    (all_df.at[idx, "Title"], pmid))
        updated += cur.rowcount

    conn.commit(); delta.commit()
    conn.close();  delta.close()
    print(f"added {added}, updated {updated}, delta rows {len(new_df)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹  exit", file=sys.stderr)
