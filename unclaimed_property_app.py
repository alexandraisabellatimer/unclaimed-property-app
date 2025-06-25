"""
unclaimed_property_app.py  (v0.4 – June 2025)
================================================
A reference implementation of an end‑to‑end, searchable database + API for
California’s Unclaimed Property records.

Features
--------
✓ Automated data sync (manual trigger or scheduled) from the State Controller’s
  downloadable CSV ZIPs.
✓ SQLite/FTS5 storage out of the box.
✓ FastAPI REST service with:
    •  GET /search?q=string&limit=100  – fuzzy owner / address search
    •  GET /property/{property_id}     – fetch full record by ID
    •  POST /claim                     – begin claim workflow

Quick start (local dev)
-----------------------
# 1) clone the file or copy its contents
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python unclaimed_property_app.py ingest   # downloads + builds DB
python unclaimed_property_app.py serve    # runs uvicorn app:api --reload
Then open http://127.0.0.1:8000/docs for Swagger UI.

Environment variables (see .env.example)
---------------------------------------
DB_URL=sqlite:///data/unclaimed.db
DATA_DIR=data
SCO_BASE=https://dpupd.sco.ca.gov

Note: If your environment throws ModuleNotFoundError: No module named '_multiprocessing',
comment out or remove any scheduler lines and handle DB syncs manually.
This issue typically occurs in restricted environments like containers or slim Python builds.

License: MIT.  Use at your own risk; not legal advice.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import pathlib
import sqlite3
import zipfile
from typing import List, Optional

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("SCO_BASE", "https://dpupd.sco.ca.gov")
ALL_FILE = "00_All_Records.zip"
TIERS = [
    "01_From_0_To_Below_10.zip",
    "02_From_10_To_Below_100.zip",
    "03_From_100_To_Below_500.zip",
    "04_From_500_To_Beyond.zip",
]
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "data"))
DB_PATH = pathlib.Path(os.getenv("DB_PATH", DATA_DIR / "unclaimed.db"))

DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Ingest / Sync Pipeline
# ---------------------------------------------------------------------------

def download_zip(relative_path: str) -> bytes:
    url = f"{BASE_URL}/{relative_path}"
    print(f"Downloading {url} …")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content

def extract_csv_from_zip(zip_bytes: bytes) -> io.StringIO:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            buf = f.read()
    return io.StringIO(buf.decode("utf-8", errors="replace"))

def build_database(csv_buffers: List[io.StringIO]):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS properties (
                property_id TEXT PRIMARY KEY,
                owner_name TEXT,
                owner_address TEXT,
                owner_city TEXT,
                owner_state TEXT,
                owner_zip TEXT,
                amount_reported REAL,
                cash_reported TEXT,
                property_type TEXT,
                holder_name TEXT,
                holder_address TEXT,
                reported_date TEXT,
                raw_json TEXT
            );"""
    )
    cur.execute(
        """CREATE VIRTUAL TABLE IF NOT EXISTS properties_fts USING fts5(
                owner_name, owner_address, owner_city, holder_name, content='properties', content_rowid='rowid');"""
    )
    conn.commit()

    for buf in csv_buffers:
        print("Streaming rows → DB …")
        reader = csv.DictReader(buf)
        rows = []
        for i, row in enumerate(reader, 1):
            rows.append(
                (
                    row.get("PROPERTY_ID") or row.get("Property ID"),
                    f"{row.get('OWNER_NAME', '')} {row.get('OWNER_FIRST_NAME', '')}",
                    row.get("OWNER_ADDRESS", ""),
                    row.get("OWNER_CITY", ""),
                    row.get("OWNER_STATE", ""),
                    row.get("OWNER_ZIP", ""),
                    float(row.get("AMOUNT_REPORTED", 0) or 0),
                    row.get("CASH_REPORTED", ""),
                    row.get("PROPERTY_TYPE", ""),
                    row.get("HOLDER_NAME", ""),
                    row.get("HOLDER_ADDRESS", ""),
                    row.get("REPORTED_DATE", ""),
                    str(row),
                )
            )
            if i % 10000 == 0:
                cur.executemany("INSERT OR IGNORE INTO properties VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.commit()
                cur.executemany("INSERT INTO properties_fts(rowid, owner_name, owner_address, owner_city, holder_name) SELECT rowid, owner_name, owner_address, owner_city, holder_name FROM properties WHERE rowid > (SELECT IFNULL(MAX(rowid),0) FROM properties_fts);")
                rows = []
        if rows:
            cur.executemany("INSERT OR IGNORE INTO properties VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
            cur.executemany("INSERT INTO properties_fts(rowid, owner_name, owner_address, owner_city, holder_name) SELECT rowid, owner_name, owner_address, owner_city, holder_name FROM properties WHERE rowid > (SELECT IFNULL(MAX(rowid),0) FROM properties_fts);")
            conn.commit()
    conn.close()

def sync():
    zip_bytes = download_zip(ALL_FILE)
    buf = extract_csv_from_zip(zip_bytes)
    build_database([buf])
    print("[sync] Complete – DB ready →", DB_PATH)

# ---------------------------------------------------------------------------
# FastAPI Service
# ---------------------------------------------------------------------------
app = FastAPI(title="CA Unclaimed Property Search API", version="0.4")

class Property(BaseModel):
    property_id: str
    owner_name: str
    owner_address: Optional[str] = None
    owner_city: Optional[str] = None
    owner_state: Optional[str] = None
    owner_zip: Optional[str] = None
    amount_reported: Optional[float] = None
    property_type: Optional[str] = None
    holder_name: Optional[str] = None
    reported_date: Optional[str] = None

def get_conn():
    return sqlite3.connect(DB_PATH)

@app.get("/search", response_model=List[Property])
def search(q: str, limit: int = 50):
    if len(q) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT p.* FROM properties p JOIN properties_fts fts ON p.rowid = fts.rowid WHERE fts MATCH ? LIMIT ?", (q, limit))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    conn.close()
    return rows

@app.get("/property/{property_id}", response_model=Property)
def get_property(property_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM properties WHERE property_id = ?", (property_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Not found")
    return dict(zip([c[0] for c in cur.description], row))

class ClaimRequest(BaseModel):
    property_id: str
    claimant_name: str
    claimant_address: str
    claimant_email: str
    claimant_phone: Optional[str] = None

@app.post("/claim")
def start_claim(payload: ClaimRequest):
    # No RON/Proof integration; just acknowledge for now.
    prop = get_property(payload.property_id)
    # In future, call DocuSign or another e-signature API here.
    return {"message": "Claim initiated", "property": prop}

def main():
    parser = argparse.ArgumentParser(description="CA Unclaimed Property Toolkit")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ingest", help="Download CSV & build DB")
    sub.add_parser("serve", help="Run FastAPI server (uvicorn)")

    args = parser.parse_args()
    if args.cmd == "ingest":
        sync()
    elif args.cmd == "serve":
        from uvicorn import run
        if not DB_PATH.exists():
            print("[warn] DB not found → building automatically …")
            sync()
        run("unclaimed_property_app:app", host="0.0.0.0", port=8000, reload=False, workers=1)

if __name__ == "__main__":
    main()
