"""
Bexar County, TX (San Antonio) — Motivated Seller Lead Scraper
==============================================================
Clerk portal : https://bexar.tx.publicsearch.us/  (GovOS/Neumo platform)
Parcel data  : Bexar Appraisal District bulk export

API discovered from real browser traffic:
  GET /results?department=RP
             &recordedDateRange=YYYYMMDD,YYYYMMDD
             &searchType=quickSearch
             &searchValue=LIS+PENDENS
             &limit=50
             &offset=0
             &searchOcrText=false
             &keywordSearch=false

Returns JSON with hits[] array.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fetch")

ROOT      = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "dashboard"
DATA_DIR  = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
for _d in (DASHBOARD, DATA_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
API_BASE      = "https://bexar.tx.publicsearch.us"
RESULTS_URL   = "https://bexar.tx.publicsearch.us/results"
DEPT          = "RP"
PAGE_SIZE     = 50
REQUEST_DELAY = 1.2
RETRY_MAX     = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://bexar.tx.publicsearch.us/",
    "Origin":          "https://bexar.tx.publicsearch.us",
}

DOC_TYPES: dict[str, dict[str, Any]] = {
    "LP":       {"label": "Lis Pendens",            "cat": "lis_pendens", "flags": ["Lis pendens", "Pre-foreclosure"]},
    "NOFC":     {"label": "Notice of Foreclosure",  "cat": "foreclosure", "flags": ["Pre-foreclosure"]},
    "TAXDEED":  {"label": "Tax Deed",               "cat": "tax_deed",    "flags": ["Tax lien"]},
    "JUD":      {"label": "Judgment",               "cat": "judgment",    "flags": ["Judgment lien"]},
    "CCJ":      {"label": "Certified Judgment",     "cat": "judgment",    "flags": ["Judgment lien"]},
    "DRJUD":    {"label": "Domestic Judgment",      "cat": "judgment",    "flags": ["Judgment lien"]},
    "LNCORPTX": {"label": "Corp Tax Lien",          "cat": "lien",        "flags": ["Tax lien"]},
    "LNIRS":    {"label": "IRS Lien",               "cat": "lien",        "flags": ["Tax lien"]},
    "LNFED":    {"label": "Federal Lien",           "cat": "lien",        "flags": ["Tax lien"]},
    "LN":       {"label": "Lien",                   "cat": "lien",        "flags": []},
    "LNMECH":   {"label": "Mechanic Lien",          "cat": "lien",        "flags": ["Mechanic lien"]},
    "LNHOA":    {"label": "HOA Lien",               "cat": "lien",        "flags": []},
    "MEDLN":    {"label": "Medicaid Lien",          "cat": "lien",        "flags": []},
    "PRO":      {"label": "Probate",                "cat": "probate",     "flags": ["Probate / estate"]},
    "NOC":      {"label": "Notice of Commencement", "cat": "notice",      "flags": []},
    "RELLP":    {"label": "Release Lis Pendens",    "cat": "release",     "flags": []},
}
TARGET_CODES = set(DOC_TYPES.keys())

INSTRUMENT_MAP: dict[str, str] = {
    "LIS PENDENS": "LP", "LP": "LP",
    "NOTICE OF FORECLOSURE": "NOFC", "FORECLOSURE": "NOFC",
    "NOTICE OF TRUSTEE SALE": "NOFC", "NOTICE OF TRUSTEE'S SALE": "NOFC",
    "SUBSTITUTE TRUSTEE'S DEED": "NOFC", "SUBSTITUTE TRUSTEE DEED": "NOFC",
    "TRUSTEE'S DEED": "NOFC", "TAX DEED": "TAXDEED",
    "CONSTABLE'S DEED": "TAXDEED", "SHERIFF'S DEED": "TAXDEED",
    "ABSTRACT OF JUDGMENT": "JUD", "ABSTRACT OF JUDGEMENT": "JUD",
    "JUDGMENT": "JUD", "JUDGEMENT": "JUD", "FOREIGN JUDGMENT": "JUD",
    "CERTIFIED JUDGMENT": "CCJ", "CERTIFIED COPY OF JUDGMENT": "CCJ",
    "DOMESTIC JUDGMENT": "DRJUD", "DOMESTIC RELATIONS ORDER": "DRJUD",
    "CORP TAX LIEN": "LNCORPTX", "CORPORATE TAX LIEN": "LNCORPTX",
    "STATE TAX LIEN": "LNCORPTX", "TWC LIEN": "LNCORPTX",
    "TEXAS WORKFORCE COMMISSION LIEN": "LNCORPTX",
    "IRS LIEN": "LNIRS", "FEDERAL TAX LIEN": "LNIRS",
    "NOTICE OF FEDERAL TAX LIEN": "LNIRS", "FEDERAL LIEN": "LNFED",
    "LIEN": "LN", "MECHANIC'S LIEN": "LNMECH", "MECHANIC LIEN": "LNMECH",
    "MATERIALMAN'S LIEN": "LNMECH", "MATERIALMAN LIEN": "LNMECH",
    "HOA LIEN": "LNHOA", "HOMEOWNERS ASSOCIATION LIEN": "LNHOA",
    "HOMEOWNER'S ASSOCIATION LIEN": "LNHOA",
    "MEDICAID LIEN": "MEDLN", "MEDICAL LIEN": "MEDLN",
    "PROBATE": "PRO", "LETTERS TESTAMENTARY": "PRO",
    "LETTERS OF ADMINISTRATION": "PRO", "MUNIMENT OF TITLE": "PRO",
    "AFFIDAVIT OF HEIRSHIP": "PRO", "WILL": "PRO",
    "NOTICE OF COMMENCEMENT": "NOC",
    "RELEASE OF LIS PENDENS": "RELLP", "RELEASE LIS PENDENS": "RELLP",
}

SEARCH_TERMS = [
    "LIS PENDENS", "NOTICE OF FORECLOSURE", "NOTICE OF TRUSTEE",
    "SUBSTITUTE TRUSTEE", "TAX DEED", "ABSTRACT OF JUDGMENT",
    "FEDERAL TAX LIEN", "IRS LIEN", "STATE TAX LIEN", "TWC LIEN",
    "MECHANIC", "HOA LIEN", "HOMEOWNER", "MEDICAID LIEN",
    "PROBATE", "LETTERS TESTAMENTARY", "NOTICE OF COMMENCEMENT",
    "RELEASE LIS PENDENS",
]

# ── helpers ────────────────────────────────────────────────────────────────────

def safe(v, default: str = "") -> str:
    return default if v is None else str(v).strip()

def parse_amount(v) -> float | None:
    if isinstance(v, (int, float)):
        return float(v) if v > 0 else None
    cleaned = re.sub(r"[$,\s]", "", safe(v))
    m = re.search(r"\d+(?:\.\d{1,2})?", cleaned)
    return float(m.group()) if m else None

def map_instrument(raw: str) -> str | None:
    if not raw:
        return None
    upper = raw.strip().upper()
    if upper in INSTRUMENT_MAP:
        return INSTRUMENT_MAP[upper]
    for key, code in INSTRUMENT_MAP.items():
        if key in upper:
            return code
    return None

def norm_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y",
                "%Y%m%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(str(raw)[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return str(raw)[:10] if len(str(raw)) >= 10 else ""

def doc_url(doc_id: str) -> str:
    return f"{API_BASE}/doc/{doc_id}" if doc_id else API_BASE

def name_variants(name: str) -> list[str]:
    name = name.strip().upper()
    variants: set[str] = {name}
    cleaned = name.rstrip(",")
    variants.add(cleaned)
    parts = re.split(r"[\s,]+", cleaned)
    parts = [p for p in parts if p]
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
        variants.add(f"{' '.join(parts[1:])} {parts[0]}")
    return [v for v in variants if v]

# ── scoring ────────────────────────────────────────────────────────────────────

def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = list(DOC_TYPES.get(rec.get("doc_type", ""), {}).get("flags", []))
    owner_up = safe(rec.get("owner", "")).upper()
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bL\.P\.\b|\bLTD\b|\bTRUST\b|\bFUND\b|\bINVEST", owner_up):
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass
    seen: set[str] = set()
    flags = [f for f in flags if not (f in seen or seen.add(f))]  # type: ignore[func-returns-value]
    score = 30
    DISTRESS = {"Lis pendens", "Pre-foreclosure", "Judgment lien",
                "Tax lien", "Mechanic lien", "Probate / estate", "LLC / corp owner"}
    score += sum(10 for f in flags if f in DISTRESS)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amt = rec.get("amount")
    if amt:
        if   amt > 100_000: score += 15
        elif amt >  50_000: score += 10
    if "New this week"   in flags: score += 5
    if rec.get("prop_address"):    score += 5
    return min(score, 100), flags

# ── parcel lookup ──────────────────────────────────────────────────────────────

class ParcelLookup:
    BCAD_URLS = [
        "https://www.bcad.org/downloads",
        "https://www.bcad.org/data-downloads",
        "https://www.bcad.org/publicinformation",
        "https://www.bcad.org",
    ]

    def __init__(self):
        self._index: dict[str, dict] = {}

    def load(self):
        if not HAS_DBF:
            log.warning("dbfread not installed — address enrichment skipped.")
            return
        dbf = self._find_dbf()
        if dbf:
            self._build_index(dbf)
        else:
            log.warning("No parcel DBF found — records will have no addresses.")

    def lookup(self, name: str) -> dict | None:
        if not name:
            return None
        for v in name_variants(name):
            hit = self._index.get(v)
            if hit:
                return hit
        token = name.strip().upper().split()[0]
        if len(token) > 3:
            for key, val in self._index.items():
                if key.startswith(token):
                    return val
        return None

    def _find_dbf(self) -> Path | None:
        return self._try_bcad() or self._try_ptad()

    def _try_bcad(self) -> Path | None:
        cache = CACHE_DIR / "bcad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached BCAD DBF.")
            return cache
        for url in self.BCAD_URLS:
            try:
                resp = requests.get(url, timeout=20, headers=HEADERS)
                if not resp.ok:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href: str = a["href"]
                    if any(kw in href.lower() for kw in
                           (".dbf", ".zip", "parcel", "apprais", "export", "download", "bulk")):
                        full = href if href.startswith("http") else urljoin(url, href)
                        dl   = self._download(full, CACHE_DIR / "bcad_raw")
                        if dl:
                            dbf = self._unpack(dl)
                            if dbf:
                                return dbf
            except Exception as e:
                log.debug("BCAD %s: %s", url, e)
        return None

    def _try_ptad(self) -> Path | None:
        year = datetime.now().year
        for y in (year, year - 1):
            for pat in [
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/bexar-county-{y}.zip",
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/bexar-{y}.zip",
            ]:
                dl = self._download(pat.format(y=y), CACHE_DIR / f"ptad_bexar_{y}.zip")
                if dl:
                    dbf = self._unpack(dl)
                    if dbf:
                        return dbf
        return None

    def _download(self, url: str, dest: Path) -> Path | None:
        try:
            with requests.get(url, stream=True, timeout=60, headers=HEADERS) as r:
                if r.status_code != 200:
                    return None
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(65_536):
                        f.write(chunk)
            return dest if dest.stat().st_size > 2048 else None
        except Exception as e:
            log.debug("Download %s: %s", url, e)
            return None

    def _unpack(self, path: Path) -> Path | None:
        if path.suffix.lower() == ".dbf":
            return path
        if path.suffix.lower() == ".zip":
            try:
                with zipfile.ZipFile(path) as zf:
                    dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                    if not dbf_names:
                        return None
                    dbf_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
                    out = CACHE_DIR / Path(dbf_names[0]).name
                    with zf.open(dbf_names[0]) as src, open(out, "wb") as dst:
                        dst.write(src.read())
                    return out
            except zipfile.BadZipFile:
                return None
        return None

    def _build_index(self, path: Path):
        log.info("Indexing parcels from %s …", path.name)
        count = 0
        try:
            table  = DBF(str(path), encoding="latin-1", ignore_missing_memofile=True)
            fields = {f.name.upper() for f in table.fields}
            def col(*candidates: str) -> str | None:
                for c in candidates:
                    if c.upper() in fields:
                        return c.upper()
                return None
            c_own    = col("OWN1",    "OWNER",       "OWNER_NAME",  "OWNERNAME",  "NAME")
            c_site   = col("SITEADDR","SITE_ADDR",   "SITUS_ADDR",  "PROP_ADDR")
            c_scity  = col("SITE_CITY","SITECITY",   "SITUS_CITY",  "PROP_CITY")
            c_szip   = col("SITE_ZIP", "SITEZIP",    "SITUS_ZIP",   "PROP_ZIP")
            c_mail1  = col("MAILADR1", "ADDR_1",     "MAIL_ADDR1",  "MAIL1")
            c_mcity  = col("MAILCITY", "CITY",       "MAIL_CITY")
            c_mstate = col("STATE",    "MAIL_STATE", "MAILSTATE")
            c_mzip   = col("MAILZIP",  "ZIP",        "MAIL_ZIP")
            for row in table:
                try:
                    owner = safe(row.get(c_own)) if c_own else ""
                    if not owner:
                        continue
                    parcel = {
                        "prop_address": safe(row.get(c_site))   if c_site   else "",
                        "prop_city":    safe(row.get(c_scity))  if c_scity  else "",
                        "prop_state":   "TX",
                        "prop_zip":     safe(row.get(c_szip))   if c_szip   else "",
                        "mail_address": safe(row.get(c_mail1))  if c_mail1  else "",
                        "mail_city":    safe(row.get(c_mcity))  if c_mcity  else "",
                        "mail_state":   safe(row.get(c_mstate)) if c_mstate else "TX",
                        "mail_zip":     safe(row.get(c_mzip))   if c_mzip   else "",
                    }
                    for v in name_variants(owner):
                        self._index[v] = parcel
                    count += 1
                except Exception:
                    continue
        except Exception as e:
            log.error("DBF read error: %s", e)
        log.info("Parcel index: %d owners, %d keys.", count, len(self._index))

# ── clerk scraper ──────────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Calls the GovOS/publicsearch.us REST API.

    Real URL pattern discovered from browser network traffic:
    GET /results?department=RP
                &recordedDateRange=20240101,20240131
                &searchType=quickSearch
                &searchValue=LIS+PENDENS
                &limit=50&offset=0
                &searchOcrText=false
                &keywordSearch=false

    Response JSON shape (from observed URLs):
    { hits: [...], totalHits: N }
    """

    def __init__(self, start: datetime, end: datetime):
        self.start    = start
        self.end      = end
        self._seen:   set[str] = set()
        self.raw:     list[dict] = []
        self._session = requests.Session()
        self._session.headers.update(HEADERS)
        # date range string for the API: YYYYMMDD,YYYYMMDD
        self._date_range = (
            f"{start.strftime('%Y%m%d')},"
            f"{end.strftime('%Y%m%d')}"
        )

    def run(self) -> list[dict]:
        log.info("Starting Bexar County GovOS API scrape …")
        log.info("Date range: %s", self._date_range)

        for term in SEARCH_TERMS:
            try:
                recs = self._fetch_term(term)
                if recs:
                    log.info("  %-35s → %d records", term, len(recs))
                self.raw.extend(recs)
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                log.warning("Term '%s' error: %s", term, e)

        log.info("Scrape done: %d raw records.", len(self.raw))
        return self.raw

    def _fetch_term(self, term: str) -> list[dict]:
        all_recs: list[dict] = []
        offset = 0

        while True:
            params = {
                "department":       DEPT,
                "recordedDateRange": self._date_range,
                "searchType":       "quickSearch",
                "searchValue":      term,
                "limit":            PAGE_SIZE,
                "offset":           offset,
                "searchOcrText":    "false",
                "keywordSearch":    "false",
            }

            data = None
            for attempt in range(RETRY_MAX):
                try:
                    r = self._session.get(
                        RESULTS_URL, params=params, timeout=30
                    )
                    if r.status_code == 200:
                        data = r.json()
                        break
                    log.debug("HTTP %s for term '%s'", r.status_code, term)
                except Exception as e:
                    log.debug("Attempt %d for '%s': %s", attempt + 1, term, e)
                time.sleep(2 * (attempt + 1))

            if not data:
                break

            # GovOS response shape
            hits = (
                data.get("hits") or
                data.get("results") or
                data.get("records") or
                (data if isinstance(data, list) else [])
            )
            total = data.get("totalHits") or data.get("total") or len(hits)

            if not hits:
                break

            batch = self._parse_hits(hits, term)
            all_recs.extend(batch)

            offset += PAGE_SIZE
            if offset >= int(total) or len(hits) < PAGE_SIZE:
                break
            if offset > 5000:
                log.warning("Hit 5000-record cap for '%s'", term)
                break

            time.sleep(REQUEST_DELAY)

        return all_recs

    def _parse_hits(self, hits: list, hint_term: str) -> list[dict]:
        records: list[dict] = []
        for hit in hits:
            try:
                if not isinstance(hit, dict):
                    continue

                # GovOS field names
                doc_num = safe(
                    hit.get("instrumentNumber") or hit.get("documentNumber") or
                    hit.get("docNumber") or hit.get("id") or hit.get("recordId") or ""
                )
                if not doc_num or doc_num in self._seen:
                    continue
                self._seen.add(doc_num)

                raw_type = safe(
                    hit.get("documentType") or hit.get("docType") or
                    hit.get("instrumentType") or hit.get("type") or hint_term
                )

                # Grantor/Grantee — may be list of dicts or plain strings
                def extract_names(field) -> str:
                    val = hit.get(field, [])
                    if isinstance(val, list):
                        parts = []
                        for item in val:
                            if isinstance(item, dict):
                                parts.append(safe(item.get("name") or item.get("value") or ""))
                            else:
                                parts.append(safe(item))
                        return "; ".join(p for p in parts if p)
                    return safe(val)

                grantor = extract_names("grantors") or extract_names("grantor")
                grantee = extract_names("grantees") or extract_names("grantee")

                recorded = safe(
                    hit.get("recordedDate") or hit.get("filedDate") or
                    hit.get("instrumentDate") or hit.get("date") or ""
                )
                amount = parse_amount(
                    hit.get("consideration") or hit.get("amount") or
                    hit.get("totalAmount") or ""
                )
                legal = safe(
                    hit.get("legalDescription") or hit.get("legal") or
                    hit.get("description") or ""
                )[:300]

                doc_id = safe(hit.get("id") or hit.get("docId") or doc_num)
                clerk_url = hit.get("url") or hit.get("documentUrl") or doc_url(doc_id)

                records.append({
                    "_raw_type": raw_type,
                    "doc_code":  map_instrument(raw_type),
                    "doc_num":   doc_num,
                    "filed":     norm_date(recorded),
                    "owner":     grantor,
                    "grantee":   grantee,
                    "legal":     legal,
                    "amount":    amount,
                    "clerk_url": safe(clerk_url),
                })
            except Exception as e:
                log.debug("Hit parse error: %s", e)
        return records

# ── filter & enrich ────────────────────────────────────────────────────────────

def filter_and_enrich(raw, parcel, start, end):
    seen: set[str] = set()
    results: list[dict] = []
    for r in raw:
        try:
            code = r.get("doc_code")
            if not code or code not in TARGET_CODES:
                continue
            num = safe(r.get("doc_num"))
            if not num or num in seen:
                continue
            seen.add(num)
            filed = safe(r.get("filed"))
            if filed:
                try:
                    fd = datetime.strptime(filed, "%Y-%m-%d")
                    if not (start <= fd <= end):
                        continue
                except ValueError:
                    pass
            meta  = DOC_TYPES[code]
            owner = safe(r.get("owner"))
            pd    = parcel.lookup(owner) or {}
            rec: dict[str, Any] = {
                "doc_num":      num,
                "doc_type":     code,
                "filed":        filed,
                "cat":          meta["cat"],
                "cat_label":    meta["label"],
                "owner":        owner,
                "grantee":      safe(r.get("grantee")),
                "amount":       r.get("amount"),
                "legal":        safe(r.get("legal")),
                "prop_address": pd.get("prop_address", ""),
                "prop_city":    pd.get("prop_city", ""),
                "prop_state":   pd.get("prop_state", "TX"),
                "prop_zip":     pd.get("prop_zip", ""),
                "mail_address": pd.get("mail_address", ""),
                "mail_city":    pd.get("mail_city", ""),
                "mail_state":   pd.get("mail_state", "TX"),
                "mail_zip":     pd.get("mail_zip", ""),
                "clerk_url":    safe(r.get("clerk_url")),
                "flags":        [],
                "score":        0,
            }
            score, flags = score_record(rec)
            rec["score"] = score
            rec["flags"] = flags
            results.append(rec)
        except Exception as e:
            log.debug("Enrich error: %s", e)
    results.sort(key=lambda x: x["score"], reverse=True)
    log.info("Enriched: %d valid records from %d raw.", len(results), len(raw))
    return results

# ── output writers ─────────────────────────────────────────────────────────────

def write_json(records, start, end):
    payload = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bexar County Clerk – bexar.tx.publicsearch.us",
        "county":       "Bexar County, TX (San Antonio)",
        "date_range":   {"from": start.strftime("%Y-%m-%d"), "to": end.strftime("%Y-%m-%d")},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    body = json.dumps(payload, indent=2, default=str)
    for dest in (DASHBOARD / "records.json", DATA_DIR / "records.json"):
        dest.write_text(body, encoding="utf-8")
        log.info("Wrote %s  (%d records)", dest, len(records))

def write_ghl_csv(records):
    out = DATA_DIR / "ghl_export.csv"
    FIELDS = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    def split_name(full):
        full = full.strip()
        if not full: return "", ""
        if "," in full:
            last, first = full.split(",", 1)
            return first.strip().title(), last.strip().title()
        parts = full.split()
        return (" ".join(parts[:-1]).title(), parts[-1].title()) if len(parts) > 1 else (parts[0].title(), "")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in records:
            first, last = split_name(r.get("owner", ""))
            w.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address",""), "Mailing City": r.get("mail_city",""),
                "Mailing State": r.get("mail_state",""), "Mailing Zip": r.get("mail_zip",""),
                "Property Address": r.get("prop_address",""), "Property City": r.get("prop_city",""),
                "Property State": r.get("prop_state",""), "Property Zip": r.get("prop_zip",""),
                "Lead Type": r.get("cat_label",""), "Document Type": r.get("doc_type",""),
                "Date Filed": r.get("filed",""), "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed": "" if r.get("amount") is None else r["amount"],
                "Seller Score": r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source": "Bexar County Clerk – bexar.tx.publicsearch.us",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info("GHL CSV: %s  (%d rows)", out, len(records))

# ── main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("━"*55)
    log.info("  Bexar County TX (San Antonio) — Motivated Seller Leads")
    log.info("━"*55)
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    end   = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    log.info("Range   : %s → %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    log.info("Step 1/4 — Parcel data")
    parcel = ParcelLookup()
    parcel.load()

    log.info("Step 2/4 — Clerk API")
    raw = ClerkScraper(start, end).run()

    log.info("Step 3/4 — Filter & enrich")
    records = filter_and_enrich(raw, parcel, start, end)

    log.info("Step 4/4 — Write outputs")
    write_json(records, start, end)
    write_ghl_csv(records)

    log.info("━"*55)
    log.info("DONE — %d leads saved.", len(records))
    log.info("━"*55)

if __name__ == "__main__":
    main()
