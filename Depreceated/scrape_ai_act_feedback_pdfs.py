#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape all feedback PDFs from the EU 'Have Your Say' page you provided,
walking all pagination pages and logging results into CSV/XLSX.

USAGE:
  python scrape_ai_act_feedback_pdfs.py \
    --start-url "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488" \
    --outdir data/raw_pdfs \
    --inventory inventory_of_304_letters_AI_Act \
    --extract-text   # optional: also extract .txt and word counts

Notes:
- Respects polite delays and retries.
- Creates two inventory files: .csv (semicolon-separated) and .xlsx
- Tries both direct PDF links and PDF links on detail pages.
"""

import argparse
import csv
import os
import re
import sys
import time
import math
import random
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# Optional: text extraction for word count
EXTRACT_LIB_OK = True
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
except Exception:
    EXTRACT_LIB_OK = False


HEADERS = {
    "User-Agent": "NiekThesisScraper/1.0 (+for academic use; contact: niek114@gmail.com)"
}
TIMEOUT = 30
RETRY = 3
BASE_DELAY = 1.2  # polite delay between requests (seconds)

PDF_EXT_RE = re.compile(r"\.pdf($|\?)", re.IGNORECASE)
# Sometimes links are like ".../document/<id>/download"
PDF_HINT_RE = re.compile(r"(document|download|/files/)", re.IGNORECASE)

def sleep_jitter(mult=1.0):
    time.sleep(BASE_DELAY * mult + random.uniform(0.2, 0.6))

def get(url, session, stream=False):
    for attempt in range(1, RETRY + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=TIMEOUT, stream=stream)
            if resp.status_code in (200, 201):
                return resp
            # Handle soft-bans/temporary issues with backoff
            if resp.status_code in (429, 502, 503, 504):
                sleep_jitter(mult=attempt)
                continue
            # Other non-OK -> give up
            return resp
        except requests.RequestException:
            sleep_jitter(mult=attempt)
    # Final failure
    return None

def normalize_url(href, base):
    if not href:
        return None
    return urljoin(base, href)

def is_pdf_href(href):
    if not href:
        return False
    if PDF_EXT_RE.search(href):
        return True
    if PDF_HINT_RE.search(href) and "pdf" in href.lower():
        return True
    return False

def find_pagination_next(soup, current_url):
    """
    EU 'Have Your Say' uses various patterns.
    Strategy: look for 'a' with rel/aria-label 'next' or text containing 'Next' or '>' that changes page= or ?page=
    """
    # Try rel=next
    a = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if a and a.get("href"):
        return normalize_url(a["href"], current_url)

    # Try common aria-label or text
    for link in soup.select("a"):
        text = (link.get_text() or "").strip().lower()
        if any(t in text for t in ("next", "volgende", ">", "›")) and link.get("href"):
            nxt = normalize_url(link["href"], current_url)
            # Avoid self loops
            if nxt and nxt != current_url:
                return nxt
    # Fallback: detect a page=N param and try to increment
    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query)
    for key in ("page", "pageno", "p"):
        if key in qs:
            try:
                n = int(qs[key][0])
                new_qs = parsed.query.replace(f"{key}={n}", f"{key}={n+1}")
                return parsed._replace(query=new_qs).geturl()
            except Exception:
                pass
    return None

def extract_feedback_items(soup, base_url):
    """
    Find feedback cards/rows. Each typically contains:
      - title / entity name
      - link to detail page
      - possibly direct file link
    We collect any <a> that looks like a PDF, and also store detail page hrefs.
    """
    items = []

    # Generic: collect all anchors in the main content area
    main = soup
    # Heuristic: likely in 'main' or specific containers; but scanning all anchors is okay with filters
    for a in main.find_all("a", href=True):
        href = normalize_url(a["href"], base_url)
        text = (a.get_text() or "").strip()
        if not href:
            continue
        items.append({"href": href, "text": text})

    return items

def download_pdf(url, outdir, session):
    os.makedirs(outdir, exist_ok=True)
    # Determine filename
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    if not name.lower().endswith(".pdf"):
        # Fallback name
        name = re.sub(r"[^a-zA-Z0-9._-]+", "_", parsed.path) + ".pdf"
    dest = os.path.join(outdir, name)

    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return dest, "exists"

    resp = get(url, session, stream=True)
    if not resp or resp.status_code != 200:
        return None, f"http_{resp.status_code if resp else 'ERR'}"

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    sleep_jitter()
    return dest, "downloaded"

def extract_pdf_text(pdf_path):
    try:
        t = pdf_extract_text(pdf_path) if EXTRACT_LIB_OK else ""
        t = t or ""
        # Basic normalization
        t = t.replace("\r", " ").replace("\t", " ")
        words = len(t.split())
        txt_path = pdf_path[:-4] + ".txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(t)
        return txt_path, words
    except Exception:
        return "", 0

def scrape_all(start_url, outdir, inventory_basename, extract_text=False, max_pages=500):
    session = requests.Session()
    visited_pages = set()
    page_url = start_url

    records = []
    seen_pdf_urls = set()
    seen_detail_urls = set()

    for page_idx in range(max_pages):
        if not page_url or page_url in visited_pages:
            break
        visited_pages.add(page_url)

        resp = get(page_url, session)
        if not resp or resp.status_code != 200:
            print(f"[!] Page fetch failed: {page_url}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        anchors = extract_feedback_items(soup, page_url)

        # Separate direct PDFs and possible detail pages
        direct_pdfs = []
        detail_pages = []

        for a in anchors:
            href = a["href"]
            if is_pdf_href(href):
                direct_pdfs.append(href)
            else:
                # If link looks like a feedback detail page (often contains '/feedback/')
                if "/feedback/" in href or "feedback" in href.lower():
                    detail_pages.append(href)

        # Download direct PDFs
        for pdf_url in set(direct_pdfs):
            if pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(pdf_url)

            local_path, status = download_pdf(pdf_url, outdir, session)
            wc = 0
            txt_path = ""
            if extract_text and local_path and status in ("downloaded", "exists"):
                txt_path, wc = extract_pdf_text(local_path)

            records.append({
                "Source_Page": page_url,
                "Found_On": "listing",
                "Detail_Page": "",
                "PDF_URL": pdf_url,
                "Local_PDF": local_path or "",
                "Download_Status": status,
                "Text_Path": txt_path,
                "Word_Count": wc
            })

        # Visit detail pages to find PDFs embedded there
        for durl in set(detail_pages):
            if durl in seen_detail_urls:
                continue
            seen_detail_urls.add(durl)

            sleep_jitter()
            dr = get(durl, session)
            if not dr or dr.status_code != 200:
                records.append({
                    "Source_Page": page_url,
                    "Found_On": "detail",
                    "Detail_Page": durl,
                    "PDF_URL": "",
                    "Local_PDF": "",
                    "Download_Status": f"detail_http_{dr.status_code if dr else 'ERR'}",
                    "Text_Path": "",
                    "Word_Count": 0
                })
                continue

            dsoup = BeautifulSoup(dr.text, "html.parser")
            # Find anchors that look like PDFs
            dpdfs = []
            for a in dsoup.find_all("a", href=True):
                h = normalize_url(a["href"], durl)
                if is_pdf_href(h):
                    dpdfs.append(h)

            if not dpdfs:
                # Sometimes PDFs are linked via buttons or JS—still record the detail page
                records.append({
                    "Source_Page": page_url,
                    "Found_On": "detail",
                    "Detail_Page": durl,
                    "PDF_URL": "",
                    "Local_PDF": "",
                    "Download_Status": "no_pdf_found",
                    "Text_Path": "",
                    "Word_Count": 0
                })
            else:
                for pdf_url in set(dpdfs):
                    if pdf_url in seen_pdf_urls:
                        continue
                    seen_pdf_urls.add(pdf_url)

                    local_path, status = download_pdf(pdf_url, outdir, session)
                    wc = 0
                    txt_path = ""
                    if extract_text and local_path and status in ("downloaded", "exists"):
                        txt_path, wc = extract_pdf_text(local_path)

                    records.append({
                        "Source_Page": page_url,
                        "Found_On": "detail",
                        "Detail_Page": durl,
                        "PDF_URL": pdf_url,
                        "Local_PDF": local_path or "",
                        "Download_Status": status,
                        "Text_Path": txt_path,
                        "Word_Count": wc
                    })

        # Find next page
        next_url = find_pagination_next(soup, page_url)
        if not next_url or next_url == page_url:
            break
        page_url = next_url
        sleep_jitter()

    # Write inventory CSV (semicolon-separated)
    csv_path = f"{inventory_basename}.csv"
    xlsx_path = f"{inventory_basename}.xlsx"
    fieldnames = [
        "Source_Page", "Found_On", "Detail_Page",
        "PDF_URL", "Local_PDF", "Download_Status",
        "Text_Path", "Word_Count"
    ]
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    # Also write XLSX
    try:
        import pandas as pd
        df = pd.DataFrame.from_records(records, columns=fieldnames)
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as xlw:
            df.to_excel(xlw, index=False, sheet_name="inventory")
    except Exception as e:
        print(f"[i] Could not write XLSX ({e}). CSV is available.")

    print(f"[✓] Done. PDFs: {sum(1 for r in records if r['Local_PDF'])} | Records: {len(records)}")
    print(f"[i] Inventory CSV: {csv_path}")
    print(f"[i] Inventory XLSX: {xlsx_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", required=True, help="Starting listing URL (your feedback_en?p_id=... link)")
    ap.add_argument("--outdir", default="data/raw_pdfs", help="Directory to save PDF files")
    ap.add_argument("--inventory", default="inventory_of_304_letters_AI_Act", help="Base name for inventory files")
    ap.add_argument("--extract-text", action="store_true", help="Also extract .txt and word counts (needs pdfminer.six)")
    ap.add_argument("--max-pages", type=int, default=500, help="Hard cap on pagination pages")
    args = ap.parse_args()

    # Friendly reminder to check robots
    print("[i] Please verify robots.txt and Terms of Use before scraping:", "https://ec.europa.eu/robots.txt")
    scrape_all(
        start_url=args.start_url,
        outdir=args.outdir,
        inventory_basename=args.inventory,
        extract_text=args.extract_text,
        max_pages=args.max_pages
    )

if __name__ == "__main__":
    main()
