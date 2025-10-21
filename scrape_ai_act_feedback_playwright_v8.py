# scrape_ai_act_feedback_playwright_v8.py
# Purpose: crawl all index pages for the AI Act initiative, open each detail page,
# click the JS-driven "Download" button, and save PDFs. Also writes an inventory CSV/XLSX.

import asyncio, os, csv, re, json, sys, argparse, time
from urllib.parse import urlparse, parse_qs, urljoin
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

START = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"

INDEX_CARD_SEL = 'a.ecl-link.ecl-link--standalone[href*="/F"][href$="_en"]'  # robust index link selector
NEXT_BUTTON_SEL = 'a[aria-label="Next"]'
DETAIL_DOWNLOAD_SEL = 'a.ecl-file__download, a[download].ecl-file__download, a.ecl-link--standalone[download]'
DETAIL_TITLE_SEL = 'h1, h2'
DETAIL_META_WRAP = 'div.ecl-u-type-paragraph, div[role="main"]'

def ensure_dirs(outdir):
    Path(outdir).mkdir(parents=True, exist_ok=True)
    Path(outdir, "pdfs").mkdir(parents=True, exist_ok=True)
    Path(outdir, "text").mkdir(parents=True, exist_ok=True)
    Path(outdir, "metadata").mkdir(parents=True, exist_ok=True)

def sanitize(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9._-]+', '_', s).strip('_')[:200]

def detail_id_from_url(u: str) -> str:
    m = re.search(r'/(F\d+)_en$', u)
    return m.group(1) if m else sanitize(urlparse(u).path.rsplit('/', 1)[-1])

async def accept_cookies(page):
    # EU site cookie banner (varies); make best-effort, ignore failures
    for sel in [
        'button:has-text("Accept")',
        'button:has-text("I accept")',
        'button:has-text("Agree")',
        'button#accept-all-cookies',
        'button[aria-label="Accept all"]',
    ]:
        try:
            await page.locator(sel).first.click(timeout=1500)
            break
        except PWTimeoutError:
            pass

async def get_detail_links_on_index(page) -> list[str]:
    await page.wait_for_load_state("domcontentloaded")
    await accept_cookies(page)
    # The list loads server-side; short wait so everything paints
    await page.wait_for_timeout(400)
    links = await page.eval_on_selector_all(
        INDEX_CARD_SEL,
        "els => els.map(e => e.href)"
    )
    # As fallback, capture any _en detail anchors
    if not links:
        links = await page.eval_on_selector_all(
            'a[href*="/F"][href$="_en"]',
            "els => els.map(e => e.href)"
        )
    return sorted(set(links))

async def goto_index_page(page, page_no: int, base_url: str):
    # The site uses a page parameter ‘page=’ on the feedback listing
    # We keep p_id same and add &page=N
    parsed = urlparse(base_url)
    q = parse_qs(parsed.query)
    q['page'] = [str(page_no - 1)]  # observed pages are 0-based server-side; UI shows 1-based
    query = "&".join([f"{k}={v[0]}" for k, v in q.items()])
    url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}"
    await page.goto(url, wait_until="domcontentloaded")

async def try_click_download(page, download_dir: Path, rec_id: str) -> str | None:
    # Returns saved file path or None
    # The anchor has no href; must click with expect_download
    btn = page.locator(DETAIL_DOWNLOAD_SEL).first
    try:
        await btn.wait_for(timeout=2500)
    except PWTimeoutError:
        return None

    # Wrap in expect_download
    try:
        async with page.expect_download(timeout=15000) as dl_info:
            await btn.click()
        download = await dl_info.value
        suggested = download.suggested_filename or f"{rec_id}.pdf"
        # enforce .pdf extension if missing
        if not suggested.lower().endswith(".pdf"):
            suggested = f"{sanitize(suggested)}.pdf"
        dest = download_dir / sanitize(f"{rec_id}__{suggested}")
        await download.save_as(dest.as_posix())
        return dest.as_posix()
    except PWTimeoutError:
        return None

async def extract_fallback_text(page) -> str:
    # If no PDF, we still try to capture the visible contribution text.
    try:
        return await page.locator('[role="main"]').inner_text(timeout=2000)
    except PWTimeoutError:
        try:
            return await page.locator('main').inner_text(timeout=2000)
        except PWTimeoutError:
            return await page.inner_text('body')

async def parse_detail_meta(page) -> dict:
    meta = {}
    try:
        title = await page.locator(DETAIL_TITLE_SEL).first.text_content(timeout=1500)
        meta["title"] = title.strip() if title else ""
    except PWTimeoutError:
        meta["title"] = ""
    # Heuristics for name/date fields that appear near the top on detail pages.
    try:
        blob = await page.locator(DETAIL_META_WRAP).first.inner_text(timeout=2000)
    except PWTimeoutError:
        blob = ""
    # Naive extractions
    m_name = re.search(r'(?i)(organisation|name)\s*:\s*(.+)', blob)
    if m_name:
        meta["submitter"] = m_name.group(2).strip()
    m_date = re.search(r'(?i)(submitted|publication|date)\s*:\s*([0-9]{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})', blob)
    if m_date:
        meta["date"] = m_date.group(2).strip()
    return meta

async def process_detail(page, url, outdir, seen_file_ids: set) -> dict:
    rec = {"detail_url": url, "file_path": "", "text_path": "", "id": "", "title": "", "submitter": "", "date": ""}
    await page.goto(url, wait_until="domcontentloaded")
    await accept_cookies(page)
    await page.wait_for_timeout(500)
    rec_id = detail_id_from_url(url)
    rec["id"] = rec_id

    # skip if already downloaded in this session
    if rec_id in seen_file_ids:
        return rec

    # Try download
    pdf_dir = Path(outdir, "pdfs")
    saved = await try_click_download(page, pdf_dir, rec_id)
    if saved:
        rec["file_path"] = saved
    else:
        # Fallback: grab visible text
        txt = await extract_fallback_text(page)
        if txt.strip():
            text_path = Path(outdir, "text", f"{rec_id}.txt")
            text_path.write_text(txt, encoding="utf-8", errors="ignore")
            rec["text_path"] = text_path.as_posix()

    # Meta
    meta = await parse_detail_meta(page)
    rec.update(meta)
    return rec

async def run(start_url, outdir, max_pages, show):
    ensure_dirs(outdir)
    inv_csv = Path(outdir, "metadata", "inventory_of_304_letters_AI_Act.csv")
    inv_xlsx = Path(outdir, "metadata", "inventory_of_304_letters_AI_Act.xlsx")

    records = []
    seen_detail_ids = set()
    seen_index_fingerprints = set()

    print(f"[i] Start: {start_url}")

    async with async_playwright() as p:
        browser = await (p.chromium.launch(headless=not show, args=["--disable-dev-shm-usage"]))
        context = await browser.new_context(accept_downloads=True)
        # optional: set a default downloads path so manual saves also land here
        context.set_default_timeout(15000)
        page = await context.new_page()

        for page_no in range(1, max_pages + 1):
            print(f"[i] INDEX page {page_no}: finding detail links…")
            await goto_index_page(page, page_no, start_url)
            # fingerprint the visible detail ids on this page to detect loops
            detail_links = await get_detail_links_on_index(page)
            ids_here = tuple(sorted(detail_id_from_url(u) for u in detail_links))
            if not detail_links:
                print(f"[i] Found 0 detail links on index page {page_no}")
            else:
                # Loop-detection: if we keep seeing the same set, we’re done
                if ids_here in seen_index_fingerprints:
                    print("[i] Repeating index page detected (same detail IDs). Stopping to avoid loop.")
                    break
                seen_index_fingerprints.add(ids_here)

                new_links = [u for u in detail_links if detail_id_from_url(u) not in seen_detail_ids]
                if new_links:
                    print(f"[i] Found {len(new_links)} new detail links on index page {page_no}")
                    for u in new_links:
                        print(f"   ↪ detail: {u}")
                else:
                    print(f"[i] Found 0 new detail links on index page {page_no}")

                # Visit each detail and attempt download
                for u in new_links:
                    try:
                        rec = await process_detail(page, u, outdir, seen_detail_ids)
                        seen_detail_ids.add(rec["id"])
                        records.append(rec)
                        # brief pause to be polite
                        await page.wait_for_timeout(250)
                    except Exception as e:
                        print(f"[!] Error processing {u}: {e}")

            # Be a good citizen; small pause between index pages
            await page.wait_for_timeout(400)

        await context.close()
        await browser.close()

    # Write inventory
    df = pd.DataFrame.from_records(records)
    df = df[["id", "title", "submitter", "date", "detail_url", "file_path", "text_path"]]
    df.to_csv(inv_csv, index=False, encoding="utf-8")
    with pd.ExcelWriter(inv_xlsx, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="inventory")

    print(f"[✓] Done. Files saved: {sum(bool(r['file_path']) for r in records)} | Records: {len(records)}")
    print(f"[i] Inventory CSV: {inv_csv}")
    print(f"[i] Inventory XLSX: {inv_xlsx}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-url", default=START, help="Start URL of the AI Act feedback listing")
    ap.add_argument("--outdir", default="data", help="Output base directory")
    ap.add_argument("--max-pages", type=int, default=60, help="Max index pages to walk")
    ap.add_argument("--show", action="store_true", help="Show browser (headed)")
    args = ap.parse_args()
    asyncio.run(run(args.start_url, args.outdir, args.max_pages, args.show))

if __name__ == "__main__":
    main()
