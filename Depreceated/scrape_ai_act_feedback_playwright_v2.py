import asyncio, os, re, sys, time, argparse
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
OUTDIR = "data/raw_pdfs"
INV_BASENAME = "data/metadata/inventory_of_304_letters_AI_Act"

PDF_RE = re.compile(r"\.pdf($|\?)", re.I)

COOKIE_BUTTON_TEXTS = [
    "Accept all", "Accept", "I agree", "Akkoord", "Alle accepteren",
    "Tout accepter", "Alle akzeptieren", "Aceptar todo", "Allow all"
]

def ensure_dirs():
    Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    Path("data/metadata").mkdir(parents=True, exist_ok=True)
    Path("debug").mkdir(parents=True, exist_ok=True)

async def click_cookie_banners(page):
    # Try common iframes and buttons
    for _ in range(2):
        try:
            for txt in COOKIE_BUTTON_TEXTS:
                btn = page.get_by_role("button", name=re.compile(txt, re.I))
                if await btn.count():
                    try:
                        await btn.first.click(timeout=1500)
                        await page.wait_for_timeout(400)
                    except PWTimeout:
                        pass
            # also try links
            for txt in COOKIE_BUTTON_TEXTS:
                lnk = page.get_by_role("link", name=re.compile(txt, re.I))
                if await lnk.count():
                    try:
                        await lnk.first.click(timeout=1500)
                        await page.wait_for_timeout(400)
                    except PWTimeout:
                        pass
        except Exception:
            pass

async def collect_detail_links(page):
    # Give the page time to render results
    await page.wait_for_timeout(1200)
    # Save debug HTML
    idx = page.url
    try:
        html = await page.content()
        with open(f"debug/page_{int(time.time())}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

    # Grab anchors which point to feedback detail pages
    links = []
    anchors = page.locator("a")
    count = await anchors.count()
    for i in range(count):
        try:
            a = anchors.nth(i)
            href = await a.get_attribute("href")
            if not href:
                continue
            if "feedback" in href.lower():
                full = await a.evaluate("el => el.href")
                if full and full not in links:
                    links.append(full)
        except Exception:
            continue
    return links

async def find_pdfs_in_detail(ctx, detail_url, page_idx):
    records = []
    dp = await ctx.new_page()
    try:
        await dp.goto(detail_url, wait_until="domcontentloaded")
        await dp.wait_for_timeout(800)

        anchors = dp.locator("a")
        cnt = await anchors.count()
        pdfs = []
        for i in range(cnt):
            try:
                a = anchors.nth(i)
                href = await a.get_attribute("href") or ""
                text = (await a.text_content()) or ""
                if not href:
                    continue
                if PDF_RE.search(href) or "attachment" in href.lower() or "download" in href.lower():
                    full = await a.evaluate("el => el.href")
                    if full:
                        pdfs.append((full, text.strip()))
            except Exception:
                pass

        if not pdfs:
            records.append({
                "Source_Page": f"page={page_idx}",
                "Found_On": "detail",
                "Detail_Page": detail_url,
                "PDF_URL": "",
                "Local_PDF": "",
                "Download_Status": "no_pdf_found",
                "Text_Path": "",
                "Word_Count": 0
            })
        else:
            seen = set()
            for pdf_url, label in pdfs:
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                # Download through Playwright context to keep cookies/session
                fname = re.sub(r"[^a-zA-Z0-9._-]+","_", pdf_url.split("/")[-1] or "file.pdf")
                if not fname.lower().endswith(".pdf"):
                    fname += ".pdf"
                dest = os.path.join(OUTDIR, fname)
                status = "download_error"
                try:
                    res = await ctx.request.get(pdf_url)
                    if res.ok:
                        with open(dest, "wb") as f:
                            f.write(await res.body())
                        status = "downloaded"
                    else:
                        status = f"http_{res.status}"
                        dest = ""
                except Exception:
                    dest = ""
                records.append({
                    "Source_Page": f"page={page_idx}",
                    "Found_On": "detail",
                    "Detail_Page": detail_url,
                    "PDF_URL": pdf_url,
                    "Local_PDF": dest,
                    "Download_Status": status,
                    "Text_Path": "",
                    "Word_Count": 0
                })
    finally:
        await dp.close()
    return records

async def run(show=False, max_pages=500):
    ensure_dirs()
    records = []
    page_idx = 1
    seen_detail = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not show)
        ctx = await browser.new_context(user_agent="NiekThesisScraper/2.1 (Playwright)")
        page = await ctx.new_page()
        print(f"[i] Navigating to start URL… {START_URL}")
        await page.goto(START_URL, wait_until="domcontentloaded")
        await click_cookie_banners(page)
        await page.wait_for_timeout(800)

        while True:
            print(f"[i] On page {page_idx} — collecting detail links…")
            details = await collect_detail_links(page)
            details = [d for d in details if d not in seen_detail]
            print(f"[i] Found {len(details)} detail links on page {page_idx}")

            # Visit detail pages and download PDFs
            found_pdfs = 0
            for d in details:
                seen_detail.add(d)
                recs = await find_pdfs_in_detail(ctx, d, page_idx)
                records.extend(recs)
                for r in recs:
                    if r["Local_PDF"]:
                        found_pdfs += 1

            print(f"[i] Page {page_idx}: saved {found_pdfs} PDFs")

            # Try to paginate
            next_btn = page.locator("a[rel='next'], a[aria-label='Next'], a:has-text('Next'), a:has-text('›')")
            try:
                if await next_btn.count() == 0:
                    print("[i] No next button found — stopping.")
                    break
                await next_btn.first.click()
                page_idx += 1
                await page.wait_for_timeout(1200)
                await click_cookie_banners(page)
            except Exception as e:
                print(f"[!] Pagination failed: {e}")
                break

            if page_idx > max_pages:
                print("[i] Hit max_pages limit — stopping.")
                break

        await browser.close()

    # Write inventory
    cols = ["Source_Page","Found_On","Detail_Page","PDF_URL","Local_PDF","Download_Status","Text_Path","Word_Count"]
    df = pd.DataFrame.from_records(records, columns=cols)
    Path(os.path.dirname(INV_BASENAME)).mkdir(parents=True, exist_ok=True)
    csv_path = f"{INV_BASENAME}.csv"
    xlsx_path = f"{INV_BASENAME}.xlsx"
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    try:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="inventory")
    except Exception:
        pass
    print(f"[✓] Done. PDFs saved: {df['Local_PDF'].astype(bool).sum()} | Records: {len(df)}")
    print(f"[i] Inventory CSV: {csv_path}")
    print(f"[i] Inventory XLSX: {xlsx_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true", help="Show the browser (not headless)")
    ap.add_argument("--max-pages", type=int, default=500)
    args = ap.parse_args()
    asyncio.run(run(show=args.show, max_pages=args.max_pages))
