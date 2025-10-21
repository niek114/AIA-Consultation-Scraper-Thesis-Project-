import asyncio, os, re, time, argparse
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
OUTDIR = "data/raw_pdfs"
INV_BASENAME = "data/metadata/inventory_of_304_letters_AI_Act"

# file extensions we’ll collect
FILE_EXT = re.compile(r"\.(pdf|doc|docx|zip)($|\?)", re.I)
# generic hints for dynamic endpoints without clear extension
HINT = re.compile(r"(attachment|download|enclosure|document|file)", re.I)

def ensure_dirs():
    Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    Path("data/metadata").mkdir(parents=True, exist_ok=True)
    Path("debug").mkdir(parents=True, exist_ok=True)

async def accept_cookies(page):
    # tolerant cookie acceptance
    labels = [
        "Accept all", "Accept", "I agree", "Akkoord", "Alle accepteren",
        "Tout accepter", "Alle akzeptieren", "Aceptar todo", "Allow all"
    ]
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if await btn.count():
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
        except PWTimeout:
            pass
        except Exception:
            pass

async def get_detail_links_on_index(page) -> list[str]:
    # Wait for cards to render
    await page.wait_for_timeout(1200)

    # Primary selector: real detail anchors live in content items and end like /F######_en
    links = []
    cards = page.locator("article.ecl-content-item a[href*='/F'][href$='_en']")
    n = await cards.count()
    for i in range(n):
        try:
            full = await cards.nth(i).evaluate("el => el.href")
            if full and full not in links:
                links.append(full)
        except Exception:
            pass

    # Fallback: broader scan of anchors in content items and filter in Python
    if not links:
        all_a = page.locator("article.ecl-content-item a[href]")
        n2 = await all_a.count()
        for i in range(n2):
            try:
                full = await all_a.nth(i).evaluate("el => el.href")
                if full and "/F" in full and full.endswith("_en") and full not in links:
                    links.append(full)
            except Exception:
                pass

    # Save a debug snapshot if still nothing (to inspect structure)
    if not links:
        try:
            html = await page.content()
            ts = int(time.time())
            with open(f"debug/index_{ts}.html", "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

    return links

async def collect_files_in_detail(page, detail_url, page_idx, ctx, records):
    """
    Navigate in SAME TAB to detail_url, discover attachments, download, then go back to index.
    """
    try:
        await page.goto(detail_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)
        # expand obvious accordions/sections that may hide attachments
        for label in ["Attachments","Attachment","Downloads","Download","Files","Show more","Expand","Document(s)"]:
            try:
                btn = page.get_by_role("button", name=re.compile(label, re.I))
                if await btn.count():
                    await btn.first.click(timeout=1200); await page.wait_for_timeout(400)
            except Exception:
                pass

        # trigger lazy loads
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(600)

        file_urls = set()

        # 1) direct CSS hits: explicit extensions or common endpoints
        css_hits = page.locator(
            "a[href$='.pdf'], a[href$='.doc'], a[href$='.docx'], a[href$='.zip'], "
            "a[href*='attachment'], a[href*='download'], a[href*='/document/'], a[download]"
        )
        for i in range(await css_hits.count()):
            try:
                full = await css_hits.nth(i).evaluate("el => el.href")
                if full and (FILE_EXT.search(full) or HINT.search(full)):
                    file_urls.add(full)
            except Exception:
                pass

        # 2) role-based fallback: look for link names indicating files
        for label in ["Attachment","Download","PDF","File","Document","Annex"]:
            hits = page.get_by_role("link", name=re.compile(label, re.I))
            for i in range(await hits.count()):
                try:
                    full = await hits.nth(i).evaluate("el => el.href")
                    if full and (FILE_EXT.search(full) or HINT.search(full)):
                        file_urls.add(full)
                except Exception:
                    pass

        # 3) final fallback: scan all anchors in detail page
        if not file_urls:
            all_a = page.locator("a")
            for i in range(await all_a.count()):
                try:
                    full = await all_a.nth(i).evaluate("el => el.href")
                    if full and (FILE_EXT.search(full) or HINT.search(full)):
                        file_urls.add(full)
                except Exception:
                    pass

        # Download through the same browser context to keep cookies/session
        if not file_urls:
            records.append({
                "Source_Page": f"index_page={page_idx}",
                "Found_On": "detail",
                "Detail_Page": detail_url,
                "File_URL": "",
                "Local_File": "",
                "Download_Status": "no_file_found",
            })
        else:
            for url in sorted(file_urls):
                name = (url.split("/")[-1] or "file").split("?")[0]
                if not FILE_EXT.search(name):
                    # default .pdf if endpoint doesn’t expose extension
                    name += ".pdf"
                safe = re.sub(r"[^a-zA-Z0-9._-]+","_", name)
                dest = os.path.join(OUTDIR, safe)
                status = "download_error"
                try:
                    res = await ctx.request.get(url)
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
                    "Source_Page": f"index_page={page_idx}",
                    "Found_On": "detail",
                    "Detail_Page": detail_url,
                    "File_URL": url,
                    "Local_File": dest,
                    "Download_Status": status,
                })

    finally:
        # return to the index page we came from
        try:
            await page.go_back(wait_until="domcontentloaded")
            await page.wait_for_timeout(800)
        except Exception:
            pass

async def run(show=False, max_pages=500):
    ensure_dirs()
    records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not show)
        ctx = await browser.new_context(user_agent="NiekThesisScraper/4.0 (Playwright)")
        page = await ctx.new_page()
        print(f"[i] Start: {START_URL}")
        await page.goto(START_URL, wait_until="domcontentloaded")
        await accept_cookies(page)
        await page.wait_for_timeout(800)

        page_idx = 1
        while True:
            print(f"[i] INDEX page {page_idx}: finding detail links…")
            detail_links = await get_detail_links_on_index(page)
            print(f"[i] Found {len(detail_links)} detail links on index page {page_idx}")
            if len(detail_links) == 0:
                # Save debug to inspect selector mismatch
                try:
                    html = await page.content()
                    with open(f"debug/index_no_links_p{page_idx}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception:
                    pass

            for durl in detail_links:
                print(f"   ↪ detail: {durl}")
                await collect_files_in_detail(page, durl, page_idx, ctx, records)

            # Try to paginate to next index page
            next_btn = page.locator("nav.ecl-pagination a[rel='next'], a[aria-label='Next'], a:has-text('Next'), a:has-text('›')")
            if await next_btn.count() == 0:
                print("[i] No further index pages. Stopping.")
                break
            try:
                await next_btn.first.click()
                page_idx += 1
                await page.wait_for_timeout(1200)
                await accept_cookies(page)
            except Exception as e:
                print(f"[!] Pagination failed: {e}")
                break

            if page_idx > max_pages:
                print("[i] Hit max_pages limit — stopping.")
                break

        await browser.close()

    # write inventory
    df = pd.DataFrame.from_records(records, columns=[
        "Source_Page","Found_On","Detail_Page","File_URL","Local_File","Download_Status"
    ])
    csv_path = f"{INV_BASENAME}.csv"
    xlsx_path = f"{INV_BASENAME}.xlsx"
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    try:
        with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="inventory")
    except Exception:
        pass
    saved = (df["Local_File"].astype(bool).sum() if not df.empty else 0)
    print(f"[✓] Done. Files saved: {saved} | Records: {len(df)}")
    print(f"[i] Inventory CSV: {csv_path}")
    print(f"[i] Inventory XLSX: {xlsx_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true", help="show the browser")
    ap.add_argument("--max-pages", type=int, default=500)
    args = ap.parse_args()
    asyncio.run(run(show=args.show, max_pages=args.max_pages))
