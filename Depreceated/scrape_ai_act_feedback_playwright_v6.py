import asyncio, os, re, time, argparse
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
OUTDIR = "data/raw_pdfs"
INV_BASENAME = "data/metadata/inventory_of_304_letters_AI_Act"

FILE_EXT = re.compile(r"\.(pdf|doc|docx|zip)($|\?)", re.I)
HINT = re.compile(r"(attachment|download|enclosure|document|file|resource)", re.I)

def ensure_dirs():
    Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    Path("data/metadata").mkdir(parents=True, exist_ok=True)
    Path("debug/detail").mkdir(parents=True, exist_ok=True)
    Path("debug/index").mkdir(parents=True, exist_ok=True)

async def accept_cookies(page):
    labels = ["Accept all","Accept","I agree","Akkoord","Alle accepteren","Tout accepter","Alle akzeptieren","Aceptar todo","Allow all"]
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if await btn.count():
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(300)
        except Exception:
            pass

async def get_detail_links_on_index(page) -> list[str]:
    await page.wait_for_timeout(900)
    links = []
    # Real pattern: anchors inside the content item ending with /F########_en
    cards = page.locator("article.ecl-content-item a[href*='/F'][href$='_en']")
    n = await cards.count()
    for i in range(n):
        try:
            full = await cards.nth(i).evaluate("el => el.href")
            if full and full not in links:
                links.append(full)
        except Exception:
            pass
    if not links:
        # Fallback, then filter in Python
        all_a = page.locator("article.ecl-content-item a[href]")
        n2 = await all_a.count()
        for i in range(n2):
            try:
                full = await all_a.nth(i).evaluate("el => el.href")
                if full and "/F" in full and full.endswith("_en") and full not in links:
                    links.append(full)
            except Exception:
                pass
    if not links:
        # Save index debug
        try:
            html = await page.content()
            ts = int(time.time())
            with open(f"debug/index/no_links_{ts}.html", "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass
    return links

async def expand_possible_sections(page):
    for label in ["Attachments","Attachment","Downloads","Download","Files","Show more","Expand","Document","Documents","Annex","Feedback","Submission","Response"]:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if await btn.count():
                await btn.first.click(timeout=1200); await page.wait_for_timeout(300)
        except Exception:
            pass
        try:
            lnk = page.get_by_role("link", name=re.compile(label, re.I))
            if await lnk.count():
                await lnk.first.click(timeout=1200); await page.wait_for_timeout(300)
        except Exception:
            pass

async def click_and_capture_downloads(page, page_idx, detail_url, records):
    """
    Capture files initiated by clicking the portal's download controls.
    This handles anchors without href (eclfiledownload).
    """
    saved_any = False
    # Preferred selector from your HTML: a.ecl-file__download[eclfiledownload]
    download_controls = page.locator("a.ecl-file__download[eclfiledownload]")
    count = await download_controls.count()

    # Also allow anything that looks like a file download control
    if count == 0:
        download_controls = page.locator("a[download], a:has-text('Download'), a[href*='download'], a[href*='attachment']")
        count = await download_controls.count()

    for i in range(count):
        ctrl = download_controls.nth(i)
        try:
            # Use Playwright's download capture
            async with page.expect_download(timeout=5000) as dl_info:
                await ctrl.click()
            dl = await dl_info.value
            suggested = dl.suggested_filename or "file.bin"
            # Give a default .pdf if extension is missing
            if not FILE_EXT.search(suggested):
                if not re.search(r"\.[A-Za-z0-9]{2,5}$", suggested):
                    suggested += ".pdf"
            safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", suggested)
            dest = os.path.join(OUTDIR, safe)
            await dl.save_as(dest)
            saved_any = True
            records.append({
                "Source_Page": f"index_page={page_idx}",
                "Found_On": "detail_click",
                "Detail_Page": detail_url,
                "File_URL": dl.url or "",
                "Local_File": dest,
                "Download_Status": "downloaded",
            })
        except PWTimeout:
            # No download fired (maybe a pure text-only item)
            continue
        except Exception:
            # Try next control
            continue
    return saved_any

async def collect_files_in_detail(page, detail_url, page_idx, ctx, records):
    """
    Navigate in SAME TAB to detail_url, click download controls to capture files, then go back to index.
    """
    try:
        await page.goto(detail_url, wait_until="domcontentloaded")
        await accept_cookies(page)
        await page.wait_for_timeout(700)
        await expand_possible_sections(page)
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)

        # 1) Try the real download widgets (no href): click + expect_download
        got = await click_and_capture_downloads(page, page_idx, detail_url, records)

        # 2) If still nothing, fallback to static links (href present)
        if not got:
            file_urls = set()
            css_hits = page.locator(
                "a[href$='.pdf'], a[href$='.doc'], a[href$='.docx'], a[href$='.zip'], "
                "a[href*='attachment'], a[href*='download'], a[href*='/document/']"
            )
            for i in range(await css_hits.count()):
                try:
                    full = await css_hits.nth(i).evaluate("el => el.href")
                    if full and (FILE_EXT.search(full) or HINT.search(full)):
                        file_urls.add(full)
                except Exception:
                    pass
            if file_urls:
                for url in sorted(file_urls):
                    name = (url.split("/")[-1] or "file").split("?")[0]
                    if not FILE_EXT.search(name):
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
                        "Found_On": "detail_href",
                        "Detail_Page": detail_url,
                        "File_URL": url,
                        "Local_File": dest,
                        "Download_Status": status,
                    })
            else:
                # Save debug if truly none
                try:
                    html = await page.content()
                    ts = int(time.time())
                    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", detail_url.split("/")[-1])
                    with open(f"debug/detail/{safe}_{ts}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception:
                    pass
                records.append({
                    "Source_Page": f"index_page={page_idx}",
                    "Found_On": "detail",
                    "Detail_Page": detail_url,
                    "File_URL": "",
                    "Local_File": "",
                    "Download_Status": "no_file_found",
                })

    finally:
        try:
            await page.go_back(wait_until="domcontentloaded")
            await page.wait_for_timeout(800)
        except Exception:
            pass

async def find_next_index_url_or_button(page, current_url: str) -> tuple[bool, str]:
    next_btn = page.locator("nav.ecl-pagination a[rel='next'], a[aria-label='Next'], a:has-text('Next'), a:has-text('›')")
    if await next_btn.count() > 0:
        try:
            await next_btn.first.click()
            return True, ""
        except Exception:
            pass
    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query)
    page_num = 1
    if "page" in qs:
        try:
            page_num = int(qs["page"][0])
        except Exception:
            page_num = 1
    new_qs = {k: v for k, v in qs.items()}
    new_qs["page"] = [str(page_num + 1)]
    new_query = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in new_qs.items()})
    new_url = urlunparse(parsed._replace(query=new_query))
    return False, new_url

async def run(show=False, max_pages=500):
    ensure_dirs()
    records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not show)
        ctx = await browser.new_context(user_agent="NiekThesisScraper/6.0 (Playwright)")
        page = await ctx.new_page()
        current_url = START_URL
        print(f"[i] Start: {current_url}")
        await page.goto(current_url, wait_until="domcontentloaded")
        await accept_cookies(page)
        await page.wait_for_timeout(800)

        page_idx = 1
        while True:
            print(f"[i] INDEX page {page_idx}: finding detail links…")
            detail_links = await get_detail_links_on_index(page)
            print(f"[i] Found {len(detail_links)} detail links on index page {page_idx}")

            for durl in detail_links:
                print(f"   ↪ detail: {durl}")
                await collect_files_in_detail(page, durl, page_idx, ctx, records)

            clicked, next_url = await find_next_index_url_or_button(page, current_url)
            if clicked:
                page_idx += 1
                current_url = page.url
                await page.wait_for_timeout(1000)
                await accept_cookies(page)
            else:
                if next_url == current_url:
                    print("[i] No further index pages. Stopping.")
                    break
                try:
                    await page.goto(next_url, wait_until="domcontentloaded")
                    page_idx += 1
                    current_url = next_url
                    await page.wait_for_timeout(1000)
                    await accept_cookies(page)
                except Exception as e:
                    print(f"[i] Could not navigate to next page URL ({next_url}): {e}")
                    break

            if page_idx > max_pages:
                print("[i] Hit max_pages limit — stopping.")
                break

        await browser.close()

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
