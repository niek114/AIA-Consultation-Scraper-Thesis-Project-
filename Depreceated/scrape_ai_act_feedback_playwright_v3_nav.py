import asyncio, os, re, time, argparse
from pathlib import Path
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
OUTDIR = "data/raw_pdfs"
INV_BASENAME = "data/metadata/inventory_of_304_letters_AI_Act"

PDF_EXT = re.compile(r"\.(pdf|doc|docx|zip)($|\?)", re.I)
ATTN_HINT = re.compile(r"(attachment|download|enclosure|file)", re.I)

def ensure_dirs():
    Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    Path("data/metadata").mkdir(parents=True, exist_ok=True)

async def accept_cookies(page):
    # tolerant cookie acceptance
    for label in ["Accept all","Accept","I agree","Akkoord","Alle accepteren","Tout accepter","Alle akzeptieren","Aceptar todo","Allow all"]:
        btn = page.get_by_role("button", name=re.compile(label, re.I))
        if await btn.count():
            try:
                await btn.first.click(timeout=1500)
                await page.wait_for_timeout(400)
            except PWTimeout:
                pass

async def get_detail_links_on_index(page) -> list[str]:
    # wait and gather all feedback-detail links on current index page
    await page.wait_for_timeout(1200)
    hrefs = set()
    anchors = page.locator("a")
    n = await anchors.count()
    for i in range(n):
        a = anchors.nth(i)
        try:
            href = await a.get_attribute("href")
            if not href:
                continue
            if "feedback" in href.lower():
                full = await a.evaluate("el => el.href")
                if full:
                    hrefs.add(full)
        except Exception:
            pass
    return sorted(hrefs)

async def collect_attachments_in_detail(page, detail_url, page_idx, ctx, records):
    # navigate IN THE SAME TAB to detail, collect and download, then go back
    try:
        await page.goto(detail_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)
        # try to expand obvious accordions
        for label in ["Attachments","Attachment","Downloads","Download","Files","Show more","Expand"]:
            btn = page.get_by_role("button", name=re.compile(label, re.I))
            if await btn.count():
                try:
                    await btn.first.click(timeout=1200); await page.wait_for_timeout(400)
                except Exception:
                    pass

        # scroll to trigger lazy loads
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(500)

        # collect potential file links
        pdf_links = set()
        # direct hints
        css_hits = page.locator("a[href$='.pdf'], a[href$='.doc'], a[href$='.docx'], a[href$='.zip'], a[href*='attachment'], a[href*='download']")
        for i in range(await css_hits.count()):
            try:
                full = await css_hits.nth(i).evaluate("el => el.href")
                if full: pdf_links.add(full)
            except Exception:
                pass
        # role-based / text hints
        for label in ["Attachment","Download","PDF","File"]:
            hits = page.get_by_role("link", name=re.compile(label, re.I))
            for i in range(await hits.count()):
                try:
                    full = await hits.nth(i).evaluate("el => el.href")
                    if full and (PDF_EXT.search(full) or ATTN_HINT.search(full)):
                        pdf_links.add(full)
                except Exception:
                    pass
        # fallback: scan all anchors
        if not pdf_links:
            anchors = page.locator("a")
            for i in range(await anchors.count()):
                try:
                    full = await anchors.nth(i).evaluate("el => el.href")
                    if full and (PDF_EXT.search(full) or ATTN_HINT.search(full)):
                        pdf_links.add(full)
                except Exception:
                    pass

        if not pdf_links:
            records.append({
                "Source_Page": f"index_page={page_idx}",
                "Found_On": "detail",
                "Detail_Page": detail_url,
                "File_URL": "",
                "Local_File": "",
                "Download_Status": "no_file_found",
            })
        else:
            for url in sorted(pdf_links):
                name = (url.split("/")[-1] or "file").split("?")[0]
                if not PDF_EXT.search(name):
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
        # go back to the index page you came from
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
        ctx = await browser.new_context(user_agent="NiekThesisScraper/3.0 (Playwright)")
        page = await ctx.new_page()
        await page.goto(START_URL, wait_until="domcontentloaded")
        await accept_cookies(page)
        await page.wait_for_timeout(800)

        page_idx = 1
        while True:
            print(f"[i] INDEX page {page_idx}: harvesting detail links…")
            detail_links = await get_detail_links_on_index(page)
            print(f"[i] Found {len(detail_links)} detail links on index page {page_idx}")

            # iterate: index -> detail -> back to index
            for durl in detail_links:
                await collect_attachments_in_detail(page, durl, page_idx, ctx, records)

            # move to next index page
            next_btn = page.locator("a[rel='next'], a[aria-label='Next'], a:has-text('Next'), a:has-text('›')")
            if await next_btn.count() == 0:
                print("[i] No further index pages — stop.")
                break
            try:
                await next_btn.first.click()
                page_idx += 1
                await page.wait_for_timeout(1200)
                await accept_cookies(page)
            except Exception as e:
                print(f"[!] Could not paginate: {e}")
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
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--max-pages", type=int, default=500)
    args = ap.parse_args()
    asyncio.run(run(show=args.show, max_pages=args.max_pages))
