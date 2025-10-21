import asyncio, os, re, time
import pandas as pd
from playwright.async_api import async_playwright

START_URL = "https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12527-Artificial-intelligence-ethical-and-legal-requirements/feedback_en?p_id=14488"
OUTDIR = "data/raw_pdfs"
INV_BASENAME = "data/metadata/inventory_of_304_letters_AI_Act"

PDF_RE = re.compile(r"\.pdf($|\?)", re.I)

async def run():
    os.makedirs(OUTDIR, exist_ok=True)
    records, seen_detail, seen_pdf = [], set(), set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent="NiekThesisScraper/2.0 (Playwright)")
        page = await ctx.new_page()
        await page.goto(START_URL, wait_until="domcontentloaded")

        page_idx = 1
        while True:
            # wait for feedback list container (tolerant selector)
            await page.wait_for_timeout(800)  # small settle
            cards = await page.locator("a:has-text('Feedback') , a:has-text('Read more') , a[href*='feedback']").all()
            if not cards:
                # fallback: grab all anchors and filter later
                cards = await page.locator("a").all()

            detail_links = set()
            for a in cards:
                href = (await a.get_attribute("href")) or ""
                if "feedback" in href.lower():
                    detail_links.add(await a.evaluate("el => el.href"))

            # Visit each detail page to find attachments
            for durl in sorted(detail_links):
                if durl in seen_detail:
                    continue
                seen_detail.add(durl)

                dp = await ctx.new_page()
                try:
                    await dp.goto(durl, wait_until="domcontentloaded")
                    await dp.wait_for_timeout(800)

                    anchors = await dp.locator("a").all()
                    pdfs = []
                    for a in anchors:
                        href = (await a.get_attribute("href")) or ""
                        text = (await a.text_content()) or ""
                        if href and ("attachment" in href.lower() or PDF_RE.search(href) or "download" in href.lower()):
                            full = await a.evaluate("el => el.href")
                            if full:
                                pdfs.append((full, text.strip()))

                    if not pdfs:
                        records.append({
                            "Source_Page": f"page={page_idx}",
                            "Found_On": "detail",
                            "Detail_Page": durl,
                            "PDF_URL": "",
                            "Local_PDF": "",
                            "Download_Status": "no_pdf_found",
                            "Text_Path": "",
                            "Word_Count": 0
                        })
                    else:
                        for pdf_url, label in set(pdfs):
                            if pdf_url in seen_pdf:
                                continue
                            seen_pdf.add(pdf_url)
                            # download via Playwright (handles cookies)
                            fname = re.sub(r"[^a-zA-Z0-9._-]+","_", pdf_url.split("/")[-1] or "file.pdf")
                            if not fname.lower().endswith(".pdf"):
                                fname += ".pdf"
                            dest = os.path.join(OUTDIR, fname)

                            try:
                                # Use the browser to fetch the file (respects auth/headers)
                                res = await ctx.request.get(pdf_url)
                                if res.ok:
                                    with open(dest, "wb") as f:
                                        f.write(await res.body())
                                    status = "downloaded"
                                else:
                                    status = f"http_{res.status}"
                                    dest = ""
                            except Exception:
                                status = "download_error"
                                dest = ""

                            records.append({
                                "Source_Page": f"page={page_idx}",
                                "Found_On": "detail",
                                "Detail_Page": durl,
                                "PDF_URL": pdf_url,
                                "Local_PDF": dest,
                                "Download_Status": status,
                                "Text_Path": "",
                                "Word_Count": 0
                            })
                finally:
                    await dp.close()

            # Look for pagination “Next”
            next_btn = page.locator("a[rel='next'], a:has-text('Next'), a[aria-label='Next'], a:has-text('›')")
            if await next_btn.count() == 0:
                break
            try:
                await next_btn.first.click()
                page_idx += 1
                await page.wait_for_timeout(1000)
            except Exception:
                break

        await browser.close()

    # Write inventory
    os.makedirs(os.path.dirname(INV_BASENAME), exist_ok=True)
    cols = ["Source_Page","Found_On","Detail_Page","PDF_URL","Local_PDF","Download_Status","Text_Path","Word_Count"]
    df = pd.DataFrame.from_records(records, columns=cols)
    df.to_csv(f"{INV_BASENAME}.csv", sep=";", index=False, encoding="utf-8")
    try:
        with pd.ExcelWriter(f"{INV_BASENAME}.xlsx", engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="inventory")
    except Exception:
        pass

asyncio.run(run())
