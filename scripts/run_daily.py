import os, json, hashlib
from datetime import datetime, timezone
import asyncio
from playwright.async_api import async_playwright

COUNTY_URL = "https://app.greenvillecounty.org/permits_issued.htm"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fp(issued_date, address, desc):
    base = f"{issued_date}|{address}|{desc}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()

async def fetch_and_debug():
    os.makedirs("data", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(COUNTY_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1500)

        # Snapshot BEFORE doing anything
        await page.screenshot(path="data/00_loaded.png", full_page=True)
        with open("data/00_loaded.html", "w", encoding="utf-8") as f:
            f.write(await page.content())

        frames = [page.main_frame] + list(page.frames)

        # Find first <select> (date dropdown is usually a select)
        date_select = None
        for fr in frames:
            sel = fr.locator("select")
            if await sel.count() > 0:
                date_select = sel.first
                break

        if date_select is None:
            raise RuntimeError("No <select> found (see data/00_loaded.png/html).")

        options = await date_select.locator("option").all_inner_texts()
        chosen = None
        for opt in options:
            t = opt.strip()
            if len(t) == 10 and t[2] == "/" and t[5] == "/":
                chosen = t
                break
        if not chosen:
            chosen = "All"

        await date_select.select_option(label=chosen)
        await page.wait_for_timeout(500)

        # Try submitting form; if none, press Enter
        form = date_select.locator("xpath=ancestor::form[1]")
        if await form.count() > 0:
            await form.evaluate("f => f.submit()")
        else:
            await date_select.press("Enter")

        # Wait for something to change
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(2000)

        # Snapshot AFTER submit
        await page.screenshot(path="data/10_post_submit.png", full_page=True)
        with open("data/10_post_submit.html", "w", encoding="utf-8") as f:
            f.write(await page.content())

        # Refresh frames and inventory what we have
        frames = [page.main_frame] + list(page.frames)

        table_report = []
        for idx, fr in enumerate(frames):
            try:
                tables = await fr.query_selector_all("table")
            except Exception:
                continue
            for t_i, t in enumerate(tables):
                try:
                    ths = await t.query_selector_all("th")
                    hdr = " | ".join([(await h.inner_text()).strip() for h in ths])
                    hdr_u = hdr.upper()
                    table_report.append({
                        "frame": idx,
                        "table_index": t_i,
                        "th_count": len(ths),
                        "header_preview": hdr[:200],
                        "looks_like_results": ("PERMIT" in hdr_u or "ADDRESS" in hdr_u or "ISSU" in hdr_u or "DESCRIPTION" in hdr_u)
                    })
                except Exception:
                    continue

        with open("data/20_table_report.json", "w", encoding="utf-8") as f:
            json.dump({
                "chosen_date": chosen,
                "table_report_count": len(table_report),
                "table_report": table_report[:200]  # cap
            }, f, indent=2)

        # Heuristic: pick best-looking results table
        best = None
        best_meta = None
        for meta in table_report:
            if meta["looks_like_results"] and meta["th_count"] >= 3:
                best_meta = meta
                break

        if best_meta is None:
            raise RuntimeError("No table with PERMIT/ADDRESS/etc found. See data/10_post_submit.png and data/20_table_report.json")

        fr = frames[best_meta["frame"]]
        tables = await fr.query_selector_all("table")
        best = tables[best_meta["table_index"]]

        # Extract first 5 rows for proof
        rows = []
        trs = await best.query_selector_all("tr")
        for tr in trs[1:6]:
            tds = await tr.query_selector_all("td")
            if not tds:
                continue
            cells = [" ".join((await td.inner_text()).split()) for td in tds]
            rows.append(cells)

        with open("data/30_sample_rows.json", "w", encoding="utf-8") as f:
            json.dump({
                "chosen_date": chosen,
                "best_table_meta": best_meta,
                "sample_rows": rows
            }, f, indent=2)

        await browser.close()
        return chosen, rows

async def main():
    chosen_date, sample_rows = await fetch_and_debug()
    # Minimal output so workflow still “does something”
    out = [{
        "source": "county",
        "jurisdiction": "Greenville County",
        "issued_date": chosen_date,
        "project": {"address": "", "description": ""},
        "signals": [],
        "fingerprint": fp(chosen_date, "", ""),
        "source_url": COUNTY_URL,
        "scraped_at": now_iso()
    }]
    with open(f"data/debug_spine_{datetime.now(timezone.utc).date().isoformat()}.json", "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("Chosen date:", chosen_date)
    print("Sample rows found:", len(sample_rows))

if __name__ == "__main__":
    asyncio.run(main())
