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

async def fetch_county_rows():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(COUNTY_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        frames = [page.main_frame] + list(page.frames)

        # Find date dropdown
        date_select = None
        for fr in frames:
            sel = fr.locator("select")
            if await sel.count() > 0:
                date_select = sel.first
                break

        if date_select is None:
            raise RuntimeError("No date dropdown found")

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

        # Submit the surrounding form (old ASP-safe)
        form = date_select.locator("xpath=ancestor::form[1]")
        if await form.count() == 0:
            raise RuntimeError("Form not found for dropdown")

        await form.evaluate("f => f.submit()")
        await page.wait_for_load_state("networkidle")

        frames = [page.main_frame] + list(page.frames)

        # Find results table
        table = None
        for fr in frames:
            for t in await fr.query_selector_all("table"):
                ths = await t.query_selector_all("th")
                header = " ".join([(await h.inner_text()).upper() for h in ths])
                if "PERMIT" in header and "ADDRESS" in header:
                    table = t
                    break
            if table:
                break

        if table is None:
            raise RuntimeError("Permit results table not found")

        rows = []
        trs = await table.query_selector_all("tr")
        for tr in trs[1:]:
            tds = await tr.query_selector_all("td")
            if not tds:
                continue
            cells = [" ".join((await td.inner_text()).split()) for td in tds]
            rows.append(cells)

        await browser.close()
        return chosen, rows

def debris_signal(desc):
    d = (desc or "").upper()
    for k in ["DEMO", "ROOF", "TEAR", "REMOVE", "REMODEL", "RENOV"]:
        if k in d:
            return [{"name": "debris_generation", "confidence": 0.7}]
    return []

async def main():
    run_date = datetime.now(timezone.utc).date().isoformat()
    os.makedirs("data", exist_ok=True)

    chosen_date, rows = await fetch_county_rows()

    out = []
    for cells in rows:
        address = cells[1] if len(cells) > 1 else ""
        desc = cells[2] if len(cells) > 2 else ""

        rec = {
            "source": "county",
            "jurisdiction": "Greenville County",
            "issued_date": chosen_date,
            "project": {
                "address": address,
                "description": desc,
                "permit_type": None,
                "value": None
            },
            "signals": debris_signal(desc),
            "fingerprint": fp(chosen_date, address, desc),
            "source_url": COUNTY_URL,
            "scraped_at": now_iso()
        }
        out.append(rec)

    path = f"data/{run_date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {len(out)} records")

if __name__ == "__main__":
    asyncio.run(main())
