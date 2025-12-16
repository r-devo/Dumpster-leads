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

        # 1) Pick a date from the dropdown (prefer most recent non-"All"/non-placeholder)
        date_select = page.locator("select").filter(has_text="Select Date").first
        if await date_select.count() == 0:
            # fallback: any select on page
            date_select = page.locator("select").first

        options = await date_select.locator("option").all_inner_texts()
        # choose first "real" option
        chosen = None
        for opt in options:
            t = opt.strip()
            if not t or t.lower() in {"select date", "all"}:
                continue
            chosen = t
            break
        if not chosen:
            raise RuntimeError("Could not find a real date option in dropdown.")

        await date_select.select_option(label=chosen)

        # 2) Click Search
        search_btn = page.locator("input[type=submit], input[type=button], button").filter(has_text="Search").first
        if await search_btn.count() == 0:
            search_btn = page.locator("input").filter(has_text="Search").first
        await search_btn.click()
        await page.wait_for_load_state("networkidle")

        # 3) Find the *results* table (not the search form)
        frames = [page.main_frame] + list(page.frames)
        best = None
        for fr in frames:
            for t in await fr.query_selector_all("table"):
                ths = await t.query_selector_all("th")
                hdr = " | ".join([(await h.inner_text()).strip() for h in ths]).upper()
                # Must look like permit results AND must NOT look like the search UI
                if any(k in hdr for k in ["PERMIT", "ADDRESS", "ISSU", "DESCRIPTION"]) and "SELECT DATE" not in hdr:
                    best = t
                    break
            if best:
                break

        if not best:
            raise RuntimeError("No permit results table found after Search (layout changed).")

        rows = []
        trs = await best.query_selector_all("tr")
        for tr in trs[1:]:
            tds = await tr.query_selector_all("td")
            if not tds:
                continue
            cells = [" ".join((await td.inner_text()).split()) for td in tds]
            rows.append(cells)

        await browser.close()
        return chosen, rows

def debris_signal(desc: str):
    d = (desc or "").upper()
    hits = []
    for k in ["DEMO", "DEMOL", "TEAR", "ROOF", "REMODEL", "RENOV", "ADDITION", "CONCRETE", "REMOVE"]:
        if k in d:
            hits.append(k)
    if not hits:
        return []
    return [{"name": "debris_generation", "confidence": 0.7, "evidence": hits[:4]}]

async def main():
    run_date = datetime.now(timezone.utc).date().isoformat()
    os.makedirs("data", exist_ok=True)

    chosen_date, rows = await fetch_county_rows()

    out = []
    for cells in rows:
        # best-effort: these indexes get refined after we inspect real columns
        issued_date = chosen_date
        address = cells[1] if len(cells) > 1 else ""
        desc = cells[2] if len(cells) > 2 else " ".join(cells)

        rec = {
            "source": "county",
            "jurisdiction": "Greenville County",
            "issued_date": issued_date,
            "project": {
                "address": address,
                "description": desc,
                "permit_type": None,
                "value": None
            },
            "contractor": {"name": None, "phone": None, "license": None},
            "owner": {"name": None, "address": None},
            "signals": debris_signal(desc),
            "fingerprint": fp(issued_date or "", address or "", desc or ""),
            "source_url": COUNTY_URL,
            "scraped_at": now_iso(),
            "confidence": 0.6
        }
        out.append(rec)

    path = f"data/{run_date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(out)} records -> {path}")

if __name__ == "__main__":
    asyncio.run(main())
