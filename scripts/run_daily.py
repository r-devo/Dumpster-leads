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

        # 1) Find a date dropdown in any frame
        date_select = None
        frame_with_form = None
        for fr in frames:
            sel = fr.locator("select")
            if await sel.count() > 0:
                date_select = sel.first
                frame_with_form = fr
                break

        if date_select is None:
            raise RuntimeError("No <select> dropdown found on page/frames.")

        options = await date_select.locator("option").all_inner_texts()

        chosen = None
        for opt in options:
            t = opt.strip()
            if len(t) == 10 and t[2] == "/" and t[5] == "/":
                chosen = t
                break

        if not chosen:
            for opt in options:
                if opt.strip().lower() == "all":
                    chosen = "All"
                    break

        if not chosen:
            preview = ", ".join([o.strip() for o in options[:8]])
            raise RuntimeError(f"Could not find date option. First options: {preview}")

        await date_select.select_option(label=chosen)

        # 2) Click Search (in same frame as the dropdown)
        fr_for_controls = frame_with_form or page.main_frame
        await page.wait_for_timeout(1500)

        candidates = [
            fr_for_controls.locator("input[type=submit]"),
            fr_for_controls.locator("input[type=button]"),
            fr_for_controls.locator("button"),
            fr_for_controls.locator("input"),
        ]

        search_btn = None
        for loc in candidates:
            if await loc.count() == 0:
                continue
            for i in range(await loc.count()):
                el = loc.nth(i)
                val = (await el.get_attribute("value")) or ""
                name = (await el.get_attribute("name")) or ""
                _id = (await el.get_attribute("id")) or ""
                blob = f"{val} {name} {_id}".lower()
                if "search" in blob or "submit" in blob or blob.strip() == "go":
                    search_btn = el
                    break
            if search_btn:
                break

        if not search_btn:
            raise RuntimeError("Could not locate Search/Submit control.")

        await search_btn.click(timeout=60000)
        await page.wait_for_load_state("networkidle")

        # Refresh frames after form submission
        frames = [page.main_frame] + list(page.frames)

        # 3) Find the results table (avoid the search UI)
        best = None
        for fr in frames:
            for t in await fr.query_selector_all("table"):
                ths = await t.query_selector_all("th")
                hdr = " | ".join([(await h.inner_text()).strip() for h in ths]).upper()
                if any(k in hdr for k in ["PERMIT", "ADDRESS", "ISSU", "DESCRIPTION"]) and "SELECT DATE" not in hdr:
                    best = t
                    break
            if best:
                break

        if not best:
            raise RuntimeError("No permit results table found after Search.")

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

    # Debug (temporary): helps us lock column mapping fast
    print("Chosen date:", chosen_date)
    print("Sample row:", rows[0] if rows else "NO ROWS")

    out = []
    for cells in rows:
        issued_date = chosen_date  # will map to real column once confirmed
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
