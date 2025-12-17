import os, json, hashlib, re
from datetime import datetime, timezone, timedelta
import asyncio
from playwright.async_api import async_playwright

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fp(issued_date, permit_no, address, desc):
    base = f"{issued_date}|{permit_no}|{address}|{desc}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()

def local_yesterday_mmddyyyy():
    # Server/user is Eastern; this is fine for “yesterday”
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

async def snap(page, stem: str):
    os.makedirs("data", exist_ok=True)
    try:
        await page.screenshot(path=f"data/{stem}.png", full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        with open(f"data/{stem}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_loaded")

    # IMPORTANT: only fill VISIBLE fields (hidden inputs cause your exact failure)
    user = page.locator("input[type='email']:visible, input[type='text']:visible").first
    pw = page.locator("input[type='password']:visible").first

    await user.wait_for(state="visible", timeout=30000)
    await pw.wait_for(state="visible", timeout=30000)

    await user.fill(username)
    await pw.fill(password)

    # Click a visible login-ish button
    btn = page.locator(
        "button:visible, input[type='submit']:visible, input[type='button']:visible"
    ).filter(has_text=re.compile(r"(log\s*in|sign\s*in|login)", re.I)).first

    # Fallback: first visible submit/button
    if await btn.count() == 0:
        btn = page.locator("input[type='submit']:visible, button:visible, input[type='button']:visible").first

    await btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)
    await snap(page, "10_after_login")

async def run_search(page):
    # Go directly to permit search
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)
    await snap(page, "20_search_loaded")

    # Fill the 3-field search:
    # Search By: ISSUED
    # Operator: Equals
    # Value: yesterday
    issued = local_yesterday_mmddyyyy()

    # These selects are usually visible; still enforce :visible
    sel_by = page.locator("select:visible").nth(0)
    sel_op = page.locator("select:visible").nth(1)
    val_in = page.locator("input[type='text']:visible").first

    await sel_by.wait_for(state="visible", timeout=30000)
    await sel_op.wait_for(state="visible", timeout=30000)
    await val_in.wait_for(state="visible", timeout=30000)

    # Choose by label if possible, otherwise by value guess
    try:
        await sel_by.select_option(label=re.compile(r"issued", re.I))
    except Exception:
        # fallback: pick option that contains ISSUED
        await sel_by.select_option(
            value=await sel_by.locator("option").filter(has_text=re.compile("issued", re.I)).first.get_attribute("value")
        )

    try:
        await sel_op.select_option(label=re.compile(r"equals", re.I))
    except Exception:
        pass

    await val_in.fill(issued)

    # Click SEARCH button
    search_btn = page.locator("button:visible, input[type='submit']:visible, input[type='button']:visible").filter(
        has_text=re.compile(r"search", re.I)
    ).first
    if await search_btn.count() == 0:
        search_btn = page.locator("button:visible, input[type='submit']:visible, input[type='button']:visible").first

    await search_btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)
    await snap(page, "30_results_loaded")

    return issued

async def extract_rows(page):
    # Look for a results table containing PERMIT_NO
    # (Your screenshot shows columns PERMIT_NO, ISSUED, Permit Type, STATUS, SITE_APN, SITE_ADDR)
    tbl = page.locator("table:visible").filter(has_text=re.compile(r"PERMIT_NO", re.I)).first
    if await tbl.count() == 0:
        # Telerik grids sometimes render headers outside <th>; still keep debug + hard fail
        raise RuntimeError("Could not find a visible table containing PERMIT_NO on the results page.")

    # Extract header cells
    header_cells = tbl.locator("tr").first.locator("th, td")
    headers = [re.sub(r"\s+", " ", (await header_cells.nth(i).inner_text()).strip())
               for i in range(await header_cells.count())]

    # Extract body rows (skip any header-like rows)
    rows = []
    tr_all = tbl.locator("tr")
    for r in range(await tr_all.count()):
        tr = tr_all.nth(r)
        tds = tr.locator("td")
        if await tds.count() == 0:
            continue
        cells = [re.sub(r"\s+", " ", (await tds.nth(i).inner_text()).strip())
                 for i in range(await tds.count())]
        # Skip empty rows
        if any(cells):
            rows.append(cells)

    # Save a small report for inspection
    os.makedirs("data", exist_ok=True)
    with open("data/40_table_report.json", "w", encoding="utf-8") as f:
        json.dump({"headers": headers, "row_count": len(rows), "sample_rows": rows[:5]}, f, indent=2)

    return headers, rows

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
    os.makedirs("data", exist_ok=True)

    user = os.environ.get("ETRAKIT_USER") or ""
    pw = os.environ.get("ETRAKIT_PASS") or ""
    if not user or not pw:
        raise RuntimeError("Missing secrets: set ETRAKIT_USER and ETRAKIT_PASS in workflow env.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await login(page, user, pw)
        issued_date = await run_search(page)
        headers, rows = await extract_rows(page)

        # Map columns by header name (robust to column order changes)
        def col_idx(name):
            for i, h in enumerate(headers):
                if name.upper() in h.upper():
                    return i
            return None

        i_permit = col_idx("PERMIT_NO")
        i_issued = col_idx("ISSUED")
        i_type   = col_idx("Permit Type")
        i_apn    = col_idx("SITE_APN")
        i_addr   = col_idx("SITE_ADDR")

        out = []
        for cells in rows:
            permit_no = cells[i_permit] if i_permit is not None and i_permit < len(cells) else ""
            issued = cells[i_issued] if i_issued is not None and i_issued < len(cells) else issued_date
            ptype = cells[i_type] if i_type is not None and i_type < len(cells) else ""
            addr = cells[i_addr] if i_addr is not None and i_addr < len(cells) else ""
            apn = cells[i_apn] if i_apn is not None and i_apn < len(cells) else ""

            desc = ptype  # for now, we’ll enrich later by clicking into permit detail pages

            rec = {
                "source": "etrakit",
                "jurisdiction": "Greenville County",
                "issued_date": issued,
                "project": {
                    "address": addr,
                    "description": desc,
                    "permit_type": ptype,
                    "value": None,
                    "site_apn": apn
                },
                "contractor": {"name": None, "phone": None, "license": None},
                "owner": {"name": None, "address": None},
                "signals": debris_signal(desc),
                "fingerprint": fp(issued or "", permit_no or "", addr or "", desc or ""),
                "source_url": SEARCH_URL,
                "scraped_at": now_iso(),
                "confidence": 0.75
            }
            out.append(rec)

        run_date = datetime.now(timezone.utc).date().isoformat()
        path = f"data/{run_date}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)

        print(f"Wrote {len(out)} records -> {path}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
