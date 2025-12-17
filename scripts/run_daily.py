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
    # Good enough for Greenville “yesterday” in practice
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

def debris_signal(text: str):
    d = (text or "").upper()
    hits = []
    for k in ["DEMO", "DEMOL", "TEAR", "ROOF", "REMODEL", "RENOV", "ADDITION", "CONCRETE", "REMOVE", "EXCAV", "NEW CONSTRUCTION"]:
        if k in d:
            hits.append(k)
    return [{"name": "debris_generation", "confidence": 0.7, "evidence": hits[:4]}] if hits else []

async def snap(page, name: str):
    os.makedirs("data", exist_ok=True)
    try:
        await page.screenshot(path=f"data/{name}.png", full_page=True)
    except Exception:
        pass

async def dump_html(page, name: str):
    os.makedirs("data", exist_ok=True)
    try:
        html = await page.content()
        with open(f"data/{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)

    # robust-ish field finds
    user = page.locator("input[type='email'], input[type='text']").first
    pw = page.locator("input[type='password']").first
    await user.fill(username)
    await pw.fill(password)

    btn = page.locator("button, input[type=submit]").filter(
        has_text=re.compile(r"log\s*in|sign\s*in", re.I)
    ).first
    if await btn.count() == 0:
        btn = page.locator("button, input[type=submit], input[type=button]").first

    await btn.click()
    await page.wait_for_load_state("networkidle")

async def find_results_table(page):
    # Find table with headers matching the screenshot
    tables = page.locator("table")
    n = await tables.count()
    best = None
    best_hdr = []
    for i in range(n):
        t = tables.nth(i)
        ths = t.locator("th")
        if await ths.count() == 0:
            continue
        hdr = [h.strip() for h in await ths.all_inner_texts()]
        hdr_u = [h.upper() for h in hdr]
        if ("PERMIT_NO" in hdr_u or "PERMIT NO" in hdr_u) and "ISSUED" in hdr_u:
            best = t
            best_hdr = hdr
            break
    return best, best_hdr

async def parse_current_page_rows(table):
    rows = []
    trs = table.locator("tr")
    trn = await trs.count()
    for i in range(1, trn):  # skip header row
        tr = trs.nth(i)
        tds = tr.locator("td")
        if await tds.count() == 0:
            continue
        cells = [" ".join(c.split()) for c in await tds.all_inner_texts()]
        # skip empty/junk rows
        if not any(cells):
            continue
        rows.append(cells)
    return rows

async def click_next_if_available(page):
    # Common eTRAKiT pager patterns: "Next", ">", or "›"
    candidates = [
        page.locator("a").filter(has_text=re.compile(r"^\s*Next\s*$", re.I)),
        page.locator("a").filter(has_text=re.compile(r"^\s*[>›]\s*$")),
        page.locator("input[type=submit], button").filter(has_text=re.compile(r"Next|>", re.I)),
    ]

    next_el = None
    for loc in candidates:
        if await loc.count() > 0:
            next_el = loc.first
            break

    if not next_el:
        return False

    # Try to detect disabled state
    aria_disabled = (await next_el.get_attribute("aria-disabled")) or ""
    cls = (await next_el.get_attribute("class")) or ""
    if aria_disabled.lower() == "true" or "disabled" in cls.lower():
        return False

    try:
        await next_el.click()
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(500)
        return True
    except Exception:
        return False

async def fetch_etrakit_rows(search_date: str):
    user = os.environ.get("ETRAKIT_USER")
    pw = os.environ.get("ETRAKIT_PASS")
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (set GitHub Secrets).")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await login(page, user, pw)
        await page.goto(SEARCH_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")
        await snap(page, "00_search_page")
        await dump_html(page, "00_search_page")

        # Locate the "Search By" select near the label (avoid sidebar selects)
        search_by = page.locator("xpath=//*[contains(normalize-space(.),'Search By')]/following::select[1]")
        op_sel   = page.locator("xpath=//*[contains(normalize-space(.),'Search Operator')]/following::select[1]")
        val_in   = page.locator("xpath=//*[contains(normalize-space(.),'Search Value')]/following::input[1]")

        if await search_by.count() == 0 or await op_sel.count() == 0 or await val_in.count() == 0:
            await snap(page, "01_missing_controls")
            await dump_html(page, "01_missing_controls")
            raise RuntimeError("Could not locate Search By / Operator / Value controls on eTRAKiT page.")

        # Select ISSUED and Equals; then type date
        # (Your dropdown options showed: permit_NO, permit type, issued, status, site_addr, site_apn)
        await search_by.first.select_option(label=re.compile(r"issued", re.I))
        await op_sel.first.select_option(label=re.compile(r"equals", re.I))
        await val_in.first.fill(search_date)

        # Click SEARCH
        btn = page.locator("button, input[type=submit], input[type=button]").filter(has_text=re.compile(r"^\s*SEARCH\s*$", re.I)).first
        if await btn.count() == 0:
            # fallback by value attribute
            btn = page.locator("input").filter(has_text=re.compile("search", re.I)).first

        await btn.click()
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(700)
        await snap(page, "10_results_page")
        await dump_html(page, "10_results_page")

        table, hdr = await find_results_table(page)
        if not table:
            await snap(page, "11_no_results_table")
            await dump_html(page, "11_no_results_table")
            raise RuntimeError("Could not find results table with PERMIT_NO + ISSUED headers on results page.")

        all_rows = []
        max_pages = 10  # safety cap
        for _ in range(max_pages):
            rows = await parse_current_page_rows(table)
            all_rows.extend(rows)

            moved = await click_next_if_available(page)
            if not moved:
                break

            # refresh table handle after navigation
            table, hdr = await find_results_table(page)
            if not table:
                break

        await browser.close()
        return hdr, all_rows

async def main():
    run_date = datetime.now(timezone.utc).date().isoformat()
    os.makedirs("data", exist_ok=True)

    search_date = local_yesterday_mmddyyyy()
    hdr, rows = await fetch_etrakit_rows(search_date)

    # Map columns by header text (so we don’t guess indices)
    hdr_u = [h.upper().strip() for h in hdr]
    def idx(name):
        name = name.upper()
        for i, h in enumerate(hdr_u):
            if h == name or h.replace(" ", "_") == name:
                return i
        return None

    i_permit = idx("PERMIT_NO") or idx("PERMIT NO")
    i_issued = idx("ISSUED")
    i_type   = idx("PERMIT TYPE")
    i_status = idx("STATUS")
    i_apn    = idx("SITE_APN")
    i_addr   = idx("SITE_ADDR")

    out = []
    for cells in rows:
        permit_no = cells[i_permit] if i_permit is not None and i_permit < len(cells) else None
        issued    = cells[i_issued] if i_issued is not None and i_issued < len(cells) else search_date
        ptype     = cells[i_type]   if i_type   is not None and i_type   < len(cells) else None
        status    = cells[i_status] if i_status is not None and i_status < len(cells) else None
        apn       = cells[i_apn]    if i_apn    is not None and i_apn    < len(cells) else None
        addr      = cells[i_addr]   if i_addr   is not None and i_addr   < len(cells) else None

        desc = " | ".join([c for c in [ptype, status] if c])

        rec = {
            "source": "etrakit",
            "jurisdiction": "Greenville County",
            "issued_date": issued,
            "project": {
                "permit_no": permit_no,
                "address": addr,
                "apn": apn,
                "permit_type": ptype,
                "status": status,
                "description": desc,
                "value": None
            },
            "contractor": {"name": None, "phone": None, "license": None},
            "owner": {"name": None, "address": None},
            "signals": debris_signal(ptype or ""),
            "fingerprint": fp(issued or "", permit_no or "", addr or "", desc or ""),
            "source_url": SEARCH_URL,
            "scraped_at": now_iso(),
            "confidence": 0.75
        }
        out.append(rec)

    path = f"data/{run_date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Searched ISSUED={search_date}")
    print(f"Headers: {hdr}")
    print(f"Wrote {len(out)} records -> {path}")

if __name__ == "__main__":
    asyncio.run(main())
