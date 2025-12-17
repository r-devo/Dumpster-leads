import os, json, hashlib, re
from datetime import datetime, timedelta
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/login.aspx?lt=either&rd=~/Search/permit.aspx"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def mmddyyyy(d: datetime) -> str:
    return d.strftime("%m/%d/%Y")

def local_yesterday_mmddyyyy():
    # GitHub runners are UTC; Greenville is ET. This is "good enough" for daily pulls.
    # If you want perfect ET, we can add zoneinfo later.
    return mmddyyyy(datetime.now() - timedelta(days=1))

def fp(issued_date, permit_no, address, permit_type, status):
    base = f"{issued_date}|{permit_no}|{address}|{permit_type}|{status}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()

async def snap(page, name):
    os.makedirs("data", exist_ok=True)
    try:
        await page.screenshot(path=f"data/{name}.png", full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        with open(f"data/{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

async def find_public_login_container(page):
    """
    We want the PUBLIC login form in the middle of the page, not the header.
    Strategy: anchor on the big text 'LOG IN BELOW TO ENTER THE PUBLIC PORTAL'
    then climb to a reasonable container.
    """
    anchor = page.locator("text=LOG IN BELOW TO ENTER THE PUBLIC PORTAL").first
    if await anchor.count() == 0:
        # fallback: "Public Login"
        anchor = page.locator("text=Public Login").first

    if await anchor.count() == 0:
        return None

    # climb up a few levels to find a div that contains inputs
    for xpath in [
        "xpath=ancestor::div[1]",
        "xpath=ancestor::div[2]",
        "xpath=ancestor::div[3]",
        "xpath=ancestor::table[1]",
        "xpath=ancestor::td[1]",
    ]:
        cand = anchor.locator(xpath)
        if await cand.count() == 0:
            continue
        # does it contain visible inputs?
        vis_inputs = cand.locator("input:visible")
        if await vis_inputs.count() >= 2:
            return cand
    return None

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(500)
    await snap(page, "00_login_loaded")

    container = await find_public_login_container(page)
    if container is None:
        # last-resort: use page-wide visible inputs (still avoids hidden header inputs)
        container = page

    user_in = container.locator("input[type='text']:visible, input[type='email']:visible").first
    pass_in = container.locator("input[type='password']:visible").first

    # Fill credentials
    await user_in.click()
    await user_in.fill(username)
    await pass_in.click()
    await pass_in.fill(password)

    # Click the visible LOGIN button in the same container
    btn = container.locator("button:visible, input[type='submit']:visible, input[type='button']:visible").filter(
        has_text=re.compile(r"^\s*login\s*$", re.I)
    ).first

    if await btn.count() == 0:
        # fallback: any visible element that says LOGIN
        btn = container.locator("text=LOGIN").first

    await btn.click()

    # Wait until we land on the search page (or at least show logged-in nav)
    try:
        await page.wait_for_url(re.compile(r".*/Search/permit\.aspx.*", re.I), timeout=20000)
    except PWTimeout:
        # Sometimes it stays same URL but logs in; attempt direct navigation:
        await page.goto(SEARCH_URL, wait_until="domcontentloaded")

    await page.wait_for_timeout(700)
    await snap(page, "01_after_login")

async def find_search_by_dropdown(page):
    """
    Find the 'Search By' dropdown by locating a <select> that has options including PERMIT_NO and ISSUED.
    """
    selects = page.locator("select:visible")
    n = await selects.count()
    for i in range(n):
        sel = selects.nth(i)
        try:
            opts = await sel.locator("option").all_inner_texts()
        except Exception:
            continue
        u = [o.strip().upper() for o in opts if o.strip()]
        if "PERMIT_NO" in u and "ISSUED" in u:
            return sel
    return None

async def select_option_case_insensitive(select_locator, desired_label_upper: str):
    opts = await select_locator.locator("option").all_inner_texts()
    for o in opts:
        if o.strip().upper() == desired_label_upper:
            await select_locator.select_option(label=o.strip())
            return True
    # sometimes the value is what we want
    try:
        await select_locator.select_option(value=desired_label_upper)
        return True
    except Exception:
        return False

async def wait_for_results_grid(page):
    """
    Wait until the Telerik RadGrid for search results has at least one data row.
    The container id usually ends with rgSearchRslts.
    """
    grid = page.locator("div[id$='rgSearchRslts'], div[id*='rgSearchRslts']").first
    await grid.wait_for(state="visible", timeout=20000)

    # Wait for a table row with TDs (data row)
    data_row = grid.locator("table tr:has(td)").first
    await data_row.wait_for(state="visible", timeout=30000)
    return grid

async def parse_grid_rows(grid):
    """
    Parse the first table under the grid container into rows (list of dicts).
    """
    table = grid.locator("table").first
    # header
    headers = await table.locator("tr").first.locator("th").all_inner_texts()
    headers = [h.strip() for h in headers if h.strip()]
    # data rows
    rows = []
    trs = table.locator("tr:has(td)")
    trn = await trs.count()
    for i in range(trn):
        tds = await trs.nth(i).locator("td").all_inner_texts()
        tds = [" ".join(t.split()) for t in tds]
        if headers and len(tds) >= len(headers):
            rec = {headers[j]: tds[j] for j in range(len(headers))}
        else:
            rec = {f"col_{j}": tds[j] for j in range(len(tds))}
        rows.append(rec)
    return headers, rows

async def click_next_if_available(page, grid, first_row_sig: str):
    """
    Try to click a RadGrid pager 'Next' button.
    Return True if we successfully advanced to a different page of results.
    """
    # Common Telerik next selectors
    next_candidates = grid.locator(
        "a.rgPageNext:visible, a[title*='Next']:visible, input[title*='Next']:visible, a:visible >> text=>"
    )

    # The last selector (text=>) is not Playwright syntax; keep it conservative:
    # We'll check a.rgPageNext or title contains Next, plus an icon-like anchor.
    candidates = [
        grid.locator("a.rgPageNext:visible").first,
        grid.locator("a[title*='Next']:visible").first,
        grid.locator("input[title*='Next']:visible").first,
    ]

    btn = None
    for c in candidates:
        if await c.count() > 0:
            btn = c
            break

    if btn is None:
        return False

    # Some next buttons are disabled; check attribute/class
    cls = (await btn.get_attribute("class")) or ""
    if "rgPageDisabled" in cls or "disabled" in cls.lower():
        return False

    await btn.click()

    # Wait for the first data row to change (page advance)
    try:
        await page.wait_for_timeout(500)
        await wait_for_results_grid(page)
        # quick heuristic: content changed
        new_grid = page.locator("div[id$='rgSearchRslts'], div[id*='rgSearchRslts']").first
        _, rows = await parse_grid_rows(new_grid)
        if not rows:
            return False
        new_sig = json.dumps(rows[0], sort_keys=True)
        return new_sig != first_row_sig
    except Exception:
        return False

async def run_search(page, issued_date_mmddyyyy: str):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(600)
    await snap(page, "10_search_page_loaded")

    # 1) Set Search By dropdown to ISSUED
    dd = await find_search_by_dropdown(page)
    if dd is None:
        await snap(page, "90_failed_no_dropdown")
        raise RuntimeError("Could not locate Search By dropdown containing PERMIT_NO and ISSUED.")

    ok = await select_option_case_insensitive(dd, "ISSUED")
    if not ok:
        await snap(page, "90_failed_select_issued")
        raise RuntimeError("Could not select ISSUED in Search By dropdown.")

    # 2) Fill Search Value
    # Try to find the input following the "Search Value" label, otherwise fallback to first visible text input inside main area.
    value_in = page.locator(
        "xpath=//*[contains(normalize-space(.),'Search Value')]/following::input[1]"
    ).filter(has=page.locator(":visible")).first

    if await value_in.count() == 0:
        # fallback: visible text input not in header (best effort)
        value_in = page.locator("input[type='text']:visible").nth(0)

    await value_in.click()
    await value_in.fill(issued_date_mmddyyyy)

    # 3) Click SEARCH button (the actual submit)
    # Prefer an element with exact text SEARCH.
    btn = page.locator("button:visible, input[type='submit']:visible, input[type='button']:visible").filter(
        has_text=re.compile(r"^\s*search\s*$", re.I)
    ).first
    if await btn.count() == 0:
        # fallback: text node SEARCH
        btn = page.locator("text=SEARCH").first

    await btn.click()

    # 4) Wait for results to populate
    await page.wait_for_timeout(400)
    await snap(page, "11_after_search_click")
    grid = await wait_for_results_grid(page)
    await snap(page, "12_results_page1")

    headers, rows1 = await parse_grid_rows(grid)
    sig1 = json.dumps(rows1[0], sort_keys=True) if rows1 else ""

    rows_all = list(rows1)

    # 5) Try to click Next page once
    advanced = await click_next_if_available(page, grid, sig1)
    if advanced:
        await page.wait_for_timeout(600)
        await snap(page, "13_results_page2")
        grid2 = page.locator("div[id$='rgSearchRslts'], div[id*='rgSearchRslts']").first
        _, rows2 = await parse_grid_rows(grid2)
        rows_all.extend(rows2)

    return headers, rows_all

def debris_signal(desc: str):
    d = (desc or "").upper()
    hits = []
    for k in ["DEMO", "DEMOL", "TEAR", "ROOF", "REMODEL", "RENOV", "ADDITION", "CONCRETE", "REMOVE", "DUMPSTER"]:
        if k in d:
            hits.append(k)
    if not hits:
        return []
    return [{"name": "debris_generation", "confidence": 0.7, "evidence": hits[:6]}]

async def main():
    user = os.environ.get("ETRAKIT_USER", "").strip()
    pw = os.environ.get("ETRAKIT_PASS", "").strip()
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (GitHub Secrets).")

    issued_date = local_yesterday_mmddyyyy()
    run_date = datetime.utcnow().date().isoformat()
    os.makedirs("data", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await login(page, user, pw)
            headers, rows = await run_search(page, issued_date)
            await snap(page, "99_final_state")
        finally:
            await browser.close()

    # Transform rows into your JSON record schema
    out = []
    for r in rows:
        permit_no = r.get("PERMIT_NO") or r.get("Permit No") or r.get("PERMIT") or ""
        issued = r.get("ISSUED") or issued_date
        permit_type = r.get("Permit Type") or r.get("PERMIT TYPE") or ""
        status = r.get("STATUS") or ""
        site_addr = r.get("SITE_ADDR") or r.get("SITE ADDR") or r.get("SITE_ADDR ") or ""
        site_apn = r.get("SITE_APN") or ""

        desc = permit_type  # we’ll enrich later; right now Permit Type is the best "description-like" field

        rec = {
            "source": "etrakit",
            "jurisdiction": "Greenville County",
            "issued_date": issued,
            "project": {
                "address": site_addr,
                "description": desc,
                "permit_type": permit_type,
                "value": None,
                "site_apn": site_apn,
                "permit_no": permit_no,
                "status": status,
            },
            "contractor": {"name": None, "phone": None, "license": None},
            "owner": {"name": None, "address": None},
            "signals": debris_signal(desc),
            "fingerprint": fp(issued, permit_no, site_addr, permit_type, status),
            "source_url": SEARCH_URL,
            "scraped_at": now_iso(),
            "confidence": 0.75
        }
        out.append(rec)

    path = f"data/{run_date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Search ISSUED={issued_date} -> {len(out)} records -> {path}")

if __name__ == "__main__":
    asyncio.run(main())
