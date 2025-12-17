import os, json, hashlib, random, re, asyncio
from datetime import datetime, timezone, timedelta
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE = "https://grvlc-trk.aspgov.com"
HOME_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"
OUT_DIR = "data"

# ---- hard caps (prevents “retrying fill action” loops) ----
STEP_TIMEOUT_MS = 12_000          # each step must succeed quickly or fail
NAV_TIMEOUT_MS  = 20_000
ACTION_TIMEOUT_MS = 6_000

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def run_date():
    return datetime.now(timezone.utc).date().isoformat()

def yesterday_mmddyyyy():
    # Greenville/Eastern “good enough” for daily delta; refine later if needed
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

def fp(*parts: str):
    base = "|".join([(p or "") for p in parts]).upper().encode("utf-8", errors="ignore")
    return hashlib.sha256(base).hexdigest()

async def jitter(page, a=400, b=1200):
    await page.wait_for_timeout(random.randint(a, b))

async def dump(page, stem: str):
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        await page.screenshot(path=f"{OUT_DIR}/{stem}.png", full_page=True)
    except Exception:
        pass
    try:
        with open(f"{OUT_DIR}/{stem}.html", "w", encoding="utf-8") as f:
            f.write(await page.content())
    except Exception:
        pass

async def first_visible(page, selectors):
    for sel in selectors:
        loc = page.locator(sel)
        try:
            if await loc.count() and await loc.first.is_visible():
                return loc.first
        except Exception:
            continue
    return None

async def must_visible(page, selectors, name):
    el = await first_visible(page, selectors)
    if not el:
        await dump(page, f"FAIL_missing_{name}")
        raise RuntimeError(f"Missing/hidden required element: {name} ({selectors})")
    return el

async def login_public(page, user: str, pw: str):
    await page.goto(HOME_URL, wait_until="domcontentloaded")
    await jitter(page)
    await dump(page, "00_home_loaded")

    # IMPORTANT: target the PUBLIC login fields (not Telerik hidden ones)
    user_el = await must_visible(page,
        ["#cplMain_txtPublicUserName", "input[id$='txtPublicUserName']"],
        "public_username"
    )
    pass_el = await must_visible(page,
        ["#cplMain_txtPublicPassword", "input[id$='txtPublicPassword']"],
        "public_password"
    )
    btn_el = await must_visible(page,
        ["#cplMain_btnPublicLogin", "input[id$='btnPublicLogin']", "button:has-text('Login')"],
        "public_login_button"
    )

    await user_el.fill(user, timeout=ACTION_TIMEOUT_MS)
    await pass_el.fill(pw, timeout=ACTION_TIMEOUT_MS)
    await jitter(page, 300, 700)

    # click + allow postback/navigation
    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
            await btn_el.click(timeout=ACTION_TIMEOUT_MS)
    except PWTimeout:
        # some installs don’t “navigate”; they update in-place. Continue.
        pass

    await jitter(page, 600, 1400)
    await dump(page, "01_after_login_attempt")

    # verify login “sticks” by going to search page
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await jitter(page)
    await dump(page, "10_search_loaded")

    # If bounced back to login/home, we’ll be missing the search controls.
    # We detect that by checking the Search-By dropdown existence.
    dd = await first_visible(page, ["select#cplMain_ddSearchBy", "select#ctl00_cplMain_ddSearchBy"])
    if not dd:
        await dump(page, "FAIL_not_logged_in_or_no_search_controls")
        raise RuntimeError("Login did not persist OR search controls not present (see FAIL_not_logged_in_or_no_search_controls.*).")

async def run_search(page, issued_date: str):
    # Find search controls (IDs vary slightly)
    dd_by = await must_visible(page, ["select#cplMain_ddSearchBy", "select#ctl00_cplMain_ddSearchBy"], "search_by")
    dd_op = await must_visible(page, ["select#cplMain_ddSearchOper", "select#ctl00_cplMain_ddSearchOperator"], "search_operator")
    txt   = await must_visible(page, ["#cplMain_txtSearchString", "#ctl00_cplMain_txtSearchString", "input[type='text']"], "search_value")

    # set dropdowns by label (more stable than value)
    await dd_by.select_option(label="ISSUED")
    await dd_op.select_option(label="Equals")
    await txt.fill(issued_date, timeout=ACTION_TIMEOUT_MS)

    await dump(page, "11_search_filled")

    btn = await must_visible(page,
        ["#cplMain_btnSearch", "#ctl00_cplMain_btnSearch", "input[value='Search']", "button:has-text('Search')"],
        "search_button"
    )

    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
            await btn.click(timeout=ACTION_TIMEOUT_MS)
    except PWTimeout:
        pass

    await jitter(page, 900, 1800)
    await dump(page, "20_results_loaded")

async def find_results_table(page):
    # Prefer Telerik grid IDs if present
    grid = await first_visible(page, ["table[id*='rgSearchRslts']", "table[id*='rgResults']", "table"])
    if not grid:
        return None

    # Score all tables by whether they contain header tokens
    tables = page.locator("table")
    best = None
    best_score = -1

    wanted = ["PERMIT_NO", "ISSUED", "SITE_ADDR", "STATUS"]
    for i in range(await tables.count()):
        t = tables.nth(i)
        try:
            txt = (await t.inner_text()).upper()
        except Exception:
            continue
        score = sum(1 for w in wanted if w in txt)
        if score > best_score:
            best_score = score
            best = t

    if not best or best_score < 2:
        return None
    return best

async def parse_table(table):
    # Extract header + rows
    headers = []
    ths = table.locator("th")
    if await ths.count():
        for i in range(await ths.count()):
            h = " ".join((await ths.nth(i).inner_text()).split())
            headers.append(h)

    rows = []
    trs = table.locator("tr")
    for r in range(await trs.count()):
        tds = trs.nth(r).locator("td")
        if await tds.count() == 0:
            continue
        cells = []
        for c in range(await tds.count()):
            cells.append(" ".join((await tds.nth(c).inner_text()).split()))
        if any(cells):
            rows.append(cells)
    return headers, rows

async def main():
    user = (os.getenv("ETRAKIT_USER") or "").strip()
    pw   = (os.getenv("ETRAKIT_PASS") or "").strip()
    if not user or not pw:
        raise RuntimeError("Missing secrets ETRAKIT_USER / ETRAKIT_PASS.")

    os.makedirs(OUT_DIR, exist_ok=True)
    issued = yesterday_mmddyyyy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        # Hard caps at the page level
        page.set_default_timeout(STEP_TIMEOUT_MS)
        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

        try:
            await login_public(page, user, pw)
            await run_search(page, issued)

            table = await find_results_table(page)
            if not table:
                await dump(page, "FAIL_no_results_table")
                raise RuntimeError("Permit results table not found (see FAIL_no_results_table.* and 20_results_loaded.*).")

            headers, rows = await parse_table(table)

            # naive column mapping (matches your visible table order)
            out = []
            for cells in rows:
                permit_no = cells[0] if len(cells) > 0 else ""
                issued_dt = cells[1] if len(cells) > 1 else issued
                permit_type = cells[2] if len(cells) > 2 else ""
                status = cells[3] if len(cells) > 3 else ""
                site_apn = cells[4] if len(cells) > 4 else ""
                site_addr = cells[5] if len(cells) > 5 else ""

                out.append({
                    "source": "etrakit",
                    "jurisdiction": "Greenville County",
                    "issued_date": issued_dt,
                    "project": {
                        "permit_no": permit_no,
                        "permit_type": permit_type,
                        "status": status,
                        "site_apn": site_apn,
                        "address": site_addr,
                        "description": permit_type,
                        "value": None
                    },
                    "fingerprint": fp(issued_dt, permit_no, site_addr, permit_type, status),
                    "source_url": SEARCH_URL,
                    "scraped_at": now_iso(),
                    "confidence": 0.75
                })

            path = f"{OUT_DIR}/{run_date()}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"issued_query": issued, "headers": headers, "records": out}, f, indent=2, ensure_ascii=False)

            print(f"OK issued={issued} rows={len(out)} wrote={path}")

        finally:
            await dump(page, "99_final_state")
            await ctx.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
