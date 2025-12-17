import os, json, hashlib, random, asyncio
from datetime import datetime, timezone, timedelta
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/login.aspx?lt=either&rd=~/Search/permit.aspx"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"
OUT_DIR = "data"

# Fail-fast caps
DEFAULT_TIMEOUT_MS = 6000
NAV_TIMEOUT_MS = 12000
ACTION_TIMEOUT_MS = 2500

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def run_date():
    return datetime.now(timezone.utc).date().isoformat()

def yesterday_mmddyyyy():
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

def fp(*parts: str):
    base = "|".join([(p or "") for p in parts]).upper().encode("utf-8", errors="ignore")
    return hashlib.sha256(base).hexdigest()

async def jitter(page, a=250, b=650):
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

async def visible(page, selector: str):
    loc = page.locator(selector)
    try:
        if await loc.count() == 0:
            return None
        if await loc.first.is_visible():
            return loc.first
    except Exception:
        return None
    return None

async def must_visible(page, selector: str, name: str):
    loc = page.locator(selector)
    try:
        await loc.first.wait_for(state="visible", timeout=DEFAULT_TIMEOUT_MS)
        return loc.first
    except Exception:
        await dump(page, f"FAIL_missing_{name}")
        raise RuntimeError(f"Missing/hidden element: {name} ({selector})")

async def login_middle_public(page, user: str, pw: str) -> bool:
    # These exist in your zip's 00_login_loaded.html
    u = await visible(page, "#cplMain_txtPublicUserName, input[id$='txtPublicUserName']")
    p = await visible(page, "#cplMain_txtPublicPassword, input[id$='txtPublicPassword']")
    b = await visible(page, "#cplMain_btnPublicLogin, input[id$='btnPublicLogin'], button:has-text('Login')")

    if not (u and p and b):
        return False

    await u.fill(user, timeout=ACTION_TIMEOUT_MS)
    await p.fill(pw, timeout=ACTION_TIMEOUT_MS)
    await jitter(page)

    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
            await b.click(timeout=ACTION_TIMEOUT_MS)
    except PWTimeout:
        await b.click(timeout=ACTION_TIMEOUT_MS)

    await jitter(page)
    await dump(page, "01_after_middle_login_click")
    return True

async def select_public_in_header_dropdown(page):
    # IMPORTANT: your zip shows the dropdown has ID but no name attribute
    ddl = await must_visible(page, "select#ucLogin_ddlSelLogin", "login_type_dropdown")

    opts = ddl.locator("option")
    n = await opts.count()
    chosen_value = None
    chosen_label = None

    for i in range(n):
        opt = opts.nth(i)
        label = ((await opt.inner_text()) or "").strip()
        value = ((await opt.get_attribute("value")) or "").strip()
        if "public" in (label.lower() + " " + value.lower()):
            chosen_value = value if value else None
            chosen_label = label if label else None
            break

    if not (chosen_value or chosen_label):
        await dump(page, "FAIL_no_public_option")
        raise RuntimeError("Could not find a 'Public' option in header dropdown.")

    if chosen_value:
        await ddl.select_option(value=chosen_value)
    else:
        await ddl.select_option(label=chosen_label)

    await jitter(page)
    try:
        await page.wait_for_load_state("networkidle", timeout=6000)
    except PWTimeout:
        pass

async def login_header(page, user: str, pw: str):
    await select_public_in_header_dropdown(page)
    await dump(page, "01_after_header_public_selected")

    u = await must_visible(page, "input#ucLogin_txtLoginId", "header_username")
    p = await must_visible(page, "input#ucLogin_txtPassword", "header_password")
    b = await must_visible(page, "input#ucLogin_btnLogin, button#ucLogin_btnLogin", "header_login_button")

    if await u.is_disabled() or await p.is_disabled():
        await dump(page, "FAIL_header_inputs_disabled")
        raise RuntimeError("Header inputs disabled after selecting Public.")

    await u.fill(user, timeout=ACTION_TIMEOUT_MS)
    await p.fill(pw, timeout=ACTION_TIMEOUT_MS)
    await jitter(page)

    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
            await b.click(timeout=ACTION_TIMEOUT_MS)
    except PWTimeout:
        await b.click(timeout=ACTION_TIMEOUT_MS)

    await jitter(page)
    await dump(page, "02_after_header_login_click")

async def assert_logged_in(page):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await jitter(page)
    await dump(page, "10_search_loaded")

    dd = page.locator("select#cplMain_ddSearchBy, select#ctl00_cplMain_ddSearchBy")
    if await dd.count() == 0:
        await dump(page, "FAIL_login_not_persisted")
        raise RuntimeError("Login did not persist (search controls missing).")

async def run_search(page, issued_date: str):
    dd_by = await must_visible(page, "select#cplMain_ddSearchBy, select#ctl00_cplMain_ddSearchBy", "search_by")
    dd_op = await must_visible(page, "select#cplMain_ddSearchOper, select#ctl00_cplMain_ddSearchOper", "search_operator")
    txt   = await must_visible(page, "#cplMain_txtSearchString, #ctl00_cplMain_txtSearchString", "search_value")

    await dd_by.select_option(label="ISSUED")
    await dd_op.select_option(label="Equals")
    await txt.fill(issued_date, timeout=ACTION_TIMEOUT_MS)
    await dump(page, "11_search_filled")

    btn = await must_visible(page,
        "#cplMain_btnSearch, #ctl00_cplMain_btnSearch, input[value='Search'], button:has-text('Search')",
        "search_button"
    )

    try:
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS):
            await btn.click(timeout=ACTION_TIMEOUT_MS)
    except PWTimeout:
        await btn.click(timeout=ACTION_TIMEOUT_MS)

    await jitter(page, 700, 1400)
    await dump(page, "20_results_loaded")

async def find_results_table(page):
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
    headers = []
    ths = table.locator("th")
    if await ths.count():
        for i in range(await ths.count()):
            headers.append(" ".join((await ths.nth(i).inner_text()).split()))
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
        page.set_default_timeout(DEFAULT_TIMEOUT_MS)
        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

        try:
            await page.goto(LOGIN_URL, wait_until="domcontentloaded")
            await jitter(page)
            await dump(page, "00_login_loaded")

            used_middle = await login_middle_public(page, user, pw)
            if not used_middle:
                await login_header(page, user, pw)

            await assert_logged_in(page)
            await run_search(page, issued)

            table = await find_results_table(page)
            if not table:
                await dump(page, "FAIL_no_results_table")
                raise RuntimeError("Results table not found (see FAIL_no_results_table.* and 20_results_loaded.*).")

            headers, rows = await parse_table(table)

            out = []
            for cells in rows:
                permit_no   = cells[0] if len(cells) > 0 else ""
                issued_dt   = cells[1] if len(cells) > 1 else issued
                permit_type = cells[2] if len(cells) > 2 else ""
                status      = cells[3] if len(cells) > 3 else ""
                site_apn    = cells[4] if len(cells) > 4 else ""
                site_addr   = cells[5] if len(cells) > 5 else ""

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
                        "value": None,
                    },
                    "fingerprint": fp(issued_dt, permit_no, site_addr, permit_type, status),
                    "source_url": SEARCH_URL,
                    "scraped_at": now_iso(),
                    "confidence": 0.75,
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
