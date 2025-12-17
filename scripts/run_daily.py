import os, json, hashlib
from datetime import datetime, timezone, timedelta
import asyncio
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright


BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def fp(issued_date, permit_no, address, permit_type):
    base = f"{issued_date}|{permit_no}|{address}|{permit_type}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def eastern_yesterday_mmddyyyy():
    tz = ZoneInfo("America/New_York")
    d = datetime.now(tz).date() - timedelta(days=1)
    return d.strftime("%m/%d/%Y")


async def save_debug(page, stem: str):
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
    await page.wait_for_load_state("networkidle")
    await save_debug(page, "00_login_loaded")

    # Select login type = Public (dropdown exists on the login bar)
    # HTML shows: <select id="ctl00_ddlLoginType"> with options like Public/Contractor/etc
    ddl = page.locator("#ctl00_ddlLoginType")
    if await ddl.count() > 0:
        # Prefer selecting by visible label if present
        try:
            await ddl.select_option(label="Public")
        except Exception:
            # Fallback: try value "Public"
            try:
                await ddl.select_option(value="Public")
            except Exception:
                pass

    # Use stable IDs from the page HTML (these exist in your artifact)
    user = page.locator("#ucLogin_txtLoginId")
    pw = page.locator("#ucLogin_txtPassword")
    btn = page.locator("#ucLogin_btnLogin")

    await user.wait_for(state="visible", timeout=60000)
    await pw.wait_for(state="visible", timeout=60000)

    await user.fill(username)
    await pw.fill(password)

    # Click login and wait for navigation / postback
    async with page.expect_navigation(wait_until="networkidle", timeout=60000):
        await btn.click()

    await save_debug(page, "01_after_login")

    # Basic sanity: logged-in UI usually has "LOG OUT" somewhere
    # If not, still proceed, but this gives better failure diagnostics.
    if await page.locator("text=LOG OUT").count() == 0 and await page.locator("text=Log Out").count() == 0:
        # Not fatal; some installs redirect differently. We'll continue.
        pass


async def find_results_table(page):
    """
    eTRAKiT results grid is usually a <table>. We find the one whose text
    contains the expected headers/columns.
    """
    tables = page.locator("table")
    n = await tables.count()

    report = []
    best_idx = None

    for i in range(n):
        t = tables.nth(i)
        try:
            txt = (await t.inner_text()).strip()
        except Exception:
            continue

        head = " ".join(txt.split())[:220]
        up = txt.upper()

        score = 0
        for k in ["PERMIT_NO", "ISSUED", "SITE_ADDR", "SITE_APN", "STATUS"]:
            if k in up:
                score += 1

        rows = 0
        try:
            rows = await t.locator("tr").count()
        except Exception:
            pass

        report.append({"table_index": i, "score": score, "row_count": rows, "preview": head})

        if score >= 3 and rows >= 2:
            best_idx = i
            break

    os.makedirs("data", exist_ok=True)
    with open("data/20_table_report.json", "w", encoding="utf-8") as f:
        json.dump({"table_count": n, "tables": report}, f, indent=2)

    return best_idx


async def scrape_for_date(page, issued_mmddyyyy: str):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await save_debug(page, "10_search_page_loaded")

    # Known IDs from the eTRAKiT search page artifact
    dd_by = page.locator("#ctl00_cplMain_ddSearchBy")
    dd_op = page.locator("#ctl00_cplMain_ddSearchOperator")
    txt = page.locator("#ctl00_cplMain_txtSearchString")
    btn = page.locator("#ctl00_cplMain_btnSearch")

    await dd_by.wait_for(state="visible", timeout=60000)
    await dd_op.wait_for(state="visible", timeout=60000)
    await txt.wait_for(state="visible", timeout=60000)

    # Select ISSUED and Equals
    await dd_by.select_option(label="ISSUED")
    await dd_op.select_option(label="Equals")

    await txt.fill(issued_mmddyyyy)

    async with page.expect_navigation(wait_until="networkidle", timeout=60000):
        await btn.click()

    await save_debug(page, "11_after_search")

    # Find results table
    best_idx = await find_results_table(page)
    if best_idx is None:
        await save_debug(page, "99_final_state")
        raise RuntimeError("Could not locate permit results table. See data/20_table_report.json and data/11_after_search.*")

    table = page.locator("table").nth(best_idx)

    # Parse header row + data rows
    trs = table.locator("tr")
    tr_count = await trs.count()
    if tr_count < 2:
        raise RuntimeError("Results table exists but has no rows.")

    # Header can be <th> or <td>
    header_cells = trs.nth(0).locator("th, td")
    hcount = await header_cells.count()
    headers = []
    for i in range(hcount):
        headers.append(" ".join((await header_cells.nth(i).inner_text()).split()))

    # Build index map (case-insensitive)
    norm = {h.upper().strip(): idx for idx, h in enumerate(headers)}

    def get_cell(cells, key, fallback_idx=None):
        idx = norm.get(key)
        if idx is None:
            idx = fallback_idx
        if idx is None or idx >= len(cells):
            return ""
        return cells[idx]

    rows = []
    for r in range(1, tr_count):
        tds = trs.nth(r).locator("td")
        c = await tds.count()
        if c == 0:
            continue
        cells = []
        for i in range(c):
            cells.append(" ".join((await tds.nth(i).inner_text()).split()))
        rows.append(cells)

    return headers, rows


def debris_signal(text: str):
    d = (text or "").upper()
    hits = []
    for k in ["DEMO", "DEMOL", "TEAR", "ROOF", "REMODEL", "RENOV", "ADDITION", "CONCRETE", "REMOVE"]:
        if k in d:
            hits.append(k)
    if not hits:
        return []
    return [{"name": "debris_generation", "confidence": 0.7, "evidence": hits[:4]}]


async def main():
    user = os.environ.get("ETRAKIT_USER", "").strip()
    pw = os.environ.get("ETRAKIT_PASS", "").strip()
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (GitHub Secrets).")

    issued = eastern_yesterday_mmddyyyy()
    run_date = datetime.now(timezone.utc).date().isoformat()
    os.makedirs("data", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await login(page, user, pw)
        headers, rows = await scrape_for_date(page, issued)

        # Write parsed leads
        out = []
        for cells in rows:
            permit_no = cells[0] if len(cells) > 0 else ""
            issued_date = issued
            permit_type = cells[2] if len(cells) > 2 else ""
            status = cells[3] if len(cells) > 3 else ""
            site_apn = cells[4] if len(cells) > 4 else ""
            site_addr = cells[5] if len(cells) > 5 else ""

            rec = {
                "source": "etrakit",
                "jurisdiction": "Greenville County",
                "issued_date": issued_date,
                "permit_no": permit_no,
                "project": {
                    "address": site_addr,
                    "description": permit_type,
                    "permit_type": permit_type,
                    "status": status,
                    "apn": site_apn,
                },
                "signals": debris_signal(permit_type),
                "fingerprint": fp(issued_date, permit_no, site_addr, permit_type),
                "source_url": SEARCH_URL,
                "scraped_at": now_iso(),
                "confidence": 0.8
            }
            out.append(rec)

        path = f"data/{run_date}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "issued_query": issued,
                    "headers": headers,
                    "records": out
                },
                f,
                indent=2,
                ensure_ascii=False
            )

        print(f"Issued query: {issued}")
        print(f"Parsed {len(out)} permits -> {path}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
