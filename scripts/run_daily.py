import os
import re
import json
import hashlib
import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/login.aspx?lt=either&rd=~/Search/permit.aspx"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

OUT_DIR = "data"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def eastern_yesterday_mmddyyyy() -> str:
    # Greenville County is Eastern Time
    et = ZoneInfo("America/New_York")
    d = datetime.now(et).date() - timedelta(days=1)
    return d.strftime("%m/%d/%Y")


def run_date_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def fp(issued_date: str, permit_no: str, address: str, desc: str) -> str:
    base = f"{issued_date}|{permit_no}|{address}|{desc}".upper().encode("utf-8", errors="ignore")
    return hashlib.sha256(base).hexdigest()


def debris_signal(desc: str):
    d = (desc or "").upper()
    hits = []
    for k in ["DEMO", "DEMOL", "TEAR", "ROOF", "REMODEL", "RENOV", "ADDITION", "CONCRETE", "REMOVE"]:
        if k in d:
            hits.append(k)
    if not hits:
        return []
    return [{"name": "debris_generation", "confidence": 0.7, "evidence": hits[:4]}]


async def snap(page, stem: str):
    """Write BOTH html + screenshot. Never throws."""
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        html = await page.content()
        with open(os.path.join(OUT_DIR, f"{stem}.html"), "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass
    try:
        await page.screenshot(path=os.path.join(OUT_DIR, f"{stem}.png"), full_page=True)
    except Exception:
        pass


async def fill_best_effort(page, selector: str, value: str):
    """
    Prefer a visible element; fallback to force-fill the first match.
    """
    loc_vis = page.locator(f"{selector}:visible")
    if await loc_vis.count() > 0:
        await loc_vis.first.fill(value)
        return

    loc = page.locator(selector)
    if await loc.count() == 0:
        raise RuntimeError(f"Could not find element for selector: {selector}")

    # Telerik sometimes keeps the actual input hidden; force-fill is a practical workaround.
    await loc.first.fill(value, force=True)


async def click_best_effort(page, selector: str):
    loc_vis = page.locator(f"{selector}:visible")
    if await loc_vis.count() > 0:
        await loc_vis.first.click()
        return

    loc = page.locator(selector)
    if await loc.count() == 0:
        raise RuntimeError(f"Could not find clickable element for selector: {selector}")
    await loc.first.click(force=True)


async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_login_loaded")

    # If there's a login-type dropdown (Public/Contractor/etc), pick Public if available.
    # On your captured HTML, it was name="ucLogin$ddlSelLogin".
    ddl = page.locator("select[name='ucLogin$ddlSelLogin']")
    if await ddl.count() > 0:
        # try to select "Public" if it exists; otherwise leave default
        options = await ddl.first.locator("option").all_inner_texts()
        options_norm = [o.strip().lower() for o in options]
        if "public" in options_norm:
            await ddl.first.select_option(label=options[options_norm.index("public")])
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(500)

    # Fill username/password using the stable IDs from the HTML you uploaded
    await fill_best_effort(page, "input#ucLogin_txtLoginId", username)
    await fill_best_effort(page, "input#ucLogin_txtPassword", password)

    # Click login button (stable ID from your HTML: ucLogin_btnLogin)
    await click_best_effort(page, "#ucLogin_btnLogin, input#ucLogin_btnLogin, button#ucLogin_btnLogin")

    # Wait for navigation / search page
    try:
        await page.wait_for_load_state("networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        pass

    await page.wait_for_timeout(800)
    await snap(page, "01_after_login")

    # Hard-assert we are not still on login page
    url = page.url.lower()
    if "login.aspx" in url:
        # Sometimes it "logs in" but stays here with an error message rendered.
        # Capture final state for inspection.
        raise RuntimeError("Login did not complete (still on login.aspx). Check data/01_after_login.* for on-page error text.")


async def run_search_and_extract(page, issued_mmddyyyy: str):
    # Go directly to the permit search page (works even if landing page differs)
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "10_search_page_loaded")

    # Select: Search By = ISSUED
    await page.locator("select#cplMain_ddSearchBy").select_option(value="Permit_Main.ISSUED")
    # Select: Operator = Equals
    await page.locator("select#cplMain_ddSearchOper").select_option(value="EQUALS")
    # Fill the search string (mm/dd/yyyy)
    await fill_best_effort(page, "input#cplMain_txtSearchString", issued_mmddyyyy)

    # Click Search
    await click_best_effort(page, "input[name='ctl00$cplMain$btnSearch'], #cplMain_btnSearch, #ctl00_cplMain_btnSearch")

    # Wait for results to render
    try:
        await page.wait_for_load_state("networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        pass

    await page.wait_for_timeout(1200)
    await snap(page, "20_after_search")

    # Results grid (Telerik) typically contains a table with id including rgSearchRslts
    table = page.locator("table[id*='rgSearchRslts']").first
    if await table.count() == 0:
        await snap(page, "99_final_state")
        raise RuntimeError("Results table not found. See data/20_after_search.* and data/99_final_state.*")

    # Extract headers
    headers = []
    ths = table.locator("th")
    for i in range(await ths.count()):
        t = (await ths.nth(i).inner_text()).strip()
        if t:
            headers.append(re.sub(r"\s+", " ", t))

    # Extract rows
    rows = []
    trs = table.locator("tr")
    for r in range(await trs.count()):
        tr = trs.nth(r)
        tds = tr.locator("td")
        if await tds.count() == 0:
            continue
        cells = []
        for c in range(await tds.count()):
            txt = (await tds.nth(c).inner_text()).strip()
            txt = re.sub(r"\s+", " ", txt)
            cells.append(txt)
        # Skip empty rows
        if any(cells):
            rows.append(cells)

    return headers, rows


async def main():
    user = os.getenv("ETRAKIT_USER", "").strip()
    pw = os.getenv("ETRAKIT_PASS", "").strip()
    if not user or not pw:
        raise RuntimeError("Missing secrets: set ETRAKIT_USER and ETRAKIT_PASS in GitHub Actions secrets.")

    os.makedirs(OUT_DIR, exist_ok=True)

    issued = eastern_yesterday_mmddyyyy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login(page, user, pw)
            headers, rows = await run_search_and_extract(page, issued)

            # Expected column order from your screenshot:
            # PERMIT_NO | ISSUED | Permit Type | STATUS | SITE_APN | SITE_ADDR
            # We'll map by header text when possible, fallback to positions.
            # Normalize headers for matching
            hnorm = [h.upper().replace(" ", "_") for h in headers]
            def idx(name, fallback):
                try:
                    return hnorm.index(name)
                except ValueError:
                    return fallback

            i_permit = idx("PERMIT_NO", 0)
            i_issued = idx("ISSUED", 1)
            i_type = idx("PERMITTYPE", 2) if "PERMITTYPE" in hnorm else idx("PERMIT_TYPE", 2)
            i_status = idx("STATUS", 3)
            i_apn = idx("SITE_APN", 4)
            i_addr = idx("SITE_ADDR", 5)

            out = []
            for cells in rows:
                permit_no = cells[i_permit] if len(cells) > i_permit else ""
                issued_date = cells[i_issued] if len(cells) > i_issued else issued
                permit_type = cells[i_type] if len(cells) > i_type else None
                status = cells[i_status] if len(cells) > i_status else None
                site_apn = cells[i_apn] if len(cells) > i_apn else None
                address = cells[i_addr] if len(cells) > i_addr else ""

                desc = permit_type or ""

                rec = {
                    "source": "etrakit",
                    "jurisdiction": "Greenville County",
                    "issued_date": issued_date,
                    "project": {
                        "address": address,
                        "description": desc,
                        "permit_type": permit_type,
                        "status": status,
                        "site_apn": site_apn,
                        "value": None
                    },
                    "contractor": {"name": None, "phone": None, "license": None},
                    "owner": {"name": None, "address": None},
                    "signals": debris_signal(desc),
                    "fingerprint": fp(issued_date or "", permit_no or "", address or "", desc or ""),
                    "source_url": SEARCH_URL,
                    "scraped_at": now_iso(),
                    "confidence": 0.75
                }
                out.append(rec)

            path = os.path.join(OUT_DIR, f"{run_date_utc()}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)

            print(f"ISSUED={issued} rows={len(rows)} wrote={path}")

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
