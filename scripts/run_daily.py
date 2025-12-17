import os
import re
import json
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from playwright.async_api import async_playwright


BASE = "https://grvlc-trk.aspgov.com"

# This is the login URL you showed (redirects to permit search after login)
LOGIN_URL = f"{BASE}/eTRAKiT/login.aspx?lt=either&rd=~/Search/permit.aspx"
PERMIT_SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"


def yesterday_mmddyyyy_tz(tz_name: str = "America/New_York") -> str:
    now = datetime.now(ZoneInfo(tz_name))
    y = now - timedelta(days=1)
    return y.strftime("%m/%d/%Y")


async def snap(page, name: str):
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


async def login_public_portal(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(500)
    await snap(page, "00_login_loaded")

    # IMPORTANT: Use the *Public Login box in the middle* (cplMain_* ids),
    # not the header Telerik/Rad login fields.
    user = page.locator("#cplMain_txtPublicUserName")
    pw = page.locator("#cplMain_txtPublicPassword")
    btn = page.locator("#cplMain_btnPublicLogin")

    await user.wait_for(state="visible", timeout=15000)
    await pw.wait_for(state="visible", timeout=15000)
    await btn.wait_for(state="visible", timeout=15000)

    await user.fill(username)
    await pw.fill(password)

    # Click login and wait for navigation / logged-in markers
    async with page.expect_navigation(wait_until="domcontentloaded", timeout=20000):
        await btn.click()

    await page.wait_for_timeout(800)
    await snap(page, "01_after_login")

    # Confirm we're actually logged in
    # The permit search page shows "LOGGED IN AS: RIDGE DEVUONO" (from your screenshot).
    # If this doesn't appear, treat as login failure.
    logged_in_marker = page.locator("text=LOGGED IN AS").first
    logout_marker = page.locator("text=LOG OUT").first

    if await logged_in_marker.count() == 0 and await logout_marker.count() == 0:
        # Sometimes it lands on a page that still requires redirect; try direct.
        await page.goto(PERMIT_SEARCH_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(800)

    await snap(page, "02_after_login_or_redirect")

    if await logged_in_marker.count() == 0 and await logout_marker.count() == 0:
        raise RuntimeError("Login did not appear successful (no LOGGED IN AS / LOG OUT found).")


async def run_permit_search(page, issued_date_mmddyyyy: str):
    # Ensure we're on the permit search page
    await page.goto(PERMIT_SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "10_search_page_loaded")

    # These IDs exist on the permit search HTML from your successful logged-in artifact:
    # - Search By dropdown: cplMain_ddSearchBy
    # - Operator dropdown: cplMain_ddSearchOper
    # - Search string input: cplMain_txtSearchString
    # - Search button: ctl00_cplMain_btnSearch
    search_by = page.locator("#cplMain_ddSearchBy")
    oper = page.locator("#cplMain_ddSearchOper")
    val = page.locator("#cplMain_txtSearchString")
    btn = page.locator("#ctl00_cplMain_btnSearch")

    await search_by.wait_for(state="visible", timeout=15000)
    await oper.wait_for(state="visible", timeout=15000)
    await val.wait_for(state="visible", timeout=15000)
    await btn.wait_for(state="visible", timeout=15000)

    # Select ISSUED in "Search By"
    # Option values on the site are uppercase like ISSUED, PERMIT_NO, etc.
    # We'll try by value first, then by label fallback.
    try:
        await search_by.select_option(value="ISSUED")
    except Exception:
        # Fallback: find any option whose label contains "issued"
        options = await search_by.locator("option").all()
        chosen = None
        for opt in options:
            t = (await opt.text_content()) or ""
            v = (await opt.get_attribute("value")) or ""
            if "issued" in t.lower() or v.upper() == "ISSUED":
                chosen = v
                break
        if not chosen:
            raise RuntimeError("Could not find ISSUED option in Search By dropdown.")
        await search_by.select_option(value=chosen)

    # Operator stays equals (but enforce it)
    try:
        await oper.select_option(value="EQUALS")
    except Exception:
        # Fallback by label
        await oper.select_option(label="Equals")

    # Fill date
    await val.fill(issued_date_mmddyyyy)

    await snap(page, "11_before_search_click")

    # Click search and wait until results area appears
    await btn.click()
    await page.wait_for_timeout(1200)
    await snap(page, "12_after_search_click")

    # Wait for the results table headers you showed: PERMIT_NO / ISSUED / Permit Type / STATUS / SITE_APN / SITE_ADDR
    # Sometimes header casing varies; check loosely.
    await page.wait_for_timeout(800)
    await snap(page, "13_post_wait")

    # Try to locate a table that contains PERMIT_NO and ISSUED somewhere near the header row
    tables = page.locator("table")
    tcount = await tables.count()

    def looks_like_header(txt: str) -> bool:
        txt_u = txt.upper()
        return ("PERMIT_NO" in txt_u) and ("ISSUED" in txt_u) and ("SITE_ADDR" in txt_u)

    found_table_index = None
    for i in range(tcount):
        txt = (await tables.nth(i).inner_text()) or ""
        if looks_like_header(txt):
            found_table_index = i
            break

    if found_table_index is None:
        # Dump a quick report for debugging
        report = {"issued_date": issued_date_mmddyyyy, "table_count": tcount, "tables": []}
        for i in range(min(tcount, 25)):
            txt = (await tables.nth(i).inner_text()) or ""
            preview = " ".join(txt.split())[:240]
            report["tables"].append({"i": i, "preview": preview})
        os.makedirs("data", exist_ok=True)
        with open("data/20_table_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        raise RuntimeError("Results table not found after search. See data/12_after_search_click.png and data/20_table_report.json")

    result_table = tables.nth(found_table_index)

    # Extract rows (best-effort)
    headers = []
    rows = []
    try:
        # Try header cells
        header_cells = result_table.locator("tr").first.locator("th,td")
        hc = await header_cells.count()
        headers = [((await header_cells.nth(j).inner_text()) or "").strip() for j in range(hc)]
        headers = [h for h in headers if h]
    except Exception:
        headers = []

    try:
        tr = result_table.locator("tr")
        rc = await tr.count()
        for r in range(1, min(rc, 2000)):  # skip header
            tds = tr.nth(r).locator("td")
            c = await tds.count()
            if c == 0:
                continue
            row = [((await tds.nth(j).inner_text()) or "").strip() for j in range(c)]
            if any(row):
                rows.append(row)
    except Exception:
        pass

    os.makedirs("data", exist_ok=True)
    with open("data/30_results.json", "w", encoding="utf-8") as f:
        json.dump({"date": issued_date_mmddyyyy, "headers": headers, "rows": rows}, f, indent=2)

    await snap(page, "14_results_detected")

    # OPTIONAL: Click "EXPORT TO EXCEL" if present (this is usually the cleanest output)
    # If it exists, save it as data/permits.xlsx
    try:
        export = page.locator("text=EXPORT TO EXCEL").first
        if await export.count() > 0:
            async with page.expect_download(timeout=15000) as dl_info:
                await export.click()
            dl = await dl_info.value
            await dl.save_as("data/permits.xlsx")
    except Exception:
        # Not fatal
        pass

    # OPTIONAL: try to click next page once (since you often see 2 pages)
    # This is best-effort; export-to-excel is preferred.
    try:
        # common pager patterns: ">" or "Next" links/buttons
        next_candidates = [
            page.locator("a[title*='Next' i]").first,
            page.locator("a:has-text('>')").first,
            page.locator("button:has-text('>')").first,
            page.locator("a:has-text('Next')").first,
        ]
        for nxt in next_candidates:
            if await nxt.count() > 0:
                await nxt.click()
                await page.wait_for_timeout(1200)
                await snap(page, "15_page2")
                break
    except Exception:
        pass


async def main():
    user = os.getenv("ETRAKIT_USER", "").strip()
    pw = os.getenv("ETRAKIT_PASS", "").strip()
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS environment variables.")

    issued = yesterday_mmddyyyy_tz("America/New_York")

    os.makedirs("data", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            accept_downloads=True
        )
        page = await context.new_page()

        try:
            await login_public_portal(page, user, pw)
            await run_permit_search(page, issued)
            await snap(page, "99_final_state")
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
