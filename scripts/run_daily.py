import os
import json
import hashlib
import re
import asyncio
from datetime import datetime, timezone, timedelta

from playwright.async_api import async_playwright


BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def fp(issued_date, permit_no, address, permit_type, status):
    base = f"{issued_date}|{permit_no}|{address}|{permit_type}|{status}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def local_yesterday_mdy():
    # Greenville is Eastern; GitHub runner is UTC. This is "good enough" for daily runs.
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")


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


async def login_public(page, username: str, password: str):
    # Load login page
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_login_loaded")
    await dump_html(page, "00_login_loaded")

    # IMPORTANT: select "Public" user type (your artifacts show default is Contractor)
    user_type = page.locator("#ucLogin_ddlUserType")
    await user_type.wait_for(state="visible", timeout=30000)
    await user_type.select_option(label="Public")

    # Fill credentials (IDs confirmed from your 00_login_loaded.html)
    user = page.locator("#ucLogin_RadTextBox2")
    pw = page.locator("#ucLogin_txtPassword")
    await user.wait_for(state="visible", timeout=30000)
    await pw.wait_for(state="visible", timeout=30000)

    await user.fill(username)
    await pw.fill(password)

    # Click login button (ID confirmed from your HTML)
    btn = page.locator("#ucLogin_btnLogin")
    await btn.click()

    # Wait for navigation / settled state
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)

    await snap(page, "01_after_login")
    await dump_html(page, "01_after_login")

    # Quick sanity check: if the page still contains "Invalid Contractor Login" or similar, fail loudly
    body_text = (await page.locator("body").inner_text()).lower()
    if "invalid contractor login" in body_text:
        raise RuntimeError("Login failed because page indicates 'Invalid Contractor Login' (user type selection may not have applied).")
    if "invalid" in body_text and "login" in body_text:
        # generic login failure
        raise RuntimeError("Login appears to have failed (page contains 'invalid' and 'login').")


async def run_search_and_extract(page, issued_date_mdy: str):
    # Go directly to the permit search page
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(500)

    await snap(page, "10_search_page_loaded")
    await dump_html(page, "10_search_page_loaded")

    # Controls confirmed from your search page HTML (10_search_page_loaded.html)
    dd_by = page.locator("#cplMain_ddSearchBy")
    dd_op = page.locator("#cplMain_ddSearchOper")
    txt = page.locator("#cplMain_txtSearchString")
    btn = page.locator("#cplMain_btnSearch")

    await dd_by.wait_for(state="visible", timeout=30000)
    await dd_op.wait_for(state="visible", timeout=30000)
    await txt.wait_for(state="visible", timeout=30000)

    await dd_by.select_option(label="ISSUED")
    await dd_op.select_option(label="Equals")
    await txt.fill(issued_date_mdy)

    await btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)

    await snap(page, "20_results_loaded")
    await dump_html(page, "20_results_loaded")

    # Find the results table by headers (matches what you see: PERMIT_NO, ISSUED, Permit Type, STATUS, SITE_APN, SITE_ADDR)
    tables = page.locator("table")
    table_count = await tables.count()

    best_table = None
    best_score = -1

    wanted = ["PERMIT_NO", "ISSUED", "PERMIT TYPE", "STATUS", "SITE_APN", "SITE_ADDR"]

    for i in range(table_count):
        t = tables.nth(i)
        txt_all = (await t.inner_text()).upper()

        score = sum(1 for w in wanted if w in txt_all)
        # Prefer tables that include multiple expected headers
        if score > best_score:
            best_score = score
            best_table = t

    if not best_table or best_score < 3:
        # Save a small diagnostic report
        report = []
        for i in range(min(table_count, 15)):
            t = tables.nth(i)
            tt = (await t.inner_text()).strip().replace("\n", " ")
            report.append({"table_index": i, "preview": tt[:240]})
        with open("data/99_table_debug.json", "w", encoding="utf-8") as f:
            json.dump({"table_count": table_count, "best_score": best_score, "tables": report}, f, indent=2)
        await snap(page, "99_final_state")
        await dump_html(page, "99_final_state")
        raise RuntimeError("Could not confidently identify the permit results table. See data/99_table_debug.json and screenshots.")

    # Extract rows
    rows = []
    trs = best_table.locator("tr")
    tr_count = await trs.count()

    for r in range(tr_count):
        tr = trs.nth(r)
        tds = tr.locator("td")
        if await tds.count() == 0:
            continue
        cells = []
        for c in range(await tds.count()):
            cell = (await tds.nth(c).inner_text()).strip()
            cell = " ".join(cell.split())
            cells.append(cell)
        if cells:
            rows.append(cells)

    return rows


async def main():
    username = os.getenv("ETRAKIT_USER", "").strip()
    password = os.getenv("ETRAKIT_PASS", "").strip()
    if not username or not password:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (GitHub Secrets).")

    os.makedirs("data", exist_ok=True)

    issued_date = local_yesterday_mdy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Login (Public)
        await login_public(page, username, password)

        # Search & extract
        rows = await run_search_and_extract(page, issued_date)

        await browser.close()

    # Map extracted columns based on what your table shows (PERMIT_NO, ISSUED, Permit Type, STATUS, SITE_APN, SITE_ADDR)
    out = []
    for cells in rows:
        permit_no = cells[0] if len(cells) > 0 else ""
        issued = cells[1] if len(cells) > 1 else issued_date
        permit_type = cells[2] if len(cells) > 2 else ""
        status = cells[3] if len(cells) > 3 else ""
        site_apn = cells[4] if len(cells) > 4 else ""
        site_addr = cells[5] if len(cells) > 5 else ""

        rec = {
            "source": "etrakit",
            "jurisdiction": "Greenville County",
            "issued_date": issued,
            "project": {
                "permit_no": permit_no,
                "permit_type": permit_type,
                "status": status,
                "site_apn": site_apn,
                "address": site_addr,
            },
            "fingerprint": fp(issued, permit_no, site_addr, permit_type, status),
            "source_url": SEARCH_URL,
            "scraped_at": now_iso(),
            "confidence": 0.75,
        }
        out.append(rec)

    run_date = datetime.now(timezone.utc).date().isoformat()
    path = f"data/{run_date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Issued date searched: {issued_date}")
    print(f"Wrote {len(out)} records -> {path}")


if __name__ == "__main__":
    asyncio.run(main())
    
