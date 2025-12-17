import os, json, hashlib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import asyncio
from playwright.async_api import async_playwright


BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

# ---- helpers ----

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fp(issued_date, permit_no, address, desc):
    base = f"{issued_date}|{permit_no}|{address}|{desc}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()

def local_yesterday_mmddyyyy():
    # GitHub runner is UTC; force Eastern time for Greenville
    eastern = ZoneInfo("America/New_York")
    dt = datetime.now(eastern) - timedelta(days=1)
    return dt.strftime("%m/%d/%Y")

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

# ---- core ----

async def login_and_open_search(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_login_loaded")

    # These IDs come from your 00_loaded.html
    # Login type dropdown: ucLogin_ddlSelLogin (Public / Contractor)
    login_type = page.locator("#ucLogin_ddlSelLogin")
    if await login_type.count() > 0:
        # prefer Public
        try:
            await login_type.select_option(label="Public")
        except Exception:
            pass

    user = page.locator("#ucLogin_txtUserName")
    pw = page.locator("#ucLogin_txtPassword")
    btn = page.locator("#ucLogin_btnLogin")

    if await user.count() == 0 or await pw.count() == 0 or await btn.count() == 0:
        await snap(page, "00_login_missing_controls")
        raise RuntimeError("Login controls not found (IDs changed). See data/00_login_missing_controls.html/png")

    await user.fill(username)
    await pw.fill(password)

    await btn.click()
    await page.wait_for_timeout(1200)
    await page.wait_for_load_state("networkidle")
    await snap(page, "01_after_login")

    # Go straight to permit search page
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await snap(page, "10_search_page_loaded")


async def run_issued_search(page, issued_date_mmddyyyy: str):
    # These IDs come from your 10_search_page_loaded.html
    dd_search_by = page.locator("#cplMain_ddSearchBy")
    dd_oper = page.locator("#cplMain_ddSearchOper")
    txt_value = page.locator("#cplMain_txtSearchString")
    btn_search = page.locator("#cplMain_btnSearch")

    missing = []
    for name, loc in [
        ("cplMain_ddSearchBy", dd_search_by),
        ("cplMain_ddSearchOper", dd_oper),
        ("cplMain_txtSearchString", txt_value),
        ("cplMain_btnSearch", btn_search),
    ]:
        if await loc.count() == 0:
            missing.append(name)

    if missing:
        await snap(page, "11_missing_search_controls")
        raise RuntimeError(f"Search controls missing: {missing}. See data/11_missing_search_controls.html/png")

    # Choose ISSUED / Equals / date
    await dd_search_by.select_option(label="ISSUED")
    try:
        await dd_oper.select_option(label="Equals")
    except Exception:
        # sometimes it's "EQUALS" or similar
        await dd_oper.select_option(index=0)

    await txt_value.fill(issued_date_mmddyyyy)

    # Click search and wait for results to appear
    await btn_search.click()
    await page.wait_for_timeout(1000)
    await page.wait_for_load_state("networkidle")

    # Results page in your screenshot has an "EXPORT TO EXCEL" button.
    # We'll wait briefly for that (or for PERMIT_NO header text).
    export_btn = page.locator("text=EXPORT TO EXCEL")
    permit_hdr = page.locator("text=PERMIT_NO")

    try:
        await export_btn.wait_for(timeout=15000)
    except Exception:
        # fallback: header shows up
        try:
            await permit_hdr.wait_for(timeout=5000)
        except Exception:
            await snap(page, "12_no_results_detected")
            raise RuntimeError("Results did not appear after search. See data/12_no_results_detected.html/png")

    await snap(page, "20_results_loaded")


def extract_results_from_html(html: str):
    """
    Pull rows from the results table by finding the table that contains PERMIT_NO.
    Works even if the site wraps header cells in <td> instead of <th>.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Find a node containing PERMIT_NO and walk up to its table
    marker = soup.find(string=lambda s: isinstance(s, str) and "PERMIT_NO" in s)
    if not marker:
        return [], {"reason": "PERMIT_NO marker not found"}

    table = marker.find_parent("table")
    if not table:
        return [], {"reason": "No ancestor table for PERMIT_NO marker"}

    # Get header row cells
    header_cells = []
    header_row = table.find("tr")
    if header_row:
        cells = header_row.find_all(["th", "td"])
        header_cells = [c.get_text(" ", strip=True) for c in cells]

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        row = [td.get_text(" ", strip=True) for td in tds]
        # skip empty rows
        if any(x.strip() for x in row):
            rows.append(row)

    return rows, {"header": header_cells, "row_count": len(rows)}


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

    username = os.environ.get("ETRAKIT_USER", "").strip()
    password = os.environ.get("ETRAKIT_PASS", "").strip()
    if not username or not password:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (GitHub secrets).")

    issued_date = local_yesterday_mmddyyyy()
    run_date = datetime.now(timezone.utc).date().isoformat()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login_and_open_search(page, username, password)
            await run_issued_search(page, issued_date)

            html = await page.content()
            rows, meta = extract_results_from_html(html)

            # Always write a debug report so artifacts are useful
            with open("data/21_extract_report.json", "w", encoding="utf-8") as f:
                json.dump(
                    {"issued_date": issued_date, "meta": meta, "sample_row": (rows[0] if rows else None)},
                    f,
                    indent=2,
                )

            if not rows:
                await snap(page, "22_no_rows_after_extract")
                raise RuntimeError("Results page loaded, but extraction found 0 rows. See data/21_extract_report.json and data/22_no_rows_after_extract.*")

            # Map columns based on the visible table in your screenshot:
            # PERMIT_NO | ISSUED | Permit Type | STATUS | SITE_APN | SITE_ADDR
            out = []
            for r in rows:
                permit_no = r[0] if len(r) > 0 else ""
                issued = r[1] if len(r) > 1 else issued_date
                permit_type = r[2] if len(r) > 2 else None
                status = r[3] if len(r) > 3 else None
                site_apn = r[4] if len(r) > 4 else None
                site_addr = r[5] if len(r) > 5 else ""

                desc = f"{permit_type or ''}".strip()

                rec = {
                    "source": "county",
                    "jurisdiction": "Greenville County",
                    "issued_date": issued,
                    "project": {
                        "address": site_addr,
                        "description": desc,
                        "permit_type": permit_type,
                        "value": None,
                        "status": status,
                        "site_apn": site_apn,
                        "permit_no": permit_no,
                    },
                    "contractor": {"name": None, "phone": None, "license": None},
                    "owner": {"name": None, "address": None},
                    "signals": debris_signal(desc),
                    "fingerprint": fp(issued or "", permit_no or "", site_addr or "", desc or ""),
                    "source_url": SEARCH_URL,
                    "scraped_at": now_iso(),
                    "confidence": 0.7,
                }
                out.append(rec)

            path = f"data/{run_date}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)

            print(f"Issued date searched: {issued_date}")
            print(f"Wrote {len(out)} records -> {path}")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
