import os, json, hashlib, re
from datetime import datetime, timezone, timedelta
import asyncio
from playwright.async_api import async_playwright

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fp(issued_date, permit_no, address, permit_type):
    base = f"{issued_date}|{permit_no}|{address}|{permit_type}".upper().encode("utf-8")
    return hashlib.sha256(base).hexdigest()

def local_yesterday_mmddyyyy():
    # Greenville is Eastern; GH runner is UTC but date precision isn't critical.
    # If you want strict ET later, we can add zoneinfo.
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

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

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_login_loaded")

    # These IDs come directly from your captured HTML
    user = page.locator("#ucLogin_txtLoginId")
    pw = page.locator("#ucLogin_RadTextBox2")
    btn = page.locator("#ucLogin_btnLogin")

    if await user.count() == 0:
        # Fallback (in case they tweak IDs)
        user = page.locator("input[id*='LoginId'], input[name*='LoginId']").first
    if await pw.count() == 0:
        pw = page.locator("input[id*='RadTextBox2'], input[name*='RadTextBox2']").first
    if await btn.count() == 0:
        btn = page.locator("input[id*='btnLogin'], input[name*='btnLogin']").first

    # Ensure visible (top bar can be offscreen in headless sometimes)
    await user.scroll_into_view_if_needed()
    await pw.scroll_into_view_if_needed()

    await user.fill(username)
    await pw.fill(password)

    # IMPORTANT: has_text doesn't work on <input value="Login">, so click by locator directly
    await btn.click()

    # eTRAKiT may do an AJAX login; wait for "LOG OUT" or a URL change
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    # Wait for an obvious logged-in marker
    logged_in = page.locator("text=LOG OUT")
    try:
        await logged_in.wait_for(timeout=15000)
    except Exception:
        # Save state for debugging
        await snap(page, "01_after_login_failed")
        raise RuntimeError("Login did not succeed (LOG OUT not found). Check data/01_after_login_failed.*")

    await snap(page, "01_after_login")

async def set_dropdown_by_label(select_locator, wanted_label: str):
    # Works for normal <select>. If it’s Telerik, it still often renders a <select> for the underlying value.
    options = await select_locator.locator("option").all_inner_texts()
    # Exact match first
    for opt in options:
        if opt.strip().lower() == wanted_label.strip().lower():
            await select_locator.select_option(label=opt.strip())
            return
    # Contains match
    for opt in options:
        if wanted_label.strip().lower() in opt.strip().lower():
            await select_locator.select_option(label=opt.strip())
            return
    raise RuntimeError(f"Could not set dropdown to '{wanted_label}'. Options were: {options[:15]}")

async def find_best_results_table(page):
    # eTRAKiT results are often Telerik grids; headers might be th OR td in a header row.
    # We score tables by whether they contain our expected header tokens.
    tokens = ["PERMIT", "ISSUED", "PERMIT TYPE", "STATUS", "SITE_APN", "SITE_ADDR"]

    tables = await page.query_selector_all("table")
    best = None
    best_score = 0

    for t in tables:
        try:
            txt = (await t.inner_text()) or ""
        except Exception:
            continue
        up = " ".join(txt.split()).upper()
        score = sum(1 for tok in tokens if tok in up)
        if score > best_score:
            best_score = score
            best = t

    return best, best_score

async def extract_table_rows(table_handle):
    # Extract rows/cells via DOM so we don’t depend on th/td quirks
    return await table_handle.evaluate(
        """(tbl) => {
            const rows = Array.from(tbl.querySelectorAll("tr"));
            return rows.map(r => Array.from(r.querySelectorAll("th,td")).map(c => c.innerText.replace(/\\s+/g,' ').trim()));
        }"""
    )

async def run_search_for_issued_date(page, issued_date: str):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "10_search_page_loaded")

    # On your UI: 3 fields: Search By (dropdown), Operator (dropdown), Value (text)
    # We'll find the first 2 <select> and the first text input near them.
    selects = page.locator("select")
    if await selects.count() < 2:
        await snap(page, "11_no_selects_found")
        raise RuntimeError("Could not find the two dropdowns on permit search page. See data/11_no_selects_found.*")

    search_by = selects.nth(0)
    op = selects.nth(1)

    await set_dropdown_by_label(search_by, "ISSUED")
    await set_dropdown_by_label(op, "Equals")

    # Search value field: prefer a normal input that is not hidden
    value_box = page.locator("input[type='text']").filter(has_not=page.locator("[style*='display:none']")).first
    if await value_box.count() == 0:
        # fallback: any input that looks editable
        value_box = page.locator("input").first

    await value_box.fill(issued_date)

    # Click Search button (on the page it’s a big SEARCH button)
    btn = page.locator("button:has-text('Search'), input[value='Search'], input[value='SEARCH']").first
    if await btn.count() == 0:
        await snap(page, "12_no_search_button")
        raise RuntimeError("Could not find Search button on permit search page. See data/12_no_search_button.*")

    await btn.click()

    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    await page.wait_for_timeout(1200)
    await snap(page, "13_results_loaded")

async def fetch_etrakit_rows():
    user = os.getenv("ETRAKIT_USER", "").strip()
    pw = os.getenv("ETRAKIT_PASS", "").strip()
    if not user or not pw:
        raise RuntimeError("Missing env vars ETRAKIT_USER / ETRAKIT_PASS (set as GitHub Secrets).")

    issued_date = local_yesterday_mmddyyyy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await login(page, user, pw)
        await run_search_for_issued_date(page, issued_date)

        best, score = await find_best_results_table(page)
        if not best or score < 4:
            await snap(page, "20_no_good_table_found")
            raise RuntimeError(
                "Could not locate results table reliably. See data/20_no_good_table_found.*"
            )

        grid = await extract_table_rows(best)

        # Find header row (the one containing PERMIT and ISSUED)
        header_idx = None
        for i, r in enumerate(grid[:15]):
            up = " | ".join([c.upper() for c in r])
            if "PERMIT" in up and "ISSUED" in up:
                header_idx = i
                break

        if header_idx is None:
            await snap(page, "21_header_not_found")
            raise RuntimeError("Results table found, but header row not detected. See data/21_header_not_found.*")

        header = grid[header_idx]
        body = [r for r in grid[header_idx+1:] if any(c.strip() for c in r)]

        # Map columns by header names
        def col_index(name_contains: str):
            nc = name_contains.upper()
            for idx, h in enumerate(header):
                if nc in (h or "").upper():
                    return idx
            return None

        idx_permit = col_index("PERMIT")
        idx_issued = col_index("ISSUED")
        idx_type = col_index("PERMIT TYPE")
        idx_status = col_index("STATUS")
        idx_apn = col_index("SITE_APN")
        idx_addr = col_index("SITE_ADDR")

        records = []
        for r in body:
            permit_no = r[idx_permit] if idx_permit is not None and idx_permit < len(r) else ""
            issued = r[idx_issued] if idx_issued is not None and idx_issued < len(r) else issued_date
            permit_type = r[idx_type] if idx_type is not None and idx_type < len(r) else ""
            status = r[idx_status] if idx_status is not None and idx_status < len(r) else ""
            apn = r[idx_apn] if idx_apn is not None and idx_apn < len(r) else ""
            addr = r[idx_addr] if idx_addr is not None and idx_addr < len(r) else ""

            rec = {
                "source": "etrakit",
                "jurisdiction": "Greenville County",
                "issued_date": issued,
                "permit_no": permit_no,
                "project": {
                    "address": addr,
                    "description": permit_type,
                    "permit_type": permit_type,
                    "status": status,
                    "site_apn": apn,
                },
                "fingerprint": fp(issued or "", permit_no or "", addr or "", permit_type or ""),
                "source_url": SEARCH_URL,
                "scraped_at": now_iso(),
                "confidence": 0.8,
            }
            records.append(rec)

        await browser.close()
        return issued_date, records

async def main():
    run_date = datetime.now(timezone.utc).date().isoformat()
    os.makedirs("data", exist_ok=True)

    issued_date, records = await fetch_etrakit_rows()

    # Write JSON output
    out_path = f"data/{run_date}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"Issued date searched: {issued_date}")
    print(f"Wrote {len(records)} records -> {out_path}")

if __name__ == "__main__":
    asyncio.run(main())
