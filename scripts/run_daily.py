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
    # Your target area is Eastern; GitHub runners run UTC but "yesterday" is fine for daily pulls.
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")


async def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)


async def snap(page, name: str):
    # Always try to leave artifacts behind
    await safe_mkdir("data")
    try:
        await page.screenshot(path=f"data/{name}.png", full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        with open(f"data/{name}.html", "w", encoding="utf-8", errors="ignore") as f:
            f.write(html)
    except Exception:
        pass


async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "00_login_loaded")

    # These IDs are from your saved HTML (ucLogin_*)
    role = page.locator("#ucLogin_ddlSelLogin")
    user = page.locator("#ucLogin_txtLoginId")
    pw = page.locator("#ucLogin_RadTextBox2")
    btn = page.locator("#ucLogin_btnLogin")

    # Wait until the username box is actually visible
    await user.wait_for(state="visible", timeout=30000)

    # Pick login type if it exists (Contractor/Public User etc.)
    if await role.count() > 0:
        try:
            # Don't guess exact wording too hard—just select first option that isn't blank.
            # (If it’s already set, no harm.)
            opts = await role.locator("option").all_inner_texts()
            pick = None
            for o in opts:
                if o.strip():
                    pick = o.strip()
                    break
            if pick:
                await role.select_option(label=pick)
        except Exception:
            pass

    await user.fill(username)
    await pw.fill(password)

    # The login button is type="button" in your HTML, so click it explicitly.
    await btn.click(timeout=30000)

    # Let any redirect finish
    await page.wait_for_timeout(1500)
    await page.wait_for_load_state("networkidle")
    await snap(page, "01_after_login")


async def set_dropdown_any(page, label_text: str, desired: str):
    """
    Tries multiple strategies:
    - standard <select>
    - Telerik-style combo inputs (best-effort)
    """
    # Strategy A: real <select> near the label text
    # Find a label-like text and then the next select in DOM
    locator = page.locator("xpath=//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
                           f" '{label_text.lower()}')]/following::select[1]")
    if await locator.count() > 0:
        sel = locator.first
        await sel.wait_for(state="visible", timeout=15000)
        await sel.select_option(label=desired)
        return

    # Strategy B: fallback — just try selects by options matching desired
    sels = page.locator("select")
    for i in range(await sels.count()):
        s = sels.nth(i)
        try:
            opts = await s.locator("option").all_inner_texts()
            if any(o.strip().lower() == desired.lower() for o in opts):
                await s.select_option(label=desired)
                return
        except Exception:
            continue

    raise RuntimeError(f"Could not set dropdown for '{label_text}' -> '{desired}' (no matching control found).")


async def run_search_for_issued_date(page, issued_date_mmddyyyy: str):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await snap(page, "10_search_page_loaded")

    # We want:
    # Search By = ISSUED
    # Search Operator = Equals
    # Search Value = MM/DD/YYYY
    #
    # The page UI you showed is simple; often these are <select><select><input> + a SEARCH button.

    # Try the “label-near-control” approach first (works even if IDs change).
    await set_dropdown_any(page, "search by", "ISSUED")
    await set_dropdown_any(page, "search operator", "Equals")

    # Search value input: best-effort find the textbox next to "Search Value"
    val = page.locator(
        "xpath=//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'search value')]/following::input[1]"
    )
    if await val.count() == 0:
        # fallback: first visible text input on the form area
        val = page.locator("input[type=text]").filter(has_text="").first

    await val.wait_for(state="visible", timeout=15000)
    await val.fill(issued_date_mmddyyyy)

    # Click SEARCH button: look for button/input with text/value "SEARCH"
    btn = page.locator("button:has-text('SEARCH')").first
    if await btn.count() == 0:
        btn = page.locator("input").filter(
            has_text=re.compile("search", re.I)
        ).first

    if await btn.count() == 0:
        # fallback: any input with value=SEARCH
        btn = page.locator("input[value='SEARCH'], input[value='Search']").first

    if await btn.count() == 0:
        raise RuntimeError("Could not find SEARCH button on permit search page.")

    await btn.click(timeout=30000)
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)
    await snap(page, "11_after_search")


async def extract_results_table(page):
    """
    Finds the table that looks like the Search Results grid.
    We do NOT rely on IDs; we match headers shown in your screenshot:
    PERMIT_NO | ISSUED | Permit Type | STATUS | SITE_APN | SITE_ADDR
    """
    html = await page.content()

    # If we’re still on a login page or got bounced, bail with a clear error.
    if "User Name" in html and "Password" in html and "LOGIN" in html:
        raise RuntimeError("Looks like we got bounced back to the login page (session not established). See data/11_after_search.*")

    tables = page.locator("table")
    best = None
    best_score = -1

    want = ["PERMIT_NO", "ISSUED", "STATUS", "SITE_ADDR"]

    for i in range(await tables.count()):
        t = tables.nth(i)
        try:
            ths = await t.locator("th").all_inner_texts()
            if not ths:
                continue
            hdr = " | ".join([h.strip().upper() for h in ths if h.strip()])
            score = sum(1 for k in want if k in hdr)
            if score > best_score:
                best_score = score
                best = t
        except Exception:
            continue

    # Dump a demonstrate-why report for visibility
    await safe_mkdir("data")
    report = {
        "table_count": await tables.count(),
        "best_score": best_score,
        "note": "best_score counts matches on PERMIT_NO/ISSUED/STATUS/SITE_ADDR"
    }
    with open("data/20_table_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if best is None or best_score < 2:
        raise RuntimeError("Permit results table not found (or headers didn’t match). See data/11_after_search.* and data/20_table_report.json")

    # Parse rows
    headers = [h.strip() for h in await best.locator("th").all_inner_texts()]
    rows = []
    trs = best.locator("tr")
    for r in range(await trs.count()):
        tr = trs.nth(r)
        tds = tr.locator("td")
        if await tds.count() == 0:
            continue
        cells = []
        for c in range(await tds.count()):
            txt = await tds.nth(c).inner_text()
            cells.append(" ".join(txt.split()))
        rows.append(cells)

    return headers, rows


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
    username = os.environ.get("ETRAKIT_USER", "").strip()
    password = os.environ.get("ETRAKIT_PASS", "").strip()
    if not username or not password:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS env vars (GitHub secrets).")

    run_date = datetime.now(timezone.utc).date().isoformat()
    issued = local_yesterday_mmddyyyy()

    await safe_mkdir("data")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1400, "height": 900})

        try:
            await login(page, username, password)
            await run_search_for_issued_date(page, issued)
            headers, rows = await extract_results_table(page)

            # Map columns by header name (more robust than fixed indexes)
            hdr_u = [h.strip().upper() for h in headers]

            def col(name):
                try:
                    return hdr_u.index(name)
                except ValueError:
                    return None

            i_permit = col("PERMIT_NO")
            i_issued = col("ISSUED")
            i_type = col("PERMIT TYPE")
            i_status = col("STATUS")
            i_apn = col("SITE_APN")
            i_addr = col("SITE_ADDR")

            out = []
            for cells in rows:
                permit_no = cells[i_permit] if i_permit is not None and i_permit < len(cells) else ""
                issued_date = cells[i_issued] if i_issued is not None and i_issued < len(cells) else issued
                permit_type = cells[i_type] if i_type is not None and i_type < len(cells) else None
                status = cells[i_status] if i_status is not None and i_status < len(cells) else None
                apn = cells[i_apn] if i_apn is not None and i_apn < len(cells) else None
                address = cells[i_addr] if i_addr is not None and i_addr < len(cells) else ""

                desc = permit_type or ""

                rec = {
                    "source": "etrakit",
                    "jurisdiction": "Greenville County",
                    "permit_no": permit_no,
                    "issued_date": issued_date,
                    "status": status,
                    "project": {
                        "address": address,
                        "apn": apn,
                        "description": desc,
                        "permit_type": permit_type,
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

            path = f"data/{run_date}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)

            print(f"Issued filter: {issued}")
            print(f"Headers: {headers}")
            print(f"Wrote {len(out)} records -> {path}")

        finally:
            try:
                await snap(page, "99_final_state")
            except Exception:
                pass
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
