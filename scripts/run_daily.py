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
    # Greenville is Eastern; for “yesterday” this is fine.
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")

async def snap(page, path: str):
    os.makedirs("data", exist_ok=True)
    try:
        await page.screenshot(path=f"data/{path}", full_page=True)
    except Exception:
        pass

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)

    # Try to find login inputs without guessing exact IDs
    pw = page.locator("input[type='password']").first
    user = page.locator("input[type='email'], input[type='text']").first

    await user.fill(username)
    await pw.fill(password)

    # Click something that looks like Log In
    btn = page.locator("button, input[type=submit]").filter(has_text=re.compile("log\\s*in|sign\\s*in", re.I)).first
    if await btn.count() == 0:
        btn = page.locator("button, input[type=submit], input[type=button]").first
    await btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)

async def do_search_by_issued(page, issued_date: str):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)

    # The page you described: 2 dropdowns + 1 blank input
    selects = page.locator("select")
    if await selects.count() < 2:
        raise RuntimeError("Could not find the two dropdowns on eTRAKiT search page.")

    search_by = selects.nth(0)
    op = selects.nth(1)
    val = page.locator("input[type='text']").first

    # Set Search By = issued
    await search_by.select_option(label="issued")
    # Operator = Equals (if present)
    try:
        await op.select_option(label="Equals")
    except Exception:
        pass

    await val.fill(issued_date)

    # Click SEARCH
    btn = page.locator("button, input[type=submit], input[type=button]").filter(
        has_text=re.compile("^\\s*search\\s*$", re.I)
    ).first
    if await btn.count() == 0:
        # fallback: anything with value="SEARCH"
        btn = page.locator("input[value='SEARCH'], input[value='Search']").first

    await btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)

async def extract_table(page):
    # eTRAKiT grids sometimes use <td> headers (no <th>), so we search by text content.
    tables = page.locator("table")
    best = None
    best_score = -1

    for i in range(await tables.count()):
        t = tables.nth(i)
        txt = (" ".join((await t.inner_text()).split())).upper()
        if len(txt) < 50:
            continue
        score = sum(k in txt for k in ["PERMIT", "ADDRESS", "STATUS", "TYPE"])
        if score > best_score:
            best_score = score
            best = t

    if not best or best_score < 2:
        return None, None

    # Get rows
    rows = []
    trs = best.locator("tr")
    for r in range(await trs.count()):
        tds = trs.nth(r).locator("td")
        if await tds.count() == 0:
            continue
        cells = [" ".join((await tds.nth(c).inner_text()).split()) for c in range(await tds.count())]
        rows.append(cells)

    return best_score, rows

async def main():
    user = os.environ.get("ETRAKIT_USER")
    pw = os.environ.get("ETRAKIT_PASS")
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS (GitHub Secrets).")

    run_date = datetime.now(timezone.utc).date().isoformat()
    issued = local_yesterday_mmddyyyy()
    os.makedirs("data", exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await login(page, user, pw)
        await snap(page, "00_after_login.png")

        await do_search_by_issued(page, issued)
        await snap(page, "10_after_search.png")

        score, rows = await extract_table(page)
        if not rows:
            # dump a tiny HTML snippet for inspection
            html = await page.content()
            with open("data/10_after_search.html", "w", encoding="utf-8") as f:
                f.write(html[:200000])  # cap size
            raise RuntimeError("Could not find results table on eTRAKiT search results page.")

        # Keep it simple: first page only for now (we add pagination after this works)
        out = []
        for cells in rows:
            joined = " | ".join(cells)
            permit_no = cells[0] if len(cells) > 0 else ""
            address = ""
            # best-effort: find something that looks like an address column
            for c in cells:
                if any(x in c.upper() for x in [" ST", " RD", " AVE", " DR", " BLVD", " HWY", " LN", " CT"]):
                    address = c
                    break

            rec = {
                "source": "etrakit",
                "jurisdiction": "Greenville County",
                "issued_date": issued,
                "permit_no": permit_no,
                "project": {
                    "address": address,
                    "description": joined,
                    "permit_type": None,
                    "value": None
                },
                "signals": [],
                "fingerprint": fp(issued, permit_no, address, joined),
                "source_url": SEARCH_URL,
                "scraped_at": now_iso(),
                "confidence": 0.85
            }
            out.append(rec)

        path = f"data/{run_date}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)

        print(f"Wrote {len(out)} records -> {path}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
