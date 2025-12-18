import os
import re
import json
import csv
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright

BASE = "https://grvlc-trk.aspgov.com"
# This URL pattern matches what you showed and lands you on the correct login flow
LOGIN_URL = f"{BASE}/eTRAKiT/login.aspx?lt=either&rd=~/Search/permit.aspx"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

OUT_DIR = "data"

def eastern_yesterday_mmddyyyy() -> str:
    # Greenville is US/Eastern
    now_et = datetime.now(ZoneInfo("America/New_York"))
    y = now_et - timedelta(days=1)
    return y.strftime("%m/%d/%Y")

async def snap(page, name: str):
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        await page.screenshot(path=os.path.join(OUT_DIR, name), full_page=True)
    except Exception:
        pass

async def save_html(page, name: str):
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        html = await page.content()
        with open(os.path.join(OUT_DIR, name), "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

async def find_public_login_box(page):
    """
    Prefer the middle 'Public Login' panel (what you described).
    We identify it by visible text and then find username/password inputs inside it.
    """
    # Look for a region containing "Public Login" and "LOG IN BELOW..."
    box = page.locator("text=Public Login").first
    if await box.count() == 0:
        # fallback: phrase on the page
        box = page.locator("text=LOG IN BELOW TO ENTER THE PUBLIC PORTAL").first

    if await box.count() == 0:
        return None

    # Get a reasonably-sized container around that text
    container = box.locator("xpath=ancestor-or-self::*[self::div or self::td or self::table][1]")
    if await container.count() == 0:
        container = box

    return container

async def login(page, username: str, password: str):
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(500)
    await snap(page, "00_login_loaded.png")

    container = await find_public_login_box(page)
    if container is None:
        # fallback to the first visible form on the page with a password field
        container = page.locator("form").filter(has=page.locator("input[type=password]")).first

    # Find *visible* inputs inside the chosen container
    user_input = container.locator("input[type=text], input[type=email]").filter(
        has_not=container.locator("[type=hidden]")
    ).filter(
        has_not=container.locator("[disabled]")
    ).filter(
        has_not=container.locator("[readonly]")
    ).filter(
        has=page.locator(":visible")
    ).first

    pass_input = container.locator("input[type=password]").filter(
        has_not=container.locator("[type=hidden]")
    ).filter(
        has_not=container.locator("[disabled]")
    ).filter(
        has_not=container.locator("[readonly]")
    ).first

    # Ensure they’re actually visible (this avoids the “resolved to hidden input” failure)
    await user_input.wait_for(state="visible", timeout=15000)
    await pass_input.wait_for(state="visible", timeout=15000)

    await user_input.fill(username)
    await pass_input.fill(password)

    # Click the visible LOGIN button near that container
    login_btn = container.locator("button, input[type=submit], input[type=button]").filter(
        has_text=re.compile(r"\blog\s*in\b|\bsign\s*in\b|\blogin\b", re.I)
    ).first

    if await login_btn.count() == 0:
        # fallback: any element literally labeled LOGIN
        login_btn = page.locator("text=LOGIN").first

    await login_btn.click()
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(800)

    await snap(page, "01_after_login.png")

    # Confirm logged-in state by looking for "LOG OUT" or "LOGGED IN AS"
    if await page.locator("text=LOG OUT").count() == 0 and await page.locator("text=LOGGED IN AS").count() == 0:
        # Not necessarily fatal, but give a strong diagnostic
        await save_html(page, "01_after_login.html")
        raise RuntimeError("Login did not appear to complete (no LOG OUT / LOGGED IN AS found).")

async def find_search_by_select(page):
    """
    Finds the 'Search By' dropdown on the permit search page.
    We search all <select> elements and choose the one that has options like PERMIT_NO and ISSUED.
    """
    selects = page.locator("select")
    n = await selects.count()
    best = None

    for i in range(n):
        sel = selects.nth(i)
        try:
            # Collect option texts quickly
            opts = await sel.locator("option").all_inner_texts()
            opts_u = [o.strip().upper() for o in opts if o and o.strip()]
            if "PERMIT_NO" in opts_u and ("ISSUED" in opts_u or any("ISSU" in x for x in opts_u)):
                best = sel
                break
        except Exception:
            continue

    if best is not None:
        return best

    # Fallback: locate by nearby label text "Search By"
    label = page.locator("text=Search By").first
    if await label.count() > 0:
        nearby = label.locator("xpath=following::select[1]")
        if await nearby.count() > 0:
            return nearby

    raise RuntimeError("Could not locate Search By dropdown.")

async def find_search_value_input(page):
    """
    Finds the 'Search Value' input on the permit search page.
    """
    # Prefer label-based adjacency
    label = page.locator("text=Search Value").first
    if await label.count() > 0:
        inp = label.locator("xpath=following::input[1]")
        if await inp.count() > 0:
            return inp

    # fallback: first visible text input in the main content area
    inp = page.locator("input[type=text]").filter(has=page.locator(":visible")).first
    if await inp.count() == 0:
        raise RuntimeError("Could not locate Search Value input.")
    return inp

async def find_results_table(page):
    """
    Find the results grid by looking for a table that contains the PERMIT_NO header.
    """
    tables = page.locator("table")
    n = await tables.count()
    for i in range(n):
        t = tables.nth(i)
        try:
            if await t.locator("th").filter(has_text=re.compile(r"PERMIT_NO", re.I)).count() > 0:
                return t
        except Exception:
            continue
    raise RuntimeError("Results table not found (no table with PERMIT_NO header).")

async def extract_table(table):
    headers = await table.locator("th").all_inner_texts()
    headers = [h.strip() for h in headers if h and h.strip()]

    rows = []
    tr = table.locator("tbody tr")
    rn = await tr.count()
    for i in range(rn):
        tds = tr.nth(i).locator("td")
        cells = await tds.all_inner_texts()
        cells = [c.strip() for c in cells]
        if any(cells):
            rows.append(cells)

    return headers, rows

async def goto_search_page(page):
    await page.goto(SEARCH_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(800)
    await snap(page, "10_search_page_loaded.png")

    # Confirm we’re on Permit Search page
    if await page.locator("text=Permit Search").count() == 0:
        await save_html(page, "10_search_page_loaded.html")
        raise RuntimeError("Did not reach Permit Search page (missing 'Permit Search' text).")

async def submit_issued_yesterday_search(page, issued_date: str):
    sel = await find_search_by_select(page)
    await sel.wait_for(state="visible", timeout=15000)

    # Select ISSUED (robustly)
    opts = await sel.locator("option").all_inner_texts()
    target_label = None
    for o in opts:
        if o and "ISSUED" in o.upper():
            target_label = o
            break
    if target_label is None:
        raise RuntimeError("Search By dropdown does not contain ISSUED option.")

    await sel.select_option(label=target_label)

    # Fill search value with yesterday date
    val = await find_search_value_input(page)
    await val.wait_for(state="visible", timeout=15000)
    await val.fill(issued_date)

    # Click SEARCH button
    btn = page.locator("button, input[type=submit], input[type=button]").filter(
        has_text=re.compile(r"\bsearch\b", re.I)
    ).first
    await btn.wait_for(state="visible", timeout=15000)
    await btn.click()

    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1200)

async def click_next_page_if_exists(page) -> bool:
    """
    Click the 'next page' control if pagination exists (like your screenshot).
    Returns True if it navigated to page 2.
    """
    # If the page text already says "page 1 of 2", there should be a pager.
    pager_text = page.locator("text=page 1 of").first
    if await pager_text.count() == 0:
        return False

    # Best: a control that does a Page$Next postback
    next_candidate = page.locator("[onclick*='Page$Next'], [href*='Page$Next']").first
    if await next_candidate.count() > 0:
        await next_candidate.click()
    else:
        # Fallback: click the third pager button in the pager cluster (first, prev, next, last)
        # We find a row of small image buttons near the "page 1 of 2" text.
        cluster = pager_text.locator("xpath=preceding::input[@type='image'][4]")
        if await cluster.count() > 0:
            # Not reliable indexing from this anchor; fallback to grabbing all image buttons and clicking the "next-ish" one.
            imgs = page.locator("input[type='image']")
            if await imgs.count() >= 3:
                await imgs.nth(2).click()
            else:
                return False
        else:
            imgs = page.locator("input[type='image']")
            if await imgs.count() >= 3:
                await imgs.nth(2).click()
            else:
                return False

    # Wait until page 2 text appears or table refresh happens
    await page.wait_for_timeout(700)
    page2 = page.locator("text=page 2 of").first
    if await page2.count() > 0:
        return True

    # Give it a bit more time
    await page.wait_for_timeout(1200)
    return (await page.locator("text=page 2 of").count()) > 0

def write_csv(path, headers, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        if headers:
            w.writerow(headers)
        for r in rows:
            w.writerow(r)

async def main():
    user = os.getenv("ETRAKIT_USER", "")
    pw = os.getenv("ETRAKIT_PASS", "")
    if not user or not pw:
        raise RuntimeError("Missing ETRAKIT_USER / ETRAKIT_PASS environment variables.")

    issued_date = eastern_yesterday_mmddyyyy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await login(page, user, pw)
            await goto_search_page(page)

            await submit_issued_yesterday_search(page, issued_date)
            await snap(page, "14_results_detected.png")

            table = await find_results_table(page)
            headers, rows1 = await extract_table(table)

            # Try to grab page 2 if it exists
            has_page2 = await click_next_page_if_exists(page)
            rows2 = []
            if has_page2:
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(1000)
                await snap(page, "15_page2.png")
                table2 = await find_results_table(page)
                _, rows2 = await extract_table(table2)

            # Merge + dedupe rows
            all_rows = rows1 + rows2
            seen = set()
            deduped = []
            for r in all_rows:
                key = tuple([c.strip() for c in r])
                if key not in seen and any(key):
                    seen.add(key)
                    deduped.append(r)

            os.makedirs(OUT_DIR, exist_ok=True)
            write_csv(os.path.join(OUT_DIR, "permits.csv"), headers, deduped)

            meta = {
                "issued_date": issued_date,
                "rows_page1": len(rows1),
                "rows_page2": len(rows2),
                "rows_total": len(deduped),
                "had_page2": bool(has_page2),
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            }
            with open(os.path.join(OUT_DIR, "30_results.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

            await snap(page, "99_final_state.png")

        finally:
            await context.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
