import os
import json
import csv
import asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

BASE = "https://grvlc-trk.aspgov.com"
LOGIN_URL = f"{BASE}/eTRAKiT/"
SEARCH_URL = f"{BASE}/eTRAKiT/Search/permit.aspx"

USER = os.environ.get("ETRAKIT_USER")
PASS = os.environ.get("ETRAKIT_PASS")

DATA_DIR = "data"


def yesterday_mmddyyyy():
    return (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")


async def snap(page, name):
    os.makedirs(DATA_DIR, exist_ok=True)
    await page.screenshot(path=f"{DATA_DIR}/{name}.png", full_page=True)


async def main():
    if not USER or not PASS:
        raise RuntimeError("Missing ETRAKIT_USER or ETRAKIT_PASS")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # 1. Load login page
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await snap(page, "01_login_loaded")

        # 2. Force login type = Public
        await page.select_option("#ucLogin_ddlSelLogin", "Public")
        await page.wait_for_timeout(300)

        # 3. Fill credentials
        await page.fill("#ucLogin_txtLoginId", USER)
        await page.fill("#ucLogin_txtPassword", PASS)

        await snap(page, "02_filled_credentials")

        # 4. Submit login
        await page.click("#ucLogin_btnLogin")
        await page.wait_for_load_state("networkidle")

        await snap(page, "03_logged_in")

        # 5. Go to permit search
        await page.goto(SEARCH_URL, wait_until="domcontentloaded")
        await snap(page, "04_search_page")

        # 6. Enter issued date (yesterday)
        issued_date = yesterday_mmddyyyy()

        await page.select_option(
            "select[name='ctl00$MainContent$ddlSearchColumn']",
            "ISSUED"
        )

        await page.fill(
            "input[name='ctl00$MainContent$txtSearchValue']",
            issued_date
        )

        await snap(page, "05_search_filled")

        # 7. Run search
        await page.click("input[value='Search']")
        await page.wait_for_load_state("networkidle")

        await snap(page, "06_results")

        # 8. Extract table rows
        rows = await page.locator("table tr").all()

        results = []

        for row in rows:
            cells = await row.locator("td").all()
            if len(cells) < 6:
                continue

            text = [await c.inner_text() for c in cells]

            results.append({
                "permit_no": text[0].strip(),
                "issued": text[1].strip(),
                "permit_type": text[2].strip(),
                "status": text[3].strip(),
                "site_apn": text[4].strip(),
                "site_address": text[5].strip(),
            })

        # 9. Save results
        os.makedirs(DATA_DIR, exist_ok=True)

        with open(f"{DATA_DIR}/results.json", "w") as f:
            json.dump(results, f, indent=2)

        with open(f"{DATA_DIR}/results.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=results[0].keys() if results else []
            )
            writer.writeheader()
            writer.writerows(results)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
