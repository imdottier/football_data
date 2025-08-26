import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto("https://www.whoscored.com/", timeout=60000)
        await page.wait_for_load_state("networkidle")

        print(await page.title())
        content = await page.content()
        print(content[:1000])  # first 1000 chars as preview

        await browser.close()

asyncio.run(main())
