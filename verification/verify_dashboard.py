import time
from playwright.sync_api import sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    try:
        page.goto("http://localhost:8501")

        # Wait for app to load
        page.wait_for_load_state("networkidle")
        time.sleep(5) # Give Streamlit extra time

        print("Checking for Tabs...")
        # Streamlit tabs
        # Often role="tab" is on the button
        tabs = page.locator("button[role='tab']")
        print(f"Found {tabs.count()} tabs.")

        # Click Crypto Tab
        crypto_tab = page.get_by_role("tab", name="₿ CRYPTO")
        if crypto_tab.count() > 0:
            print("Clicking Crypto Tab...")
            crypto_tab.click()
            time.sleep(2)

            # Check for Crypto Status Radio
            print("Checking for Crypto Status...")
            # We can look for the label "CRYPTO STATUS"
            # Streamlit radio buttons usually have a label near them
            status_label = page.get_by_text("CRYPTO STATUS")
            if status_label.count() > 0:
                print("Found CRYPTO STATUS label.")
            else:
                print("CRYPTO STATUS label NOT found.")

            # Take screenshot
            page.screenshot(path="verification/dashboard.png")
            print("Screenshot saved to verification/dashboard.png")
        else:
            print("Crypto Tab NOT found.")
            page.screenshot(path="verification/dashboard_failed.png")
    except Exception as e:
        print(f"Error: {e}")
        page.screenshot(path="verification/error.png")
    finally:
        browser.close()

with sync_playwright() as playwright:
    run(playwright)
