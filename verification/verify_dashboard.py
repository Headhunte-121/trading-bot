import time
from playwright.sync_api import sync_playwright

def verify_dashboard():
    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            print("Navigating to dashboard...")
            page.goto("http://localhost:8501")

            # Wait for main title
            print("Waiting for main title...")
            page.wait_for_selector("text=QUANT TERMINAL", timeout=30000)

            print("Looking for SYSTEM POWER MODE...")
            page.wait_for_selector("text=SYSTEM POWER MODE", timeout=10000)

            print("Found Power Mode control.")

            # Click "⚡ FORCE AWAKE"
            print("Clicking '⚡ FORCE AWAKE'...")
            force_awake = page.get_by_text("⚡ FORCE AWAKE").first
            force_awake.click()

            # Wait for status update
            print("Waiting for status update to 'Force Awake'...")
            page.wait_for_selector("text=Force Awake", timeout=10000)

            print("Status updated successfully!")

            # Wait a bit for rendering stability
            time.sleep(2)

            # Take screenshot
            print("Taking screenshot...")
            page.screenshot(path="verification/dashboard_verified.png", full_page=True)
            print("Screenshot saved to verification/dashboard_verified.png")

        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="verification/error.png")
            raise e
        finally:
            browser.close()

if __name__ == "__main__":
    verify_dashboard()
