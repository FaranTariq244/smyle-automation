"""
Browser Manager for maintaining persistent Chrome sessions.
Allows manual login and maintains session across automation runs.
"""

import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time


class BrowserManager:
    """Manages a persistent Chrome browser session."""

    def __init__(self, profile_dir=None, use_existing_chrome=False, chrome_debugger_port=9222):
        """
        Initialize the Browser Manager.

        Args:
            profile_dir: Path to Chrome profile directory. If None, creates one in ./chrome_profile
            use_existing_chrome: If True, connects to an already running Chrome instance
            chrome_debugger_port: Port for Chrome debugger (default: 9222)
        """
        self.driver = None
        self.use_existing_chrome = use_existing_chrome
        self.chrome_debugger_port = chrome_debugger_port

        # Set up profile directory
        if profile_dir is None:
            self.profile_dir = os.path.join(os.getcwd(), "chrome_profile")
        else:
            self.profile_dir = profile_dir

        # Create profile directory if it doesn't exist
        if not os.path.exists(self.profile_dir) and not use_existing_chrome:
            os.makedirs(self.profile_dir)

    def start_browser(self, headless=False):
        """
        Start the Chrome browser with persistent profile.

        Args:
            headless: If True, runs Chrome in headless mode (not recommended for manual login)

        Returns:
            WebDriver instance
        """
        chrome_options = Options()

        if self.use_existing_chrome:
            # Connect to existing Chrome instance
            chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.chrome_debugger_port}")
        else:
            # Use persistent profile
            chrome_options.add_argument(f"user-data-dir={self.profile_dir}")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--no-default-browser-check")

        # Additional useful options
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        if headless:
            chrome_options.add_argument("--headless")

        # Initialize the driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # Make browser look more like a real user
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": self.driver.execute_script("return navigator.userAgent").replace('HeadlessChrome', 'Chrome')
        })

        return self.driver

    def navigate_to(self, url):
        """Navigate to a URL."""
        if self.driver is None:
            raise Exception("Browser not started. Call start_browser() first.")
        self.driver.get(url)

    def wait_for_manual_login(self, check_url_contains=None, timeout=300):
        """
        Wait for user to manually login.

        Args:
            check_url_contains: Optional string to check if URL contains this after login
            timeout: Maximum time to wait in seconds (default: 5 minutes)
        """
        print("\n" + "="*60)
        print("MANUAL LOGIN REQUIRED")
        print("="*60)
        print("Please login manually in the browser window.")
        if check_url_contains:
            print(f"Waiting for URL to contain: '{check_url_contains}'")
        print(f"You have {timeout} seconds to complete the login.")
        print("="*60 + "\n")

        start_time = time.time()
        while time.time() - start_time < timeout:
            current_url = self.driver.current_url

            if check_url_contains and check_url_contains in current_url:
                print("Login detected! Session is now active.")
                return True

            time.sleep(2)

        print("Timeout waiting for login.")
        return False

    def close(self):
        """Close the browser."""
        if self.driver:
            self.driver.quit()

    def get_driver(self):
        """Get the WebDriver instance."""
        return self.driver


def create_chrome_shortcut_command(port=9222):
    """
    Returns the command to start Chrome with remote debugging.
    You can run this manually to start Chrome that automation can connect to.
    """
    chrome_paths = {
        'windows': r'"C:\Program Files\Google\Chrome\Application\chrome.exe"',
        'mac': '"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"',
        'linux': 'google-chrome'
    }

    import platform
    system = platform.system().lower()

    if 'windows' in system:
        chrome_path = chrome_paths['windows']
    elif 'darwin' in system:
        chrome_path = chrome_paths['mac']
    else:
        chrome_path = chrome_paths['linux']

    return f"{chrome_path} --remote-debugging-port={port}"


if __name__ == "__main__":
    # Example usage
    print("Browser Manager - Example Usage\n")
    print("Option 1: Use persistent profile (recommended for first-time setup)")
    print("Option 2: Connect to existing Chrome instance")
    print("\nTo use Option 2, first start Chrome with:")
    print(create_chrome_shortcut_command())
    print()

    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "2":
        # Connect to existing Chrome
        manager = BrowserManager(use_existing_chrome=True)
    else:
        # Use persistent profile
        manager = BrowserManager()

    # Start browser
    driver = manager.start_browser()

    # Navigate to Looker Studio
    manager.navigate_to("https://lookerstudio.google.com/u/0/reporting/ddcef9f1-b6d4-4ed3-86e3-38c70a521a2c/page/M05qB")

    # Wait for manual login
    manager.wait_for_manual_login(check_url_contains="lookerstudio.google.com")

    print("\nSession is now active! You can now run your automation tasks.")
    print("Press Enter to close the browser...")
    input()

    manager.close()
