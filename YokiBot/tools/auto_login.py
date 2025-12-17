import os
import time
import pyotp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- USER CONFIG (ENTER YOUR DETAILS) ---
MOBILE = "8754192312"
PASSWORD = "YOUR_PASSWORD"
# This is the secret text from the QR Code setup (NOT the 6 digit code)
TOTP_SECRET = "" 

# Path to .env file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "live_feed_microservice", ".env")

def get_token():
    print("🤖 Starting Auto-Login Robot...")
    
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Uncomment to run invisibly
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # 1. Open Dhan Login
        driver.get("https://web.dhan.co/login")
        wait = WebDriverWait(driver, 15)
        
        # 2. Enter Mobile
        print("   Typing Mobile Number...")
        # Note: These XPaths might change if Dhan updates their UI
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@data-testid='mobile-input']"))).send_keys(MOBILE)
        driver.find_element(By.XPATH, "//button/div[text()='Proceed']").click()
        
        # 3. Enter Password
        print("   Typing Password...")
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))).send_keys(PASSWORD)
        driver.find_element(By.XPATH, "//button/div[text()='Proceed']").click()
        
        # 4. Generate & Enter TOTP
        print("   Generating TOTP...")
        totp = pyotp.TOTP(TOTP_SECRET).now()
        otp_fields = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "otp-input")))
        for i, digit in enumerate(totp):
            otp_fields[i].send_keys(digit)
            
        print("   Logging in...")
        
        # 5. Wait for Dashboard and Extract Token
        # We wait for the URL to change to dashboard or an element to appear
        time.sleep(5) # Allow redirect
        
        # Extract Token from Local Storage
        token = driver.execute_script("return localStorage.getItem('access_token');") # Key might vary
        # Fallback: Dhan sometimes puts it in cookies or specific storage keys like 'dhan_token'
        # If the above returns None, inspect your browser Application tab to find the key name.
        
        if not token:
             # Try getting it from API Logs (Advanced) or print instructions
             print("❌ Could not auto-extract token from LocalStorage. Dhan might hide it.")
             # NOTE: Dhan's web token is different from API Token. 
             # Usually, for API Access, you generate it via the 'api-access' page.
             # Full automation of API Token generation is complex due to security.
             # Recommendation: Use the manual token update script below if this fails.
             return None

        print(f"✅ Token Found: {token[:10]}...")
        return token

    except Exception as e:
        print(f"❌ Login Failed: {e}")
        return None
    finally:
        driver.quit()

def update_env_file(new_token):
    # Read existing env
    with open(ENV_PATH, "r") as f:
        lines = f.readlines()
        
    # Write new env
    with open(ENV_PATH, "w") as f:
        found = False
        for line in lines:
            if line.startswith("DHAN_ACCESS_TOKEN="):
                f.write(f"DHAN_ACCESS_TOKEN={new_token}\n")
                found = True
            else:
                f.write(line)
        if not found:
            f.write(f"\nDHAN_ACCESS_TOKEN={new_token}\n")
            
    print("✅ .env file updated successfully!")

if __name__ == "__main__":
    # WARNING: Fully automating API Token generation is very hard as Dhan 
    # protects the 'Generate New Token' button heavily.
    # Most users use a helper script where they paste the token.
    
    # If you want to just PASTE the token daily and have it update the file:
    token = input("Paste new Dhan Access Token (or press Enter to try Robot): ")
    if token.strip():
        update_env_file(token.strip())
    else:
        # Try Robot (Only works if you configured Selenium perfectly)
        t = get_token()
        if t: update_env_file(t)