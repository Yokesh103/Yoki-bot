import os
import requests
import shutil

# --- CONFIG ---
# URL for Dhan Scrip Master
CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

# Path where the live feed service expects the file
# We assume this script runs from YokiBot root or tools folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_PATH = os.path.join(BASE_DIR, "live_feed_microservice", "api-scrip-master-detailed.csv")

def update_csv():
    print(f"⬇️  Downloading Scrip Master from Dhan...")
    try:
        response = requests.get(CSV_URL, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(TARGET_PATH, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
            
        print(f"✅ CSV Updated Successfully at: {TARGET_PATH}")
        return True
    except Exception as e:
        print(f"❌ Failed to download CSV: {e}")
        return False

if __name__ == "__main__":
    update_csv()