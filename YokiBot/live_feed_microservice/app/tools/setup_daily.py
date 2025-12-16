# tools/setup_daily.py
import requests
import sys

CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
CSV_FILE = "api-scrip-master-detailed.csv"

def download_master():
    print("⬇️  Downloading latest Scrip Master from Dhan...")
    try:
        with requests.get(CSV_URL, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(CSV_FILE, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"✅  Saved to {CSV_FILE}")
    except Exception as e:
        print(f"❌  Download failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    download_master()
    print("\nNext steps:")
    print("python tools/get_ids.py BANKNIFTY <PRE_OPEN_PRICE>")
