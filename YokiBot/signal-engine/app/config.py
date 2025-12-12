import os
from dotenv import load_dotenv

load_dotenv()

# Service config
OPTIONCHAIN_SERVICE_URL = os.getenv("OPTIONCHAIN_SERVICE_URL", "http://127.0.0.1:8000")
ALERT_WEBHOOK = os.getenv("ALERT_WEBHOOK", "http://127.0.0.1:9000/alerts")

# Capital & Risk
WORKING_CAPITAL = int(os.getenv("WORKING_CAPITAL", "50000"))
BACKUP_CAPITAL = int(os.getenv("BACKUP_CAPITAL", "15000"))
MAX_RISK_PER_TRADE = int(os.getenv("MAX_RISK_PER_TRADE", "1750"))  # per trade risk cap
MONTHLY_LOSS_LIMIT = int(os.getenv("MONTHLY_LOSS_LIMIT", "5000"))

# Strategy thresholds
ADX_TREND = int(os.getenv("ADX_TREND", "30"))
ADX_RANGE_LOW = int(os.getenv("ADX_RANGE_LOW", "20"))

# Consolidation filter
RSI_LOW = float(os.getenv("RSI_LOW", "48"))
RSI_HIGH = float(os.getenv("RSI_HIGH", "57"))

# Condor-specific volatility filter
IVRANK_CONDOR = int(os.getenv("IVRANK_CONDOR", "40"))

# Premium requirement for credit spreads
PREMIUM_THRESHOLD = int(os.getenv("PREMIUM_THRESHOLD", "50"))

# Charges for simulation (brokerage, STT, exchange costs etc.)
SIMULATED_CHARGES = int(os.getenv("SIMULATED_CHARGES", "250"))

# ATR Filter for early rejection
ATR_K = float(os.getenv("ATR_K", "1.0"))
