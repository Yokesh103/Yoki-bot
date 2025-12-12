# Dhan Live Feed + Greeks Microservices (local)

## Prereqs
- Python 3.10+ (3.12 recommended)
- Docker Desktop (for Kafka + Zookeeper)
- Set env vars: DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID

## Install Python deps
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
