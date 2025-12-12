
YokiBot Monitoring service - standalone package
===============================================

Place this folder under your YokiBot repo at services/monitoring or run it standalone.

Run:
    KAFKA_BOOTSTRAP=host:9092 uvicorn main:app --port 9000

Endpoints:
- POST /ingest  (JSON) - send events from other services
- GET  /events/stream - Server-Sent Events stream for live UI
- GET  /dashboard - simple front-end dashboard
- GET  /events/latest?n=50 - last n events
- GET  /health - service health
