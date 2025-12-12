
Production deployment guide
===========================

1. Prerequisites:
   - Linux server (Ubuntu 22.04 recommended) with Docker and docker-compose installed.
   - A public domain name and DNS A record pointing to the server's public IP.
   - Optional: SSL certificate via Let's Encrypt (we recommend using Traefik or Nginx proxy).

2. Start stack (simple):
   - Copy this repo to the server.
   - Edit `docker-compose.prod.yml` if needed (ports, POSTGRES password).
   - Run:
       docker-compose -f docker-compose.prod.yml up -d --build

3. Expose services:
   - Monitoring UI will be accessible at http://<server-ip>:9000
   - Grafana: http://<server-ip>:3000
   - Prometheus: http://<server-ip>:9090

4. To get a direct production-ready HTTPS link:
   - Install Traefik or configure Nginx as reverse proxy with certbot.
   - Configure TLS certificates for your domain and proxy /monitoring to the monitoring container.
   - Ensure you secure /ingest and /dashboard endpoints with authentication (see below).

5. Security hardening (required):
   - Do not expose /ingest or /dashboard publicly without authentication.
   - Add basic auth or OAuth in front of the monitoring service (Traefik middleware or Nginx).
   - Use Postgres and persistent volumes (already configured).
   - Rotate secrets and store them in a secrets manager (Vault or Docker secrets).

6. Making the Monitoring production-ready:
   - Switch MONITOR_DB env var to a Postgres connection string.
   - Configure aiokafka consumer (KAFKA_BOOTSTRAP) and ensure topics exist.
   - Add authentication to /ingest (API key or JWT).
   - Add Prometheus metrics endpoints to other services and configure Grafana dashboards.

