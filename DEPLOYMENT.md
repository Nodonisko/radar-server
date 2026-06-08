# Deployment Guide

## Server Environment
- **OS**: Debian 13
- **App Directory**: `/opt/radar-server`
- **System Dependencies**: `git`, `python3`, `python3-pip`, `python3-venv`, `cron`, `cargo`

## Application Setup
- **Python**: Runs in a virtual environment at `/opt/radar-server/venv`
- **pysteps**: Installed from source (`pip install git+https://github.com/pySTEPS/pysteps`)
- **oxipng**: Installed via Cargo (v9.1.5) and symlinked to `/opt/radar-server/venv/bin/oxipng` for PNG optimization.
- **Environment Variables**: Stored in `/opt/radar-server/.env` (e.g., `METEOGATE_API_KEY`).

## Systemd Service
- **Service Name**: `radar_server.service`
- **Location**: `/etc/systemd/system/radar_server.service`
- **Command**: `/opt/radar-server/venv/bin/python3 -m radar_server run`
- **Management**: 
  - Status: `systemctl status radar_server`
  - Logs: `journalctl -u radar_server -f`
  - Restart: `systemctl restart radar_server`

## Auto-Deployment
- **Cron Job**: Runs every minute.
- **Script**: `/opt/radar-server/deploy.sh`
- **Behavior**: Checks `origin/main` for new commits. If found, it merges changes, reinstalls dependencies from `requirements.txt`, and restarts the `radar_server` service.
- **Logs**: Output saved to `/var/log/radar_deploy.log`
