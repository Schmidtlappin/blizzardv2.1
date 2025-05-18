# Production Setup Guide

This guide covers the steps needed to deploy Blizzard 2.1 in a production environment.

## System Requirements

- **Operating System**: Linux (Ubuntu 20.04+ or similar)
- **Python**: 3.8+
- **Database**: PostgreSQL 12+
- **Storage**: At least 100GB of free disk space for database and XML files
- **Memory**: Minimum 8GB RAM, 16GB+ recommended for large processing jobs

## Installation

### 1. System Preparation

Update your system and install dependencies:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv postgresql postgresql-contrib
```

### 2. Create a Virtual Environment

```bash
python3 -m venv blizzard_venv
source blizzard_venv/bin/activate
```

### 3. Install Blizzard

Clone the repository and install:

```bash
git clone https://github.com/your-org/blizzard.git
cd blizzard/2.1
pip install -r requirements.txt
pip install -e .
```

## Database Setup

### 1. Configure PostgreSQL

```bash
sudo -u postgres psql
```

Inside the PostgreSQL command line:

```sql
CREATE USER blizzard WITH PASSWORD 'secure_password';
CREATE DATABASE irs990 OWNER blizzard;
\q
```

### 2. Create Configuration File

Create a file at `config/settings.json`:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "irs990",
    "user": "blizzard",
    "password": "secure_password",
    "ssl_mode": "prefer",
    "connection_timeout": 30,
    "pool_size": 10,
    "pool_timeout": 30,
    "connection_retries": 3
  },
  "processing": {
    "batch_size": 100,
    "workers": 4,
    "log_level": "INFO",
    "max_files_per_run": 5000,
    "continue_on_error": true
  },
  "storage": {
    "xml_archive_path": "/path/to/xml_archive",
    "reports_path": "/path/to/reports",
    "cache_enabled": true,
    "cache_max_size": 1000
  }
}
```

### 3. Initialize the Database

```bash
python -m scripts.setup_database --config config/settings.json
```

## Production Deployment

### Option 1: Systemd Service

Create a systemd service for scheduled processing:

```bash
sudo nano /etc/systemd/system/blizzard-etl.service
```

Add the following content:

```
[Unit]
Description=Blizzard 2.1 ETL Process
After=network.target postgresql.service

[Service]
User=blizzard
Group=blizzard
WorkingDirectory=/path/to/blizzard/2.1
ExecStart=/path/to/blizzard_venv/bin/python -m scripts.run_etl --config config/settings.json --xml-dir /path/to/xml_files
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl enable blizzard-etl.service
sudo systemctl start blizzard-etl.service
```

### Option 2: Cron Job

For periodic batch processing:

```bash
crontab -e
```

Add a line to run the ETL process daily at 2 AM:

```
0 2 * * * cd /path/to/blizzard/2.1 && /path/to/blizzard_venv/bin/python -m scripts.batch_etl.py --config config/settings.json --xml-dir /path/to/xml_files >> /path/to/blizzard/2.1/logs/cron_etl.log 2>&1
```

## Monitoring

### Logs

- Application logs are stored in the `logs` directory
- Check systemd service logs with: `sudo journalctl -u blizzard-etl.service`

### Database Monitoring

Check database status:

```bash
python -m scripts.check_database --config config/settings.json
```

### Performance Tuning

For improved performance:
- Increase `pool_size` for database connections (if needed)
- Adjust `batch_size` based on your system resources
- Set `workers` to match your CPU cores (minus 1-2 cores)
