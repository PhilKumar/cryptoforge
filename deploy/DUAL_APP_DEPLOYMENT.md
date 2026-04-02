# PhilForge + CryptoForge Dual-App AWS Deployment Guide

Run both **PhilForge** (Indian equities) and **CryptoForge** (crypto perpetual futures) on the same AWS EC2 instance.

---

## Architecture Overview

```
AWS EC2 Instance (Ubuntu 22.04, t3.micro)
├── PhilForge FastAPI app (port 8000)
├── CryptoForge FastAPI app (port 9000)
└── Nginx reverse proxy (port 80)
    ├── algoforge.YOUR_IP → :8000
    ├── cryptoforge.YOUR_IP → :9000
    └── YOUR_IP → redirects to algoforge
```

**Total resource usage:** ~200-300MB RAM, negligible CPU when idle.

---

## Prerequisites

- **AWS EC2 Instance:**
  - AMI: Ubuntu 22.04 LTS
  - Instance type: `t3.micro` (1 GB RAM, sufficient for both)
  - Security Group: Allow inbound port 80 (HTTP)
  - Elastic IP: Attached to instance

- **API Credentials:**
  - Dhan API key + secret (for PhilForge)
  - Delta Exchange API key + secret (for CryptoForge)

---

## Step-by-Step Deployment

### 1. SSH into EC2 Instance

```bash
ssh -i your-key.pem ubuntu@YOUR_ELASTIC_IP
```

### 2. Clone Both Repositories

```bash
cd /home/ubuntu

# PhilForge (if not already deployed)
git clone https://github.com/YOUR_GITHUB/New_Algo.git algoforge
cd algoforge
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create .env for PhilForge
cat > /home/ubuntu/algoforge/.env <<'EOF'
DHAN_API_KEY=your_dhan_key_here
DHAN_API_SECRET=your_dhan_secret_here
DEBUG=false
ALGOFORGE_PIN=202603
EOF
```

```bash
# CryptoForge
git clone https://github.com/YOUR_GITHUB/CryptoForge.git cryptoforge
cd cryptoforge
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create .env for CryptoForge
cat > /home/ubuntu/cryptoforge/.env <<'EOF'
DELTA_API_KEY=your_delta_key_here
DELTA_API_SECRET=your_delta_secret_here
DEBUG=false
CRYPTOFORGE_PIN=202603
EOF
```

### 3. Install System Dependencies (Run Once)

```bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3.11 python3.11-venv python3-pip nginx git
```

### 4. Install Systemd Services

```bash
# PhilForge systemd service
sudo cp /home/ubuntu/algoforge/deploy/algoforge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable algoforge
sudo systemctl start algoforge

# CryptoForge systemd service
sudo cp /home/ubuntu/cryptoforge/deploy/cryptoforge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cryptoforge
sudo systemctl start cryptoforge
```

Check status:
```bash
sudo systemctl status algoforge
sudo systemctl status cryptoforge
```

### 5. Configure Nginx

Update the nginx config with your Elastic IP:

```bash
ELASTIC_IP="YOUR_ELASTIC_IP_HERE"  # e.g., 54.123.45.67

# Replace placeholder in CryptoForge nginx config
sed -i "s/YOUR_ELASTIC_IP/$ELASTIC_IP/g" /home/ubuntu/cryptoforge/deploy/nginx.conf

# Install the config
sudo cp /home/ubuntu/cryptoforge/deploy/nginx.conf /etc/nginx/sites-available/dual-app
sudo ln -sf /etc/nginx/sites-available/dual-app /etc/nginx/sites-enabled/dual-app
sudo rm -f /etc/nginx/sites-enabled/default

# Test config
sudo nginx -t

# Restart nginx
sudo systemctl restart nginx
```

---

## Accessing the Apps

### Local Network (if using direct IP)

- **PhilForge:** `http://YOUR_ELASTIC_IP:8000`
- **CryptoForge:** `http://YOUR_ELASTIC_IP:9000`

### Via Nginx Subdomains (Recommended)

You'll need to either:

**Option A: Update your local `/etc/hosts` file** (for testing):
```
54.123.45.67   algoforge.local cryptoforge.local
```

Then access:
- `http://algoforge.local`
- `http://cryptoforge.local`

**Option B: Use Route 53 / DNS** (production):
- Create DNS records: `algoforge.yourdomain.com` → YOUR_ELASTIC_IP
- Create DNS records: `cryptoforge.yourdomain.com` → YOUR_ELASTIC_IP

Then access:
- `https://algoforge.yourdomain.com` (after setting up SSL)
- `https://cryptoforge.yourdomain.com` (after setting up SSL)

---

## Post-Deployment

### 1. Verify Both Apps Are Running

```bash
# Check logs
sudo journalctl -u algoforge -f    # Ctrl+C to exit
sudo journalctl -u cryptoforge -f

# Check ports
lsof -i :8000  # PhilForge
lsof -i :9000  # CryptoForge
lsof -i :80    # Nginx
```

### 2. Test API Endpoints

```bash
# PhilForge
curl http://YOUR_ELASTIC_IP:8000/api/health

# CryptoForge
curl http://YOUR_ELASTIC_IP:9000/api/health

# Via Nginx
curl -H "Host: algoforge.YOUR_ELASTIC_IP" http://YOUR_ELASTIC_IP/api/health
curl -H "Host: cryptoforge.YOUR_ELASTIC_IP" http://YOUR_ELASTIC_IP/api/health
```

### 3. Login and Configure

- **PhilForge:** http://YOUR_ELASTIC_IP:8000 → PIN: 202603
- **CryptoForge:** http://YOUR_ELASTIC_IP:9000 → PIN: 202603

Edit API credentials:
```bash
# PhilForge
nano /home/ubuntu/algoforge/.env

# CryptoForge
nano /home/ubuntu/cryptoforge/.env
```

Restart services after changes:
```bash
sudo systemctl restart algoforge
sudo systemctl restart cryptoforge
```

---

## Common Commands

```bash
# Tail logs
sudo journalctl -u algoforge -f
sudo journalctl -u cryptoforge -f

# Restart services
sudo systemctl restart algoforge
sudo systemctl restart cryptoforge

# View configs
cat /home/ubuntu/algoforge/.env
cat /home/ubuntu/cryptoforge/.env

# Restart nginx
sudo systemctl restart nginx

# Check nginx config
sudo nginx -t
```

---

## SSL/HTTPS Setup (Optional but Recommended)

Install Certbot and Let's Encrypt certificates:

```bash
sudo apt-get install -y certbot python3-certbot-nginx

# For subdomain-based setup:
sudo certbot certonly --nginx -d algoforge.yourdomain.com -d cryptoforge.yourdomain.com

# Update nginx to use SSL (add to each server block):
#   listen 443 ssl http2;
#   ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
#   ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
```

---

## Troubleshooting

### "Connection refused" on port 8000/9000

```bash
# Check if services are running
sudo systemctl status algoforge
sudo systemctl status cryptoforge

# Restart
sudo systemctl restart algoforge
sudo systemctl restart cryptoforge
```

### 404 on nginx proxy

```bash
# Verify nginx is proxying correctly
sudo nginx -t

# Check if the upstream apps are responding
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:9000/api/health
```

### High RAM usage

- Each app uses ~100-150MB RAM
- If EC2 runs out of RAM, enable swap:
  ```bash
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  ```

---

## Resource Estimates

| Component | CPU | RAM | Disk |
|-----------|-----|-----|------|
| Ubuntu base | Idle | 80MB | 2GB |
| PhilForge | <5% idle | 100MB | 500MB |
| CryptoForge | <5% idle | 100MB | 500MB |
| Nginx | <1% | 10MB | <1MB |
| **Total** | <5% idle | ~290MB | ~3.5GB |

**t3.micro:** 1 GB RAM (plenty of headroom)

---

## Next Steps

1. ✅ Deploy both apps
2. ✅ Configure Nginx
3. ✅ Set API credentials in `.env` files
4. ⏭ Monitor logs: `sudo journalctl -u algoforge -f`
5. ⏭ Set up SSL/HTTPS
6. ⏭ Configure DNS (if using subdomains)
