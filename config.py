# ─────────────────────────────────────────────────────────────────────────────
# config.py  –  Edit these values to match your MySQL installation
# ─────────────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    'host':     'localhost',      # MySQL host
    'port':     3306,             # MySQL port
    'user':     'root',           # MySQL username
    'password': 'Santhosh@22',  # MySQL password  ← CHANGE THIS
    'database': 'oss_hc',         # Database name (will be created automatically)
    'charset':  'utf8mb4',
}

# Flask secret key – change in production
SECRET_KEY = 'oss_hc_secret_key_change_in_prod'
