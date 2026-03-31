"""
database.py  –  Full MySQL data layer using PyMySQL
Install:  pip install pymysql cryptography
"""
import pymysql
import pymysql.cursors
import hashlib
from contextlib import contextmanager
from config import DB_CONFIG

# ── HC data seeded from MAster_hc.xlsx ───────────────────────────────────────
HC_DATA = [
    ('ABE-FIN',  'United Kingdom',    'London',   'EMEA', 4, 4, 2),
    ('AHM-WES',  'India',             'Malaysia', 'APAC', 5, 1, 1),
    ('AMS',      'Netherlands',       'Rijswijk', 'EMEA', 6, 9, 2),
    ('AMS-RIG',  'Netherlands',       'Rijswijk', 'EMEA', 6,  3, 2),
    ('ASS',      'Netherlands',       'Rijswijk', 'EMEA', 6, 7, 2),
    ('BAN',      'Thailand',          'Malaysia', 'APAC', 5, 2, 1),
    ('BEJ-WTC',  'China',             'Singapore','APAC', 7, 4, 1),
    ('BKV-LG',   'Canada',            'Calgary',  'AMER', 1, 1, 3),
    ('BNG-ECO',  'India',             'Malaysia', 'APAC', 5, 11, 1),
    ('BNG-EST',  'India',             'Malaysia', 'APAC', 5, 4, 1),
    ('BNG-RET',  'India',             'Malaysia', 'APAC', 5, 1, 1),
    ('BPT-BOC',  'Hungary',           'Rijswijk', 'EMEA', 6,  1, 2),
    ('BRA',      'Slovakia',          'Rijswijk', 'EMEA', 6, 1, 2),
    ('BRI-GST',  'Australia',         'Malaysia', 'APAC', 5, 5, 1),
    ('BRU-KTE',  'Belgium',           'Rijswijk', 'EMEA', 6, 3, 2),
    ('BRY',      'South Africa',      'London',   'EMEA', 4, 4, 2),
    ('CAI-NCC',  'Egypt',             'London',   'EMEA', 4,  1, 2),
    ('CAL-SCB',  'Canada',            'Calgary',  'AMER', 1, 16, 3),
    ('CBJ-WS',   'Malaysia',          'Malaysia', 'APAC', 5, 9, 1),
    ('CHN-SC',   'India',             'Malaysia', 'APAC', 5, 10, 1),
    ('CIC',      'Australia',         'Malaysia', 'APAC', 5, 6, 1),
    ('CLN-GOD',  'Germany',           'Rijswijk', 'EMEA', 6, 2, 2),
    ('CUR-ISL',  'Australia',         'Malaysia', 'APAC', 5, 2, 1),
    ('DEN-ODS',  'Denmark',           'Rijswijk', 'EMEA', 6,  1, 2),
    ('DOH-COR',  'Qatar',             'Dubai',    'APAC', 2, 4, 1),
    ('DPK',      'USA',               'Houston',  'AMER', 3, 3, 3),
    ('DRS-KWA',  'Tanzania',          'Rijswijk', 'EMEA', 6,  1, 2),
    ('DUB-ONE',  'UAE',               'Dubai',    'APAC', 2, 1, 1),
    ('DUR-OFF',  'South Africa',      'London',   'EMEA', 4,  1, 2),
    ('FSJ-DO',   'Canada',            'Calgary',  'AMER', 1, 1, 3),
    ('GEP',      'USA',               'Houston',  'AMER', 3, 4, 3),
    ('GTY-NQ',   'United Kingdom',    'London',   'EMEA', 4, 1, 2),
    ('GUR-2HC',  'India',             'Malaysia', 'APAC', 5, 1, 1),
    ('GUZ',      'China',             'Singapore','APAC', 7, 1, 1),
    ('HAG',      'Netherlands',       'Rijswijk', 'EMEA', 6, 14, 2),
    ('HAM-AMN',  'Germany',           'Rijswijk', 'EMEA', 6, 15, 2),
    ('HAM-GRW',  'Germany',           'Rijswijk', 'EMEA', 6, 1, 2),
    ('HAM-PAE',  'Germany',           'Rijswijk', 'EMEA', 6, 4, 2),
    ('HAM-YRD',  'Germany',           'Rijswijk', 'EMEA', 6,  2, 2),
    ('HAZ',      'India',             'Malaysia', 'APAC', 5, 1, 1),
    ('HCM',      'Vietnam',           'Singapore','APAC', 7, 1, 1),
    ('HON-MLN',  'Hong Kong',         'Singapore','APAC', 7, 1, 1),
    ('HOU-EPC',  'USA',               'Houston',  'AMER', 3, 22, 3),
    ('HOU-OTM',  'USA',               'Houston',  'AMER', 3, 4, 3),
    ('HOU-THC',  'USA',               'Houston',  'AMER', 3, 2, 3),
    ('HOU-WTC',  'USA',               'Houston',  'AMER', 3, 8, 3),
    ('JAK',      'Indonesia',         'Singapore','APAC', 7, 3, 1),
    ('KIT-CMT',  'Canada',            'Calgary',  'AMER', 1, 3, 3),
    ('KLU',      'Netherlands',       'Rijswijk', 'EMEA', 6,  5, 2),
    ('KMB-RBJ',  'Australia',         'Malaysia', 'APAC', 5, 2, 1),
    ('KRW-DOT',  'Poland',            'Rijswijk', 'EMEA', 6,  12, 2),
    ('KUL-MS',   'Malaysia',          'Malaysia', 'APAC', 5, 6, 1),
    ('KWT-OC',   'Kuwait',            'Dubai',    'APAC', 2, 1, 1),
    ('LEU',      'Germany',           'Rijswijk', 'EMEA', 6, 1, 2),
    ('LJU',      'Slovenia',          'Rijswijk', 'EMEA', 6,  1, 2),
    ('LON-SC (8 hours)',  'United Kingdom', 'London','EMEA', 4, 15, 2),
    ('LON-SC (7.5 hours)','United Kingdom', 'London','EMEA', 4, 8, 2),
    ('LON-YR',   'United Kingdom',    'London',   'EMEA', 4, 2, 2),
    ('LSA-HTP',  'USA',               'Houston',  'AMER', 3, 2, 3),
    ('LSA-NRC',  'USA',               'Houston',  'AMER', 3, 6, 3),
    ('MAD-PCA',  'Spain',             'Rijswijk', 'EMEA', 6,  1, 2),
    ('MAU',      'Japan',             'Singapore','APAC', 7, 2, 1),
    ('MCA',      'USA',               'Houston',  'AMER', 3, 6, 3),
    ('MEX-IOX',  'Mexico',            'Houston',  'AMER', 3, 1, 3),
    ('MIC-MCT',  'USA',               'Houston',  'AMER', 3, 1, 3),
    ('MIL-EDI',  'Italy',             'Rijswijk', 'EMEA', 6, 2, 2),
    ('MLA-DEL',  'Philippines',       'Singapore','APAC', 7, 11, 1),
    ('MLA-TFC',  'Philippines',       'Singapore','APAC', 7, 8, 1),
    ('MLB-DXS',  'Australia',         'Malaysia', 'APAC', 5, 1, 1),
    ('MLT',      'United Kingdom',    'London',   'EMEA', 4, 1, 2),
    ('MUM-BHO',  'India',             'Malaysia', 'APAC', 5, 2, 1),
    ('NOR',      'USA',               'Houston',  'AMER', 3, 10, 3),
    ('NWL',      'USA',               'Houston',  'AMER', 3, 1, 3),
    ('PAR-LDL',  'France',            'Rijswijk', 'EMEA', 6, 3, 2),
    ('NAN-QUE',  'France',            'Rijswijk', 'EMEA', 6,  1, 2),
    ('PER',      'Netherlands',       'Rijswijk', 'EMEA', 6, 10, 2),
    ('PFN-CLI',  'TRINIDAD & TOBAGO', 'Houston',  'AMER', 3, 2, 3),
    ('PRA',      'Czech Republic',    'Rijswijk', 'EMEA', 6,  1, 2),
    ('PRL',      'USA',               'Houston',  'AMER', 3, 1, 3),
    ('PTH-SH',   'Australia',         'Malaysia', 'APAC', 5, 4, 1),
    ('QAT-RLC',  'Qatar',             'Dubai',    'APAC', 2, 5, 1),
    ('RIO-VEN',  'Brazil',            'Houston',  'AMER', 3, 6, 3),
    ('ROM-PSS',  'Italy',             'Rijswijk', 'EMEA', 6, 1, 2),
    ('ROT-WEE',  'Netherlands',       'Rijswijk', 'EMEA', 6, 5, 2),
    ('SCT-REF',  'Canada',            'Calgary',  'AMER', 1, 9, 3),
    ('SGP-MTP',  'Singapore',         'Singapore','APAC', 7, 6, 1),
    ('SHA-HUB',  'China',             'Singapore','APAC', 7, 2, 1),
    ('SNA-REF',  'Canada',            'Calgary',  'AMER', 1, 3, 3),
    ('SOF-STY',  'Bulgaria',          'Rijswijk', 'EMEA', 6,  1, 2),
    ('SPL-SNO',  'Brazil',            'Houston',  'AMER', 3, 1, 3),
    ('SPL-PEC',  'Brazil',            'Houston',  'AMER', 3, 1, 3),
    ('STC-B',    'India',             'Malaysia', 'APAC', 5, 3, 1),
    ('STC-S',    'China',             'Singapore','APAC', 7, 1, 1),
    ('STE',      'Switzerland',       'Rijswijk', 'EMEA', 6,  1, 2),
    ('TAB',      'Philippines',       'Singapore','APAC', 7, 2, 1),
    ('TEX-HLP',  'USA',               'Houston',  'AMER', 3, 1, 3),
    ('VDD',      'India',             'Malaysia', 'APAC', 5, 1, 1),
    ('WAR-JER',  'Poland',            'Rijswijk', 'EMEA', 6,  2, 2),
    ('WES-LUD',  'Germany',           'Rijswijk', 'EMEA', 6, 7, 2),
    ('WND-WBK',  'Australia',         'Malaysia', 'APAC', 5, 2, 1),
    ('YOH-TSU',  'Japan',             'Singapore','APAC', 7, 1, 1),
    ('LMH',      'Peru',              'Houston',  'AMER', 3, 1, 3),
]


def hash_password(pwd: str) -> str:
    return hashlib.sha256(pwd.encode()).hexdigest()


@contextmanager
def get_conn():
    """Return a PyMySQL connection; commits on success, rolls back on error."""
    conn = pymysql.connect(
        **DB_CONFIG,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class DatabaseManager:

    # ── Schema bootstrap ──────────────────────────────────────────────────────

    def init_db(self):
        """Create database + tables if they don't exist, seed initial data."""
        # Create the database itself first (connect without specifying db)
        cfg = {k: v for k, v in DB_CONFIG.items() if k != 'database'}
        conn = pymysql.connect(**cfg, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute(
                "CREATE DATABASE IF NOT EXISTS `%s` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci" % DB_CONFIG['database']
            )
        conn.commit()
        conn.close()

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sites (
                        id            INT AUTO_INCREMENT PRIMARY KEY,
                        site_name     VARCHAR(100) NOT NULL,
                        country       VARCHAR(100) NOT NULL,
                        hub           VARCHAR(100) NOT NULL,
                        region        VARCHAR(20)  NOT NULL,
                        user_id       INT          NOT NULL,
                        approved_count INT         NOT NULL DEFAULT 0,
                        approver_id   INT          NOT NULL,
                        INDEX idx_region (region),
                        INDEX idx_hub    (hub),
                        INDEX idx_uid    (user_id),
                        INDEX idx_appid  (approver_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id         INT AUTO_INCREMENT PRIMARY KEY,
                        name       VARCHAR(150) NOT NULL,
                        username   VARCHAR(80)  NOT NULL UNIQUE,
                        password   VARCHAR(64)  NOT NULL,
                        user_type  ENUM('admin','user','approver') NOT NULL,
                        ref_id     INT          DEFAULT NULL,
                        created_at DATETIME     DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_utype (user_type)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS hc_changelog (
                        id          INT AUTO_INCREMENT PRIMARY KEY,
                        site_id     INT          NOT NULL,
                        site_name   VARCHAR(100) NOT NULL,
                        old_count   INT          NOT NULL,
                        new_count   INT          NOT NULL,
                        changed_by  VARCHAR(80)  NOT NULL,
                        changed_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_site (site_id),
                        INDEX idx_who  (changed_by)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS requests (
                        id               INT AUTO_INCREMENT PRIMARY KEY,
                        site_id          INT          NOT NULL,
                        site_name        VARCHAR(100) NOT NULL,
                        country          VARCHAR(100) NOT NULL,
                        hub              VARCHAR(100) NOT NULL,
                        region           VARCHAR(20)  NOT NULL,
                        current_count    INT          NOT NULL,
                        new_count        INT          NOT NULL,
                        change_type      ENUM('increase','decrease') NOT NULL,
                        decrease_type    ENUM('RSE','DSE','ITC','CAR') DEFAULT NULL,
                        justification    TEXT         NOT NULL,
                        status           VARCHAR(50)  NOT NULL DEFAULT 'New',
                        raised_by        VARCHAR(80)  NOT NULL,
                        raised_by_type   VARCHAR(20)  NOT NULL,
                        approver_id      INT          DEFAULT NULL,
                        approver_comment TEXT         DEFAULT NULL,
                        approved         TINYINT(1)   DEFAULT NULL,
                        created_at       DATETIME     DEFAULT CURRENT_TIMESTAMP,
                        updated_at       DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        updated_by       VARCHAR(80)  DEFAULT NULL,
                        process_comment  TEXT         DEFAULT NULL,
                        processed_at     DATETIME     DEFAULT NULL,
                        processed_by     VARCHAR(80)  DEFAULT NULL,
                        INDEX idx_status      (status),
                        INDEX idx_raised_by   (raised_by),
                        INDEX idx_approver_id (approver_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)

            # Seed sites
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM sites")
                if cur.fetchone()['cnt'] == 0:
                    cur.executemany(
                        "INSERT INTO sites "
                        "(site_name,country,hub,region,user_id,approved_count,approver_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        HC_DATA
                    )

            # Seed admin user
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE username=%s", ('admin',))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO users (name,username,password,user_type,ref_id) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        ('Administrator', 'admin', hash_password('admin123'), 'admin', None)
                    )

    # ── HC ────────────────────────────────────────────────────────────────────

    def get_hc(self, region='', hub='', search='', user_type='admin', ref_id=None):
        sql = "SELECT * FROM sites WHERE 1=1"
        params = []
        if region:
            sql += " AND region=%s";  params.append(region)
        if hub:
            sql += " AND hub=%s";     params.append(hub)
        if search:
            like = f'%{search}%'
            sql += " AND (site_name LIKE %s OR country LIKE %s OR hub LIKE %s OR region LIKE %s)"
            params.extend([like, like, like, like])
        if user_type == 'user':
            sql += " AND user_id=%s"; params.append(ref_id)
        elif user_type == 'approver':
            sql += " AND approver_id=%s"; params.append(ref_id)
        sql += " ORDER BY region, hub, site_name"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def get_distinct(self, col):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT DISTINCT `{col}` FROM sites ORDER BY `{col}`")
                return [r[col] for r in cur.fetchall()]

    def get_distinct_user_ids(self):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT user_id FROM sites ORDER BY user_id")
                return [r['user_id'] for r in cur.fetchall()]

    def get_distinct_approver_ids(self):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT approver_id FROM sites ORDER BY approver_id")
                return [r['approver_id'] for r in cur.fetchall()]

    def update_hc(self, site_id, new_count, changed_by):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT approved_count, site_name FROM sites WHERE id=%s", (site_id,))
                old = cur.fetchone()
                if not old:
                    raise ValueError("Site not found")
                cur.execute("UPDATE sites SET approved_count=%s WHERE id=%s", (new_count, site_id))
                cur.execute(
                    "INSERT INTO hc_changelog (site_id,site_name,old_count,new_count,changed_by) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (site_id, old['site_name'], old['approved_count'], new_count, changed_by)
                )

    def get_changelog(self, search=''):
        sql = "SELECT * FROM hc_changelog WHERE 1=1"
        params = []
        if search:
            like = f'%{search}%'
            sql += " AND (site_name LIKE %s OR changed_by LIKE %s)"
            params.extend([like, like])
        sql += " ORDER BY changed_at DESC"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def get_site_by_id(self, site_id):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM sites WHERE id=%s", (site_id,))
                return cur.fetchone()

    def get_sites_for_request(self, user_type='admin', ref_id=None):
        sql = ("SELECT id, site_name, country, hub, region, approved_count, approver_id "
               "FROM sites WHERE 1=1")
        params = []
        if user_type == 'user':
            sql += " AND user_id=%s";      params.append(ref_id)
        elif user_type == 'approver':
            sql += " AND approver_id=%s";  params.append(ref_id)
        sql += " ORDER BY site_name"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    # ── Users ─────────────────────────────────────────────────────────────────

    def get_user_by_credentials(self, username, password):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM users WHERE username=%s AND password=%s",
                    (username, hash_password(password))
                )
                return cur.fetchone()

    def get_all_users(self):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id,name,username,user_type,ref_id,created_at "
                    "FROM users ORDER BY id"
                )
                return cur.fetchall()

    def create_user(self, data):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (name,username,password,user_type,ref_id) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (data['name'], data['username'], hash_password(data['password']),
                     data['user_type'], data.get('ref_id'))
                )

    def delete_user(self, uid):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM users WHERE id=%s", (uid,))

    # ── Requests ──────────────────────────────────────────────────────────────

    def get_requests(self, user_type='admin', ref_id=None, search='',
                     view='all', username=''):
        sql = "SELECT * FROM requests WHERE 1=1"
        params = []
        if user_type == 'user':
            sql += " AND raised_by=%s"; params.append(username)
        elif user_type == 'approver':
            if view == 'mine':
                sql += " AND raised_by=%s"; params.append(username)
            elif view == 'approver':
                sql += " AND approver_id=%s"; params.append(ref_id)
            # view=='all' → approver sees nothing extra (stays as-is)
        # admin + view=='all' → no filter, sees everything

        if search:
            like = f'%{search}%'
            sql += (" AND (site_name LIKE %s OR country LIKE %s "
                    "OR raised_by LIKE %s OR status LIKE %s)")
            params.extend([like, like, like, like])
        sql += " ORDER BY created_at DESC"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def create_request(self, data, raised_by, raised_by_type):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM sites WHERE id=%s", (data['site_id'],))
                site = cur.fetchone()
                if not site:
                    raise ValueError("Site not found")
                cur.execute("""
                    INSERT INTO requests
                    (site_id, site_name, country, hub, region,
                     current_count, new_count, change_type, decrease_type,
                     justification, status, raised_by, raised_by_type, approver_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    site['id'], site['site_name'], site['country'],
                    site['hub'], site['region'],
                    site['approved_count'], data['new_count'],
                    data['change_type'], data.get('decrease_type'),
                    data['justification'], 'New',
                    raised_by, raised_by_type, site['approver_id']
                ))

    def mark_request_processed(self, req_id, processed_by, comment=''):
        """Irreversibly marks a request as Processed. Only admin can call this."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE requests
                    SET status=%s, updated_by=%s, updated_at=NOW(),
                        process_comment=%s, processed_at=NOW(), processed_by=%s
                    WHERE id=%s
                """, ('Processed', processed_by, comment, processed_by, req_id))

    def get_request_by_id(self, req_id):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM requests WHERE id=%s", (req_id,))
                return cur.fetchone()

    def update_request_status(self, req_id, status, updated_by,
                               comment=None, approved=None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                if comment is not None or approved is not None:
                    cur.execute("""
                        UPDATE requests
                        SET status=%s, updated_by=%s, updated_at=NOW(),
                            approver_comment=%s, approved=%s
                        WHERE id=%s
                    """, (status, updated_by, comment,
                          1 if approved else 0, req_id))
                else:
                    cur.execute("""
                        UPDATE requests
                        SET status=%s, updated_by=%s, updated_at=NOW()
                        WHERE id=%s
                    """, (status, updated_by, req_id))


db_manager = DatabaseManager()
