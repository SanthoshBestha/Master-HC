# OSS Master HC Portal  –  MySQL Edition

Flask 3 + PyMySQL + Bootstrap 5 headcount management portal.

---

## 1. Install dependencies

```bash
pip install flask pymysql cryptography
```

---

## 2. Configure your MySQL connection

Open **`config.py`** and edit:

```python
DB_CONFIG = {
    'host':     'localhost',
    'port':     3306,
    'user':     'root',
    'password': 'YOUR_MYSQL_PASSWORD',   # ← change this
    'database': 'oss_hc',
    'charset':  'utf8mb4',
}
```

The database `oss_hc` **will be created automatically** the first time you run the app.

---

## 3. Run the application

```bash
python app.py
```

Open **http://localhost:5000**

Default admin login: **`admin` / `admin123`**

---

## 4. Project structure

```
oss_hc_mysql/
├── app.py          ← Flask routes
├── database.py     ← All MySQL queries (PyMySQL)
├── config.py       ← DB credentials  ← EDIT THIS
├── requirements.txt
└── templates/
    ├── base.html     ← Sidebar layout, DataTables, shared JS
    ├── login.html
    ├── admin.html    ← 5 tabs
    ├── user.html     ← 3 tabs
    └── approver.html ← 5 tabs
```

---

## 5. MySQL tables created automatically

| Table          | Purpose |
|----------------|---------|
| `sites`        | 102 sites from MAster_hc.xlsx |
| `users`        | Portal user accounts |
| `hc_changelog` | Every HC edit tracked with username + timestamp |
| `requests`     | All HC change requests |

---

## 6. User roles

| Role     | Access |
|----------|--------|
| **Admin**    | All 102 sites · Edit HC · Manage users · All requests |
| **User**     | Sites scoped to their User ID · Raise & view own requests |
| **Approver** | Sites scoped to their Approver ID · Approve/disapprove requests |

### User ID → Hub mapping (from Excel)

| User ID | Hub       | Region |
|---------|-----------|--------|
| 1       | Calgary   | AMER   |
| 2       | Dubai     | APAC   |
| 3       | Houston   | AMER   |
| 4       | London    | EMEA   |
| 5       | Malaysia  | APAC   |
| 6       | Rijswijk  | EMEA   |
| 7       | Singapore | APAC   |

### Approver ID → Sites mapping

| Approver ID | Region / Hubs |
|-------------|---------------|
| 1           | APAC – Dubai, Malaysia, Singapore |
| 2           | EMEA – London, Rijswijk |
| 3           | AMER – Calgary, Houston |

---

## 7. Request status flow

```
New  →  Pending from Approver  →  Ready to Process
```

- **Admin** can set any status on any request
- **Approver** Approve → "Ready to Process" · Disapprove → "Pending from Approver"
- **User** can only view status (read-only)

---

## 8. Features

- ✅ All tables have **search bars** + **Excel / CSV / PDF / Print** download
- ✅ Region + Hub filter dropdowns on every HC view
- ✅ HC Change Log records every edit (who + when + old/new value)
- ✅ Decrease type classification: **RSE / DSE / ITC / CAR**
- ✅ Approver comments stored per request and visible to all
- ✅ Sites grouped by country in the request dropdown
- ✅ Responsive sidebar layout

---

## 9. Production tips

- Set `debug=False` in `app.py`
- Change `SECRET_KEY` in `config.py`
- Use a dedicated MySQL user with limited permissions (not root)
- Deploy with `gunicorn`: `gunicorn -w 4 app:app`
