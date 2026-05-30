import calendar
import hmac
import io
import json
import os
import secrets
import select
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, date, timedelta, timezone
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo

import resend

from dotenv import load_dotenv
load_dotenv()

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
from flask import Flask, jsonify, g, render_template, request, session, redirect, url_for, send_file, abort
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')


def static_v(path: str) -> str:
    """Return /static/{path}?v={mtime} for cache-busting on deploys."""
    try:
        mtime = int(os.path.getmtime(os.path.join(app.static_folder, path)))
    except OSError:
        mtime = 0
    return f"/static/{path}?v={mtime}"


app.jinja_env.globals['static_v'] = static_v

DB_PATH = os.path.join(os.path.dirname(__file__), 'db', 'makolet_chain.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'db', 'schema.sql')
IL_TZ = ZoneInfo('Asia/Jerusalem')

# Valid user roles.
#   admin   — full access incl. /ops + /admin/*; sees every active branch.
#   ceo     — sees every active branch automatically (no user_branches rows),
#             but is blocked from /ops + /admin/*.
#   manager — sees only branches listed in user_branches.
VALID_ROLES = ('admin', 'ceo', 'manager')
ROLES_ALL_BRANCHES = ('admin', 'ceo')
ROLES_NOT_TRACKED = ('admin', 'ceo')


def _should_track(role):
    """Single source of truth for analytics exclusion. Admin and CEO are
    excluded from user_events to keep the dataset focused on operator activity."""
    return role not in ROLES_NOT_TRACKED

HEBREW_MONTHS = {
    1: 'ינואר', 2: 'פברואר', 3: 'מרץ', 4: 'אפריל',
    5: 'מאי', 6: 'יוני', 7: 'יולי', 8: 'אוגוסט',
    9: 'ספטמבר', 10: 'אוקטובר', 11: 'נובמבר', 12: 'דצמבר'
}

# Earliest month with real operational data. Routes clamp URL/session to this
# value (so /?month=2026-01 silently lands on April), and the month-back arrow
# is hidden when navigating it would cross the floor.
DATA_FLOOR_MONTH = '2026-04'


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH, timeout=30)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    with open(SCHEMA_PATH, 'r') as f:
        conn.executescript(f.read())
    # Migrations: add columns if missing
    _migrate_add_columns(conn)
    conn.close()


def _migrate_add_columns(conn):
    """Add new columns/tables to existing DB (safe to run repeatedly)."""
    migrations = [
        ('live_sales', 'cancellation_total', 'REAL DEFAULT 0'),
        ('live_sales', 'discount_total', 'REAL DEFAULT 0'),
        ('live_sales', 'running_total', 'REAL DEFAULT 0'),
        ('live_sales', 'running_count', 'INTEGER DEFAULT 0'),
        ('employees', 'aviv_employee_id', 'INTEGER'),
        ('employee_match_pending', 'aviv_employee_id', 'INTEGER'),
        ('employee_match_pending', 'source', "TEXT DEFAULT 'csv'"),
        ('employee_match_pending', 'is_new_employee', 'INTEGER DEFAULT 0'),
        ('employee_match_pending', 'is_csv_only', 'INTEGER DEFAULT 0'),
        ('employee_hours', 'verified_by_csv', 'INTEGER DEFAULT 0'),
    ]
    for table, col, col_type in migrations:
        try:
            conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_type}')
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
    # Ensure hourly_sales table exists
    conn.execute('''CREATE TABLE IF NOT EXISTS hourly_sales (
        branch_id INTEGER NOT NULL REFERENCES branches(id),
        date TEXT NOT NULL,
        hour INTEGER NOT NULL,
        amount REAL DEFAULT 0,
        transactions INTEGER DEFAULT 0,
        PRIMARY KEY (branch_id, date, hour)
    )''')
    # Ensure employee_aliases table exists
    conn.execute('''CREATE TABLE IF NOT EXISTS employee_aliases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER NOT NULL,
        alias_name TEXT NOT NULL,
        branch_id INTEGER NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(branch_id, alias_name)
    )''')
    # Ensure employee_hours_discrepancies table exists
    conn.execute('''CREATE TABLE IF NOT EXISTS employee_hours_discrepancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        branch_id INTEGER NOT NULL REFERENCES branches(id),
        month TEXT NOT NULL,
        employee_id INTEGER,
        employee_name TEXT NOT NULL,
        api_hours REAL,
        csv_hours REAL,
        difference REAL,
        created_at TEXT DEFAULT (datetime('now')),
        resolved INTEGER DEFAULT 0,
        resolution TEXT
    )''')
    conn.commit()


def seed_admin():
    """Seed the admin user if not exists."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    existing = conn.execute("SELECT id FROM users WHERE email = ?", ('makoletdashboard@gmail.com',)).fetchone()
    if not existing:
        admin_password = os.environ.get('ADMIN_PASSWORD', secrets.token_urlsafe(16))
        pw_hash = generate_password_hash(admin_password)
        conn.execute(
            "INSERT INTO users (name, email, password_hash, role) VALUES (?, ?, ?, ?)",
            ('מנהל ראשי', 'makoletdashboard@gmail.com', pw_hash, 'admin')
        )
        conn.commit()
        # Get user id
        user_row = conn.execute("SELECT id FROM users WHERE email = ?", ('makoletdashboard@gmail.com',)).fetchone()
        if user_row:
            conn.execute(
                "INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?, ?)",
                (user_row['id'], 126)
            )
            conn.commit()
    conn.close()


def send_reset_email(to_email: str, reset_url: str, user_name: str = ''):
    resend.api_key = os.environ.get('RESEND_API_KEY')
    resend.Emails.send({
        "from": "רשת המכולת <noreply@makoletdashboard.com>",
        "to": [to_email],
        "subject": "איפוס סיסמה — רשת המכולת",
        "html": f"""
        <div dir="rtl" style="font-family: Arial, sans-serif; max-width: 520px;
             margin: auto; padding: 32px; background: #f9f9f9; border-radius: 12px;">
          <div style="text-align: center; margin-bottom: 24px;">
            <h1 style="color: #0d1526; font-size: 24px; margin: 0;">רשת המכולת</h1>
            <p style="color: #666; margin: 4px 0 0;">מערכת ניהול</p>
          </div>
          <div style="background: white; padding: 24px; border-radius: 8px;
               border: 1px solid #e0e0e0;">
            <h2 style="color: #0d1526; font-size: 18px;">איפוס סיסמה</h2>
            <p style="color: #444; line-height: 1.6;">
              קיבלנו בקשה לאיפוס הסיסמה לחשבון שלך.
              לחץ על הכפתור למטה כדי לאפס את הסיסמה:
            </p>
            <div style="text-align: center; margin: 24px 0;">
              <a href="{reset_url}" style="
                background: #6366f1;
                color: white;
                padding: 14px 32px;
                border-radius: 8px;
                text-decoration: none;
                font-size: 16px;
                font-weight: bold;
                display: inline-block;
              ">איפוס סיסמה</a>
            </div>
            <p style="color: #888; font-size: 13px; border-top: 1px solid #eee;
               padding-top: 16px; margin-top: 16px;">
              הקישור תקף ל-30 דקות בלבד.<br>
              אם לא ביקשת לאפס סיסמה, התעלם מהמייל הזה.
            </p>
          </div>
        </div>
        """
    })


# ── Auth ──────────────────────────────────────────────────────

def _record_event(event_type, page=None, branch_id=None, duration_seconds=None):
    """Record a user_event. Admin events are silently dropped (per design).

    MUST be silent on any failure — analytics must never break a user request.
    """
    try:
        if 'user_id' not in session:
            return
        if not _should_track(session.get('user_role')):
            return
        ua = request.headers.get('User-Agent', '') if request else ''
        db = get_db()
        db.execute(
            'INSERT INTO user_events '
            '(user_id, event_type, page, branch_id, duration_seconds, user_agent) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (session['user_id'], event_type, page, branch_id,
             duration_seconds, ua[:255])
        )
        db.commit()
    except Exception as e:
        try:
            app.logger.warning(f"_record_event failed: {e}")
        except Exception:
            pass


@app.before_request
def _track_page_view():
    """Track authenticated GETs to HTML pages. Skips API, static, beacons."""
    if request.method != 'GET':
        return
    path = request.path
    if path.startswith('/api/'):
        return
    if path.startswith('/static/'):
        return
    if path == '/login' or path == '/logout':
        return
    if path == '/forgot-password' or path == '/reset-password':
        return
    if 'user_id' not in session:
        return
    branch_id = request.args.get('branch_id', type=int) or session.get('branch_id')
    _record_event('page_view', page=path, branch_id=branch_id)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            # Return JSON 401 for API requests, redirect for page requests
            if request.path.startswith('/api/') or request.is_json:
                return jsonify({'error': 'יש להתחבר מחדש', 'redirect': '/login'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def _serve_service_worker():
    """Serve sw.js with Service-Worker-Allowed: / so it can control the whole
    origin even though it lives under /static/. Browsers require this header
    when the worker URL is not at root.
    """
    sw_path = os.path.join(app.static_folder, 'sw.js')
    resp = send_file(sw_path, mimetype='application/javascript')
    resp.headers['Service-Worker-Allowed'] = '/'
    resp.headers['Cache-Control'] = 'no-cache'
    return resp


@app.route('/sw.js')
def service_worker_root():
    return _serve_service_worker()


@app.route('/static/sw.js')
def service_worker_static():
    # Override Flask's default /static handler so the header is attached.
    return _serve_service_worker()


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if 'user_id' in session:
            return redirect('/')
        message = request.args.get('message', '')
        return render_template('login.html', error=None, message=message)

    email = request.form.get('email', '').strip().lower()
    password = request.form.get('password', '')

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE LOWER(email) = ? AND active = 1", (email,)).fetchone()

    if user and check_password_hash(user['password_hash'], password):
        if request.form.get('remember'):
            session.permanent = True
            app.permanent_session_lifetime = timedelta(days=30)

        session['user_id'] = user['id']
        session['user_name'] = user['name']
        session['user_role'] = user['role']

        # Get user's branches
        branches = db.execute(
            "SELECT branch_id FROM user_branches WHERE user_id = ?", (user['id'],)
        ).fetchall()
        branch_ids = [r['branch_id'] for r in branches]
        session['user_branches'] = branch_ids

        # Set default branch
        if branch_ids:
            session['branch_id'] = branch_ids[0]

        _record_event('login', branch_id=session.get('branch_id'))
        return redirect('/')

    return render_template('login.html', error='אימייל או סיסמה שגויים')


@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'GET':
        return render_template('forgot_password.html')

    email = request.form.get('email', '').strip().lower()
    db = get_db()
    user = db.execute('SELECT id FROM users WHERE LOWER(email)=? AND active=1', (email,)).fetchone()
    if user:
        token = secrets.token_urlsafe(32)
        expires = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
        db.execute('INSERT INTO reset_tokens (user_id, token, expires_at) VALUES (?,?,?)',
                   (user['id'], token, expires))
        db.commit()
        reset_url = f"https://app.makoletdashboard.com/reset-password?token={token}"
        try:
            send_reset_email(email, reset_url)
        except Exception as e:
            app.logger.error(f"Failed to send reset email: {e}")
    return render_template('forgot_password.html',
                           sent=True,
                           message="אם האימייל קיים במערכת, נשלח קישור לאיפוס סיסמה תוך מספר שניות")


@app.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    if request.method == 'GET':
        token = request.args.get('token', '')
        db = get_db()
        row = db.execute('''SELECT rt.*, u.email FROM reset_tokens rt
                            JOIN users u ON rt.user_id = u.id
                            WHERE rt.token=? AND rt.used=0''', (token,)).fetchone()
        if not row:
            return render_template('reset_password.html', error="הקישור לא תקין")
        expires = datetime.fromisoformat(row['expires_at'])
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expires:
            return render_template('reset_password.html', error="הקישור פג תוקף. בקש קישור חדש.")
        return render_template('reset_password.html', token=token)

    token = request.form.get('token', '')
    password = request.form.get('password', '')
    confirm = request.form.get('confirm_password', '')
    if password != confirm:
        return render_template('reset_password.html', token=token, error="הסיסמאות אינן תואמות")
    if len(password) < 8:
        return render_template('reset_password.html', token=token, error="הסיסמה חייבת להכיל לפחות 8 תווים")
    db = get_db()
    row = db.execute('''SELECT rt.*, u.id as uid FROM reset_tokens rt
                        JOIN users u ON rt.user_id = u.id
                        WHERE rt.token=? AND rt.used=0''', (token,)).fetchone()
    if not row:
        return render_template('reset_password.html', error="הקישור לא תקין")
    expires = datetime.fromisoformat(row['expires_at'])
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expires:
        return render_template('reset_password.html', error="הקישור פג תוקף")
    db.execute('UPDATE users SET password_hash=? WHERE id=?',
               (generate_password_hash(password), row['uid']))
    db.execute('UPDATE reset_tokens SET used=1 WHERE token=?', (token,))
    db.commit()
    return redirect(url_for('login', message="הסיסמה עודכנה בהצלחה! התחבר עם הסיסמה החדשה"))


# ── Helpers ───────────────────────────────────────────────────

def _now_il():
    return datetime.now(IL_TZ)


def _utc_str_to_il_iso(utc_str):
    """Convert a SQLite datetime('now') UTC string to Israel-local ISO.

    SQLite's datetime('now') returns naive 'YYYY-MM-DD HH:MM:SS' in UTC. We
    surface fetched_at to the UI as an Israel-local timestamp, DST-safe via
    zoneinfo. Returns None on missing/unparseable input. Output format
    matches what sales.html already slices (YYYY-MM-DDTHH:MM:SS).
    """
    if not utc_str:
        return None
    s = str(utc_str).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S.%f'):
        try:
            dt = datetime.strptime(s, fmt)
            break
        except ValueError:
            dt = None
    if dt is None:
        return s
    return dt.replace(tzinfo=timezone.utc).astimezone(IL_TZ).strftime(
        '%Y-%m-%dT%H:%M:%S')


def _parse_month():
    """Return the active month, clamped to DATA_FLOOR_MONTH.

    A URL `?month=` below the floor (or a stale session value) is silently
    bumped up to the floor — never below it.
    """
    month = request.args.get('month')
    if month:
        if month < DATA_FLOOR_MONTH:
            month = DATA_FLOOR_MONTH
        session['selected_month'] = month
    else:
        month = session.get('selected_month')
    if not month:
        month = _now_il().strftime('%Y-%m')
    if month < DATA_FLOOR_MONTH:
        month = DATA_FLOOR_MONTH
    return month


def _month_nav(selected):
    year, mon = map(int, selected.split('-'))
    pm = mon - 1 if mon > 1 else 12
    py = year if mon > 1 else year - 1
    prev_candidate = f'{py:04d}-{pm:02d}'
    # Hide the back arrow at the data floor — there's nothing useful to show
    # earlier than DATA_FLOOR_MONTH.
    prev_month = prev_candidate if prev_candidate >= DATA_FLOOR_MONTH else None
    current = _now_il().strftime('%Y-%m')
    nm = mon + 1 if mon < 12 else 1
    ny = year if mon < 12 else year + 1
    next_str = f'{ny:04d}-{nm:02d}'
    next_month = next_str if next_str <= current else None
    display = f'{HEBREW_MONTHS[mon]} {year}'
    show_today = selected != current
    return prev_month, next_month, display, show_today, current


def get_branch_id():
    """Resolve branch_id for the current request.

    Precedence: ?branch_id= URL param (if user is allowed to see it) →
    session['branch_id'] → first user_branches entry → first branch in DB
    (admin/ceo only). Never mutates session — API calls stay idempotent;
    session writes happen only in _get_branch_id() / _page_context().
    """
    url_bid = request.args.get('branch_id', type=int)
    if url_bid:
        role = session.get('user_role')
        allowed = session.get('user_branches', [])
        if role in ROLES_ALL_BRANCHES or url_bid in allowed:
            return url_bid
    bid = session.get('branch_id')
    if bid:
        return bid
    # Fallback: first assigned branch
    user_branches = session.get('user_branches', [])
    if user_branches:
        return user_branches[0]
    # Admin/CEO have no user_branches rows: fall back to first branch in DB
    if session.get('user_role') in ROLES_ALL_BRANCHES:
        db = get_db()
        row = db.execute('SELECT id FROM branches ORDER BY id LIMIT 1').fetchone()
        return row['id'] if row else None
    return None


def _get_branch_id():
    """Get branch_id for page routes — allows URL param switching with access validation."""
    bid = request.args.get('branch_id')
    if bid:
        bid = int(bid)
        role = session.get('user_role')
        branches = session.get('user_branches', [])
        if role in ROLES_ALL_BRANCHES or bid in branches:
            session['branch_id'] = bid
    return get_branch_id()


def _branch_name(branch_id):
    db = get_db()
    row = db.execute('SELECT name FROM branches WHERE id = ?', (branch_id,)).fetchone()
    return row['name'] if row else 'סניף לא ידוע'


# Stable per-branch colors, assigned by branch_id sort order so the same branch
# gets the same color across every chart on the network-overview page.
BRANCH_PALETTE = ['#378ADD', '#1D9E75', '#D85A30', '#7F77DD', '#E0A82E', '#888780']


def _list_visible_branches(user_id, role):
    """Return [{id, name}, ...] of active branches the user can see.

    admin / ceo → every active branch.
    manager     → only branches listed in user_branches.

    Sorted by branch id so colors assigned later are stable.
    """
    db = get_db()
    if role in ROLES_ALL_BRANCHES:
        rows = db.execute(
            "SELECT id, name FROM branches WHERE active = 1 ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]
    rows = db.execute(
        "SELECT b.id, b.name FROM branches b "
        "JOIN user_branches ub ON ub.branch_id = b.id "
        "WHERE b.active = 1 AND ub.user_id = ? ORDER BY b.id",
        (user_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def _page_context(active_page):
    requested = request.args.get('month')
    selected = _parse_month()
    # True iff the URL explicitly asked for a pre-floor month — used by the
    # template to render the "first month with data" notice.
    floor_clamped = bool(requested and requested < DATA_FLOOR_MONTH)
    branch_id = _get_branch_id()
    prev_month, next_month, month_display, show_today, current = _month_nav(selected)
    role = session.get('user_role')
    user_branches = session.get('user_branches', [])
    # Same "multi-branch account" definition the navbar branch switcher uses
    # (base.html:35): admin/ceo see every branch; managers with 2+ user_branches.
    is_multi_branch = role in ROLES_ALL_BRANCHES or (user_branches and len(user_branches) > 1)
    return {
        'active_page': active_page,
        'selected_month': selected,
        'data_floor_month': DATA_FLOOR_MONTH,
        'floor_clamped': floor_clamped,
        'branch_id': branch_id,
        'branch_name': _branch_name(branch_id),
        'prev_month': prev_month,
        'next_month': next_month,
        'month_display': month_display,
        'show_today_btn': show_today,
        'current_month': current,
        'is_multi_branch': bool(is_multi_branch),
    }


# ── Page Routes ──────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    ctx = _page_context('home')
    role = session.get('user_role')
    mode = session.get('home_view_mode', 'branch')
    if mode == 'network' and role in ROLES_ALL_BRANCHES:
        ctx['active_page'] = 'home'
        ctx['view_mode'] = 'network'
        return render_template('home_network.html', **ctx)
    ctx['view_mode'] = 'branch'
    return render_template('index.html', **ctx)


@app.route('/api/set-view-mode', methods=['POST'])
@login_required
def api_set_view_mode():
    """Persist the home-page toggle between 'branch' and 'network'.
    Only admin + ceo can switch into network mode."""
    role = session.get('user_role')
    if role not in ROLES_ALL_BRANCHES:
        return jsonify({'error': 'forbidden'}), 403
    data = request.get_json(silent=True) or {}
    mode = data.get('mode')
    if mode not in ('branch', 'network'):
        return jsonify({'error': 'invalid mode'}), 400
    session['home_view_mode'] = mode
    return jsonify({'ok': True, 'mode': mode})


@app.route('/network')
@login_required
def network_page():
    """Dedicated multi-branch live network page.

    Access: admin/ceo (sees all assigned branches) and managers with 2+
    user_branches (sees only their assigned branches). Single-branch
    accounts are redirected home — the page would be a one-tile grid.

    Data is loaded client-side via /api/live-sales/network, which already
    enforces user_branches access control (URL params can't leak).
    """
    role = session.get('user_role')
    user_branches = session.get('user_branches', [])
    is_multi_branch = role in ROLES_ALL_BRANCHES or (user_branches and len(user_branches) > 1)
    if not is_multi_branch:
        return redirect(url_for('index'))
    ctx = _page_context('network')
    return render_template('network.html', **ctx)


@app.route('/network/revenue')
@login_required
def network_revenue_page():
    """Chain-wide daily revenue headline (total-first). Admin/CEO only —
    managers keep their per-store /sales. Data via /api/network/revenue."""
    role = session.get('user_role')
    if role not in ROLES_ALL_BRANCHES:
        return redirect(url_for('index'))
    ctx = _page_context('network_revenue')
    return render_template('network_revenue.html', **ctx)


@app.route('/network/revenue-v2')
@login_required
def network_revenue_v2_page():
    """EXPERIMENTAL sandbox — a revenue page with a 'my network' ⇄ 'single
    store' toggle. Same access model as /sales: any logged-in user, each
    scoped to their own stores. Aggregate mode is scoped to the viewer's
    visible branches (admin/ceo → all; manager → theirs). Single mode reuses
    the existing /sales body via the _sales_* includes for a picked store.

    A single-store user has no aggregate worth showing, so the toggle is
    hidden and they land directly on their one store.
    """
    role = session.get('user_role')
    user_id = session.get('user_id')
    visible = _list_visible_branches(user_id, role)
    single_store = len(visible) <= 1

    if single_store:
        mode = 'single'
    else:
        mode = request.args.get('mode') or session.get('rev2_mode') or 'network'
        if mode not in ('network', 'single'):
            mode = 'network'
        session['rev2_mode'] = mode

    ctx = _page_context('revenue_v2')
    ctx['rev2_mode'] = mode
    ctx['rev2_single_store'] = single_store
    ctx['rev2_branches'] = visible

    if mode == 'single':
        # Pick the store: ?store= only if the viewer is allowed to see it
        # (never trust the URL), else their first visible branch.
        visible_ids = [b['id'] for b in visible]
        store = request.args.get('store', type=int) or session.get('rev2_store')
        if store not in visible_ids:
            store = visible_ids[0] if visible_ids else None
        session['rev2_store'] = store
        # Build the exact ctx /sales builds so the reused _sales_* partials
        # render identically for the picked store.
        db = get_db()
        rows = db.execute(
            "SELECT date, amount, transactions FROM daily_sales "
            "WHERE branch_id = ? AND strftime('%Y-%m', date) = ? ORDER BY date ASC",
            (store, ctx['selected_month'])
        ).fetchall()
        z_reports = [dict(r) for r in rows]
        ctx['charts_data'] = _sales_charts_data(z_reports)
        ctx['sales_footer'] = _build_sales_footer(z_reports)
        ctx['branch_id'] = store
        ctx['branch_name'] = _branch_name(store) if store else ctx['branch_name']
        ctx['rev2_store'] = store

    return render_template('revenue_v2.html', **ctx)


@app.route('/network/goods-v2')
@login_required
def network_goods_v2_page():
    """EXPERIMENTAL goods sandbox — mirrors /network/revenue-v2 for BilBoy
    goods spend. Toggle 'הסניפים שלי' (chain aggregate) ⇄ 'סניף בודד'. Same
    access model as /goods: any logged-in user, scoped to their own stores
    (admin/ceo → all; manager → theirs). Single mode reuses the /goods body
    via the _goods_* includes for a picked store. Single-store users get no
    toggle and land on their store's goods detail."""
    role = session.get('user_role')
    user_id = session.get('user_id')
    visible = _list_visible_branches(user_id, role)
    single_store = len(visible) <= 1

    if single_store:
        mode = 'single'
    else:
        mode = request.args.get('mode') or session.get('goods2_mode') or 'network'
        if mode not in ('network', 'single'):
            mode = 'network'
        session['goods2_mode'] = mode

    ctx = _page_context('goods_v2')
    ctx['goods2_mode'] = mode
    ctx['goods2_single_store'] = single_store
    ctx['goods2_branches'] = visible

    if mode == 'single':
        visible_ids = [b['id'] for b in visible]
        store = request.args.get('store', type=int) or session.get('goods2_store')
        if store not in visible_ids:
            store = visible_ids[0] if visible_ids else None
        session['goods2_store'] = store

        view = request.args.get('view')
        if view in ('list', 'grouped'):
            session['goods_view_mode'] = view
        view_mode = session.get('goods_view_mode', 'list')

        db = get_db()
        ctx.update(_goods_doc_context(store, ctx['selected_month'], db))
        ctx['view_mode'] = view_mode
        ctx['branch_id'] = store
        ctx['branch_name'] = _branch_name(store) if store else ctx['branch_name']
        ctx['goods2_store'] = store

    return render_template('goods_v2.html', **ctx)


@app.route('/network/employees-v2')
@login_required
def network_employees_v2_page():
    """EXPERIMENTAL employees sandbox — mirrors /network/goods-v2 for chain
    labor. Toggle 'הסניפים שלי' (chain aggregate) ⇄ 'סניף בודד'. Same access
    model as /employees: any logged-in user, scoped to their own stores
    (admin/ceo → all; manager → theirs). Single mode reuses the /employees body
    via the _employees_* includes for a picked store. Single-store users get no
    toggle and land on their store's employee detail."""
    role = session.get('user_role')
    user_id = session.get('user_id')
    visible = _list_visible_branches(user_id, role)
    single_store = len(visible) <= 1

    if single_store:
        mode = 'single'
    else:
        mode = request.args.get('mode') or session.get('emp2_mode') or 'network'
        if mode not in ('network', 'single'):
            mode = 'network'
        session['emp2_mode'] = mode

    ctx = _page_context('employees_v2')
    ctx['emp2_mode'] = mode
    ctx['emp2_single_store'] = single_store
    ctx['emp2_branches'] = visible

    if mode == 'single':
        visible_ids = [b['id'] for b in visible]
        store = request.args.get('store', type=int) or session.get('emp2_store')
        if store not in visible_ids:
            store = visible_ids[0] if visible_ids else None
        session['emp2_store'] = store
        # The reused _employees_* partials are fully client-side: they fetch
        # /api/employees + /api/labor-cost-ratio using BRANCH_ID. Both honor
        # ?branch_id= with server-side access validation, so setting branch_id
        # in ctx is all that's needed — a manager can never pull another
        # store's data even if they edit the URL.
        ctx['branch_id'] = store
        ctx['branch_name'] = _branch_name(store) if store else ctx['branch_name']
        ctx['emp2_store'] = store

    return render_template('employees_v2.html', **ctx)


# ── Sales charts ─────────────────────────────────────────────
# datetime.weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
_HE_WEEKDAY = {6: 'ראשון', 0: 'שני', 1: 'שלישי', 2: 'רביעי',
               3: 'חמישי', 4: 'שישי', 5: 'שבת'}


def _parse_z_rows(z_reports):
    """[{date,'amount'}, ...] → sorted [(date, amount)] ascending by date."""
    out = []
    for z in z_reports:
        d = z['date']
        if isinstance(d, str):
            d = datetime.strptime(d, '%Y-%m-%d').date()
        out.append((d, float(z['amount'] or 0)))
    out.sort(key=lambda t: t[0])
    return out


def _has_saturday_z(z_reports):
    return any(d.weekday() == 5 for d, _ in _parse_z_rows(z_reports))


def _build_daily_chart_data(z_reports):
    """One bar per Z-report date.

    Red = a bar that represents COMBINED Saturday+Sunday revenue.
    Any single-day bar (Saturday alone, Sunday alone, anything else) is
    blue. Friday is always blue.

    - has_saturday_z True (branch runs Saturday Zs): every bar is its own
      day, all blue, no secondary day-name label.
    - has_saturday_z False: a Sunday with no preceding Saturday Z is the
      combined שבת+ראשון bar → red + secondary label. (A Sunday that does
      have a preceding Saturday Z stays blue — unreachable while
      has_saturday_z is False, but coded defensively.)
    """
    rows = _parse_z_rows(z_reports)
    date_set = {d for d, _ in rows}
    has_sat = any(d.weekday() == 5 for d in date_set)
    out = []
    for d, amt in rows:
        secondary, color = None, 'blue'
        if not has_sat and d.weekday() == 6:          # Sunday, CASE B
            if (d - timedelta(days=1)) not in date_set:  # no preceding Sat Z
                secondary, color = 'שבת+ראשון', 'red'
        out.append({'date': d.strftime('%d/%m'),
                    'label_secondary': secondary,
                    'value': round(amt, 2), 'color': color})
    return out


def _build_dow_chart_data(z_reports):
    """Average revenue per weekday.

    Red = the combined שבת+ראשון bar only. Friday is always blue.

    - has_saturday_z True: 7 separate bars ראשון…שבת, ALL blue.
    - has_saturday_z False: 6 bars — combined שבת+ראשון first (= average
      of all Sunday revenues, since no Saturday data exists), red; then
      שני…שישי, all blue.
    """
    rows = _parse_z_rows(z_reports)
    has_sat = any(d.weekday() == 5 for d, _ in rows)
    buckets = {}
    for d, amt in rows:
        buckets.setdefault(d.weekday(), []).append(amt)

    def avg(wd):
        vals = buckets.get(wd, [])
        return round(sum(vals) / len(vals), 2) if vals else 0

    if has_sat:
        # 7 bars: ראשון … שבת — all blue (no combined-weekend bar).
        return [{'label': _HE_WEEKDAY[wd], 'value': avg(wd), 'color': 'blue'}
                for wd in (6, 0, 1, 2, 3, 4, 5)]
    # CASE B: 6 bars — combined שבת+ראשון first (all Sundays), then Mon..Fri.
    sun = buckets.get(6, [])
    combined = round(sum(sun) / len(sun), 2) if sun else 0
    out = [{'label': 'שבת+ראשון', 'value': combined, 'color': 'red'}]
    out += [{'label': _HE_WEEKDAY[wd], 'value': avg(wd), 'color': 'blue'}
            for wd in (0, 1, 2, 3, 4)]
    return out


def _build_cumulative_chart_data(z_reports):
    out, running = [], 0.0
    for d, amt in _parse_z_rows(z_reports):
        running += amt
        out.append({'date': d.strftime('%d/%m'), 'value': round(running, 2)})
    return out


def _sales_charts_data(z_reports):
    return {
        'daily': _build_daily_chart_data(z_reports),
        'dow': _build_dow_chart_data(z_reports),
        'cumulative': _build_cumulative_chart_data(z_reports),
        'has_saturday_z': _has_saturday_z(z_reports),
    }


def _build_sales_footer(z_reports):
    """Server-rendered table-footer totals (one cell per data column).
    Returns None when there are no rows."""
    if not z_reports:
        return None
    total_rev = 0.0
    total_txn = 0
    for z in z_reports:
        total_rev += float(z.get('amount') or 0)
        total_txn += int(z.get('transactions') or 0)
    return {
        'total_revenue': round(total_rev, 2),
        'total_transactions': total_txn,
        'avg_basket': round(total_rev / total_txn) if total_txn else 0,
    }


@app.route('/sales')
@login_required
def sales():
    ctx = _page_context('sales')
    db = get_db()
    rows = db.execute(
        "SELECT date, amount, transactions FROM daily_sales "
        "WHERE branch_id = ? AND strftime('%Y-%m', date) = ? ORDER BY date ASC",
        (ctx['branch_id'], ctx['selected_month'])
    ).fetchall()
    z_reports = [dict(r) for r in rows]
    ctx['charts_data'] = _sales_charts_data(z_reports)
    ctx['sales_footer'] = _build_sales_footer(z_reports)
    return render_template('sales.html', **ctx)


def _goods_doc_context(branch_id, month, db):
    """Build the server-rendered goods-page context (docs, supplier groups,
    totals) for one branch + month. Shared by /goods and the single-store mode
    of /network/goods-v2 so the reused _goods_* partials render identically.
    Does NOT include view_mode — the caller owns that (session-driven)."""
    rows = db.execute(
        "SELECT id, doc_date, supplier, ref_number, amount, doc_type "
        "FROM goods_documents WHERE branch_id = ? AND strftime('%Y-%m', doc_date) = ? "
        "ORDER BY doc_date DESC, id DESC",
        (branch_id, month)
    ).fetchall()
    docs = [dict(r) for r in rows]

    total = sum(d['amount'] for d in docs)
    total_before_vat = round(total / 1.17, 2)
    invoices_total = sum(d['amount'] for d in docs if d['doc_type'] == 3)
    delivery_total = sum(d['amount'] for d in docs if d['doc_type'] == 2)
    returns_total = sum(d['amount'] for d in docs if d['doc_type'] in (4, 5))
    count = len(docs)

    for d in docs:
        d['amount_before_vat'] = round(d['amount'] / 1.17, 2)

    groups_map = {}
    for d in docs:
        s = d['supplier'] or '—'
        g = groups_map.setdefault(s, {
            'supplier': s, 'count': 0, 'total': 0.0,
            'total_before_vat': 0.0, 'docs': []
        })
        g['count'] += 1
        g['total'] += d['amount']
        g['total_before_vat'] += d['amount_before_vat']
        g['docs'].append(d)
    groups = sorted(groups_map.values(), key=lambda g: g['total'], reverse=True)

    return {
        'docs': docs,
        'groups': groups,
        'total': total,
        'total_before_vat': total_before_vat,
        'invoices_total': invoices_total,
        'delivery_total': delivery_total,
        'returns_total': returns_total,
        'count': count,
    }


@app.route('/goods')
@login_required
def goods():
    ctx = _page_context('goods')

    view = request.args.get('view')
    if view in ('list', 'grouped'):
        session['goods_view_mode'] = view
    view_mode = session.get('goods_view_mode', 'list')

    db = get_db()
    ctx.update(_goods_doc_context(ctx['branch_id'], ctx['selected_month'], db))
    ctx['view_mode'] = view_mode
    return render_template('goods.html', **ctx)


@app.route('/employees')
@login_required
def employees():
    ctx = _page_context('employees')
    return render_template('employees.html', **ctx)


@app.route('/fixed-expenses')
@login_required
def fixed_expenses():
    ctx = _page_context('fixed')
    return render_template('fixed_expenses.html', **ctx)


# ── Shared helpers ────────────────────────────────────────────

def _calculate_salary_cost(branch_id: int, current_month: str) -> dict:
    """Single source of truth for salary calculation.
    Used by both /employees page and /api/summary.

    Current month: ONLY source='aviv_api' rows count.
    Past months: all sources count.
    Salary = SUM(employee_hours.total_hours × employees.hourly_rate) for the month.

    Returns {'amount', 'source', 'hours', 'label'}
    """
    db = get_db()

    # UPDATED 2026-04-18: Always use API-only rows (CSV path retired).
    # UPDATED 2026-05-09: Include 'aviv_report' source (new employer-report agent).
    # Only include hours for ACTIVE employees (inactive employees excluded from salary).
    rows = db.execute('''
        SELECT eh.employee_name, eh.total_hours, eh.total_salary, eh.source,
               e.hourly_rate, e.id as emp_id
        FROM employee_hours eh
        JOIN employees e ON (
            e.branch_id = eh.branch_id AND e.name = eh.employee_name AND e.active = 1
        )
        WHERE eh.branch_id = ? AND eh.month = ?
          AND eh.source IN ('aviv_api', 'aviv_report')
    ''', (branch_id, current_month)).fetchall()

    if not rows:
        return {'amount': 0, 'source': 'none', 'hours': 0, 'label': 'אין נתונים'}

    total_salary = 0
    total_hours = 0
    sources = set()
    for r in rows:
        hours = r['total_hours'] or 0
        rate = r['hourly_rate'] or 0
        salary = round(hours * rate, 2) if rate > 0 else (r['total_salary'] or 0)
        total_salary += salary
        total_hours += hours
        sources.add(r['source'] or 'unknown')

    # Determine source label
    has_api = ('aviv_api' in sources) or ('aviv_report' in sources)
    has_csv = 'csv' in sources
    if has_api and has_csv:
        source = 'api+csv'
    elif has_csv:
        source = 'csv'
    elif has_api:
        source = 'api'
    else:
        source = 'unknown'

    return {
        'amount': round(total_salary, 2),
        'source': source,
        'hours': round(total_hours, 2),
        'label': f'{round(total_hours, 1)} שעות'
    }


def _recalculate_avg_rate(branch_id: int, conn):
    """Recalculate weighted avg hourly rate for a branch.
    Called whenever an employee rate changes.
    Uses last month's CSV hours distribution as weights."""
    today = date.today()
    prev_month = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

    prev_rows = conn.execute('''
        SELECT eh.employee_name, eh.total_hours
        FROM employee_hours eh
        WHERE eh.branch_id=? AND eh.month=?
    ''', (branch_id, prev_month)).fetchall()

    employees = conn.execute('''
        SELECT name, hourly_rate FROM employees
        WHERE branch_id=? AND active=1 AND hourly_rate > 0
    ''', (branch_id,)).fetchall()

    if not employees:
        return

    if not prev_rows:
        avg = sum(e['hourly_rate'] for e in employees) / len(employees)
        conn.execute('UPDATE branches SET avg_hourly_rate=? WHERE id=?',
                     (round(avg, 2), branch_id))
        return

    emp_rates = {e['name']: e['hourly_rate'] for e in employees}

    total_weighted = 0
    total_hours = 0
    for row in prev_rows:
        rate = 0
        csv_clean = _clean_display_name(row['employee_name'], '')
        csv_tokens = set(csv_clean.split())
        for emp_name, emp_rate in emp_rates.items():
            emp_clean = _clean_display_name(emp_name, '')
            emp_tokens = set(emp_clean.split())
            if len(emp_tokens & csv_tokens) >= 2 or (len(emp_tokens) == 1 and emp_tokens & csv_tokens):
                rate = emp_rate
                break
        if rate > 0:
            total_weighted += row['total_hours'] * rate
            total_hours += row['total_hours']

    if total_hours > 0:
        avg = round(total_weighted / total_hours, 2)
        conn.execute('UPDATE branches SET avg_hourly_rate=? WHERE id=?',
                     (avg, branch_id))


# ── API Routes ───────────────────────────────────────────────

@app.route('/api/events/heartbeat', methods=['POST'])
@login_required
def api_heartbeat():
    """Time-on-page heartbeat. Fires every 30s + once on page-leave (beacon).
    Duration is cumulative from page-load (not delta)."""
    data = request.get_json(silent=True) or {}
    page = data.get('page')
    branch_id = data.get('branch_id')
    duration = data.get('duration_seconds')
    if not isinstance(duration, (int, float)) or duration < 0 or duration > 86400:
        return '', 204
    try:
        bid = int(branch_id) if branch_id not in (None, '') else None
    except (TypeError, ValueError):
        bid = None
    if bid is None:
        bid = session.get('branch_id')
    _record_event('heartbeat', page=page, branch_id=bid,
                  duration_seconds=int(duration))
    return '', 204


@app.route('/api/branches')
@login_required
def api_branches():
    db = get_db()
    role = session.get('user_role')
    if role in ROLES_ALL_BRANCHES:
        # admin/ceo: every branch, no user_branches rows needed
        rows = db.execute('SELECT id, name, city, active FROM branches ORDER BY id').fetchall()
    else:
        user_branches = session.get('user_branches', [])
        if not user_branches:
            return jsonify([])
        placeholders = ','.join('?' * len(user_branches))
        rows = db.execute(
            f'SELECT id, name, city, active FROM branches WHERE id IN ({placeholders}) ORDER BY id',
            user_branches
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/summary')
@login_required
def api_summary():
    """Return KPI summary for a branch + month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))

    db = get_db()
    # Income from daily_sales
    income = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM daily_sales "
        "WHERE branch_id = ? AND strftime('%Y-%m', date) = ?",
        (branch_id, month)
    ).fetchone()[0]

    # Goods from goods_documents
    goods = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM goods_documents "
        "WHERE branch_id = ? AND strftime('%Y-%m', doc_date) = ?",
        (branch_id, month)
    ).fetchone()[0]

    # Ensure monthly carry-forward before totals
    _ensure_monthly_expenses(branch_id, month, db)

    # Salary — single source of truth
    salary_data = _calculate_salary_cost(branch_id, month)
    salary = salary_data['amount']

    # Live income logic: if current month and today has no Z-report, add live amount
    today = _now_il().strftime('%Y-%m-%d')
    current_month = _now_il().strftime('%Y-%m')
    has_z = False
    live_amount_today = 0
    fresh_today = False
    stale_row = None

    if month == current_month:
        z_row = db.execute(
            "SELECT id FROM daily_sales WHERE branch_id = ? AND date = ?",
            (branch_id, today)
        ).fetchone()
        has_z = z_row is not None

        live_row = db.execute(
            'SELECT amount, transactions, last_updated, '
            'cancellation_total, discount_total, running_total, running_count '
            'FROM live_sales WHERE branch_id = ? AND date = ?',
            (branch_id, today)
        ).fetchone()

        fresh_today = bool(live_row and live_row['amount']
                           and live_row['last_updated'] != 'PAUSED')

        if fresh_today:
            # POLICY: live_amount_today is the FRESH today-only value. Stale
            # (prior-day) values feed the tile via live.is_stale but are
            # NEVER added to income — that would double-count a day whose
            # Z-report already landed in daily_sales. Keep the two separate.
            live_amount_today = live_row['amount']
            if not has_z:
                income += live_amount_today
        elif not has_z:
            # Calendar date has rolled over and no fresh pull yet → store-
            # closed state. Surface the most recent past-day live row only as
            # last_amount/last_date for context (never as the live number,
            # never into income math above).
            stale_row = db.execute(
                'SELECT amount, date FROM live_sales '
                'WHERE branch_id = ? AND amount > 0 AND date < ? '
                'ORDER BY date DESC, fetched_at DESC LIMIT 1',
                (branch_id, today)
            ).fetchone()
    else:
        live_row = None

    # Fixed expenses (% rows computed live from final income) + electricity
    fixed_data = _get_fixed_total(branch_id, month, income, db)
    fixed = fixed_data['total']

    profit = income - goods - fixed - salary

    live = None
    cancellation_total = 0
    discount_total = 0
    running_total = 0
    running_count = 0
    # Fresh today, or the has_z path (today's row shown as-is, unchanged).
    live_src = live_row if (fresh_today or (has_z and live_row)) else None
    if live_src is not None:
        live = {
            'amount': live_src['amount'],
            'transactions': live_src['transactions'],
            'last_updated': live_src['last_updated'],
            'is_stale': False,
            'is_closed': False,
        }
        try:
            cancellation_total = round(float(live_src['cancellation_total'] or 0), 2)
            discount_total = round(float(live_src['discount_total'] or 0), 2)
            running_total = round(float(live_src['running_total'] or 0), 2)
            running_count = int(live_src['running_count'] or 0)
        except (KeyError, TypeError):
            pass
    elif stale_row:
        # Calendar date has rolled to a new day, no fresh pull, no Z →
        # store-closed state. Past-day amount surfaces as last_amount only,
        # never as the live number. Not in income.
        live = {
            'amount': None,
            'transactions': None,
            'last_updated': None,
            'is_stale': False,
            'is_closed': True,
            'last_amount': stale_row['amount'],
            'last_date': stale_row['date'],
        }

    # Latest electricity invoice for the strip
    latest_elec = db.execute(
        "SELECT period_label, amount, due_date FROM electricity_invoices "
        "WHERE branch_id = ? ORDER BY due_date DESC LIMIT 1",
        (branch_id,)
    ).fetchone()
    # IEC last sync time + electricity_source
    branch_elec = db.execute(
        "SELECT iec_last_sync_at, electricity_source FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()

    return jsonify({
        'income': income,
        'goods': goods,
        'fixed': fixed,
        'fixed_only': fixed_data['fixed_only'],
        'electricity': fixed_data['electricity'],
        'salary': salary,
        'salary_source': salary_data['source'],
        'salary_label': salary_data['label'],
        'profit': profit,
        'live': live,
        'has_z': has_z,
        'live_amount_today': live_amount_today,
        'branch_id': branch_id,
        'month': month,
        'cancellation_total': cancellation_total,
        'discount_total': discount_total,
        'running_total': running_total,
        'running_count': running_count,
        'latest_electricity': {
            'period_label': latest_elec['period_label'],
            'amount': latest_elec['amount'],
            'due_date': latest_elec['due_date'],
        } if latest_elec else None,
        'iec_last_sync_at': branch_elec['iec_last_sync_at'] if branch_elec and branch_elec['iec_last_sync_at'] else None,
        'electricity_source': branch_elec['electricity_source'] if branch_elec else None,
    })


# Department codes surfaced as KPI tiles on the home page. The full ~35-row
# breakdown is stored in z_department_sales nightly; only these are
# highlighted on /. Adding a 4th dept is a one-line list edit — no schema
# change. dept_code is the source of truth; the display label here is the
# Hebrew tag managers want to see (Aviv's own names are stored too but can
# read awkwardly out of context, e.g. dept 2 = "ירקות פירות").
HOME_DEPT_TILES: list[dict] = [
    {'code': 5,  'label': 'מקרר חלב', 'icon': '🥛'},
    {'code': 83, 'label': 'סיגריות',  'icon': '🚬'},
    {'code': 2,  'label': 'ירקות',    'icon': '🥬'},
]


@app.route('/api/department-sales')
@login_required
def api_department_sales():
    """Return per-department sales for the selected branch.

    Default: most recent date with any z_department_sales rows for the branch
    (so the tile keeps showing yesterday's number when today's Z hasn't
    landed yet, instead of showing "—"). Override with ?date=YYYY-MM-DD.

    Response:
      {
        "branch_id": 127,
        "date": "2026-05-27" | null,
        "departments": [{"code": 5, "amount": 4150.33, "qty": 518.0,
                         "name": "מקרר-מוצרי חלב ותחליפים"}, ...],
        "tiles": [{"code": 5, "label": "מקרר חלב", "icon": "🥛",
                   "amount": 4150.33}, ...]
      }

    `tiles` is the home page's preferred renderer payload — codes from
    HOME_DEPT_TILES with their amounts looked up. Missing depts → amount=None
    so the template can render "—" gracefully.
    """
    branch_id = get_branch_id()
    db = get_db()

    target_date = request.args.get('date')
    if not target_date:
        # Latest date this branch has any dept data for.
        row = db.execute(
            'SELECT MAX(date) AS d FROM z_department_sales WHERE branch_id=?',
            (branch_id,)
        ).fetchone()
        target_date = row['d'] if row and row['d'] else None

    departments: list[dict] = []
    by_code: dict[int, dict] = {}
    if target_date:
        rows = db.execute(
            'SELECT dept_code, dept_name, amount, qty FROM z_department_sales '
            'WHERE branch_id=? AND date=? ORDER BY amount DESC',
            (branch_id, target_date)
        ).fetchall()
        for r in rows:
            entry = {
                'code': r['dept_code'],
                'name': r['dept_name'],
                'amount': r['amount'],
                'qty': r['qty'],
            }
            departments.append(entry)
            by_code[r['dept_code']] = entry

    tiles = []
    for t in HOME_DEPT_TILES:
        entry = by_code.get(t['code'])
        tiles.append({
            'code': t['code'],
            'label': t['label'],
            'icon': t['icon'],
            'amount': entry['amount'] if entry else None,
        })

    return jsonify({
        'branch_id': branch_id,
        'date': target_date,
        'departments': departments,
        'tiles': tiles,
    })


# Department tiles shown at the top of /sales — same 3 depts as the (now
# removed) home tiles, but each tile's hero number is the AVERAGE DAILY
# PERCENTAGE of that dept's share of the day's Z, across the selected month.
# Codes/colors mirror the per-day expand panel in sales.html.
SALES_DEPT_TILES: list[dict] = [
    {'code': 5,  'label': 'חלב',     'icon': '🥛', 'accent': '#60a5fa'},
    {'code': 83, 'label': 'סיגריות', 'icon': '🚬', 'accent': '#fbbf24'},
    {'code': 2,  'label': 'ירקות',   'icon': '🥬', 'accent': '#4ade80'},
]


@app.route('/api/department-sales-monthly')
@login_required
def api_department_sales_monthly():
    """Per-dept monthly summary for /sales: average daily % + ₪ total.

    For the selected branch + month, the hero number per dept is the
    EQUAL-WEIGHT AVERAGE of each qualifying day's percentage
    (dept_amount / day_Z_total * 100) — NOT month-total / month-total.

    A "qualifying day" is one that has a real Z (daily_sales row, amount > 0,
    non-provisional source) AND has z_department_sales rows (proving the 902
    was actually parsed for that day). Days with no Z, closed-day sentinels,
    or days where the 902 was never fetched are excluded entirely — they are
    NOT counted as 0%. A day that DID parse a 902 but has no row for a given
    dept counts as 0% for that dept (genuine zero — other depts were itemized
    that day, so this one simply sold nothing).

    Response:
      {
        "branch_id": 127, "month": "2026-05", "days_counted": 21,
        "tiles": [{"code": 5, "label": "חלב", "icon": "🥛",
                   "accent": "#60a5fa", "avg_pct": 18.3, "total": 87654.32}, ...]
      }
    """
    branch_id = get_branch_id()
    month = request.args.get('month') or session.get('selected_month')
    db = get_db()

    # Qualifying days: real Z (amount > 0, non-provisional) that also has a
    # parsed 902 (at least one z_department_sales row that day).
    day_rows = db.execute(
        "SELECT ds.date AS date, ds.amount AS z_total FROM daily_sales ds "
        "WHERE ds.branch_id=? AND strftime('%Y-%m', ds.date)=? "
        "AND ds.amount > 0 AND ds.source NOT IN ('live_provisional', 'provisional') "
        "AND EXISTS (SELECT 1 FROM z_department_sales z "
        "            WHERE z.branch_id=ds.branch_id AND z.date=ds.date) "
        "ORDER BY ds.date ASC",
        (branch_id, month)
    ).fetchall()
    qualifying = {r['date']: r['z_total'] for r in day_rows}

    # Per-dept amount per qualifying day, for the 3 tile codes only.
    codes = [t['code'] for t in SALES_DEPT_TILES]
    placeholders = ','.join('?' * len(codes))
    dept_rows = db.execute(
        f"SELECT date, dept_code, amount FROM z_department_sales "
        f"WHERE branch_id=? AND strftime('%Y-%m', date)=? "
        f"AND dept_code IN ({placeholders})",
        (branch_id, month, *codes)
    ).fetchall()
    # {code: {date: amount}}
    by_code: dict[int, dict] = {c: {} for c in codes}
    for r in dept_rows:
        if r['date'] in qualifying:
            by_code[r['dept_code']][r['date']] = r['amount']

    tiles = []
    for t in SALES_DEPT_TILES:
        per_day = by_code[t['code']]
        total = 0.0
        pct_sum = 0.0
        for date, z_total in qualifying.items():
            amt = per_day.get(date, 0.0) or 0.0
            total += amt
            if z_total and z_total > 0:
                pct_sum += amt / z_total * 100
        avg_pct = round(pct_sum / len(qualifying), 1) if qualifying else None
        tiles.append({
            'code': t['code'],
            'label': t['label'],
            'icon': t['icon'],
            'accent': t['accent'],
            'avg_pct': avg_pct,
            'total': round(total, 2),
        })

    return jsonify({
        'branch_id': branch_id,
        'month': month,
        'days_counted': len(qualifying),
        'tiles': tiles,
    })


@app.route('/api/network-overview')
@login_required
def api_network_overview():
    """Chain-wide aggregate for the CEO 'network' view.

    Returns the single payload that feeds all six chart sections on
    home_network.html (monthly revenue, 6-month trend, profitability,
    average basket, expense breakdown, leaderboard). Reuses
    _calculate_salary_cost and _get_fixed_total so every number on this
    page matches the per-branch home page.
    """
    role = session.get('user_role')
    if role not in ROLES_ALL_BRANCHES:
        return jsonify({'error': 'forbidden'}), 403

    db = get_db()
    visible = _list_visible_branches(session.get('user_id'), role)
    if not visible:
        return jsonify({
            'branches': [],
            'monthly_revenue': [],
            'trend_6m': {'months': [], 'series': []},
            'profitability': [],
            'avg_basket': [],
            'expense_breakdown': {'goods': 0, 'salary': 0, 'electricity': 0, 'fixed_other': 0},
            'leaderboard': [],
        })

    # Stable color per branch by sort order.
    branches = []
    for i, b in enumerate(visible):
        branches.append({
            'id': b['id'],
            'name': b['name'],
            'color': BRANCH_PALETTE[i % len(BRANCH_PALETTE)],
        })

    current_month = _now_il().strftime('%Y-%m')

    # Build the trailing 6-month window (oldest → current), then drop any
    # months earlier than the data floor — those would render as flat zeros
    # and look broken.
    cy, cm = map(int, current_month.split('-'))
    trend_months = []
    y, m = cy, cm
    for _ in range(6):
        trend_months.append(f'{y:04d}-{m:02d}')
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    trend_months.reverse()
    trend_months = [ms for ms in trend_months if ms >= DATA_FLOOR_MONTH]
    trend_labels = [f'{int(ms.split("-")[1])}/{ms.split("-")[0]}' for ms in trend_months]

    monthly_revenue = []
    profitability = []
    avg_basket = []
    trend_series = []
    total_goods = 0.0
    total_salary = 0.0
    total_elec = 0.0
    total_fixed_other = 0.0

    for b in branches:
        bid = b['id']
        bname = b['name']

        # Current-month revenue + transactions
        row = db.execute(
            "SELECT COALESCE(SUM(amount),0) AS revenue, "
            "COALESCE(SUM(transactions),0) AS txn "
            "FROM daily_sales WHERE branch_id=? AND strftime('%Y-%m',date)=?",
            (bid, current_month)
        ).fetchone()
        revenue = round(float(row['revenue'] or 0), 2)
        txn = int(row['txn'] or 0)

        goods = db.execute(
            "SELECT COALESCE(SUM(amount),0) FROM goods_documents "
            "WHERE branch_id=? AND strftime('%Y-%m',doc_date)=?",
            (bid, current_month)
        ).fetchone()[0]
        goods = round(float(goods or 0), 2)

        _ensure_monthly_expenses(bid, current_month, db)
        fix_data = _get_fixed_total(bid, current_month, revenue, db)
        fixed_only = round(float(fix_data['fixed_only']), 2)
        electricity = round(float(fix_data['electricity']['amount']), 2)

        salary = _calculate_salary_cost(bid, current_month)['amount']
        salary = round(float(salary), 2)

        profit = round(revenue - goods - salary - fixed_only - electricity, 2)
        profit_pct = round((profit / revenue) * 100, 1) if revenue > 0 else 0

        basket = round(revenue / txn, 2) if txn > 0 else 0

        monthly_revenue.append({
            'branch_id': bid, 'branch_name': bname, 'value': revenue,
        })
        avg_basket.append({
            'branch_id': bid, 'branch_name': bname, 'value': basket,
        })
        profitability.append({
            'branch_id': bid, 'branch_name': bname,
            'revenue': revenue, 'goods': goods, 'salary': salary,
            'fixed': fixed_only, 'electricity': electricity,
            'profit': profit, 'profit_pct': profit_pct,
        })

        total_goods += goods
        total_salary += salary
        total_elec += electricity
        total_fixed_other += fixed_only

        # 6-month revenue trend
        trend_data = []
        for ms in trend_months:
            tr = db.execute(
                "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
                "WHERE branch_id=? AND strftime('%Y-%m',date)=?",
                (bid, ms)
            ).fetchone()[0]
            trend_data.append(round(float(tr or 0), 2))
        trend_series.append({
            'branch_id': bid, 'branch_name': bname, 'color': b['color'],
            'data': trend_data,
        })

    # Leaderboard — sorted by profit descending
    ranked = sorted(profitability, key=lambda r: r['profit'], reverse=True)
    leaderboard = [{
        'rank': i + 1,
        'branch_id': r['branch_id'],
        'branch_name': r['branch_name'],
        'revenue': r['revenue'],
        'profit': r['profit'],
        'profit_pct': r['profit_pct'],
    } for i, r in enumerate(ranked)]

    return jsonify({
        'branches': branches,
        'month': current_month,
        'monthly_revenue': monthly_revenue,
        'trend_6m': {'months': trend_labels, 'series': trend_series},
        'profitability': profitability,
        'avg_basket': avg_basket,
        'expense_breakdown': {
            'goods': round(total_goods, 2),
            'salary': round(total_salary, 2),
            'electricity': round(total_elec, 2),
            'fixed_other': round(total_fixed_other, 2),
        },
        'leaderboard': leaderboard,
    })


@app.route('/api/network/revenue')
@login_required
def api_network_revenue():
    """Chain-wide DAILY revenue headline for the admin/CEO network view.

    Total-first: chain total for a single day, % vs the prior calendar day,
    average per reporting store, a 7-day chain-total sparkline, a truthful
    coverage line (how many active branches actually have a daily_sales row
    that day + which are missing), and a ranked per-branch strip.

    Source: daily_sales (Z-reports) — the only feed with reliable per-branch
    daily totals across every chain store. Default date = the most recent day
    with ANY data (today is empty before the nightly Z sync / when agents are
    off), so the headline never opens blank. Admin/CEO only.
    """
    role = session.get('user_role')
    if role not in ROLES_ALL_BRANCHES:
        return jsonify({'error': 'forbidden'}), 403
    visible = _list_visible_branches(session.get('user_id'), role)
    return jsonify(_network_revenue_payload(visible, request.args.get('date'), get_db()))


def _network_revenue_payload(visible, req_date, db):
    """Daily chain-revenue payload for a set of visible branches.

    Shared by /api/network/revenue (admin/CEO, all branches) and
    /api/network/revenue-v2 (any user, scoped to their own stores). The caller
    is responsible for access control + supplying `visible` — this function
    only aggregates whatever branches it is handed, so a manager can never see
    another manager's stores. Source: daily_sales.
    """
    total_branches = len(visible)
    empty = {
        'date': None, 'chain_total': 0, 'avg_per_store': 0,
        'prev_date': None, 'prev_total': 0, 'pct_vs_prev': None,
        'total_branches': total_branches, 'reported': 0, 'missing': [],
        'per_branch': [], 'top': None, 'bottom': None, 'series_14d': [],
        'momentum': [],
    }
    if not visible:
        return empty

    branch_ids = [b['id'] for b in visible]
    names = {b['id']: b['name'] for b in visible}
    ph = ','.join('?' * len(branch_ids))

    # Resolve the date: explicit YYYY-MM-DD wins (validated), else the most
    # recent day with any daily_sales row among visible branches.
    sel_date = None
    if req_date:
        try:
            sel_date = datetime.strptime(req_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            sel_date = None
    if not sel_date:
        row = db.execute(
            f"SELECT MAX(date) AS d FROM daily_sales WHERE branch_id IN ({ph})",
            branch_ids
        ).fetchone()
        sel_date = row['d'] if row and row['d'] else _now_il().strftime('%Y-%m-%d')

    def _day_total(d):
        r = db.execute(
            f"SELECT COALESCE(SUM(amount),0) AS t FROM daily_sales "
            f"WHERE date=? AND branch_id IN ({ph})",
            [d] + branch_ids
        ).fetchone()
        return round(float(r['t'] or 0), 2)

    # Per-branch rows for the selected day (only branches WITH a row).
    rows = db.execute(
        f"SELECT branch_id, COALESCE(SUM(amount),0) AS amount, "
        f"COALESCE(SUM(transactions),0) AS txn "
        f"FROM daily_sales WHERE date=? AND branch_id IN ({ph}) "
        f"GROUP BY branch_id",
        [sel_date] + branch_ids
    ).fetchall()

    per_branch = sorted(
        [{'branch_id': r['branch_id'],
          'branch_name': names.get(r['branch_id'], 'סניף לא ידוע'),
          'amount': round(float(r['amount'] or 0), 2)} for r in rows],
        key=lambda x: x['amount'], reverse=True
    )
    reported_ids = {r['branch_id'] for r in rows}
    missing = [{'branch_id': b['id'], 'branch_name': b['name']}
               for b in visible if b['id'] not in reported_ids]

    chain_total = round(sum(r['amount'] for r in per_branch), 2)
    reported = len(per_branch)
    avg_per_store = round(chain_total / reported, 2) if reported else 0

    total_transactions = int(sum(int(r['txn'] or 0) for r in rows))
    avg_basket = round(chain_total / total_transactions, 2) if total_transactions else 0

    prev_date = db.execute("SELECT date(?, '-1 day') AS d", (sel_date,)).fetchone()['d']
    prev_total = _day_total(prev_date)
    pct_vs_prev = round((chain_total - prev_total) / prev_total * 100, 1) if prev_total > 0 else None

    # Rolling month-to-date (month-start → sel_date) vs the same span of the
    # previous month (fair partial-vs-partial comparison, day clamped).
    sd = datetime.strptime(sel_date, '%Y-%m-%d').date()
    month_start = sd.replace(day=1)
    py, pm = (sd.year, sd.month - 1) if sd.month > 1 else (sd.year - 1, 12)
    prev_start = date(py, pm, 1)
    prev_end = date(py, pm, min(sd.day, calendar.monthrange(py, pm)[1]))

    def _range_total(d0, d1):
        r = db.execute(
            f"SELECT COALESCE(SUM(amount),0) AS t FROM daily_sales "
            f"WHERE date BETWEEN ? AND ? AND branch_id IN ({ph})",
            [d0.isoformat(), d1.isoformat()] + branch_ids
        ).fetchone()
        return round(float(r['t'] or 0), 2)

    month_to_date_total = _range_total(month_start, sd)
    prev_month_total = _range_total(prev_start, prev_end)
    pct_vs_prev_month = (round((month_to_date_total - prev_month_total) / prev_month_total * 100, 1)
                         if prev_month_total > 0 else None)

    # 14-day chain-total trend ending on sel_date (zero-filled).
    start_date = (datetime.strptime(sel_date, '%Y-%m-%d') - timedelta(days=13)).strftime('%Y-%m-%d')
    series_rows = db.execute(
        f"SELECT date, COALESCE(SUM(amount),0) AS t FROM daily_sales "
        f"WHERE date BETWEEN ? AND ? AND branch_id IN ({ph}) GROUP BY date",
        [start_date, sel_date] + branch_ids
    ).fetchall()
    by_day = {r['date']: round(float(r['t'] or 0), 2) for r in series_rows}
    series_14d = []
    for i in range(14):
        d = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
        series_14d.append({'date': d, 'total': by_day.get(d, 0)})

    # Per-store momentum: this-month-to-date vs last-month-to-SAME-DAY (the
    # same partial windows used for the chain pct_vs_prev_month above), so the
    # comparison is apples-to-apples mid-month. New stores (this-month data,
    # no last-month) → 'new'; stores that reported last month but not this
    # month → 'missing'; no data either side → excluded. Powers the
    # /network/revenue-v2 "מגמת סניפים" panel (series_14d unused there).
    def _range_by_branch(d0, d1):
        rows = db.execute(
            f"SELECT branch_id, COALESCE(SUM(amount),0) AS t FROM daily_sales "
            f"WHERE date BETWEEN ? AND ? AND branch_id IN ({ph}) GROUP BY branch_id",
            [d0.isoformat(), d1.isoformat()] + branch_ids
        ).fetchall()
        return {r['branch_id']: round(float(r['t'] or 0), 2) for r in rows}

    this_by_branch = _range_by_branch(month_start, sd)
    last_by_branch = _range_by_branch(prev_start, prev_end)
    momentum = []
    for b in visible:
        bid = b['id']
        this_t = this_by_branch.get(bid, 0)
        last_t = last_by_branch.get(bid, 0)
        if this_t == 0 and last_t == 0:
            continue  # no data either side → exclude
        if this_t > 0 and last_t == 0:
            status, pct = 'new', None
        elif this_t == 0 and last_t > 0:
            status, pct = 'missing', None
        else:
            pct = round((this_t - last_t) / last_t * 100, 1)
            status = 'flat' if abs(pct) < 1 else ('up' if pct > 0 else 'down')
        momentum.append({
            'branch_id': bid, 'branch_name': names.get(bid, 'סניף לא ידוע'),
            'this_total': this_t, 'last_total': last_t, 'pct': pct, 'status': status,
        })
    # Movers first (biggest |%| change desc), then new stores, then missing.
    _grp = {'up': 0, 'down': 0, 'flat': 0, 'new': 1, 'missing': 2}
    momentum.sort(key=lambda m: (_grp[m['status']],
                                 -(abs(m['pct']) if m['pct'] is not None else 0)))

    return {
        'date': sel_date,
        'chain_total': chain_total,
        'avg_per_store': avg_per_store,
        'total_transactions': total_transactions,
        'avg_basket': avg_basket,
        'month_to_date_total': month_to_date_total,
        'prev_month_total': prev_month_total,
        'pct_vs_prev_month': pct_vs_prev_month,
        'prev_date': prev_date,
        'prev_total': prev_total,
        'pct_vs_prev': pct_vs_prev,
        'total_branches': total_branches,
        'reported': reported,
        'missing': missing,
        'per_branch': per_branch,
        'top': per_branch[0] if per_branch else None,
        'bottom': per_branch[-1] if per_branch else None,
        'series_14d': series_14d,
        'momentum': momentum,
    }


@app.route('/api/network/revenue-v2')
@login_required
def api_network_revenue_v2():
    """Same daily chain-revenue payload as /api/network/revenue, but for ANY
    logged-in user — scoped to their OWN visible branches (admin/ceo → all
    active; manager → only their user_branches). Powers the 'הרשת שלי'
    aggregate mode of the experimental /network/revenue-v2 page."""
    visible = _list_visible_branches(session.get('user_id'), session.get('user_role'))
    return jsonify(_network_revenue_payload(visible, request.args.get('date'), get_db()))


def _network_goods_payload(visible, req_month, db):
    """Monthly chain GOODS (BilBoy) payload for a set of visible branches.

    Caller supplies `visible` (access already enforced) so a manager can never
    see another manager's stores. Returns the chain goods total + average per
    reporting store, a truthful coverage count, the top-10 suppliers by chain
    spend (data verified clean — grouped on the canonical `supplier` string),
    and a per-branch ranked list. Source: goods_documents.
    """
    total_branches = len(visible)
    empty = {
        'month': None, 'chain_goods_total': 0, 'avg_per_store': 0,
        'total_branches': total_branches, 'reported': 0, 'missing': [],
        'per_branch': [], 'top_suppliers': [], 'supplier_total_count': 0,
    }
    if not visible:
        return empty

    branch_ids = [b['id'] for b in visible]
    names = {b['id']: b['name'] for b in visible}
    ph = ','.join('?' * len(branch_ids))

    # Resolve month: explicit YYYY-MM wins (validated), else the most recent
    # month with any goods doc among visible branches, else current month.
    month = None
    if req_month:
        try:
            month = datetime.strptime(req_month, '%Y-%m').strftime('%Y-%m')
        except ValueError:
            month = None
    if not month:
        row = db.execute(
            f"SELECT MAX(strftime('%Y-%m', doc_date)) AS m FROM goods_documents "
            f"WHERE branch_id IN ({ph})", branch_ids
        ).fetchone()
        month = row['m'] if row and row['m'] else _now_il().strftime('%Y-%m')

    rows = db.execute(
        f"SELECT branch_id, COALESCE(SUM(amount),0) AS amount FROM goods_documents "
        f"WHERE strftime('%Y-%m', doc_date)=? AND branch_id IN ({ph}) GROUP BY branch_id",
        [month] + branch_ids
    ).fetchall()

    per_branch = sorted(
        [{'branch_id': r['branch_id'],
          'branch_name': names.get(r['branch_id'], 'סניף לא ידוע'),
          'amount': round(float(r['amount'] or 0), 2)} for r in rows],
        key=lambda x: x['amount'], reverse=True
    )
    reported_ids = {r['branch_id'] for r in rows}
    missing = [{'branch_id': b['id'], 'branch_name': b['name']}
               for b in visible if b['id'] not in reported_ids]

    chain_goods_total = round(sum(r['amount'] for r in per_branch), 2)
    reported = len(per_branch)
    avg_per_store = round(chain_goods_total / reported, 2) if reported else 0

    # Top suppliers by chain spend this month (clean canonical strings).
    sup_rows = db.execute(
        f"SELECT TRIM(supplier) AS s, COALESCE(SUM(amount),0) AS t FROM goods_documents "
        f"WHERE strftime('%Y-%m', doc_date)=? AND branch_id IN ({ph}) "
        f"AND TRIM(COALESCE(supplier,''))<>'' GROUP BY TRIM(supplier) ORDER BY t DESC",
        [month] + branch_ids
    ).fetchall()
    supplier_total_count = len(sup_rows)
    # Return ALL suppliers (descending) so the page can expand the long tail
    # client-side without another fetch; the page shows the top N and hides
    # the rest behind a toggle.
    top_suppliers = [{
        'supplier': r['s'],
        'amount': round(float(r['t'] or 0), 2),
        'pct': round(float(r['t'] or 0) / chain_goods_total * 100, 1) if chain_goods_total else 0,
    } for r in sup_rows]

    return {
        'month': month,
        'chain_goods_total': chain_goods_total,
        'avg_per_store': avg_per_store,
        'total_branches': total_branches,
        'reported': reported,
        'missing': missing,
        'per_branch': per_branch,
        'top_suppliers': top_suppliers,
        'supplier_total_count': supplier_total_count,
    }


@app.route('/api/network/goods-v2')
@login_required
def api_network_goods_v2():
    """Monthly chain-goods payload for ANY logged-in user, scoped to their own
    visible branches (admin/ceo → all active; manager → their stores). Powers
    the 'הסניפים שלי' aggregate mode of /network/goods-v2."""
    visible = _list_visible_branches(session.get('user_id'), session.get('user_role'))
    return jsonify(_network_goods_payload(visible, request.args.get('month'), get_db()))


def _network_employees_payload(visible, req_month, db):
    """Monthly chain LABOR payload for a set of visible branches.

    Caller supplies `visible` (access already enforced). Hero metric is total
    salary cost this month via the single source of truth
    (`_calculate_salary_cost`), so every number ties to /employees + home.

    Three coverage tiers (the chain is sparse — most new stores have no
    employees configured yet):
      - reported   : branches with active employees configured. Ranked desc by
                     salary, clickable through to single mode.
      - missing    : branches with NO active employees. Greyed, non-clickable.
                     `pending` counts unreviewed name matches — a store with
                     pending > 0 is one onboarding step from data, distinct
                     from a genuinely empty store.
    Source: employee_hours (via _calculate_salary_cost) + employees + pending.
    """
    total_branches = len(visible)
    empty = {
        'month': None, 'chain_salary_total': 0, 'avg_per_store': 0,
        'total_branches': total_branches, 'reported': 0,
        'per_branch': [], 'missing': [],
    }
    if not visible:
        return empty

    # Resolve month: explicit YYYY-MM wins (validated), else current month.
    month = None
    if req_month:
        try:
            month = datetime.strptime(req_month, '%Y-%m').strftime('%Y-%m')
        except ValueError:
            month = None
    if not month:
        month = _now_il().strftime('%Y-%m')

    branch_ids = [b['id'] for b in visible]
    names = {b['id']: b['name'] for b in visible}
    ph = ','.join('?' * len(branch_ids))

    # Active-employee count per branch (configured = has data tier).
    emp_rows = db.execute(
        f"SELECT branch_id, COUNT(*) AS c FROM employees "
        f"WHERE active = 1 AND branch_id IN ({ph}) GROUP BY branch_id",
        branch_ids
    ).fetchall()
    emp_count = {r['branch_id']: r['c'] for r in emp_rows}

    # Pending name matches per branch (for the onboarding worklist backlog).
    pend_rows = db.execute(
        f"SELECT branch_id, COUNT(*) AS c FROM employee_match_pending "
        f"WHERE branch_id IN ({ph}) GROUP BY branch_id",
        branch_ids
    ).fetchall()
    pending_count = {r['branch_id']: r['c'] for r in pend_rows}

    # Monthly revenue per branch (for the labor-cost-% metric). Sourced from
    # daily_sales so it ties to /sales and the home page.
    rev_rows = db.execute(
        f"SELECT branch_id, COALESCE(SUM(amount),0) AS rev FROM daily_sales "
        f"WHERE strftime('%Y-%m', date)=? AND branch_id IN ({ph}) GROUP BY branch_id",
        [month] + branch_ids
    ).fetchall()
    revenue = {r['branch_id']: float(r['rev'] or 0) for r in rev_rows}

    reported, missing = [], []
    for b in visible:
        bid = b['id']
        if emp_count.get(bid, 0) > 0:
            sal = _calculate_salary_cost(bid, month)
            reported.append({
                'branch_id': bid,
                'branch_name': names.get(bid, 'סניף לא ידוע'),
                'salary': round(float(sal['amount'] or 0), 2),
                'hours': round(float(sal['hours'] or 0), 2),
                'emp_count': emp_count.get(bid, 0),
                'revenue': round(revenue.get(bid, 0), 2),
            })
        else:
            missing.append({
                'branch_id': bid,
                'branch_name': names.get(bid, 'סניף לא ידוע'),
                'pending': pending_count.get(bid, 0),
            })

    reported.sort(key=lambda x: x['salary'], reverse=True)
    # Worklist: biggest onboarding backlog first; 0-pending stores fall last.
    missing.sort(key=lambda x: x['pending'], reverse=True)

    chain_salary_total = round(sum(r['salary'] for r in reported), 2)
    n = len(reported)
    avg_per_store = round(chain_salary_total / n, 2) if n else 0

    # Labor cost % — chain salary ÷ chain revenue. To make the ratio reconcile,
    # numerator and denominator use the SAME store set: only stores that have
    # BOTH salary > 0 AND revenue > 0 this month. A store with salary but no
    # imported revenue (or vice-versa) would distort the ratio, so it is
    # excluded from both sides. `labor_pct_stores` reports how many qualified.
    ratio_rows = [r for r in reported if r['salary'] > 0 and r['revenue'] > 0]
    ratio_salary = round(sum(r['salary'] for r in ratio_rows), 2)
    chain_revenue = round(sum(r['revenue'] for r in ratio_rows), 2)
    labor_pct = round(ratio_salary / chain_revenue * 100, 1) if chain_revenue > 0 else None

    return {
        'month': month,
        'chain_salary_total': chain_salary_total,
        'avg_per_store': avg_per_store,
        'labor_pct': labor_pct,
        'labor_pct_salary': ratio_salary,
        'chain_revenue': chain_revenue,
        'labor_pct_stores': len(ratio_rows),
        'total_branches': total_branches,
        'reported': n,
        'per_branch': reported,
        'missing': missing,
    }


@app.route('/api/network/employees-v2')
@login_required
def api_network_employees_v2():
    """Monthly chain-labor payload for ANY logged-in user, scoped to their own
    visible branches (admin/ceo → all active; manager → their stores). Powers
    the 'הסניפים שלי' aggregate mode of /network/employees-v2."""
    visible = _list_visible_branches(session.get('user_id'), session.get('user_role'))
    return jsonify(_network_employees_payload(visible, request.args.get('month'), get_db()))


@app.route('/api/history')
@login_required
def api_history():
    """Return monthly data from first month with real data to selected month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))

    db = get_db()
    start = get_branch_start_month(branch_id, db)
    if not start:
        return jsonify([])

    start_y, start_m = start
    end_y, end_m = map(int, month.split('-'))
    months = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        m_str = f'{y:04d}-{m:02d}'
        label = f'{m}/{y}'
        months.append({'month': m_str, 'label': label})
        m += 1
        if m > 12:
            m = 1
            y += 1

    result = []
    for m in months:
        ms = m['month']
        inc = db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM daily_sales WHERE branch_id = ? AND strftime('%Y-%m', date) = ?",
            (branch_id, ms)
        ).fetchone()[0]
        gds = db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM goods_documents WHERE branch_id = ? AND strftime('%Y-%m', doc_date) = ?",
            (branch_id, ms)
        ).fetchone()[0]
        _ensure_monthly_expenses(branch_id, ms, db)
        fix_data = _get_fixed_total(branch_id, ms, inc, db)
        sal_data = _calculate_salary_cost(branch_id, ms)
        sal = sal_data['amount']
        sal_source = sal_data['source']
        profit = inc - gds - fix_data['total'] - sal
        result.append({
            'label': m['label'],
            'month': ms,
            'income': inc,
            'goods': gds,
            'fixed': fix_data['total'],
            'fixed_only': fix_data['fixed_only'],
            'electricity_source': fix_data['electricity']['source'],
            'salary': sal,
            'salary_source': sal_source,
            'profit': profit,
        })
    return jsonify(result)


@app.route('/api/live-sales')
@login_required
def api_live_sales():
    """Return today's live sales for a branch.

    Read-time rule (no scheduled job, no writes): live data is shown ONLY
    for the current calendar day (Asia/Jerusalem). When the date has rolled
    over and no fresh pull exists for the new day yet, returns is_closed
    with last_amount/last_date for context — the tile renders "החנות סגורה"
    rather than resurfacing yesterday's closing number as live.
    Z-report (daily_sales row for today) always wins.
    """
    branch_id = get_branch_id()
    today = _now_il().strftime('%Y-%m-%d')
    db = get_db()
    row = db.execute(
        'SELECT amount, transactions, last_updated FROM live_sales WHERE branch_id = ? AND date = ?',
        (branch_id, today)
    ).fetchone()

    fresh_today = bool(row and row['amount'] and row['last_updated'] != 'PAUSED')
    if fresh_today:
        return jsonify({
            'amount': row['amount'],
            'transactions': row['transactions'],
            'last_updated': row['last_updated'],
            'is_stale': False,
            'is_closed': False,
        })

    has_z = db.execute(
        "SELECT 1 FROM daily_sales WHERE branch_id = ? AND date = ?",
        (branch_id, today)
    ).fetchone() is not None

    if has_z:
        # Z wins — no is_closed even if no live row for today.
        if row:
            return jsonify({
                'amount': row['amount'],
                'transactions': row['transactions'],
                'last_updated': row['last_updated'],
                'is_stale': False,
                'is_closed': False,
            })
        return jsonify({'amount': None, 'transactions': None,
                        'last_updated': None, 'is_stale': False,
                        'is_closed': False})

    # Calendar date has rolled to a new day, no fresh pull yet, no Z.
    # Look up the most recent past-day live row for is_closed context.
    latest = db.execute(
        'SELECT amount, date FROM live_sales '
        'WHERE branch_id = ? AND amount > 0 AND date < ? '
        'ORDER BY date DESC, fetched_at DESC LIMIT 1',
        (branch_id, today)
    ).fetchone()
    if latest:
        return jsonify({
            'amount': None,
            'transactions': None,
            'last_updated': None,
            'is_stale': False,
            'is_closed': True,
            'last_amount': latest['amount'],
            'last_date': latest['date'],
        })
    return jsonify({'amount': None, 'transactions': None,
                    'last_updated': None, 'is_stale': False,
                    'is_closed': False})


def _live_row_for_branch(db, branch_id, today):
    """Per-branch live-sales read using the same read-time rule as
    /api/live-sales (today's row, Z-wins, is_closed fallback).
    Returns a dict matching the per-branch tile payload."""
    row = db.execute(
        'SELECT amount, transactions, last_updated FROM live_sales '
        'WHERE branch_id = ? AND date = ?',
        (branch_id, today)
    ).fetchone()
    fresh_today = bool(row and row['amount'] and row['last_updated'] != 'PAUSED')
    if fresh_today:
        return {
            'amount': row['amount'],
            'transactions': row['transactions'],
            'last_updated': row['last_updated'],
            'is_closed': False,
        }
    has_z = db.execute(
        "SELECT 1 FROM daily_sales WHERE branch_id = ? AND date = ?",
        (branch_id, today)
    ).fetchone() is not None
    if has_z:
        if row:
            return {
                'amount': row['amount'],
                'transactions': row['transactions'],
                'last_updated': row['last_updated'],
                'is_closed': False,
            }
        return {'amount': None, 'transactions': None,
                'last_updated': None, 'is_closed': False}
    latest = db.execute(
        'SELECT amount, date FROM live_sales '
        'WHERE branch_id = ? AND amount > 0 AND date < ? '
        'ORDER BY date DESC, fetched_at DESC LIMIT 1',
        (branch_id, today)
    ).fetchone()
    if latest:
        return {
            'amount': None, 'transactions': None, 'last_updated': None,
            'is_closed': True,
            'last_amount': latest['amount'], 'last_date': latest['date'],
        }
    return {'amount': None, 'transactions': None,
            'last_updated': None, 'is_closed': False}


@app.route('/api/live-sales/network')
@login_required
def api_live_sales_network():
    """Per-branch live tile payload for multi-branch accounts.

    Returns one entry per ASSIGNED branch (admin/ceo → all active branches;
    manager → only user_branches). Each entry uses the same read-time rule
    as /api/live-sales — today's row wins, Z wins, otherwise is_closed with
    last_amount/last_date context.

    Access control: derives the branch list from _list_visible_branches —
    URL params are ignored, a multi-store manager cannot leak other branches.
    """
    db = get_db()
    role = session.get('user_role')
    user_id = session.get('user_id')
    visible = _list_visible_branches(user_id, role)
    today = _now_il().strftime('%Y-%m-%d')

    branches = []
    chain_total = 0.0
    active_count = 0
    for b in visible:
        live = _live_row_for_branch(db, b['id'], today)
        entry = {
            'branch_id': b['id'],
            'branch_name': b['name'],
            **live,
        }
        branches.append(entry)
        if live.get('amount') and not live.get('is_closed'):
            chain_total += float(live['amount'])
            active_count += 1

    return jsonify({
        'is_multi_branch': len(visible) > 1,
        'branches': branches,
        'chain_total': round(chain_total, 2),
        'active_count': active_count,
        'total_count': len(visible),
    })


@app.route('/api/sales-by-hour')
@login_required
def api_sales_by_hour():
    """Return revenue breakdown by hour + 2-hour buckets from hourly_sales table."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()

    rows = db.execute(
        '''SELECT hour, SUM(amount) as total, SUM(transactions) as count
           FROM hourly_sales
           WHERE branch_id = ? AND strftime('%Y-%m', date) = ?
           GROUP BY hour ORDER BY hour''',
        (branch_id, month)
    ).fetchall()

    rows_by_hour = {r['hour']: r for r in rows}
    hourly = []
    for h in range(24):
        row = rows_by_hour.get(h)
        hourly.append({
            'hour': h,
            'total': round(float(row['total']), 2) if row else 0,
            'count': int(row['count']) if row else 0,
        })

    # 2-hour buckets aligned to 7:00 opening
    # Hours 0-6 excluded (early-morning deliveries handled separately)
    bucket_defs = [
        ('7:00',  '9:00',  [7, 8]),
        ('9:00',  '11:00', [9, 10]),
        ('11:00', '13:00', [11, 12]),
        ('13:00', '15:00', [13, 14]),
        ('15:00', '17:00', [15, 16]),
        ('17:00', '19:00', [17, 18]),
        ('19:00', '21:00', [19, 20]),
        ('21:00', '23:00', [21, 22]),
    ]

    buckets = []
    for start, end, hours in bucket_defs:
        total = sum(hourly[h]['total'] for h in hours)
        count = sum(hourly[h]['count'] for h in hours)
        buckets.append({
            'start': start,
            'end': end,
            'label': f'\u200E{start}-{end}',
            'total': round(total, 2),
            'count': count,
            'average': round(total / count, 2) if count > 0 else 0,
        })

    # Stats
    active_buckets = [b for b in buckets if b['total'] > 0]
    if active_buckets:
        peak = max(active_buckets, key=lambda b: b['total'])
        quiet = min(active_buckets, key=lambda b: b['total'])
        active_hours = sum(len(h) for _, _, h in bucket_defs
                          if any(hourly[hr]['total'] > 0 for hr in h))
        total_revenue = sum(b['total'] for b in buckets)
        hourly_avg = round(total_revenue / max(active_hours, 1), 2)
    else:
        peak = quiet = None
        hourly_avg = 0

    days_with_data = db.execute(
        '''SELECT COUNT(DISTINCT date) FROM hourly_sales
           WHERE branch_id = ? AND strftime('%Y-%m', date) = ?''',
        (branch_id, month)
    ).fetchone()[0]

    stats = {
        'peak_bucket': peak['label'] if peak else None,
        'peak_total': peak['total'] if peak else 0,
        'quiet_bucket': quiet['label'] if quiet else None,
        'quiet_total': quiet['total'] if quiet else 0,
        'hourly_avg': hourly_avg,
        'total_days_data': days_with_data,
    }

    return jsonify({'hourly': hourly, 'buckets': buckets, 'stats': stats})


# ── Amazon Deliveries (branch 126 only) ─────────────────────
AMAZON_BRANCH_ID = 126
AMAZON_MIN_AMOUNT = 400
AMAZON_MAX_HOUR = 6  # hours 0-6 (before 07:00)


@app.route('/api/amazon-deliveries')
@login_required
def api_amazon_deliveries():
    """Return early-morning large transactions for branch 126 (Amazon deliveries)."""
    branch_id = get_branch_id()
    if branch_id != AMAZON_BRANCH_ID:
        return jsonify({'deliveries': [], 'total_amount': 0, 'total_count': 0, 'month_label': ''})

    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    # Don't return data for future months
    current_month = _now_il().strftime('%Y-%m')
    if month > current_month:
        return jsonify({'deliveries': [], 'total_amount': 0, 'total_count': 0, 'month_label': month})

    db = get_db()
    rows = db.execute(
        '''SELECT date, SUM(amount) as day_total, SUM(transactions) as day_count
           FROM hourly_sales
           WHERE branch_id = ? AND strftime('%Y-%m', date) = ? AND hour <= ?
           GROUP BY date
           HAVING day_total >= ?
           ORDER BY date DESC''',
        (AMAZON_BRANCH_ID, month, AMAZON_MAX_HOUR, AMAZON_MIN_AMOUNT)
    ).fetchall()

    deliveries = []
    total_amount = 0
    total_count = 0
    for r in rows:
        amt = round(float(r['day_total']), 2)
        cnt = int(r['day_count'])
        deliveries.append({
            'date': r['date'],
            'amount': amt,
            'count': cnt,
        })
        total_amount += amt
        total_count += cnt

    year, mon = map(int, month.split('-'))
    month_label = f'{HEBREW_MONTHS[mon]} {year}'

    return jsonify({
        'deliveries': deliveries,
        'total_amount': round(total_amount, 2),
        'total_count': total_count,
        'month_label': month_label,
    })


@app.route('/api/hourly-health')
@login_required
def api_hourly_health():
    """Data-health monitor for hourly_sales pipeline. CEO only."""
    if session.get('user_role') != 'admin':
        return jsonify({'error': 'unauthorized'}), 403
    from agents.hourly_sales_monitor import run_all_checks
    branch_id = int(request.args.get('branch_id', 126))
    date = request.args.get('date', _now_il().strftime('%Y-%m-%d'))
    db = get_db()
    result = run_all_checks(branch_id, date, db)
    return jsonify(result)


@app.route('/api/employees', methods=['GET'])
@login_required
def api_employees_list():
    """List employees for current branch with their hours for selected month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()

    # All active employees from employees table
    emp_rows = db.execute(
        "SELECT id, name, role, hourly_rate FROM employees "
        "WHERE branch_id = ? AND active = 1 ORDER BY name",
        (branch_id,)
    ).fetchall()
    employees = [dict(r) for r in emp_rows]

    # Hours for this month from employee_hours
    # UPDATED 2026-04-18: Always use API-only rows (CSV path retired).
    # UPDATED 2026-05-09: Include 'aviv_report' rows alongside 'aviv_api'.
    hours_rows = db.execute(
        "SELECT employee_name, total_hours, total_salary, source FROM employee_hours "
        "WHERE branch_id = ? AND month = ? AND source IN ('aviv_api', 'aviv_report')",
        (branch_id, month)
    ).fetchall()
    hours_map = {r['employee_name']: dict(r) for r in hours_rows}
    csv_processed = len(hours_map) > 0

    # Branch KPI data
    branch_row = db.execute(
        "SELECT name, hours_this_month, avg_hourly_rate, hours_updated_at FROM branches WHERE id = ?",
        (branch_id,)
    ).fetchone()
    branch_name = (branch_row['name'] or '') if branch_row else ''
    hours_this_month = (branch_row['hours_this_month'] or 0) if branch_row else 0
    avg_hourly_rate = (branch_row['avg_hourly_rate'] or 0) if branch_row else 0
    hours_updated_at = (branch_row['hours_updated_at'] or '') if branch_row else ''

    # Clean display names and match employees to hours data
    for emp in employees:
        emp['name'] = _clean_display_name(emp['name'], branch_name)
        matched = _match_employee_hours(emp['name'], hours_map, branch_name)
        if matched:
            emp['hours'] = matched['total_hours']
            emp['salary'] = matched['total_salary']
            emp['hours_source'] = matched.get('source', 'unknown')
        else:
            emp['hours'] = 0
            emp['salary'] = 0
            emp['hours_source'] = 'none'

    # Salary — single source of truth
    salary_data = _calculate_salary_cost(branch_id, month)
    salary_cost = salary_data['amount']
    salary_hours = salary_data['hours']
    salary_source = salary_data['source']

    # History: only months with real data (daily_sales or goods_documents)
    earliest = db.execute('''
        SELECT MIN(month) as m FROM (
            SELECT strftime('%Y-%m', date) as month
            FROM daily_sales WHERE branch_id=?
            UNION
            SELECT strftime('%Y-%m', doc_date) as month
            FROM goods_documents WHERE branch_id=?
        )
    ''', (branch_id, branch_id)).fetchone()

    history = []
    if earliest and earliest['m']:
        start_y, start_m = map(int, earliest['m'].split('-'))
        end_y, end_m = map(int, month.split('-'))
        y, m2 = start_y, start_m
        while (y, m2) <= (end_y, end_m):
            m_str = f'{y:04d}-{m2:02d}'
            # UPDATED 2026-04-18: Always use API-only rows (CSV path retired).
            # UPDATED 2026-05-09: Include 'aviv_report' rows alongside 'aviv_api'.
            h_row = db.execute(
                "SELECT COALESCE(SUM(total_hours), 0) as hours, COALESCE(SUM(total_salary), 0) as salary, "
                "COUNT(*) as cnt FROM employee_hours "
                "WHERE branch_id = ? AND month = ? AND source IN ('aviv_api', 'aviv_report')",
                (branch_id, m_str)
            ).fetchone()
            h_hours = h_row['hours']
            h_salary = h_row['salary']
            h_source = 'api' if h_row['cnt'] > 0 else 'none'
            h_rate = round(h_salary / h_hours, 2) if h_hours > 0 and h_salary > 0 else avg_hourly_rate
            history.append({
                'month': m_str, 'hours': h_hours, 'salary': h_salary,
                'avg_rate': h_rate, 'source': h_source,
            })
            m2 += 1
            if m2 > 12:
                m2 = 1
                y += 1

    return jsonify({
        'employees': employees,
        'hours_this_month': hours_this_month,
        'avg_hourly_rate': avg_hourly_rate,
        'hours_updated_at': hours_updated_at,
        'salary_cost': salary_cost,
        'salary_hours': salary_hours,
        'salary_source': salary_source,
        'csv_processed': csv_processed,
        'history': history,
    })


def _clean_display_name(name: str, branch_name: str = '') -> str:
    """Strip store name suffix from employee names for clean display."""
    store_words = ['איינשטיין', 'אינשטיין', 'einstein']
    if branch_name:
        store_words.extend(branch_name.strip().split())
    words = name.split()
    while words and any(w.lower() == words[-1].lower() for w in store_words):
        words.pop()
    return ' '.join(words).strip() or name


def _match_employee_hours(emp_name: str, hours_map: dict, branch_name: str = '') -> dict | None:
    """Match an employee name to CSV hours data using smart fuzzy matching."""
    # Clean the employee name
    emp_clean = _clean_display_name(emp_name, branch_name)
    emp_tokens = emp_clean.split()

    # 1. Exact match
    if emp_name in hours_map:
        return hours_map[emp_name]

    best_match = None
    best_score = 0

    for csv_name, data in hours_map.items():
        csv_clean = _clean_display_name(csv_name, branch_name)
        csv_tokens = csv_clean.split()

        # 2. Exact match after cleaning
        if emp_clean == csv_clean:
            return data

        # 3. One contains the other (prefix/suffix)
        if csv_clean.startswith(emp_clean) or emp_clean.startswith(csv_clean):
            return data

        # 3b. Single-word name matches first word of multi-word CSV name
        if len(emp_tokens) == 1 and csv_tokens and emp_tokens[0] == csv_tokens[0]:
            return data

        # 4. First + last name match (ignore middle names)
        if len(emp_tokens) >= 2:
            first, last = emp_tokens[0], emp_tokens[-1]
            if first in csv_tokens and last in csv_tokens:
                score = 3
                if score > best_score:
                    best_score = score
                    best_match = data

        # 5. Reversed — CSV first + last match emp
        if len(csv_tokens) >= 2:
            first, last = csv_tokens[0], csv_tokens[-1]
            if first in emp_tokens and last in emp_tokens:
                score = 3
                if score > best_score:
                    best_score = score
                    best_match = data

        # 6. Token overlap
        common = set(emp_tokens) & set(csv_tokens)
        if len(common) >= 2 and len(common) > best_score:
            best_score = len(common)
            best_match = data

    return best_match


@app.route('/api/employees', methods=['POST'])
@login_required
def api_employees_create():
    """Add a new employee to the employees table."""
    data = request.get_json()
    branch_id = get_branch_id()
    name = data.get('name', '').strip()
    role = data.get('role', 'ערב')
    hourly_rate = float(data.get('hourly_rate', 0))
    if not name:
        return jsonify({'error': 'name required'}), 400
    if hourly_rate < 0:
        return jsonify({'error': 'hourly_rate must be non-negative'}), 400
    db = get_db()
    db.execute(
        "INSERT OR IGNORE INTO employees (branch_id, name, role, hourly_rate) VALUES (?, ?, ?, ?)",
        (branch_id, name, role, hourly_rate)
    )
    if hourly_rate > 0:
        _recalculate_avg_rate(branch_id, db)
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employees/<int:emp_id>', methods=['PUT'])
@login_required
def api_employees_update(emp_id):
    """Update an employee."""
    data = request.get_json()
    db = get_db()
    row = db.execute("SELECT * FROM employees WHERE id = ?", (emp_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    branch_id = get_branch_id()
    if row['branch_id'] != branch_id:
        return jsonify({'error': 'forbidden'}), 403
    name = data.get('name', row['name'])
    role = data.get('role', row['role'])
    hourly_rate = float(data.get('hourly_rate', row['hourly_rate']))
    db.execute(
        "UPDATE employees SET name=?, role=?, hourly_rate=? WHERE id=?",
        (name, role, hourly_rate, emp_id)
    )
    _recalculate_avg_rate(branch_id, db)
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employees/<int:emp_id>', methods=['DELETE'])
@login_required
def api_employees_delete(emp_id):
    """Soft-delete an employee (set active=0) and cascade cleanup."""
    db = get_db()
    row = db.execute("SELECT branch_id, name FROM employees WHERE id = ?", (emp_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    branch_id = get_branch_id()
    if row['branch_id'] != branch_id:
        return jsonify({'error': 'forbidden'}), 403

    # Collect alias names before deleting them (needed for reopening pending records)
    alias_rows = db.execute(
        'SELECT alias_name FROM employee_aliases WHERE employee_id = ?', (emp_id,)
    ).fetchall()
    alias_names = [r['alias_name'] for r in alias_rows]
    alias_names.append(row['name'])

    # Reopen resolved pending records for this employee's names/aliases
    for name in set(alias_names):
        db.execute('''
            UPDATE employee_match_pending
            SET resolved = 0, is_new_employee = 1, suggested_employee_id = NULL
            WHERE branch_id = ? AND csv_name = ? AND resolved = 1
        ''', (branch_id, name))

    # Delete unresolved pending records that suggest this employee
    db.execute(
        'DELETE FROM employee_match_pending WHERE suggested_employee_id = ? AND resolved = 0',
        (emp_id,))

    # Cascade-delete aliases
    db.execute('DELETE FROM employee_aliases WHERE employee_id = ?', (emp_id,))

    # Soft-delete the employee
    db.execute("UPDATE employees SET active=0 WHERE id=?", (emp_id,))
    _recalculate_avg_rate(branch_id, db)
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employee-match-pending', methods=['GET'])
@login_required
def api_employee_match_pending():
    """Return unresolved pending employee matches for branch/month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()

    # Ensure table exists
    db.execute('''
        CREATE TABLE IF NOT EXISTS employee_match_pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id INTEGER, month TEXT, csv_name TEXT,
            suggested_employee_id INTEGER, confidence TEXT,
            hours REAL, salary REAL,
            created_at TEXT DEFAULT (datetime('now')),
            resolved INTEGER DEFAULT 0
        )
    ''')

    rows = db.execute('''
        SELECT p.id, p.csv_name, p.suggested_employee_id, p.confidence,
               p.hours, p.salary, p.month, p.aviv_employee_id,
               COALESCE(p.source, 'csv') as source,
               COALESCE(p.is_new_employee, 0) as is_new_employee,
               COALESCE(p.is_csv_only, 0) as is_csv_only,
               e.name as suggested_name, e.hourly_rate as suggested_rate
        FROM employee_match_pending p
        LEFT JOIN employees e ON e.id = p.suggested_employee_id
        WHERE p.branch_id = ? AND p.month = ? AND p.resolved = 0
          AND COALESCE(p.source, 'csv') IN ('csv', 'aviv_api', 'aviv_report')
        ORDER BY p.hours DESC
    ''', (branch_id, month)).fetchall()

    # Also get all active employees for reassignment dropdown
    employees = [dict(r) for r in db.execute(
        "SELECT id, name, hourly_rate FROM employees WHERE branch_id = ? AND active = 1 ORDER BY name",
        (branch_id,)
    ).fetchall()]

    return jsonify({
        'pending': [dict(r) for r in rows],
        'employees': employees,
    })


@app.route('/api/employee-match-pending/<int:pending_id>/approve', methods=['POST'])
@login_required
def api_pending_approve(pending_id):
    """Approve a pending match — save to employee_hours."""
    db = get_db()
    branch_id = get_branch_id()
    row = db.execute(
        "SELECT * FROM employee_match_pending WHERE id = ? AND branch_id = ?",
        (pending_id, branch_id)
    ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404

    # Get the employee rate for salary calculation
    emp = db.execute(
        "SELECT hourly_rate FROM employees WHERE id = ?",
        (row['suggested_employee_id'],)
    ).fetchone()
    rate = emp['hourly_rate'] if emp else 0
    salary = round(row['hours'] * rate, 2) if rate > 0 else 0

    # Determine source from pending row
    source = 'csv'
    try:
        source = row['source'] or 'csv'
    except (IndexError, KeyError):
        pass

    db.execute(
        "INSERT OR REPLACE INTO employee_hours "
        "(branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (branch_id, row['month'], row['csv_name'], row['hours'], salary, source)
    )

    # Save aviv_employee_id link for future auto-matching
    try:
        aviv_emp_id = row['aviv_employee_id']
        if aviv_emp_id and row['suggested_employee_id']:
            db.execute(
                'UPDATE employees SET aviv_employee_id=? WHERE id=? AND (aviv_employee_id IS NULL OR aviv_employee_id != ?)',
                (aviv_emp_id, row['suggested_employee_id'], aviv_emp_id))
    except (IndexError, KeyError):
        pass

    db.execute("UPDATE employee_match_pending SET resolved = 1 WHERE id = ?", (pending_id,))
    db.commit()

    _recalculate_avg_rate(branch_id, db)
    db.commit()
    return jsonify({'ok': True, 'hours': row['hours'], 'salary': salary})


@app.route('/api/employee-match-pending/<int:pending_id>/reject', methods=['POST'])
@login_required
def api_pending_reject(pending_id):
    """Reject a pending match — mark resolved, no hours saved."""
    db = get_db()
    branch_id = get_branch_id()
    row = db.execute(
        "SELECT id FROM employee_match_pending WHERE id = ? AND branch_id = ?",
        (pending_id, branch_id)
    ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    db.execute("UPDATE employee_match_pending SET resolved = 1 WHERE id = ?", (pending_id,))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employee-match-pending/<int:pending_id>/reassign', methods=['POST'])
@login_required
def api_pending_reassign(pending_id):
    """Reassign a pending match to a different employee."""
    db = get_db()
    branch_id = get_branch_id()
    data = request.get_json()
    employee_id = data.get('employee_id')
    if not employee_id:
        return jsonify({'error': 'employee_id required'}), 400

    row = db.execute(
        "SELECT * FROM employee_match_pending WHERE id = ? AND branch_id = ?",
        (pending_id, branch_id)
    ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404

    # Verify employee belongs to this branch
    emp = db.execute(
        "SELECT hourly_rate FROM employees WHERE id = ? AND branch_id = ?",
        (employee_id, branch_id)
    ).fetchone()
    if not emp:
        return jsonify({'error': 'employee not found'}), 404

    salary = round(row['hours'] * emp['hourly_rate'], 2) if emp['hourly_rate'] > 0 else 0

    db.execute(
        "INSERT OR REPLACE INTO employee_hours "
        "(branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (?, ?, ?, ?, ?, 'csv')",
        (branch_id, row['month'], row['csv_name'], row['hours'], salary)
    )
    db.execute("UPDATE employee_match_pending SET resolved = 1 WHERE id = ?", (pending_id,))
    db.commit()

    _recalculate_avg_rate(branch_id, db)
    db.commit()
    return jsonify({'ok': True, 'hours': row['hours'], 'salary': salary})


@app.route('/api/employee-match-pending/<int:pending_id>/add-new', methods=['POST'])
@login_required
def api_pending_add_new(pending_id):
    """Create a new employee from a pending match and save their hours."""
    db = get_db()
    branch_id = get_branch_id()
    row = db.execute(
        "SELECT * FROM employee_match_pending WHERE id = ? AND branch_id = ?",
        (pending_id, branch_id)
    ).fetchone()
    if not row or row['resolved']:
        return jsonify({'error': 'not found'}), 404

    data = request.get_json()
    name = (data.get('name') or '').strip()
    hourly_rate = float(data.get('hourly_rate', 0))
    role = (data.get('role') or 'ערב').strip()

    if not name or hourly_rate <= 0:
        return jsonify({'error': 'name and hourly_rate required'}), 400

    # Get aviv_employee_id from pending row if available
    aviv_emp_id = None
    try:
        aviv_emp_id = row['aviv_employee_id']
    except (IndexError, KeyError):
        pass

    # Check if employee with this name already exists (possibly inactive)
    existing = db.execute(
        'SELECT id, active FROM employees WHERE branch_id = ? AND name = ?',
        (branch_id, name)).fetchone()

    if existing and existing['active']:
        return jsonify({'error': f'עובד/ת בשם {name} כבר קיים/ת ופעיל/ה'}), 409

    if existing:
        # Reactivate inactive employee with updated details
        new_emp_id = existing['id']
        db.execute(
            'UPDATE employees SET hourly_rate = ?, role = ?, active = 1, aviv_employee_id = ? '
            'WHERE id = ?',
            (hourly_rate, role, aviv_emp_id, new_emp_id))
    else:
        cur = db.execute(
            'INSERT INTO employees (branch_id, name, hourly_rate, role, active, aviv_employee_id) '
            'VALUES (?, ?, ?, ?, 1, ?)',
            (branch_id, name, hourly_rate, role, aviv_emp_id))
        new_emp_id = cur.lastrowid

    # Promote hours from EVERY unresolved pending row that shares the same
    # (branch_id, csv_name, source) — this covers the case where the same
    # person has rows for both current and previous month.
    source = 'aviv_api'
    try:
        source = row['source'] or 'csv'
    except (IndexError, KeyError):
        pass
    csv_name = (row['csv_name'] or '').strip()

    sibling_rows = db.execute(
        "SELECT id, month, hours FROM employee_match_pending "
        "WHERE branch_id=? AND csv_name=? AND COALESCE(source,'csv')=? AND resolved=0",
        (branch_id, csv_name, source)).fetchall()
    if not sibling_rows:
        # Defensive: at minimum process the row the user clicked.
        sibling_rows = [row]

    promoted_months = []
    total_promoted_hours = 0.0
    for sib in sibling_rows:
        sib_hours = float(sib['hours'] or 0)
        sib_month = sib['month']
        sib_id = sib['id']
        sib_salary = round(sib_hours * hourly_rate, 2)
        db.execute(
            "INSERT OR REPLACE INTO employee_hours "
            "(branch_id, month, employee_name, total_hours, total_salary, source) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (branch_id, sib_month, name, sib_hours, sib_salary, source))
        db.execute('UPDATE employee_match_pending SET resolved = 1 WHERE id = ?', (sib_id,))
        promoted_months.append(sib_month)
        total_promoted_hours += sib_hours

    # Always create alias for the original Aviv/CSV name (prevents re-flagging)
    if csv_name:
        db.execute(
            'INSERT OR IGNORE INTO employee_aliases (employee_id, alias_name, branch_id) VALUES (?, ?, ?)',
            (new_emp_id, csv_name, branch_id))
        # If manager changed the name, also save the final name as alias
        if csv_name != name:
            db.execute(
                'INSERT OR IGNORE INTO employee_aliases (employee_id, alias_name, branch_id) VALUES (?, ?, ?)',
                (new_emp_id, name, branch_id))

    _recalculate_avg_rate(branch_id, db)
    db.commit()

    return jsonify({
        'ok': True,
        'employee_id': new_emp_id,
        'promoted_months': promoted_months,
        'promoted_hours': round(total_promoted_hours, 2),
    })


@app.route('/api/labor-cost-ratio')
@login_required
def api_labor_cost_ratio():
    """Return labor cost ratio for months with real data (income or goods)."""
    branch_id = get_branch_id()
    db = get_db()

    # Only months with real data — same logic as history table
    months_rows = db.execute('''
        SELECT DISTINCT m FROM (
            SELECT strftime('%Y-%m', date) as m FROM daily_sales WHERE branch_id = ?
            UNION
            SELECT strftime('%Y-%m', doc_date) as m FROM goods_documents WHERE branch_id = ?
        ) ORDER BY m DESC LIMIT 6
    ''', (branch_id, branch_id)).fetchall()

    months = sorted([r['m'] for r in months_rows if r['m']])
    result = []
    for m_str in months:
        sal = _calculate_salary_cost(branch_id, m_str)
        income_row = db.execute(
            "SELECT COALESCE(SUM(amount), 0) as total FROM daily_sales "
            "WHERE branch_id = ? AND strftime('%Y-%m', date) = ?",
            (branch_id, m_str)).fetchone()
        income = income_row['total'] if income_row else 0
        salary = sal['amount']
        ratio = round((salary / income) * 100, 2) if income > 0 else 0
        result.append({'month': m_str, 'salary': round(salary, 2), 'income': round(income, 2), 'ratio': ratio})

    return jsonify(result)


# DISABLED 2026-04-18: Discrepancy routes retired — CSV path dropped in favor of API-only.
# Routes kept as comments for reference. Agent file and DB table also kept.
#
# @app.route('/api/employee-hours-discrepancies')
# @login_required
# def api_discrepancies(): ...
#
# @app.route('/api/employee-hours-discrepancies/<int:disc_id>/resolve', methods=['POST'])
# @login_required
# def api_resolve_discrepancy(disc_id): ...


def _prorate_invoice(from_date_str: str, to_date_str: str, amount: float, year: int, month: int) -> float:
    """Return the portion of an invoice amount that falls into (year, month)."""
    # Parse ISO dates from raw_json (e.g. "2026-01-22T00:00:00+02:00")
    from_d = date.fromisoformat(from_date_str[:10])
    to_d = date.fromisoformat(to_date_str[:10])
    total_days = (to_d - from_d).days
    if total_days <= 0:
        return 0.0
    # Month boundaries
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    # Overlap
    overlap_start = max(from_d, month_start)
    overlap_end = min(to_d, month_end)
    overlap_days = (overlap_end - overlap_start).days + 1
    if overlap_days <= 0:
        return 0.0
    return round(amount * overlap_days / total_days, 2)


def _get_real_electricity(branch_id: int, year: int, month: int, db) -> float:
    """Sum prorated electricity from invoices (<=90 days) that intersect (year, month). Returns 0 if none."""
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    rows = db.execute(
        "SELECT amount, raw_json FROM electricity_invoices WHERE branch_id = ?",
        (branch_id,)
    ).fetchall()
    total = 0.0
    for r in rows:
        try:
            rj = json.loads(r['raw_json'])
        except (json.JSONDecodeError, TypeError):
            continue
        from_d_str = rj.get('from_date', '')
        to_d_str = rj.get('to_date', '')
        if not from_d_str or not to_d_str:
            continue
        from_d = date.fromisoformat(from_d_str[:10])
        to_d = date.fromisoformat(to_d_str[:10])
        span = (to_d - from_d).days
        if span <= 0 or span > 90:
            continue
        # Check intersection with target month
        if to_d < month_start or from_d > month_end:
            continue
        total += _prorate_invoice(from_d_str, to_d_str, r['amount'], year, month)
    return round(total, 2)


def get_electricity_for_month(branch_id: int, year: int, month: int, db=None) -> dict:
    """
    Returns electricity contribution for a branch in a given month.
    Returns: {amount: float, source: 'real'|'estimate'|'none'|'manual', estimate_basis: str|None}
    """
    if db is None:
        db = get_db()

    # Check branch electricity_source setting
    branch = db.execute(
        "SELECT electricity_source, iec_token FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    if not branch:
        return {'amount': 0, 'source': 'none', 'estimate_basis': None}

    elec_source = branch['electricity_source']

    # Manual mode: look for manual entry for this month
    if elec_source == 'manual':
        month_str = f'{year:04d}-{month:02d}'
        manual_row = db.execute(
            "SELECT amount FROM electricity_invoices WHERE branch_id = ? AND source = 'manual' AND month = ?",
            (branch_id, month_str)
        ).fetchone()
        if manual_row:
            return {'amount': manual_row['amount'], 'source': 'manual', 'estimate_basis': None}
        return {'amount': 0, 'source': 'manual_missing', 'estimate_basis': None}

    # IEC mode (or legacy: no electricity_source but has iec_token)
    if elec_source == 'iec' or (elec_source is None and branch['iec_token']):
        # Check if any invoices exist at all
        any_invoice = db.execute(
            "SELECT 1 FROM electricity_invoices WHERE branch_id = ? LIMIT 1", (branch_id,)
        ).fetchone()
        if not any_invoice:
            return {'amount': 0, 'source': 'none', 'estimate_basis': None}
        # Try REAL
        real_amount = _get_real_electricity(branch_id, year, month, db)
        if real_amount > 0:
            return {'amount': real_amount, 'source': 'real', 'estimate_basis': None}
        # ESTIMATE: try same month last year
        prev_year_amount = _get_real_electricity(branch_id, year - 1, month, db)
        if prev_year_amount > 0:
            return {'amount': prev_year_amount, 'source': 'estimate', 'estimate_basis': f'{month:02d}/{year - 1}'}
        # Search closest month within ±12 months that has real data
        best_amount = 0.0
        best_distance = 999
        best_basis = None
        for offset in range(1, 13):
            for direction in (-1, 1):
                search_m = month + offset * direction
                search_y = year
                while search_m < 1:
                    search_m += 12
                    search_y -= 1
                while search_m > 12:
                    search_m -= 12
                    search_y += 1
                amt = _get_real_electricity(branch_id, search_y, search_m, db)
                if amt > 0 and offset < best_distance:
                    best_amount = amt
                    best_distance = offset
                    best_basis = f'{search_m:02d}/{search_y}'
                    break
            if best_distance <= offset:
                break
        if best_amount > 0:
            return {'amount': best_amount, 'source': 'estimate', 'estimate_basis': best_basis}
        return {'amount': 0, 'source': 'none', 'estimate_basis': None}

    # Not configured at all
    return {'amount': 0, 'source': 'none', 'estimate_basis': None}


def get_branch_start_month(branch_id: int, db=None) -> tuple:
    """Return (year, month) of first month visible in the UI, or None.

    Checks branches.ui_start_month override first. If not set, falls back
    to auto-detection from operational data (daily_sales, goods_documents,
    fixed_expenses, employee_hours). Does NOT consider electricity_invoices
    because those are pulled retroactively from IEC.
    """
    if db is None:
        db = get_db()
    # 1. Check per-branch UI override
    override = db.execute(
        'SELECT ui_start_month FROM branches WHERE id=?', (branch_id,)
    ).fetchone()
    if override and override['ui_start_month']:
        y, m = map(int, override['ui_start_month'].split('-'))
        return (y, m)
    # 2. Auto-detect from operational data
    earliest = db.execute('''
        SELECT MIN(month) as m FROM (
            SELECT strftime('%Y-%m', date) as month FROM daily_sales WHERE branch_id=?
            UNION
            SELECT strftime('%Y-%m', doc_date) as month FROM goods_documents WHERE branch_id=?
            UNION
            SELECT month FROM fixed_expenses WHERE branch_id=?
            UNION
            SELECT month FROM employee_hours WHERE branch_id=?
        )
    ''', (branch_id, branch_id, branch_id, branch_id)).fetchone()
    if not earliest or not earliest['m']:
        return None
    y, m = map(int, earliest['m'].split('-'))
    return (y, m)


def _get_fixed_total(branch_id: int, month: str, income: float, db) -> dict:
    """Sum fixed expenses for a branch+month. % rows calculated live from income.
    Returns dict: {fixed_only, electricity: {amount, source, estimate_basis}, total}"""
    rows = db.execute(
        'SELECT amount, pct_value FROM fixed_expenses WHERE branch_id=? AND month=?',
        (branch_id, month)
    ).fetchall()
    fixed_sum = 0
    for r in rows:
        if r['pct_value'] and r['pct_value'] > 0:
            fixed_sum += income * r['pct_value'] / 100
        else:
            fixed_sum += r['amount']
    fixed_sum = round(fixed_sum, 2)
    y, m = map(int, month.split('-'))
    elec = get_electricity_for_month(branch_id, y, m, db)
    return {
        'fixed_only': fixed_sum,
        'electricity': elec,
        'total': round(fixed_sum + elec['amount'], 2),
    }


def _ensure_monthly_expenses(branch_id: int, month: str, db):
    """Carry forward 'חודשי' expenses from the most recent prior month if target month is empty."""
    existing = db.execute(
        'SELECT COUNT(*) FROM fixed_expenses WHERE branch_id=? AND month=?',
        (branch_id, month)
    ).fetchone()[0]
    if existing > 0:
        return
    prev = db.execute(
        '''SELECT DISTINCT month FROM fixed_expenses
           WHERE branch_id=? AND month < ? AND expense_type='monthly'
           ORDER BY month DESC LIMIT 1''',
        (branch_id, month)
    ).fetchone()
    if not prev:
        return
    rows = db.execute(
        '''SELECT name, amount, expense_type, pct_value
           FROM fixed_expenses WHERE branch_id=? AND month=? AND expense_type='monthly' ''',
        (branch_id, prev['month'])
    ).fetchall()
    for r in rows:
        # % expenses: store 0, always calculated live from income
        amt = 0 if (r['pct_value'] and r['pct_value'] > 0) else r['amount']
        db.execute(
            '''INSERT OR IGNORE INTO fixed_expenses
               (branch_id, month, name, amount, expense_type, pct_value)
               VALUES (?,?,?,?,?,?)''',
            (branch_id, month, r['name'], amt, r['expense_type'], r['pct_value'])
        )
    db.commit()


@app.route('/api/fixed-expenses', methods=['GET'])
@login_required
def api_fixed_expenses_list():
    """List fixed expenses for a branch + month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    _ensure_monthly_expenses(branch_id, month, db)
    income = db.execute(
        "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
        "WHERE branch_id=? AND strftime('%Y-%m',date)=?",
        (branch_id, month)
    ).fetchone()[0]
    # Add today's live amount to income if we're viewing current month with no Z yet
    current_month = _now_il().strftime('%Y-%m')
    if month == current_month:
        today = _now_il().strftime('%Y-%m-%d')
        has_z = db.execute(
            "SELECT 1 FROM daily_sales WHERE branch_id=? AND date=?",
            (branch_id, today)
        ).fetchone()
        if not has_z:
            live = db.execute(
                "SELECT amount FROM live_sales WHERE branch_id=? AND date=?",
                (branch_id, today)
            ).fetchone()
            if live and live['amount']:
                income += live['amount']
    rows = db.execute(
        "SELECT id, name, amount, expense_type, pct_value, locked FROM fixed_expenses "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d['pct_value'] and d['pct_value'] > 0:
            d['amount'] = round(income * d['pct_value'] / 100, 2)
        result.append(d)
    return jsonify(result)


@app.route('/api/fixed-expenses', methods=['POST'])
@login_required
def api_fixed_expenses_create():
    """Add a new fixed expense."""
    data = request.get_json()
    branch_id = get_branch_id()
    month = data.get('month', _now_il().strftime('%Y-%m'))
    name = data.get('name', '').strip()
    amount = float(data.get('amount', 0))
    expense_type = data.get('expense_type', 'monthly')
    pct_value = data.get('pct_value')
    if not name:
        return jsonify({'error': 'name required'}), 400
    # % expenses: never store a stale amount — always computed live from income
    if pct_value and float(pct_value) > 0:
        amount = 0
    db = get_db()
    db.execute(
        "INSERT INTO fixed_expenses (branch_id, month, name, amount, expense_type, pct_value) VALUES (?, ?, ?, ?, ?, ?)",
        (branch_id, month, name, amount, expense_type, pct_value)
    )
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses/<int:exp_id>', methods=['PUT'])
@login_required
def api_fixed_expenses_update(exp_id):
    """Update a fixed expense (name, amount, type, pct_value)."""
    data = request.get_json()
    db = get_db()
    row = db.execute(
        'SELECT branch_id, name, amount, expense_type, pct_value FROM fixed_expenses WHERE id=?',
        (exp_id,)
    ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    if row['branch_id'] != get_branch_id():
        return jsonify({'error': 'forbidden'}), 403
    name = data.get('name', row['name'])
    amount = float(data.get('amount', row['amount']))
    expense_type = data.get('expense_type', row['expense_type'])
    pct_value = data.get('pct_value', row['pct_value'])
    # % expenses: never store a stale amount — always computed live from income
    if pct_value and float(pct_value) > 0:
        amount = 0
    db.execute(
        'UPDATE fixed_expenses SET name=?, amount=?, expense_type=?, pct_value=? WHERE id=?',
        (name, amount, expense_type, pct_value, exp_id)
    )
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses/<int:exp_id>', methods=['DELETE'])
@login_required
def api_fixed_expenses_delete(exp_id):
    """Delete a fixed expense."""
    db = get_db()
    row = db.execute('SELECT branch_id FROM fixed_expenses WHERE id=?', (exp_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    if row['branch_id'] != get_branch_id():
        return jsonify({'error': 'forbidden'}), 403
    db.execute("DELETE FROM fixed_expenses WHERE id = ?", (exp_id,))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses-summary')
@login_required
def api_fixed_expenses_summary():
    """Return fixed expenses summary with prorated monthly electricity.
    Same math as _get_fixed_total used by /api/summary (home page).
    Returns: {fixed_only, electricity: {amount, source, estimate_basis}, total, month_label}
    """
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    _ensure_monthly_expenses(branch_id, month, db)

    # Income calc — same logic as api_fixed_expenses_list
    income = db.execute(
        "SELECT COALESCE(SUM(amount),0) FROM daily_sales "
        "WHERE branch_id=? AND strftime('%Y-%m',date)=?",
        (branch_id, month)
    ).fetchone()[0]
    current_month = _now_il().strftime('%Y-%m')
    if month == current_month:
        today = _now_il().strftime('%Y-%m-%d')
        has_z = db.execute(
            "SELECT 1 FROM daily_sales WHERE branch_id=? AND date=?",
            (branch_id, today)
        ).fetchone()
        if not has_z:
            live = db.execute(
                "SELECT amount FROM live_sales WHERE branch_id=? AND date=?",
                (branch_id, today)
            ).fetchone()
            if live and live['amount']:
                income += live['amount']

    data = _get_fixed_total(branch_id, month, income, db)
    y, m = map(int, month.split('-'))
    data['month_label'] = f'{HEBREW_MONTHS[m]} {y}'
    return jsonify(data)


@app.route('/api/electricity-latest')
@login_required
def api_electricity_latest():
    """Return the most recent electricity invoice for the branch, or null."""
    branch_id = get_branch_id()
    db = get_db()
    row = db.execute(
        "SELECT period_label, amount, due_date FROM electricity_invoices "
        "WHERE branch_id = ? ORDER BY due_date DESC LIMIT 1",
        (branch_id,)
    ).fetchone()
    if not row:
        return jsonify(None)
    return jsonify({
        'period_label': row['period_label'],
        'amount': row['amount'],
        'due_date': row['due_date'],
    })


PDF_BASE = os.path.join(os.path.dirname(__file__), 'data', 'pdfs')


@app.route('/api/sales')
@login_required
def api_sales():
    """Return daily sales for a branch + month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    rows = db.execute(
        "SELECT date, amount, transactions, source, fetched_at FROM daily_sales "
        "WHERE branch_id = ? AND strftime('%Y-%m', date) = ? ORDER BY date DESC",
        (branch_id, month)
    ).fetchall()
    sales = [dict(r) for r in rows]
    for s in sales:
        s['fetched_at'] = _utc_str_to_il_iso(s.get('fetched_at'))

    total = sum(s['amount'] for s in sales)
    days = len(sales)
    avg = round(total / days, 2) if days else 0
    highest = max((s['amount'] for s in sales), default=0)
    lowest = min((s['amount'] for s in sales), default=0)

    # Per-row average per transaction
    for s in sales:
        s['avg_per_txn'] = round(s['amount'] / s['transactions']) if s['transactions'] else None

    # Average daily transaction count (only days with transactions)
    txn_days = [s['transactions'] for s in sales if s['transactions']]
    avg_daily_txn = round(sum(txn_days) / len(txn_days)) if txn_days else 0

    # Monthly average transaction value: total income / total transactions
    total_txns = sum(txn_days)
    avg_txn_value = round(total / total_txns) if total_txns else 0

    # Check which dates have PDFs
    pdf_dir = os.path.join(PDF_BASE, str(branch_id))
    for s in sales:
        pdf_path = os.path.join(pdf_dir, f"z_{s['date']}.pdf")
        s['has_pdf'] = os.path.isfile(pdf_path)

    return jsonify({
        'sales': sales,
        'total': total,
        'avg': avg,
        'highest': highest,
        'lowest': lowest,
        'days': days,
        'avg_daily_txn': avg_daily_txn,
        'avg_txn_value': avg_txn_value,
    })


@app.route('/api/sales/pdf/<sale_date>')
@login_required
def api_sales_pdf(sale_date):
    """Serve the original PDF for a Z-report."""
    branch_id = get_branch_id()
    pdf_path = os.path.join(PDF_BASE, str(branch_id), f"z_{sale_date}.pdf")
    if not os.path.isfile(pdf_path):
        abort(404)
    return send_file(pdf_path, mimetype='application/pdf')


@app.route('/api/sales/pdf-image/<sale_date>/<int:page>')
@login_required
def api_sales_pdf_image(sale_date, page):
    """Render a PDF page as PNG image using PyMuPDF."""
    branch_id = get_branch_id()
    pdf_path = os.path.join(PDF_BASE, str(branch_id), f"z_{sale_date}.pdf")
    if not os.path.isfile(pdf_path):
        abort(404)
    if fitz is None:
        abort(500)
    try:
        doc = fitz.open(pdf_path)
        if page < 0 or page >= len(doc):
            abort(404)
        pix = doc[page].get_pixmap(dpi=150)
        img_bytes = pix.tobytes("png")
        doc.close()
        return send_file(io.BytesIO(img_bytes), mimetype='image/png')
    except Exception:
        abort(500)


@app.route('/api/sales/pdf-pages/<sale_date>')
@login_required
def api_sales_pdf_pages(sale_date):
    """Return the number of pages in a PDF."""
    branch_id = get_branch_id()
    pdf_path = os.path.join(PDF_BASE, str(branch_id), f"z_{sale_date}.pdf")
    if not os.path.isfile(pdf_path) or fitz is None:
        return jsonify({'pages': 0})
    try:
        doc = fitz.open(pdf_path)
        pages = len(doc)
        doc.close()
        return jsonify({'pages': pages})
    except Exception:
        return jsonify({'pages': 0})


def _admin_required(f):
    """Allow only role='admin'. CEO and manager are explicitly rejected here —
    /ops and /admin/* are operator-only surfaces."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated


def _collect_chain_stores(db):
    """Return the list of active chain stores (rows with aviv_branch_id set,
    excluding HQ/legacy ids) with a needs_setup flag — same shape the
    /admin/branches enrich-form expects. Lifted out so /ops can render the
    same dropdown without duplicating the loop.
    """
    from agents.aviv_z_report import EXCLUDED_CHAIN_AVIV_IDS
    excluded = set(EXCLUDED_CHAIN_AVIV_IDS)
    manager_map = {}
    for row in db.execute(
        "SELECT ub.branch_id FROM user_branches ub JOIN users u ON u.id = ub.user_id "
        "WHERE u.active = 1 AND u.role = 'manager'"
    ).fetchall():
        manager_map[row['branch_id']] = manager_map.get(row['branch_id'], 0) + 1
    stores = []
    for b in db.execute(
        'SELECT id, name, city, aviv_branch_id, bilboy_branch_id, '
        'franchise_supplier FROM branches WHERE active=1 ORDER BY id'
    ).fetchall():
        if b['aviv_branch_id'] is None or b['aviv_branch_id'] in excluded:
            continue
        has_franchise = bool((b['franchise_supplier'] or '').strip())
        has_bilboy = b['bilboy_branch_id'] is not None
        has_manager = manager_map.get(b['id'], 0) > 0
        needs_setup = not (has_franchise and has_bilboy and has_manager)
        stores.append({
            'id': b['id'],
            'name': b['name'] or f"סניף {b['aviv_branch_id']}",
            'aviv_branch_id': b['aviv_branch_id'],
            'city': b['city'] or '',
            'needs_setup': needs_setup,
        })
    return stores


@app.route('/ops')
@_admin_required
def ops():
    ctx = _page_context('ops')
    db = get_db()
    return render_template('ops.html', chain_stores=_collect_chain_stores(db), **ctx)


def _to_il_time(utc_str):
    """Convert UTC datetime string from SQLite to Israel time HH:MM:SS."""
    if not utc_str:
        return ''
    try:
        from datetime import timezone
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        il_dt = dt.astimezone(IL_TZ)
        return il_dt.strftime('%H:%M:%S')
    except Exception:
        return utc_str


def _to_il_datetime(utc_str):
    """Convert UTC datetime string to Israel time DD/MM HH:MM."""
    if not utc_str:
        return ''
    try:
        from datetime import timezone
        dt = datetime.fromisoformat(utc_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        il_dt = dt.astimezone(IL_TZ)
        return il_dt.strftime('%d/%m %H:%M')
    except Exception:
        return utc_str


def _convert_run_times(row_dict):
    """Convert started_at (DD/MM HH:MM) and finished_at (HH:MM:SS) to Israel time."""
    row_dict['started_at'] = _to_il_datetime(row_dict.get('started_at'))
    row_dict['finished_at'] = _to_il_time(row_dict.get('finished_at'))
    return row_dict


@app.route('/api/ops-status')
@_admin_required
def api_ops_status():
    from agents.aviv_z_report import EXCLUDED_CHAIN_AVIV_IDS
    excluded = set(EXCLUDED_CHAIN_AVIV_IDS)

    db = get_db()
    current_month = _now_il().strftime('%Y-%m')
    # Branches
    branches_rows = db.execute(
        'SELECT id, name, city, active, aviv_branch_id FROM branches WHERE active = 1'
    ).fetchall()
    branches = []
    for b in branches_rows:
        bid = b['id']
        # Last run per agent — exactly one row per agent
        agents_data = {}
        for agent in ('bilboy', 'gmail', 'aviv_live', 'aviv_report'):
            row = db.execute(
                "SELECT status, message, started_at, duration_seconds, docs_count, amount "
                "FROM agent_runs WHERE branch_id=? AND agent=? "
                "ORDER BY started_at DESC LIMIT 1",
                (bid, agent)
            ).fetchone()
            if row:
                d = dict(row)
                d['started_at'] = _to_il_datetime(d.get('started_at'))
                agents_data[agent] = d
            else:
                agents_data[agent] = None

        # IEC agent — uses iec_last_sync_at from branches + agent_runs for iec
        iec_row = db.execute(
            "SELECT iec_token, iec_last_sync_at FROM branches WHERE id = ?", (bid,)
        ).fetchone()
        has_iec = iec_row and iec_row['iec_token']
        if has_iec:
            iec_run = db.execute(
                "SELECT status, message, started_at, duration_seconds, docs_count, amount "
                "FROM agent_runs WHERE branch_id=? AND agent='iec' "
                "ORDER BY started_at DESC LIMIT 1",
                (bid,)
            ).fetchone()
            if iec_run:
                d = dict(iec_run)
                d['started_at'] = _to_il_datetime(d.get('started_at'))
                agents_data['iec'] = d
            else:
                # No agent_runs yet but has token — show last sync time
                agents_data['iec'] = {
                    'status': 'success' if iec_row['iec_last_sync_at'] else 'skipped',
                    'message': '',
                    'started_at': _to_il_datetime(iec_row['iec_last_sync_at']) if iec_row['iec_last_sync_at'] else '',
                    'duration_seconds': None,
                    'docs_count': None,
                    'amount': None,
                }
        else:
            agents_data['iec'] = None

        # Determine overall status. 'skipped' is NOT 'ok' — a chain branch
        # whose only runs were no_credentials skips would otherwise show a
        # green dot. Treat all-skipped (or no rows) as 'unknown'; any real
        # success alongside skips is 'ok'.
        statuses = [a['status'] for a in agents_data.values() if a]
        if 'error' in statuses:
            overall = 'error'
        elif 'warning' in statuses:
            overall = 'warning'
        elif 'success' in statuses:
            overall = 'ok'
        else:
            overall = 'unknown'

        # Hourly rate info
        rate_row = db.execute(
            "SELECT avg_hourly_rate, hours_this_month FROM branches WHERE id = ?",
            (bid,)
        ).fetchone()

        # Count employees with defined rates
        emp_rate_count = db.execute(
            "SELECT COUNT(*) as cnt FROM employees WHERE branch_id = ? AND active = 1 AND hourly_rate > 0",
            (bid,)
        ).fetchone()['cnt']

        # Salary — single source of truth, same function /employees uses.
        # /ops previously estimated salary as branches.hours_this_month *
        # avg_hourly_rate (Aviv-scraped branch total) which double-counted
        # vs the per-employee tracked sum on /employees. Reconcile here.
        salary_data = _calculate_salary_cost(bid, current_month)

        aviv_chain_id = b['aviv_branch_id']
        is_chain_store = aviv_chain_id is not None and aviv_chain_id not in excluded
        branches.append({
            'id': bid, 'name': b['name'], 'city': b['city'],
            'status': overall, 'agents': agents_data,
            'avg_hourly_rate': rate_row['avg_hourly_rate'] if rate_row else 0,
            'hours_this_month': rate_row['hours_this_month'] if rate_row else 0,
            'salary_cost': salary_data['amount'],
            'salary_hours': salary_data['hours'],
            'salary_source': salary_data['source'],
            'employees_with_rates': emp_rate_count,
            'has_iec_token': bool(has_iec),
            'aviv_branch_id': aviv_chain_id,
            'is_chain_store': is_chain_store,
        })

    # Recent agent runs
    runs = db.execute(
        "SELECT ar.*, b.name as branch_name FROM agent_runs ar "
        "LEFT JOIN branches b ON ar.branch_id = b.id "
        "WHERE (ar.message NOT LIKE '%orphaned%' OR ar.message IS NULL) "
        "ORDER BY ar.started_at DESC LIMIT 20"
    ).fetchall()
    agent_runs = [_convert_run_times(dict(r)) for r in runs]

    # Alerts — errors and warnings from last 7 days
    alerts_rows = db.execute(
        "SELECT ar.*, b.name as branch_name FROM agent_runs ar "
        "LEFT JOIN branches b ON ar.branch_id = b.id "
        "WHERE ar.status IN ('error', 'warning') AND ar.started_at >= datetime('now', '-7 days') "
        "AND (ar.dismissed IS NULL OR ar.dismissed = 0) "
        "ORDER BY ar.started_at DESC LIMIT 20"
    ).fetchall()
    alerts = [_convert_run_times(dict(r)) for r in alerts_rows]

    # Summary stats
    active_count = len(branches)
    error_count_row = db.execute(
        "SELECT COUNT(*) as cnt FROM agent_runs WHERE status='error' AND started_at >= datetime('now', '-1 day')"
    ).fetchone()
    error_count = error_count_row['cnt'] if error_count_row else 0

    last_nightly_row = db.execute(
        "SELECT started_at FROM agent_runs WHERE agent='bilboy' ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    last_nightly = _to_il_datetime(last_nightly_row['started_at']) if last_nightly_row else ''

    # Aviv status: is store open now?
    from agents.aviv_live import _is_store_hours, get_next_opening
    store_open = _is_store_hours()
    next_opening = get_next_opening() if not store_open else ''

    return jsonify({
        'branches': branches,
        'agent_runs': agent_runs,
        'alerts': alerts,
        'summary': {
            'active_branches': active_count,
            'errors_24h': error_count,
            'last_nightly': last_nightly,
            'store_open': store_open,
            'next_opening': next_opening,
        },
    })


@app.route('/ops/run-agent', methods=['POST'])
@_admin_required
def ops_run_agent():
    data = request.get_json()
    branch_id = data.get('branch_id')
    agent = data.get('agent')

    if not branch_id or agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_report', 'iec'):
        return jsonify({'status': 'error', 'message': 'Invalid parameters'}), 400

    t0 = time.time()
    # Resolve chain-mode eligibility once: a branch is chain-eligible when it
    # has aviv_branch_id set. The auth path actually used is logged below so
    # /ops shows which one ran (mirrors the bilboy pattern).
    db = get_db()
    branch_row = db.execute(
        'SELECT aviv_branch_id FROM branches WHERE id=?', (int(branch_id),)
    ).fetchone()
    has_aviv_chain_id = bool(branch_row and branch_row['aviv_branch_id'] is not None)
    auth_path = 'per_store'

    try:
        if agent == 'bilboy':
            from agents.bilboy import run_bilboy
            result = run_bilboy(int(branch_id))
            msg = f"{result.get('docs_count', 0)} docs, ₪{result.get('total_amount', 0):,.0f}"
        elif agent == 'gmail':
            from agents.gmail_agent import run_gmail_sync
            result = run_gmail_sync(int(branch_id))
            msg = f"{result.get('new_reports', 0)} דוחות חדשים"
        elif agent == 'aviv_live':
            # Manual /ops trigger: bypass the store-hours guard (admin clicked
            # the button on purpose). Only aviv_live takes force.
            from agents.aviv_live import (
                run_aviv_live, run_aviv_live_chain_one,
                USE_CHAIN_AUTH as AVIV_LIVE_USE_CHAIN,
            )
            if AVIV_LIVE_USE_CHAIN and has_aviv_chain_id:
                auth_path = 'chain'
                result = run_aviv_live_chain_one(int(branch_id), force=True)
            else:
                result = run_aviv_live(int(branch_id), force=True)
            msg = f"₪{result.get('amount', 0):,.0f} ({result.get('transactions', 0)} tx)"
        elif agent == 'iec':
            # IEC API is geo-blocked outside Israel — must run on Israeli VPS via SSH
            bid = int(branch_id)
            ssh_cmd = [
                'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                'makolet-iec',
                f'/opt/makolet-iec/venv/bin/python /opt/makolet-iec/iec_sync.py --branch-id {bid}'
            ]
            proc = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=120)
            if proc.returncode == 0:
                result = {'success': True}
                # Extract message from stdout (last INFO line)
                lines = [l for l in proc.stdout.strip().split('\n') if l.strip()]
                msg = lines[-1] if lines else 'IEC sync completed'
            else:
                result = {'success': False}
                msg = proc.stderr.strip() or proc.stdout.strip() or 'SSH to VPS failed'
        else:  # aviv_report → chain-aware aviv_employees_report.run_for_branch
            from agents.aviv_employees_report import (
                run_for_branch, _login_chain_account, _refresh,
                USE_CHAIN_AUTH as AVIV_EMP_USE_CHAIN,
            )
            chain_token = None
            if AVIV_EMP_USE_CHAIN and has_aviv_chain_id:
                auth_path = 'chain'
                chain_token = _refresh(_login_chain_account())
            report_res = run_for_branch(int(branch_id), include_previous_month=False,
                                        chain_token=chain_token)
            # Normalize to the shape the rest of the handler expects.
            if report_res.get('ok'):
                if report_res.get('skipped'):
                    result = {'success': True, 'skipped': report_res.get('reason')}
                    msg = report_res.get('reason') or 'skipped'
                else:
                    result = {'success': True}
                    msg = (f"matched={report_res.get('matched',0)} "
                           f"unmatched={report_res.get('unmatched',0)} "
                           f"hours={report_res.get('total_hours',0):.1f}")
            else:
                result = {'success': False, 'error': report_res.get('error', 'unknown')}
                msg = report_res.get('error', 'unknown')

        duration = round(time.time() - t0, 1)
        # Classify outcome: real success vs no-op skip vs error. Skipped runs
        # do NOT count as success — they're surfaced as 'skipped' so the /ops
        # status dot reflects reality and brrr stays quiet.
        if not result.get('success'):
            status = 'error'
            msg = result.get('error', msg or 'Unknown error')
        elif result.get('skipped'):
            status = 'skipped'
            msg = f"דילוג: {result.get('skipped')}"
        else:
            status = 'success'

        app.logger.info("ops_run_agent agent=%s branch=%s auth_path=%s status=%s",
                        agent, branch_id, auth_path, status)

        from utils.notify import notify
        # Only emit brrr for real outcomes — not for skipped/no-op manual runs.
        if status == 'error':
            notify(f"❌ {agent}", f"סניף {branch_id} — {msg}")
        elif status == 'success':
            notify(f"✅ {agent}", f"סניף {branch_id} — {msg}")
        return jsonify({'status': status, 'message': msg,
                        'duration': duration, 'auth_path': auth_path})

    except Exception as e:
        duration = round(time.time() - t0, 1)
        return jsonify({'status': 'error', 'message': str(e), 'duration': duration})


@app.route('/ops/logs/<int:branch_id>/<agent>')
@_admin_required
def ops_logs(branch_id, agent):
    import re as _re
    if agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_report', 'iec'):
        abort(400)

    # Get branch name for modal title
    db = get_db()
    brow = db.execute('SELECT name FROM branches WHERE id = ?', (branch_id,)).fetchone()
    branch_name = brow['name'] if brow else f'#{branch_id}'

    log_path = os.path.join(os.path.dirname(__file__), 'logs', f'{agent}_{branch_id}.log')
    if not os.path.isfile(log_path):
        return jsonify({'branch_name': branch_name, 'lines': [{'message': 'אין קובץ לוגים.', 'level': 'default'}]})
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            raw_lines = f.readlines()

        # Reverse (most recent first), limit to 30
        raw_lines = list(reversed(raw_lines))[:30]

        # Strip timestamp + log level, classify
        log_strip_re = _re.compile(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+\s+(INFO|WARNING|ERROR|DEBUG)\s+')
        result = []
        for line in raw_lines:
            line = line.rstrip('\n')
            if not line.strip():
                continue
            msg = log_strip_re.sub('', line)

            lower = msg.lower()
            if any(kw in lower for kw in ('error', 'נכשל', 'failed', 'exception')):
                level = 'error'
            elif any(kw in lower for kw in ('warning', 'diff', '⚠', 'mismatch')):
                level = 'warning'
            elif any(kw in lower for kw in ('success', '✅', ' ok', 'complete', 'saved')):
                level = 'success'
            else:
                level = 'default'

            result.append({'message': msg, 'level': level})

        return jsonify({'branch_name': branch_name, 'lines': result})
    except Exception as e:
        return jsonify({'branch_name': branch_name, 'lines': [{'message': f'שגיאה: {e}', 'level': 'error'}]})


@app.route('/ops/dismiss-alert', methods=['POST'])
@_admin_required
def ops_dismiss_alert():
    data = request.get_json()
    alert_id = data.get('alert_id')
    if not alert_id:
        return jsonify({'error': 'missing alert_id'}), 400
    db = get_db()
    db.execute("UPDATE agent_runs SET dismissed = 1 WHERE id = ?", (alert_id,))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/ops-health')
@_admin_required
def api_ops_health():
    def _run(cmd):
        try:
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            return r.stdout.strip()
        except Exception as e:
            return str(e)

    # Derive project root + service names from this file's location so the
    # same code reports staging's state on staging and prod's on prod. Prior
    # version hardcoded /opt/makolet-chain and showed prod data on staging.
    project_root = os.path.dirname(os.path.abspath(__file__))
    service_name = os.path.basename(project_root)
    svc1 = _run(f"systemctl is-active {service_name}")
    # Staging runs Flask only (no separate scheduler unit per STAGING.md);
    # mark scheduler as 'n/a' there so the health card doesn't false-error.
    if service_name.endswith('-staging'):
        svc2 = 'n/a'
    else:
        svc2 = _run(f"systemctl is-active {service_name}-scheduler")
    disk = _run(f"df -h {project_root} --output=used,size,pcent | tail -1")
    memory = _run("free -m | awk 'NR==2{printf \"%s/%s\", $3, $2}'")
    uptime = _run("uptime -p")
    deploy_ago = _run(f"git -C {project_root} log -1 --format='%ar'")
    deploy_msg = _run(f"git -C {project_root} log -1 --format='%s'")

    # Parse disk: "3.4G  150G   3%"  →  "3.4G / 150G (3%)"
    disk_pct = 0
    disk_display = disk.strip()
    try:
        parts = disk.split()
        used, size, pct = parts[0], parts[1], parts[2]
        disk_pct = int(pct.replace('%', ''))
        disk_display = f"{used} / {size} ({pct})"
    except Exception:
        pass

    # Truncate commit message to 30 chars
    if len(deploy_msg) > 30:
        deploy_msg = deploy_msg[:30] + '...'
    last_deploy = f"{deploy_ago} — {deploy_msg}"

    # Only count expected services. Staging has Flask only (svc2='n/a'), so
    # total=1 — keeps the "X/N פעילים" tile honest across environments.
    expected = [s for s in (svc1, svc2) if s != 'n/a']
    services_active = sum(1 for s in expected if s == 'active')
    services_total = len(expected)
    services_ok = services_active == services_total
    disk_status = 'ok' if disk_pct < 70 else ('warning' if disk_pct < 90 else 'error')

    return jsonify({
        'services': {'app': svc1, 'scheduler': svc2, 'ok': services_ok,
                     'active': services_active, 'total': services_total},
        'disk': {'raw': disk_display, 'pct': disk_pct, 'status': disk_status},
        'memory': memory,
        'uptime': uptime,
        'last_deploy': last_deploy,
    })


@app.route('/admin/branches')
@_admin_required
def admin_branches():
    db = get_db()
    from agents.aviv_z_report import EXCLUDED_CHAIN_AVIV_IDS
    excluded = set(EXCLUDED_CHAIN_AVIV_IDS)

    branch_rows = db.execute('SELECT * FROM branches ORDER BY id').fetchall()
    manager_map = {}
    for row in db.execute(
        "SELECT ub.branch_id, u.id AS user_id, u.name, u.email "
        "FROM user_branches ub JOIN users u ON u.id = ub.user_id "
        "WHERE u.active = 1 AND u.role = 'manager' "
        "ORDER BY ub.branch_id, u.id"
    ).fetchall():
        manager_map.setdefault(row['branch_id'], []).append(
            {'id': row['user_id'], 'name': row['name'], 'email': row['email']})

    branches = []
    chain_stores = []
    for b in branch_rows:
        bd = dict(b)
        managers = manager_map.get(bd['id'], [])
        bd['manager_count'] = len(managers)
        bd['manager_names'] = ', '.join(m['name'] for m in managers)
        bd['has_bilboy'] = bd.get('bilboy_branch_id') is not None
        bd['has_franchise'] = bool((bd.get('franchise_supplier') or '').strip())
        bd['has_manager'] = len(managers) > 0
        is_chain = (bd.get('aviv_branch_id') is not None
                    and bd['aviv_branch_id'] not in excluded)
        bd['is_chain_store'] = is_chain
        # Status keys on DATA-SOURCE config only (Aviv id + BilBoy), matching
        # the Aviv#/BilBoy ✓ columns. is_chain already guarantees aviv_branch_id.
        # Manager assignment (user_branches) is a SEPARATE concern and must NOT
        # drive configured/unconfigured — chain stores are admin-only by design.
        bd['needs_setup'] = is_chain and not bd['has_bilboy']
        branches.append(bd)
        if is_chain and bd.get('active'):
            chain_stores.append({
                'id': bd['id'],
                'name': bd.get('name') or f"סניף {bd['aviv_branch_id']}",
                'aviv_branch_id': bd['aviv_branch_id'],
                'city': bd.get('city') or '',
                'needs_setup': bd['needs_setup'],
            })

    users = db.execute(
        "SELECT u.*, GROUP_CONCAT(ub.branch_id) as branch_ids "
        "FROM users u LEFT JOIN user_branches ub ON u.id = ub.user_id "
        "GROUP BY u.id ORDER BY u.id"
    ).fetchall()
    return render_template('admin_branches.html',
                           branches=branches,
                           chain_stores=chain_stores,
                           users=[dict(u) for u in users],
                           **_page_context('admin'))


@app.route('/api/admin/branches', methods=['POST'])
@_admin_required
def api_admin_branch_create():
    """Enrich an autoseed-discovered chain store with per-store config.

    This endpoint NO LONGER creates rows — autoseed (from /account/branches)
    owns the store roster. The form picks an existing branch_id from the
    chain-stores dropdown; we UPDATE that row in place. Rejects any call that
    targets a row without aviv_branch_id set or one in EXCLUDED_CHAIN_AVIV_IDS,
    which prevents the form from recreating the NULL-aviv_branch_id collision
    that pre-dated this change.
    """
    from agents.aviv_z_report import EXCLUDED_CHAIN_AVIV_IDS
    data = request.get_json() or {}
    try:
        branch_id = int(data.get('branch_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'branch_id required'}), 400

    db = get_db()
    row = db.execute(
        'SELECT id, aviv_branch_id, city, franchise_supplier '
        'FROM branches WHERE id=?', (branch_id,)).fetchone()
    if row is None:
        return jsonify({'error': 'unknown branch_id'}), 404
    if row['aviv_branch_id'] is None:
        return jsonify({'error': 'branch is not a chain store (no aviv_branch_id)'}), 400
    if row['aviv_branch_id'] in EXCLUDED_CHAIN_AVIV_IDS:
        return jsonify({'error': 'branch is HQ/legacy and cannot be enriched'}), 400

    updates = {}
    city = (data.get('city') or '').strip()
    if city and not (row['city'] or '').strip():
        updates['city'] = city
    for f in ('franchise_supplier',):
        if f in data and (data.get(f) or '').strip():
            updates[f] = data[f].strip()
    if updates:
        sql = 'UPDATE branches SET ' + ', '.join(f + '=?' for f in updates) + ' WHERE id=?'
        db.execute(sql, list(updates.values()) + [branch_id])
        db.commit()

    manager_email = (data.get('manager_email') or '').strip().lower()
    manager_name = (data.get('manager_name') or '').strip()
    if manager_email and manager_name:
        temp_password = secrets.token_urlsafe(8)
        pw_hash = generate_password_hash(temp_password)
        db.execute(
            "INSERT OR IGNORE INTO users (name, email, password_hash, role) VALUES (?,?,?,'manager')",
            (manager_name, manager_email, pw_hash))
        db.commit()
        user_row = db.execute('SELECT id FROM users WHERE LOWER(email)=?',
                              (manager_email,)).fetchone()
        if user_row:
            db.execute('INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?,?)',
                       (user_row['id'], branch_id))
            db.commit()
        return jsonify({'ok': True, 'branch_id': branch_id, 'temp_password': temp_password})
    return jsonify({'ok': True, 'branch_id': branch_id})


@app.route('/api/admin/branches/<int:branch_id>')
@_admin_required
def api_admin_branch_get(branch_id):
    db = get_db()
    row = db.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(dict(row))


@app.route('/api/admin/branches/<int:branch_id>', methods=['PUT'])
@_admin_required
def api_admin_branch_update(branch_id):
    data = request.get_json()
    db = get_db()
    fields = ['name', 'city', 'active', 'aviv_user_id', 'aviv_password',
              'gmail_label', 'franchise_supplier', 'iec_contract']
    updates = {f: data[f] for f in fields if f in data}
    if not updates:
        return jsonify({'ok': True})
    sql = 'UPDATE branches SET ' + ', '.join(f + '=?' for f in updates) + ' WHERE id=?'
    db.execute(sql, list(updates.values()) + [branch_id])
    db.commit()
    return jsonify({'ok': True})


# ── Admin Users & Branch Assignments ────────────────────────────────────

@app.route('/admin/users')
@_admin_required
def admin_users():
    return render_template('admin_users.html',
                           current_user_id=session.get('user_id'),
                           **_page_context('admin'))


def _z_status_rows(db, target_date):
    """Build /z-status rows: one per active branch with aviv_branch_id,
    joined LEFT to z_report_902 for target_date. Sorted by local branch id.

    Derived status values:
      got      — z_number AND amount NOT NULL
      closed   — row exists, z_number IS NULL (closed-day sentinel)
      missing  — no row in z_report_902 for (branch, date)
      parse    — z_number NOT NULL but amount IS NULL (parse failure edge)
    """
    # Non-store chain entries (HQ, legacy) must never show on /z-status. The
    # agent's EXCLUDED_CHAIN_AVIV_IDS is the source of truth; mirror it here so
    # a stray seeded row can't sneak back into the diagnostic.
    from agents.aviv_z_report import EXCLUDED_CHAIN_AVIV_IDS
    exclude_csv = ','.join(str(x) for x in sorted(EXCLUDED_CHAIN_AVIV_IDS)) or 'NULL'
    rows = db.execute(
        "SELECT b.id AS branch_id, b.name AS branch_name, "
        "       b.aviv_branch_id, "
        "       z.z_number, z.amount, z.transactions, z.fetched_at, "
        "       z.trigger_type, z.auth_source "
        "FROM branches b "
        "LEFT JOIN z_report_902 z "
        "  ON z.branch_id = b.id AND z.date = ? "
        "WHERE b.active = 1 AND b.aviv_branch_id IS NOT NULL "
        f"  AND b.aviv_branch_id NOT IN ({exclude_csv}) "
        "ORDER BY b.id",
        (target_date,)
    ).fetchall()
    out = []
    for r in rows:
        has_row = r['fetched_at'] is not None
        if not has_row:
            status = 'missing'
        elif r['z_number'] is None:
            status = 'closed'
        elif r['amount'] is None:
            status = 'parse'
        else:
            status = 'got'
        out.append({
            'branch_id': r['branch_id'],
            'branch_name': r['branch_name'],
            'aviv_branch_id': r['aviv_branch_id'],
            'z_number': r['z_number'],
            'amount': r['amount'],
            'transactions': r['transactions'],
            'fetched_at_il': _utc_str_to_il_iso(r['fetched_at']),
            'trigger_type': r['trigger_type'],
            'auth_source': r['auth_source'],
            'status': status,
        })
    return out


@app.route('/z-status')
@_admin_required
def z_status():
    """Diagnostic page: per-branch Z pull status for a chosen date.

    Read-only — reads only z_report_902 + branches. Default date is
    yesterday in Israel time (matches the agent's default target).
    """
    requested = (request.args.get('date') or '').strip()
    if requested:
        try:
            target_date = datetime.strptime(requested, '%Y-%m-%d').date().isoformat()
        except ValueError:
            target_date = (_now_il().date() - timedelta(days=1)).isoformat()
    else:
        target_date = (_now_il().date() - timedelta(days=1)).isoformat()

    db = get_db()
    rows = _z_status_rows(db, target_date)

    summary = {
        'total': len(rows),
        'got': sum(1 for r in rows if r['status'] == 'got'),
        'closed': sum(1 for r in rows if r['status'] == 'closed'),
        'missing': sum(1 for r in rows if r['status'] == 'missing'),
        'parse': sum(1 for r in rows if r['status'] == 'parse'),
    }
    ctx = _page_context('z_status')
    return render_template('z_status.html', rows=rows, target_date=target_date,
                           summary=summary, **ctx)


@app.route('/api/admin/users')
@_admin_required
def api_admin_users():
    db = get_db()
    users = db.execute('SELECT id, email, name, role, active FROM users ORDER BY id').fetchall()
    result = []
    for u in users:
        branches = db.execute(
            '''SELECT b.id, b.name, b.city FROM user_branches ub
               JOIN branches b ON b.id = ub.branch_id
               WHERE ub.user_id = ? ORDER BY b.id''', (u['id'],)
        ).fetchall()
        result.append({
            'id': u['id'], 'email': u['email'], 'name': u['name'],
            'role': u['role'], 'active': u['active'],
            'branches': [{'id': b['id'], 'name': b['name'], 'city': b['city']} for b in branches]
        })
    return jsonify(result)


@app.route('/api/admin/users', methods=['POST'])
@_admin_required
def api_admin_user_create():
    """Create a manager or CEO user. Admin users are not creatable from the UI."""
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    email = (data.get('email') or '').strip().lower()
    password = (data.get('password') or '').strip()
    role = (data.get('role') or 'manager').strip()

    if not name or not email or not password:
        return jsonify({'error': 'missing name, email, or password'}), 400
    if role not in ('manager', 'ceo'):
        return jsonify({'error': 'role must be manager or ceo'}), 400
    if len(password) < 6:
        return jsonify({'error': 'password must be at least 6 chars'}), 400

    db = get_db()
    existing = db.execute('SELECT id FROM users WHERE LOWER(email)=?', (email,)).fetchone()
    if existing:
        return jsonify({'error': 'email already exists'}), 409

    pw_hash = generate_password_hash(password)
    cur = db.execute(
        'INSERT INTO users (name, email, password_hash, role, active) VALUES (?,?,?,?,1)',
        (name, email, pw_hash, role))
    db.commit()
    return jsonify({'ok': True, 'user_id': cur.lastrowid, 'role': role}), 201


@app.route('/api/admin/users/<int:user_id>/active', methods=['POST'])
@_admin_required
def api_admin_user_set_active(user_id):
    """Reversibly (de)activate a user account — sets users.active 0/1 ONLY.

    NEVER deletes the row and never touches user_branches; reactivation
    restores access fully. active=0 blocks login + all data access because the
    login + password-reset queries require `active = 1`. Admin-only via
    _admin_required (ceo/manager → 403). An admin CANNOT deactivate their own
    currently-logged-in account (would lock themselves out) — enforced
    server-side, not just hidden in the UI.
    """
    data = request.get_json(silent=True) or {}
    active = data.get('active')
    if active not in (0, 1, True, False):
        return jsonify({'error': 'active must be 0 or 1'}), 400
    active = 1 if active in (1, True) else 0

    db = get_db()
    user = db.execute('SELECT id, email FROM users WHERE id = ?', (user_id,)).fetchone()
    if not user:
        return jsonify({'error': 'user not found'}), 404

    if active == 0 and user_id == session.get('user_id'):
        return jsonify({'error': 'cannot deactivate your own account'}), 403

    db.execute('UPDATE users SET active = ? WHERE id = ?', (active, user_id))
    db.commit()
    return jsonify({'ok': True, 'user_id': user_id, 'active': active})


@app.route('/api/admin/users/<int:user_id>/branches', methods=['POST'])
@_admin_required
def api_admin_user_add_branch(user_id):
    db = get_db()
    user = db.execute('SELECT id, role FROM users WHERE id = ?', (user_id,)).fetchone()
    if not user:
        return jsonify({'error': 'user not found'}), 404
    if user['role'] in ROLES_ALL_BRANCHES:
        return jsonify({'error': 'cannot assign branches to admin/ceo users — they see all branches automatically'}), 403
    data = request.get_json()
    branch_id = data.get('branch_id')
    branch = db.execute('SELECT id FROM branches WHERE id = ?', (branch_id,)).fetchone()
    if not branch:
        return jsonify({'error': 'branch not found'}), 404
    existing = db.execute(
        'SELECT 1 FROM user_branches WHERE user_id = ? AND branch_id = ?',
        (user_id, branch_id)).fetchone()
    if existing:
        return jsonify({'error': 'branch already assigned'}), 409
    db.execute('INSERT INTO user_branches (user_id, branch_id) VALUES (?, ?)',
               (user_id, branch_id))
    db.commit()
    return jsonify({'ok': True, 'user_id': user_id, 'branch_id': branch_id}), 201


@app.route('/api/admin/users/<int:user_id>/branches/<int:branch_id>', methods=['DELETE'])
@_admin_required
def api_admin_user_remove_branch(user_id, branch_id):
    db = get_db()
    user = db.execute('SELECT id, role FROM users WHERE id = ?', (user_id,)).fetchone()
    if not user:
        return jsonify({'error': 'user not found'}), 404
    existing = db.execute(
        'SELECT 1 FROM user_branches WHERE user_id = ? AND branch_id = ?',
        (user_id, branch_id)).fetchone()
    if not existing:
        return '', 204
    count = db.execute(
        'SELECT COUNT(*) as cnt FROM user_branches WHERE user_id = ?',
        (user_id,)).fetchone()['cnt']
    if count <= 1 and user['role'] == 'manager':
        return jsonify({'error': 'cannot leave manager without any branches — delete or deactivate the user instead'}), 422
    db.execute('DELETE FROM user_branches WHERE user_id = ? AND branch_id = ?',
               (user_id, branch_id))
    db.commit()
    return '', 204


@app.route('/api/admin/branches-list')
@_admin_required
def api_admin_branches_list():
    db = get_db()
    rows = db.execute('SELECT id, name, city FROM branches WHERE active = 1 ORDER BY id').fetchall()
    return jsonify([{'id': r['id'], 'name': r['name'], 'city': r['city']} for r in rows])


# ── Manual electricity endpoints ──────────────────────────────────────────

@app.route('/api/electricity/manual', methods=['POST'])
@login_required
def api_electricity_manual_create():
    """Create or update a manual electricity entry for a month."""
    branch_id = get_branch_id()
    data = request.get_json(force=True)
    month = data.get('month', '').strip()
    amount = data.get('amount')
    if not month or amount is None:
        return jsonify({'error': 'month and amount required'}), 400
    try:
        amount = round(float(amount), 2)
    except (ValueError, TypeError):
        return jsonify({'error': 'invalid amount'}), 400

    db = get_db()
    branch = db.execute(
        "SELECT electricity_source FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    elec_source = branch['electricity_source'] if branch else None

    if elec_source == 'iec':
        return jsonify({'error': 'Branch is on IEC mode. Switch source first.'}), 409

    # Upsert manual entry
    existing = db.execute(
        "SELECT id FROM electricity_invoices WHERE branch_id = ? AND source = 'manual' AND month = ?",
        (branch_id, month)
    ).fetchone()
    if existing:
        db.execute(
            "UPDATE electricity_invoices SET amount = ? WHERE id = ?",
            (amount, existing['id'])
        )
    else:
        db.execute(
            "INSERT INTO electricity_invoices (branch_id, amount, source, month, period_label) VALUES (?, ?, 'manual', ?, ?)",
            (branch_id, amount, month, month)
        )

    # Auto-set source to 'manual' if not yet configured
    if elec_source is None:
        db.execute("UPDATE branches SET electricity_source = 'manual' WHERE id = ?", (branch_id,))

    db.commit()
    return jsonify({'ok': True})


@app.route('/api/electricity/manual/<int:entry_id>', methods=['PUT'])
@login_required
def api_electricity_manual_update(entry_id):
    """Edit an existing manual electricity entry."""
    branch_id = get_branch_id()
    data = request.get_json(force=True)
    amount = data.get('amount')
    if amount is None:
        return jsonify({'error': 'amount required'}), 400
    try:
        amount = round(float(amount), 2)
    except (ValueError, TypeError):
        return jsonify({'error': 'invalid amount'}), 400

    db = get_db()
    row = db.execute(
        "SELECT id, source FROM electricity_invoices WHERE id = ? AND branch_id = ?",
        (entry_id, branch_id)
    ).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    if row['source'] != 'manual':
        return jsonify({'error': 'Cannot edit IEC entries via manual endpoint'}), 403

    db.execute("UPDATE electricity_invoices SET amount = ? WHERE id = ?", (amount, entry_id))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/electricity/source', methods=['POST'])
@login_required
def api_electricity_source_switch():
    """Switch electricity source between 'iec' and 'manual'. Future-only."""
    branch_id = get_branch_id()
    data = request.get_json(force=True)
    new_source = data.get('source', '').strip()
    if new_source not in ('iec', 'manual'):
        return jsonify({'error': "source must be 'iec' or 'manual'"}), 400

    db = get_db()
    db.execute("UPDATE branches SET electricity_source = ? WHERE id = ?", (new_source, branch_id))
    db.commit()
    return jsonify({'ok': True, 'source': new_source})


@app.route('/api/electricity/status')
@login_required
def api_electricity_status():
    """Return electricity configuration status for the branch."""
    branch_id = get_branch_id()
    db = get_db()
    branch = db.execute(
        "SELECT electricity_source, iec_token, iec_last_sync_at FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    elec_source = branch['electricity_source'] if branch else None

    try:
        latest = db.execute(
            "SELECT month FROM electricity_invoices WHERE branch_id = ? ORDER BY month DESC, due_date DESC LIMIT 1",
            (branch_id,)
        ).fetchone()
    except Exception:
        latest = None

    # Find months with manual entries for current year
    current_year = _now_il().year
    try:
        manual_months = [r['month'] for r in db.execute(
            "SELECT DISTINCT month FROM electricity_invoices WHERE branch_id = ? AND source = 'manual' AND month LIKE ?",
            (branch_id, f'{current_year}-%')
        ).fetchall()]
    except Exception:
        manual_months = []

    return jsonify({
        'branch_id': branch_id,
        'source': elec_source,
        'has_iec_token': bool(branch and branch['iec_token']),
        'latest_month_with_data': latest['month'] if latest and latest['month'] else None,
        'manual_months_this_year': manual_months,
    })


@app.route('/api/electricity/history')
@login_required
def api_electricity_history():
    """Return all electricity entries for the branch, for history display."""
    branch_id = get_branch_id()
    db = get_db()
    rows = db.execute(
        "SELECT id, invoice_number, period_label, amount, due_date, is_paid, source, month, created_at "
        "FROM electricity_invoices WHERE branch_id = ? ORDER BY COALESCE(month, due_date) DESC",
        (branch_id,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ── IEC status & accuracy endpoints ─────────────────────────────────────

@app.route('/api/iec-status')
@_admin_required
def api_iec_status():
    branch_id = request.args.get('branch_id', type=int)
    if not branch_id:
        return jsonify({'error': 'missing branch_id'}), 400
    db = get_db()
    row = db.execute(
        "SELECT iec_token, iec_last_sync_at FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    if not row or not row['iec_token']:
        return jsonify({'last_sync_at': None, 'last_sync_status': 'never', 'invoice_count': 0})
    inv_count = db.execute(
        "SELECT COUNT(*) as cnt FROM electricity_invoices WHERE branch_id = ?", (branch_id,)
    ).fetchone()['cnt']
    # Determine status from last agent run
    last_run = db.execute(
        "SELECT status FROM agent_runs WHERE branch_id=? AND agent='iec' ORDER BY started_at DESC LIMIT 1",
        (branch_id,)
    ).fetchone()
    if last_run:
        status = 'ok' if last_run['status'] == 'success' else 'failed'
    elif row['iec_last_sync_at']:
        status = 'ok'
    else:
        status = 'never'
    return jsonify({
        'last_sync_at': row['iec_last_sync_at'],
        'last_sync_status': status,
        'invoice_count': inv_count,
    })


def _get_iec_accuracy_data(branch_id: int, db=None) -> list:
    """Return 12 months of accuracy data starting from current month."""
    if db is None:
        db = get_db()
    # Check if branch has IEC
    has_iec = db.execute(
        "SELECT iec_token, name FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    if not has_iec or not has_iec['iec_token']:
        return []

    branch_name = has_iec['name']
    now = datetime.now(IL_TZ)
    rows = []

    for offset in range(12):
        m = now.month + offset
        y = now.year
        while m > 12:
            m -= 12
            y += 1

        # Get estimate
        elec = get_electricity_for_month(branch_id, y, m, db)

        # Determine real value — check if every day in month is covered by invoices
        month_start = date(y, m, 1)
        month_end = date(y, m, calendar.monthrange(y, m)[1])
        total_days = (month_end - month_start).days + 1

        # Get all invoices that intersect this month
        inv_rows = db.execute(
            "SELECT amount, raw_json FROM electricity_invoices WHERE branch_id = ?",
            (branch_id,)
        ).fetchall()

        covered_days = set()
        real_amount = 0.0
        for inv in inv_rows:
            try:
                rj = json.loads(inv['raw_json'])
            except (json.JSONDecodeError, TypeError):
                continue
            from_d_str = rj.get('from_date', '')
            to_d_str = rj.get('to_date', '')
            if not from_d_str or not to_d_str:
                continue
            from_d = date.fromisoformat(from_d_str[:10])
            to_d = date.fromisoformat(to_d_str[:10])
            span = (to_d - from_d).days
            if span <= 0 or span > 90:
                continue
            if to_d < month_start or from_d > month_end:
                continue
            # Mark covered days
            overlap_start = max(from_d, month_start)
            overlap_end = min(to_d, month_end)
            for d_offset in range((overlap_end - overlap_start).days + 1):
                covered_days.add(overlap_start + timedelta(days=d_offset))
            real_amount += _prorate_invoice(from_d_str, to_d_str, inv['amount'], y, m)

        full_coverage = len(covered_days) >= total_days
        real_val = round(real_amount, 2) if full_coverage and real_amount > 0 else None

        estimate_val = elec['amount'] if elec['source'] in ('estimate', 'real') else None
        estimate_basis = elec.get('estimate_basis')

        # If real data exists and source is 'real', the estimate is the real value itself
        # For accuracy purposes we want the estimate that was used BEFORE real arrived
        if elec['source'] == 'real' and full_coverage:
            # The "real" is the actual amount; estimate would have been from prior year
            estimate_val = elec['amount']  # In this case estimate = real (it was the real data)

        delta = None
        accuracy_pct = None
        if real_val is not None and estimate_val is not None:
            delta = round(real_val - estimate_val, 2)
            if real_val > 0:
                accuracy_pct = round(100 - abs(delta) / real_val * 100, 1)

        if estimate_val is None and real_val is None:
            status = 'no_estimate'
        elif real_val is not None:
            status = 'final'
        else:
            status = 'pending'

        rows.append({
            'branch_id': branch_id,
            'branch_name': branch_name,
            'year': y,
            'month': m,
            'month_label': f"{HEBREW_MONTHS[m]} {y}",
            'estimate': estimate_val,
            'estimate_basis': estimate_basis,
            'real': real_val,
            'delta': delta,
            'accuracy_pct': accuracy_pct,
            'status': status,
        })

    return rows


@app.route('/api/iec-accuracy')
@_admin_required
def api_iec_accuracy():
    branch_id = request.args.get('branch_id', type=int)
    db = get_db()
    if branch_id:
        return jsonify(_get_iec_accuracy_data(branch_id, db))
    # All branches with IEC
    branches = db.execute(
        "SELECT id FROM branches WHERE active = 1 AND iec_token IS NOT NULL"
    ).fetchall()
    result = []
    for b in branches:
        result.extend(_get_iec_accuracy_data(b['id'], db))
    return jsonify(result)


# ── Internal sync endpoints (VPS → Hetzner, secret-protected) ──────────

def _check_iec_sync_secret():
    expected = os.getenv('IEC_SYNC_SECRET')
    if not expected:
        abort(503, 'IEC sync not configured')
    provided = request.headers.get('X-Sync-Secret', '')
    if not hmac.compare_digest(expected, provided):
        abort(401, 'Invalid sync secret')


@app.route('/api/internal/iec-branches')
def api_internal_iec_branches():
    _check_iec_sync_secret()
    db = get_db()
    rows = db.execute('''
        SELECT id AS branch_id, iec_user_id, iec_token, iec_bp_number, iec_contract_id
        FROM branches
        WHERE active = 1 AND iec_token IS NOT NULL
    ''').fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/internal/iec-sync', methods=['POST'])
def api_internal_iec_sync():
    _check_iec_sync_secret()
    data = request.get_json()
    if not data:
        return jsonify({'error': 'no JSON body'}), 400

    branch_id = data.get('branch_id')
    db = get_db()
    branch = db.execute('SELECT id FROM branches WHERE id = ?', (branch_id,)).fetchone()
    if not branch:
        return jsonify({'error': f'branch {branch_id} not found'}), 404

    invoices = data.get('invoices', [])
    upserted = 0
    for inv in invoices:
        db.execute('''
            INSERT INTO electricity_invoices
                (branch_id, invoice_number, period_label, amount, due_date, is_paid, source, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, 'iec_api', ?)
            ON CONFLICT (branch_id, invoice_number) DO UPDATE SET
                amount = excluded.amount,
                due_date = excluded.due_date,
                is_paid = excluded.is_paid,
                period_label = excluded.period_label,
                raw_json = excluded.raw_json
        ''', (branch_id, inv.get('invoice_number'), inv.get('period_label'),
              inv.get('amount', 0), inv.get('due_date'), 1 if inv.get('is_paid') else 0,
              json.dumps(inv.get('raw_json'), default=str, ensure_ascii=False) if inv.get('raw_json') else None))
        upserted += 1

    rotated_token = data.get('rotated_token')
    if rotated_token:
        db.execute('UPDATE branches SET iec_token = ? WHERE id = ?', (rotated_token, branch_id))

    synced_at = data.get('synced_at', datetime.utcnow().isoformat())
    db.execute('UPDATE branches SET iec_last_sync_at = ? WHERE id = ?', (synced_at, branch_id))
    db.commit()

    return jsonify({'ok': True, 'upserted': upserted, 'rotated_token': bool(rotated_token)})


@app.route('/api/internal/iec-sync-error', methods=['POST'])
def api_internal_iec_sync_error():
    _check_iec_sync_secret()
    data = request.get_json()
    if not data:
        return jsonify({'error': 'no JSON body'}), 400

    branch_id = data.get('branch_id')
    error_msg = data.get('error', 'unknown error')
    occurred_at = data.get('occurred_at', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

    db = get_db()
    db.execute('''
        INSERT INTO agent_runs (branch_id, agent, started_at, finished_at, status, message)
        VALUES (?, 'iec_sync', ?, ?, 'error', ?)
    ''', (branch_id, occurred_at, occurred_at, error_msg))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/internal/iec-onboard', methods=['POST'])
def api_internal_iec_onboard():
    _check_iec_sync_secret()
    data = request.get_json()
    if not data:
        return jsonify({'error': 'no JSON body'}), 400

    branch_id = data.get('branch_id')
    db = get_db()
    branch = db.execute('SELECT id FROM branches WHERE id = ?', (branch_id,)).fetchone()
    if not branch:
        return jsonify({'error': f'branch {branch_id} not found'}), 404

    db.execute('''
        UPDATE branches SET iec_user_id = ?, iec_token = ?, iec_bp_number = ?, iec_contract_id = ?
        WHERE id = ?
    ''', (data.get('iec_user_id'), data.get('iec_token'),
          data.get('iec_bp_number'), data.get('iec_contract_id'), branch_id))
    db.commit()
    return jsonify({'ok': True})


# ── IEC Onboarding Wizard ────────────────────────────────────
# Uses SSH to Israeli VPS running iec_wizard.py (JSON-over-stdin/stdout)
# Each wizard session holds an SSH subprocess with the IecClient alive in memory.

_iec_wizard_sessions = {}  # {token: {proc, created_at, branch_id}}
_iec_wizard_lock = threading.Lock()


def _cleanup_wizard_sessions():
    """Kill expired wizard sessions (>12 min old)."""
    now = time.time()
    with _iec_wizard_lock:
        expired = [k for k, v in _iec_wizard_sessions.items() if now - v['created_at'] > 720]
        for k in expired:
            try:
                _iec_wizard_sessions[k]['proc'].kill()
            except Exception:
                pass
            del _iec_wizard_sessions[k]


def _wizard_send_recv(proc, cmd, timeout=60):
    """Send JSON command to wizard subprocess and read JSON response."""
    try:
        proc.stdin.write(json.dumps(cmd) + '\n')
        proc.stdin.flush()
    except (BrokenPipeError, OSError):
        return {"ok": False, "error": "התהליך הסתיים. נסה שוב."}
    ready, _, _ = select.select([proc.stdout], [], [], timeout)
    if not ready:
        return {"ok": False, "error": "תם הזמן. נסה שוב."}
    line = proc.stdout.readline()
    if not line:
        return {"ok": False, "error": "התהליך הסתיים. נסה שוב."}
    try:
        return json.loads(line.strip())
    except json.JSONDecodeError:
        return {"ok": False, "error": "תגובה לא תקינה מהשרת"}


def _check_branch_permission(branch_id):
    """Check if current user has permission for this branch."""
    if session.get('user_role') in ROLES_ALL_BRANCHES:
        return True
    return session.get('branch_id') == branch_id


@app.route('/api/iec/onboard/start', methods=['POST'])
@login_required
def iec_onboard_start():
    _cleanup_wizard_sessions()
    data = request.get_json() or {}
    branch_id = data.get('branch_id')
    id_number = (data.get('id_number') or '').strip()

    if not branch_id or not id_number:
        return jsonify({"ok": False, "error": "חסרים פרטים"}), 400
    branch_id = int(branch_id)

    if not _check_branch_permission(branch_id):
        return jsonify({"ok": False, "error": "אין הרשאה לסניף זה"}), 403

    # Validate ID format (1-9 digits)
    if not id_number.isdigit() or len(id_number) < 1 or len(id_number) > 9:
        return jsonify({"ok": False, "error": "תעודת זהות לא תקינה"}), 400

    # Start SSH wizard process on VPS
    try:
        proc = subprocess.Popen(
            ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
             'makolet-iec',
             '/opt/makolet-iec/venv/bin/python /opt/makolet-iec/iec_wizard.py'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1
        )
    except Exception:
        return jsonify({"ok": False, "error": "שגיאה בחיבור לשרת"}), 500

    # Send start command — id_number is NOT logged anywhere
    result = _wizard_send_recv(proc, {"action": "start", "id_number": id_number}, timeout=60)

    if not result.get("ok"):
        try:
            proc.kill()
        except Exception:
            pass
        error_msg = result.get("error", "שגיאה לא ידועה")
        # Sanitize — never expose the ID number in error responses
        if id_number in error_msg:
            error_msg = "חשבון חשמל לא נמצא עבור תעודת הזהות שהוזנה"
        return jsonify({"ok": False, "error": error_msg}), 400

    session_token = secrets.token_urlsafe(32)
    with _iec_wizard_lock:
        _iec_wizard_sessions[session_token] = {
            'proc': proc,
            'created_at': time.time(),
            'branch_id': branch_id,
        }

    return jsonify({
        "ok": True,
        "session_token": session_token,
        "factor": result.get("factor", "SMS"),
        "expires_at": time.time() + 600,  # 10 min for OTP
    })


@app.route('/api/iec/onboard/verify', methods=['POST'])
@login_required
def iec_onboard_verify():
    data = request.get_json() or {}
    session_token = data.get('session_token', '')
    otp = (data.get('otp') or '').strip()
    branch_id = data.get('branch_id')

    if not session_token or not otp or not branch_id:
        return jsonify({"ok": False, "error": "חסרים פרטים"}), 400
    branch_id = int(branch_id)

    if not _check_branch_permission(branch_id):
        return jsonify({"ok": False, "error": "אין הרשאה לסניף זה"}), 403

    with _iec_wizard_lock:
        sess = _iec_wizard_sessions.get(session_token)

    if not sess:
        return jsonify({"ok": False, "error": "פג תוקף ההגדרה. נסה שוב."}), 400

    if time.time() - sess['created_at'] > 720:
        with _iec_wizard_lock:
            try:
                sess['proc'].kill()
            except Exception:
                pass
            _iec_wizard_sessions.pop(session_token, None)
        return jsonify({"ok": False, "error": "פג תוקף ההגדרה. נסה שוב."}), 400

    if sess['branch_id'] != branch_id:
        return jsonify({"ok": False, "error": "session mismatch"}), 400

    # OTP is NOT logged anywhere
    result = _wizard_send_recv(sess['proc'], {"action": "verify", "otp": otp}, timeout=60)

    if not result.get("ok"):
        error_msg = result.get("error", "אימות נכשל")
        if otp in error_msg:
            error_msg = "קוד אימות שגוי"
        return jsonify({"ok": False, "error": error_msg}), 400

    return jsonify({
        "ok": True,
        "contracts": result.get("contracts", []),
    })


@app.route('/api/iec/onboard/save', methods=['POST'])
@login_required
def iec_onboard_save():
    data = request.get_json() or {}
    session_token = data.get('session_token', '')
    branch_id = data.get('branch_id')
    contract_id = (data.get('contract_id') or '').strip()

    if not session_token or not contract_id or not branch_id:
        return jsonify({"ok": False, "error": "חסרים פרטים"}), 400
    branch_id = int(branch_id)

    if not _check_branch_permission(branch_id):
        return jsonify({"ok": False, "error": "אין הרשאה לסניף זה"}), 403

    with _iec_wizard_lock:
        sess = _iec_wizard_sessions.pop(session_token, None)

    if not sess:
        return jsonify({"ok": False, "error": "פג תוקף ההגדרה. נסה שוב."}), 400

    if sess['branch_id'] != branch_id:
        return jsonify({"ok": False, "error": "session mismatch"}), 400

    result = _wizard_send_recv(sess['proc'], {"action": "save", "contract_id": contract_id}, timeout=30)

    # Always clean up the subprocess
    try:
        sess['proc'].kill()
    except Exception:
        pass

    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error", "שגיאה בשמירה")}), 400

    # Save to branches table
    try:
        db = get_db()
        db.execute('''
            UPDATE branches SET iec_user_id = ?, iec_token = ?, iec_bp_number = ?, iec_contract_id = ?,
                   electricity_source = 'iec'
            WHERE id = ?
        ''', (result.get('iec_user_id'), result.get('iec_token'),
              result.get('iec_bp_number'), contract_id, branch_id))
        db.commit()
    except Exception:
        return jsonify({"ok": False, "error": "שגיאה בשמירת הנתונים"}), 500

    return jsonify({"ok": True})


@app.route('/api/iec/sync', methods=['POST'])
@login_required
def api_iec_sync():
    """Trigger IEC invoice sync for a branch. Managers can sync their own branch."""
    data = request.get_json() or {}
    branch_id = data.get('branch_id')
    if not branch_id:
        return jsonify({"ok": False, "error": "missing branch_id"}), 400
    branch_id = int(branch_id)

    if not _check_branch_permission(branch_id):
        return jsonify({"ok": False, "error": "אין הרשאה"}), 403

    try:
        ssh_cmd = [
            'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            'makolet-iec',
            f'/opt/makolet-iec/venv/bin/python /opt/makolet-iec/iec_sync.py --branch-id {branch_id}'
        ]
        proc = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=120)
        if proc.returncode == 0:
            return jsonify({"ok": True})
        else:
            msg = proc.stderr.strip() or proc.stdout.strip() or 'sync failed'
            return jsonify({"ok": False, "error": msg}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── /admin/analytics ────────────────────────────────────────────────────

# Sessions are blocks of contiguous events <=30min apart. A 'login' event
# always starts a fresh session even if the gap is smaller.
_SESSION_GAP_SECONDS = 30 * 60

# Bump when the cached payload structure changes — old entries are dropped.
_ANALYTICS_CACHE_VERSION = 3

# Line colors assigned to managers in user_id order (stable across renders).
USER_LINE_COLORS = ['#378ADD', '#1D9E75', '#D85A30', '#B5739D', '#E0B341', '#7D5BA6']

PAGE_LABELS = {
    '/': 'בית',
    '/sales': 'הכנסות',
    '/goods': 'סחורה',
    '/employees': 'עובדים',
    '/fixed-expenses': 'הוצאות קבועות',
    '/electricity-history': 'חשמל',
    '/ops': 'בקרה',
    '/admin/branches': 'ניהול סניפים',
    '/admin/users': 'ניהול משתמשים',
    '/admin/analytics': 'ניתוח שימוש',
}


def format_duration_he(seconds):
    """Format a duration in seconds as Hebrew text.
    0 → '—', <60 → 'פחות מדקה', <3600 → 'N דקות',
    >=3600 → 'H שעות [M דקות]'."""
    if not seconds or seconds <= 0:
        return '—'
    if seconds < 60:
        return 'פחות מדקה'
    if seconds < 3600:
        return f'{seconds // 60} דקות'
    hours = seconds // 3600
    rem_min = (seconds % 3600) // 60
    if rem_min == 0:
        return f'{hours} שעות'
    return f'{hours} שעות {rem_min} דקות'


def _classify_device(ua):
    """mobile vs desktop from user_agent (keyword match)."""
    if not ua:
        return 'desktop'
    ua_l = ua.lower()
    if 'iphone' in ua_l or 'ipad' in ua_l or 'android' in ua_l:
        return 'mobile'
    return 'desktop'


def _parse_event_ts(s):
    """user_events.created_at is 'YYYY-MM-DD HH:MM:SS' in UTC."""
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)


def _compute_sessions(events):
    """events: iterable of dicts with at least {created_at, event_type, user_id}.
    Returns a list of sessions. Each session is a list of events. Sorted by user
    then time; a new session starts on a 'login' event OR a >30min gap from the
    previous event by the SAME user."""
    by_user = {}
    for e in events:
        by_user.setdefault(e['user_id'], []).append(e)
    sessions = []
    for uid, ulist in by_user.items():
        ulist.sort(key=lambda r: r['created_at'])
        current = []
        prev_ts = None
        for e in ulist:
            ts = _parse_event_ts(e['created_at'])
            is_login = e['event_type'] == 'login'
            gap_too_big = prev_ts is not None and (ts - prev_ts).total_seconds() > _SESSION_GAP_SECONDS
            if not current or is_login or gap_too_big:
                if current:
                    sessions.append(current)
                current = [e]
            else:
                current.append(e)
            prev_ts = ts
        if current:
            sessions.append(current)
    return sessions


def _active_seconds_from_sessions(sessions):
    """Sum of (last_event_ts - first_event_ts) per session, in seconds.
    A single-event session contributes 0."""
    total = 0
    for s in sessions:
        if len(s) < 2:
            continue
        first = _parse_event_ts(s[0]['created_at'])
        last = _parse_event_ts(s[-1]['created_at'])
        total += int((last - first).total_seconds())
    return total


def _range_bounds(range_key, db):
    """Return (start_dt_utc, end_dt_utc, label_days_in_window). end is now (UTC).
    For 'all', start = first event's created_at (or epoch fallback)."""
    now = datetime.now(timezone.utc)
    if range_key == '7d':
        start = now - timedelta(days=7)
        days = 7
    elif range_key == '30d':
        start = now - timedelta(days=30)
        days = 30
    elif range_key == 'month':
        il_now = now.astimezone(IL_TZ)
        start_il = il_now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start = start_il.astimezone(timezone.utc)
        days = il_now.day  # days elapsed in current month
    else:  # 'all'
        row = db.execute("SELECT MIN(created_at) AS m FROM user_events").fetchone()
        if row and row['m']:
            start = _parse_event_ts(row['m'])
        else:
            start = now - timedelta(days=1)
        delta = now - start
        days = max(1, int(delta.total_seconds() / 86400) + 1)
    return start, now, days


def _fetch_events_range(db, start_utc, end_utc, user_id=None):
    sql = ("SELECT id, user_id, event_type, page, branch_id, "
           "duration_seconds, user_agent, created_at "
           "FROM user_events WHERE created_at >= ? AND created_at <= ?")
    params = [start_utc.strftime('%Y-%m-%d %H:%M:%S'),
              end_utc.strftime('%Y-%m-%d %H:%M:%S')]
    if user_id:
        sql += " AND user_id = ?"
        params.append(user_id)
    sql += " ORDER BY created_at"
    return [dict(r) for r in db.execute(sql, params).fetchall()]


def _daily_per_user(events, start_utc, end_utc, db):
    """Daily per-user event counts for the chart. Zero-fills missing days
    so each user's data array has one entry per calendar day in the range
    (Israel time), ordered ascending."""
    # Build the inclusive list of IL date strings in the window.
    start_il = start_utc.astimezone(IL_TZ).date()
    end_il = end_utc.astimezone(IL_TZ).date()
    days = []
    d = start_il
    while d <= end_il:
        days.append(d)
        d += timedelta(days=1)
    day_index = {d.isoformat(): i for i, d in enumerate(days)}
    labels = [d.strftime('%d/%m') for d in days]

    # Bucket events by (user_id, IL-date).
    per_user = {}
    for e in events:
        ts_il = _parse_event_ts(e['created_at']).astimezone(IL_TZ).date()
        idx = day_index.get(ts_il.isoformat())
        if idx is None:
            continue
        u = per_user.setdefault(e['user_id'], [0] * len(days))
        u[idx] += 1

    if not per_user:
        return {'labels': labels, 'users': []}

    # Resolve user names; assign color by sorted user_id.
    ids = sorted(per_user.keys())
    placeholders = ','.join(['?'] * len(ids))
    rows = db.execute(
        f"SELECT id, name FROM users WHERE id IN ({placeholders})", ids
    ).fetchall()
    name_by_id = {r['id']: (r['name'] or f'#{r["id"]}') for r in rows}

    users_out = []
    for i, uid in enumerate(ids):
        users_out.append({
            'user_id': uid,
            'name': name_by_id.get(uid, f'#{uid}'),
            'color': USER_LINE_COLORS[i % len(USER_LINE_COLORS)],
            'data': per_user[uid],
        })
    return {'labels': labels, 'users': users_out}


def _analytics_aggregate(range_key, user_id=None):
    """Compute aggregates for the requested window. Returns a dict that the
    template renders directly. NEVER call this when the cache should be hit
    — the route handles cache lookup before calling."""
    db = get_db()
    start_utc, end_utc, days_in_window = _range_bounds(range_key, db)
    events = _fetch_events_range(db, start_utc, end_utc, user_id=user_id)

    # Empty state.
    if not events:
        return {
            '_v': _ANALYTICS_CACHE_VERSION,
            'empty': True,
            'range': range_key,
            'user_id': user_id,
            'days_in_window': days_in_window,
        }

    # Tile 1 — logins + delta vs previous comparable window.
    login_count = sum(1 for e in events if e['event_type'] == 'login')
    prev_login_count = None
    if range_key != 'all' and not user_id:
        if range_key == 'month':
            il_start = start_utc.astimezone(IL_TZ)
            prev_month_last_day = il_start - timedelta(days=1)
            prev_start_il = prev_month_last_day.replace(day=1, hour=0, minute=0,
                                                       second=0, microsecond=0)
            prev_start = prev_start_il.astimezone(timezone.utc)
            prev_end = start_utc
        else:
            window = end_utc - start_utc
            prev_end = start_utc
            prev_start = start_utc - window
        prow = db.execute(
            "SELECT COUNT(*) AS c FROM user_events "
            "WHERE event_type='login' AND created_at >= ? AND created_at < ?",
            (prev_start.strftime('%Y-%m-%d %H:%M:%S'),
             prev_end.strftime('%Y-%m-%d %H:%M:%S'))
        ).fetchone()
        prev_login_count = prow['c']

    # Tile 2 — sessions.
    sessions = _compute_sessions(events)
    session_count = len(sessions)
    sessions_per_day = round(session_count / days_in_window, 1)

    # Tile 3 — active time.
    active_seconds = _active_seconds_from_sessions(sessions)
    active_minutes_per_day = round((active_seconds / 60) / days_in_window)

    # Tile 4 — days active.
    distinct_days = len({e['created_at'][:10] for e in events})

    # Daily per-user line chart payload.
    daily_per_user = _daily_per_user(events, start_utc, end_utc, db)

    # Top pages.
    page_counts = {}
    for e in events:
        if e['event_type'] != 'page_view' or not e['page']:
            continue
        page_counts[e['page']] = page_counts.get(e['page'], 0) + 1
    top_pages = sorted(page_counts.items(), key=lambda kv: -kv[1])[:5]
    total_pv = sum(page_counts.values()) or 1
    top_pages_out = [
        {'page': p, 'label': PAGE_LABELS.get(p, p),
         'count': c, 'pct': round(c * 100 / total_pv)}
        for p, c in top_pages
    ]

    # Device split.
    mobile = sum(1 for e in events if _classify_device(e['user_agent']) == 'mobile')
    desktop = len(events) - mobile
    total_dev = mobile + desktop or 1
    device = {
        'mobile_pct': round(mobile * 100 / total_dev),
        'desktop_pct': round(desktop * 100 / total_dev),
    }

    # Per-user table.
    user_rows = {}
    for e in events:
        u = user_rows.setdefault(e['user_id'], {
            'user_id': e['user_id'],
            'logins': 0,
            'events': [],
        })
        u['events'].append(e)
        if e['event_type'] == 'login':
            u['logins'] += 1
    # User name + branch.
    user_meta = {}
    if user_rows:
        ids = tuple(user_rows.keys())
        placeholders = ','.join(['?'] * len(ids))
        rows = db.execute(
            f"SELECT u.id, u.name, "
            f"(SELECT b.name FROM user_branches ub JOIN branches b ON b.id=ub.branch_id "
            f"  WHERE ub.user_id=u.id ORDER BY b.id LIMIT 1) AS branch_name "
            f"FROM users u WHERE u.id IN ({placeholders})",
            ids
        ).fetchall()
        for r in rows:
            user_meta[r['id']] = {'name': r['name'] or '—',
                                  'branch_name': r['branch_name'] or '—'}
    users_table = []
    for uid, u in user_rows.items():
        sess_for_user = _compute_sessions(u['events'])
        active_s = _active_seconds_from_sessions(sess_for_user)
        last_event = u['events'][-1]['created_at']
        meta = user_meta.get(uid, {'name': '—', 'branch_name': '—'})
        users_table.append({
            'user_id': uid,
            'name': meta['name'],
            'initial': (meta['name'][:1] if meta['name'] else '?'),
            'branch_name': meta['branch_name'],
            'logins': u['logins'],
            'active_time': format_duration_he(active_s),
            'last_active_utc': last_event,
        })
    users_table.sort(key=lambda r: -r['logins'])

    # Active-time tile subtitle: "Y דק' ליום בממוצע" unless avg >= 60 min,
    # in which case use the Hebrew duration helper.
    avg_active_seconds_per_day = int(active_seconds / days_in_window) if days_in_window else 0
    if avg_active_seconds_per_day >= 3600:
        active_per_day_label = format_duration_he(avg_active_seconds_per_day) + ' ליום בממוצע'
    else:
        active_per_day_label = f"{active_minutes_per_day} דק' ליום בממוצע"

    return {
        '_v': _ANALYTICS_CACHE_VERSION,
        'empty': False,
        'range': range_key,
        'user_id': user_id,
        'days_in_window': days_in_window,
        'login_count': login_count,
        'prev_login_count': prev_login_count,
        'session_count': session_count,
        'sessions_per_day': sessions_per_day,
        'active_seconds': active_seconds,
        'active_time': format_duration_he(active_seconds),
        'active_minutes_per_day': active_minutes_per_day,
        'active_per_day_label': active_per_day_label,
        'distinct_days': distinct_days,
        'daily_per_user': daily_per_user,
        'top_pages': top_pages_out,
        'device': device,
        'users_table': users_table,
        'computed_at_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    }


def _analytics_cache_get(range_key):
    """Return cached payload dict or None. Always silent on errors."""
    try:
        db = get_db()
        row = db.execute(
            "SELECT payload, computed_at FROM analytics_cache WHERE range = ?",
            (range_key,)
        ).fetchone()
        if not row:
            return None
        payload = json.loads(row['payload'])
        if payload.get('_v') != _ANALYTICS_CACHE_VERSION:
            return None
        payload['computed_at_utc'] = row['computed_at']
        return payload
    except Exception:
        return None


def _analytics_cache_set(range_key, payload):
    try:
        db = get_db()
        db.execute(
            "INSERT INTO analytics_cache (range, payload, computed_at) "
            "VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(range) DO UPDATE SET "
            "  payload = excluded.payload, computed_at = excluded.computed_at",
            (range_key, json.dumps(payload, ensure_ascii=False))
        )
        db.commit()
    except Exception:
        pass


_VALID_ANALYTICS_RANGES = ('7d', '30d', 'month', 'all')


@app.route('/admin/analytics')
@_admin_required
def admin_analytics():
    range_key = request.args.get('range', 'all')
    if range_key not in _VALID_ANALYTICS_RANGES:
        range_key = 'all'
    user_id = request.args.get('user_id', type=int)

    # Cache only for unfiltered queries.
    payload = None
    if user_id is None:
        payload = _analytics_cache_get(range_key)
    if payload is None:
        payload = _analytics_aggregate(range_key, user_id=user_id)
        if user_id is None:
            _analytics_cache_set(range_key, payload)

    # Selected-user name for filter chip.
    selected_user_name = None
    if user_id:
        db = get_db()
        urow = db.execute("SELECT name FROM users WHERE id = ?", (user_id,)).fetchone()
        if urow:
            selected_user_name = urow['name']

    return render_template(
        'admin_analytics.html',
        analytics=payload,
        range_key=range_key,
        selected_user_id=user_id,
        selected_user_name=selected_user_name,
        **_page_context('admin')
    )


@app.route('/api/admin/analytics/recent-activity')
@_admin_required
def api_admin_analytics_recent_activity():
    """Lightweight endpoint for the user table's 60s auto-refresh.
    Returns just the users_table portion for the requested range/user filter."""
    range_key = request.args.get('range', 'all')
    if range_key not in _VALID_ANALYTICS_RANGES:
        range_key = 'all'
    user_id = request.args.get('user_id', type=int)
    payload = _analytics_aggregate(range_key, user_id=user_id)
    return jsonify({'users_table': payload.get('users_table', []),
                    'empty': payload.get('empty', False)})


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


# Initialize DB and seed admin on import (Gunicorn loads app:app)
init_db()
seed_admin()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
