import io
import os
import secrets
import sqlite3
import subprocess
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

DB_PATH = os.path.join(os.path.dirname(__file__), 'db', 'makolet_chain.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'db', 'schema.sql')
IL_TZ = ZoneInfo('Asia/Jerusalem')

HEBREW_MONTHS = {
    1: 'ינואר', 2: 'פברואר', 3: 'מרץ', 4: 'אפריל',
    5: 'מאי', 6: 'יוני', 7: 'יולי', 8: 'אוגוסט',
    9: 'ספטמבר', 10: 'אוקטובר', 11: 'נובמבר', 12: 'דצמבר'
}


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
    existing = conn.execute("SELECT id FROM users WHERE email = ?", ('admin@makolet.com',)).fetchone()
    if not existing:
        admin_password = os.environ.get('ADMIN_PASSWORD', secrets.token_urlsafe(16))
        pw_hash = generate_password_hash(admin_password)
        conn.execute(
            "INSERT INTO users (name, email, password_hash, role) VALUES (?, ?, ?, ?)",
            ('מנהל ראשי', 'admin@makolet.com', pw_hash, 'admin')
        )
        conn.commit()
        # Get user id
        user_row = conn.execute("SELECT id FROM users WHERE email = ?", ('admin@makolet.com',)).fetchone()
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

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if 'user_id' in session:
            return redirect('/')
        message = request.args.get('message', '')
        return render_template('login.html', error=None, message=message)

    email = request.form.get('email', '').strip()
    password = request.form.get('password', '')

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ? AND active = 1", (email,)).fetchone()

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
    user = db.execute('SELECT id FROM users WHERE email=? AND active=1', (email,)).fetchone()
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


def _parse_month():
    month = request.args.get('month')
    if month:
        session['selected_month'] = month
    else:
        month = session.get('selected_month')
    if not month:
        month = _now_il().strftime('%Y-%m')
    return month


def _month_nav(selected):
    year, mon = map(int, selected.split('-'))
    pm = mon - 1 if mon > 1 else 12
    py = year if mon > 1 else year - 1
    prev_month = f'{py:04d}-{pm:02d}'
    current = _now_il().strftime('%Y-%m')
    nm = mon + 1 if mon < 12 else 1
    ny = year if mon < 12 else year + 1
    next_str = f'{ny:04d}-{nm:02d}'
    next_month = next_str if next_str <= current else None
    display = f'{HEBREW_MONTHS[mon]} {year}'
    show_today = selected != current
    return prev_month, next_month, display, show_today, current


def get_branch_id():
    """Get branch_id from session only — never from request args/form."""
    role = session.get('user_role')
    if role == 'admin':
        return session.get('branch_id', 126)
    elif role == 'manager':
        return session.get('branch_id')
    return None


def _get_branch_id():
    """Get branch_id for page routes — allows URL param switching with access validation."""
    bid = request.args.get('branch_id')
    if bid:
        bid = int(bid)
        role = session.get('user_role')
        branches = session.get('user_branches', [])
        if role == 'admin' or bid in branches:
            session['branch_id'] = bid
    return get_branch_id()


def _branch_name(branch_id):
    db = get_db()
    row = db.execute('SELECT name FROM branches WHERE id = ?', (branch_id,)).fetchone()
    return row['name'] if row else 'סניף לא ידוע'


def _page_context(active_page):
    selected = _parse_month()
    branch_id = _get_branch_id()
    prev_month, next_month, month_display, show_today, current = _month_nav(selected)
    return {
        'active_page': active_page,
        'selected_month': selected,
        'branch_id': branch_id,
        'branch_name': _branch_name(branch_id),
        'prev_month': prev_month,
        'next_month': next_month,
        'month_display': month_display,
        'show_today_btn': show_today,
        'current_month': current,
    }


# ── Page Routes ──────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    ctx = _page_context('home')
    return render_template('index.html', **ctx)


@app.route('/sales')
@login_required
def sales():
    ctx = _page_context('sales')
    return render_template('sales.html', **ctx)


@app.route('/goods')
@login_required
def goods():
    ctx = _page_context('goods')
    branch_id = ctx['branch_id']
    month = ctx['selected_month']
    db = get_db()
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

    # Add before_vat to each doc
    for d in docs:
        d['amount_before_vat'] = round(d['amount'] / 1.17, 2)

    ctx.update({
        'docs': docs,
        'total': total,
        'total_before_vat': total_before_vat,
        'invoices_total': invoices_total,
        'delivery_total': delivery_total,
        'returns_total': returns_total,
        'count': count,
    })
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
    is_current = current_month == _now_il().strftime('%Y-%m')

    # Current month: only API rows. Past months: all rows.
    if is_current:
        rows = db.execute('''
            SELECT eh.employee_name, eh.total_hours, eh.total_salary, eh.source,
                   e.hourly_rate, e.id as emp_id
            FROM employee_hours eh
            LEFT JOIN employees e ON (
                e.branch_id = eh.branch_id AND e.name = eh.employee_name AND e.active = 1
            )
            WHERE eh.branch_id = ? AND eh.month = ? AND eh.source = 'aviv_api'
        ''', (branch_id, current_month)).fetchall()
    else:
        rows = db.execute('''
            SELECT eh.employee_name, eh.total_hours, eh.total_salary, eh.source,
                   e.hourly_rate, e.id as emp_id
            FROM employee_hours eh
            LEFT JOIN employees e ON (
                e.branch_id = eh.branch_id AND e.name = eh.employee_name AND e.active = 1
            )
            WHERE eh.branch_id = ? AND eh.month = ?
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
    has_api = 'aviv_api' in sources
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

@app.route('/api/branches')
@login_required
def api_branches():
    db = get_db()
    rows = db.execute('SELECT id, name, city, active FROM branches').fetchall()
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

        if live_row and live_row['amount']:
            live_amount_today = live_row['amount']
            # If no Z-report for today, add live amount to income
            if not has_z:
                income += live_amount_today
    else:
        live_row = None

    # Fixed expenses (% rows computed live from final income)
    fixed = _get_fixed_total(branch_id, month, income, db)

    profit = income - goods - fixed - salary

    live = None
    cancellation_total = 0
    discount_total = 0
    running_total = 0
    running_count = 0
    if live_row:
        live = {
            'amount': live_row['amount'],
            'transactions': live_row['transactions'],
            'last_updated': live_row['last_updated'],
        }
        try:
            cancellation_total = round(float(live_row['cancellation_total'] or 0), 2)
            discount_total = round(float(live_row['discount_total'] or 0), 2)
            running_total = round(float(live_row['running_total'] or 0), 2)
            running_count = int(live_row['running_count'] or 0)
        except (KeyError, TypeError):
            pass

    return jsonify({
        'income': income,
        'goods': goods,
        'fixed': fixed,
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
    })


@app.route('/api/history')
@login_required
def api_history():
    """Return monthly data from first month with real data to selected month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))

    db = get_db()
    earliest = db.execute('''
        SELECT MIN(month) as m FROM (
            SELECT strftime('%Y-%m', date) as month
            FROM daily_sales WHERE branch_id=?
            UNION
            SELECT strftime('%Y-%m', doc_date) as month
            FROM goods_documents WHERE branch_id=?
        )
    ''', (branch_id, branch_id)).fetchone()

    if not earliest or not earliest['m']:
        return jsonify([])

    start_y, start_m = map(int, earliest['m'].split('-'))
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
        fix = _get_fixed_total(branch_id, ms, inc, db)
        sal_data = _calculate_salary_cost(branch_id, ms)
        sal = sal_data['amount']
        sal_source = sal_data['source']
        profit = inc - gds - fix - sal
        result.append({
            'label': m['label'],
            'month': ms,
            'income': inc,
            'goods': gds,
            'fixed': fix,
            'salary': sal,
            'salary_source': sal_source,
            'profit': profit,
        })
    return jsonify(result)


@app.route('/api/live-sales')
@login_required
def api_live_sales():
    """Return today's live sales for a branch."""
    branch_id = get_branch_id()
    today = _now_il().strftime('%Y-%m-%d')
    db = get_db()
    row = db.execute(
        'SELECT amount, transactions, last_updated FROM live_sales WHERE branch_id = ? AND date = ?',
        (branch_id, today)
    ).fetchone()
    if row:
        return jsonify({
            'amount': row['amount'],
            'transactions': row['transactions'],
            'last_updated': row['last_updated'],
        })
    return jsonify({'amount': None, 'transactions': None, 'last_updated': None})


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

    # 2-hour buckets aligned to 6:30 opening
    # Map integer hours to bucket indices:
    # bucket 0 (6:30-8:30) ← hours 7,8
    # bucket 1 (8:30-10:30) ← hours 9,10  etc.
    bucket_defs = [
        ('6:30',  '8:30',  [7, 8]),
        ('8:30',  '10:30', [9, 10]),
        ('10:30', '12:30', [11, 12]),
        ('12:30', '14:30', [13, 14]),
        ('14:30', '16:30', [15, 16]),
        ('16:30', '18:30', [17, 18]),
        ('18:30', '20:30', [19, 20]),
        ('20:30', '22:30', [21, 22]),
        ('22:30', '23:00', [23]),
    ]

    buckets = []
    for label, end, hours in bucket_defs:
        total = sum(hourly[h]['total'] for h in hours)
        count = sum(hourly[h]['count'] for h in hours)
        buckets.append({'label': label, 'end': end, 'total': round(total, 2), 'count': count})

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
        'peak_bucket': f"\u200E{peak['label']}–{peak['end']}" if peak else None,
        'peak_total': peak['total'] if peak else 0,
        'quiet_bucket': f"\u200E{quiet['label']}–{quiet['end']}" if quiet else None,
        'quiet_total': quiet['total'] if quiet else 0,
        'hourly_avg': hourly_avg,
        'total_days_data': days_with_data,
    }

    return jsonify({'hourly': hourly, 'buckets': buckets, 'stats': stats})


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
    # Current month: only API-sourced rows. Past months: all sources.
    is_current_month = (month == _now_il().strftime('%Y-%m'))
    if is_current_month:
        hours_rows = db.execute(
            "SELECT employee_name, total_hours, total_salary, source FROM employee_hours "
            "WHERE branch_id = ? AND month = ? AND source = 'aviv_api'",
            (branch_id, month)
        ).fetchall()
    else:
        hours_rows = db.execute(
            "SELECT employee_name, total_hours, total_salary, source FROM employee_hours "
            "WHERE branch_id = ? AND month = ?",
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
            is_cur = (m_str == _now_il().strftime('%Y-%m'))
            if is_cur:
                h_row = db.execute(
                    "SELECT COALESCE(SUM(total_hours), 0) as hours, COALESCE(SUM(total_salary), 0) as salary, "
                    "COUNT(*) as cnt FROM employee_hours WHERE branch_id = ? AND month = ? AND source = 'aviv_api'",
                    (branch_id, m_str)
                ).fetchone()
            else:
                h_row = db.execute(
                    "SELECT COALESCE(SUM(total_hours), 0) as hours, COALESCE(SUM(total_salary), 0) as salary, "
                    "COUNT(*) as cnt FROM employee_hours WHERE branch_id = ? AND month = ?",
                    (branch_id, m_str)
                ).fetchone()
            h_hours = h_row['hours']
            h_salary = h_row['salary']
            # Determine source from actual data
            src_row = db.execute(
                "SELECT DISTINCT source FROM employee_hours WHERE branch_id = ? AND month = ?",
                (branch_id, m_str)
            ).fetchall()
            src_set = {r['source'] for r in src_row} if src_row else set()
            disc_row = db.execute(
                "SELECT COUNT(*) as cnt FROM employee_hours_discrepancies "
                "WHERE branch_id = ? AND month = ? AND resolved = 0",
                (branch_id, m_str)
            ).fetchone()
            has_unresolved = disc_row['cnt'] > 0 if disc_row else False
            if 'aviv_api' in src_set and 'csv' in src_set:
                h_source = 'api_neq_csv' if has_unresolved else 'api_verified'
            elif 'aviv_api' in src_set:
                h_source = 'api'
            elif 'csv' in src_set:
                h_source = 'csv'
            else:
                h_source = 'none' if h_row['cnt'] == 0 else 'unknown'
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

    # Create new employee
    cur = db.execute(
        'INSERT INTO employees (branch_id, name, hourly_rate, role, active, aviv_employee_id) '
        'VALUES (?, ?, ?, ?, 1, ?)',
        (branch_id, name, hourly_rate, role, aviv_emp_id))
    new_emp_id = cur.lastrowid

    # Save hours
    hours = row['hours']
    salary = round(hours * hourly_rate, 2)
    source = 'aviv_api'
    try:
        source = row['source'] or 'csv'
    except (IndexError, KeyError):
        pass

    db.execute(
        "INSERT OR REPLACE INTO employee_hours "
        "(branch_id, month, employee_name, total_hours, total_salary, source) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (branch_id, row['month'], name, hours, salary, source))

    # Always create alias for the original Aviv/CSV name (prevents re-flagging)
    csv_name = (row['csv_name'] or '').strip()
    if csv_name:
        db.execute(
            'INSERT OR IGNORE INTO employee_aliases (employee_id, alias_name, branch_id) VALUES (?, ?, ?)',
            (new_emp_id, csv_name, branch_id))
        # If manager changed the name, also save the final name as alias
        if csv_name != name:
            db.execute(
                'INSERT OR IGNORE INTO employee_aliases (employee_id, alias_name, branch_id) VALUES (?, ?, ?)',
                (new_emp_id, name, branch_id))

    db.execute('UPDATE employee_match_pending SET resolved = 1 WHERE id = ?', (pending_id,))
    _recalculate_avg_rate(branch_id, db)
    db.commit()

    return jsonify({'ok': True, 'employee_id': new_emp_id})


@app.route('/api/employee-hours-discrepancies')
@login_required
def api_discrepancies():
    """Return unresolved discrepancies for a branch+month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    rows = db.execute(
        "SELECT * FROM employee_hours_discrepancies "
        "WHERE branch_id = ? AND month = ? AND resolved = 0 "
        "ORDER BY difference DESC",
        (branch_id, month)
    ).fetchall()
    return jsonify({'discrepancies': [dict(r) for r in rows]})


@app.route('/api/employee-hours-discrepancies/<int:disc_id>/resolve', methods=['POST'])
@login_required
def api_resolve_discrepancy(disc_id):
    """Resolve a discrepancy: accept API hours, CSV hours, or ignore."""
    db = get_db()
    branch_id = get_branch_id()
    row = db.execute(
        "SELECT * FROM employee_hours_discrepancies WHERE id = ? AND branch_id = ?",
        (disc_id, branch_id)
    ).fetchone()
    if not row or row['resolved']:
        return jsonify({'error': 'not found'}), 404

    data = request.get_json()
    choice = data.get('choice', 'ignore')  # 'api' | 'csv' | 'ignore'

    if choice in ('api', 'csv'):
        hours = row['api_hours'] if choice == 'api' else row['csv_hours']
        emp_name = row['employee_name']
        # Get hourly rate
        emp = db.execute(
            'SELECT hourly_rate FROM employees WHERE id = ? AND branch_id = ?',
            (row['employee_id'], branch_id)
        ).fetchone()
        rate = emp['hourly_rate'] if emp else 0
        salary = round(hours * rate, 2)

        db.execute(
            "UPDATE employee_hours SET total_hours = ?, total_salary = ? "
            "WHERE branch_id = ? AND month = ? AND employee_name = ?",
            (hours, salary, branch_id, row['month'], emp_name)
        )

    db.execute(
        "UPDATE employee_hours_discrepancies SET resolved = 1, resolution = ? WHERE id = ?",
        (choice, disc_id)
    )
    db.commit()
    return jsonify({'ok': True})


def _get_fixed_total(branch_id: int, month: str, income: float, db) -> float:
    """Sum fixed expenses for a branch+month. % rows calculated live from income."""
    rows = db.execute(
        'SELECT amount, pct_value FROM fixed_expenses WHERE branch_id=? AND month=?',
        (branch_id, month)
    ).fetchall()
    total = 0
    for r in rows:
        if r['pct_value'] and r['pct_value'] > 0:
            total += income * r['pct_value'] / 100
        else:
            total += r['amount']
    return round(total, 2)


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


PDF_BASE = os.path.join(os.path.dirname(__file__), 'data', 'pdfs')


@app.route('/api/sales')
@login_required
def api_sales():
    """Return daily sales for a branch + month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    rows = db.execute(
        "SELECT date, amount, transactions, source FROM daily_sales "
        "WHERE branch_id = ? AND strftime('%Y-%m', date) = ? ORDER BY date DESC",
        (branch_id, month)
    ).fetchall()
    sales = [dict(r) for r in rows]

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


def _ceo_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated


@app.route('/ops')
@_ceo_required
def ops():
    ctx = _page_context('ops')
    return render_template('ops.html', **ctx)


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
@_ceo_required
def api_ops_status():
    db = get_db()
    # Branches
    branches_rows = db.execute('SELECT id, name, city, active FROM branches WHERE active = 1').fetchall()
    branches = []
    for b in branches_rows:
        bid = b['id']
        # Last run per agent — exactly one row per agent
        agents_data = {}
        for agent in ('bilboy', 'gmail', 'aviv_live'):
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

        # Determine overall status
        statuses = [a['status'] for a in agents_data.values() if a]
        if 'error' in statuses:
            overall = 'error'
        elif 'warning' in statuses:
            overall = 'warning'
        elif statuses:
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

        branches.append({
            'id': bid, 'name': b['name'], 'city': b['city'],
            'status': overall, 'agents': agents_data,
            'avg_hourly_rate': rate_row['avg_hourly_rate'] if rate_row else 0,
            'hours_this_month': rate_row['hours_this_month'] if rate_row else 0,
            'employees_with_rates': emp_rate_count,
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
@_ceo_required
def ops_run_agent():
    data = request.get_json()
    branch_id = data.get('branch_id')
    agent = data.get('agent')

    if not branch_id or agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_employees'):
        return jsonify({'status': 'error', 'message': 'Invalid parameters'}), 400

    t0 = time.time()
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
            from agents.aviv_live import run_aviv_live
            result = run_aviv_live(int(branch_id))
            msg = f"₪{result.get('amount', 0):,.0f} ({result.get('transactions', 0)} tx)"
        else:  # aviv_employees
            from agents.aviv_employees import run_aviv_employees
            result = run_aviv_employees(int(branch_id))
            msg = result.get('message', 'done')

        duration = round(time.time() - t0, 1)
        status = 'success' if result.get('success') else 'error'
        if not result.get('success'):
            msg = result.get('error', 'Unknown error')

        from utils.notify import notify
        notify(f"{'✅' if status == 'success' else '❌'} {agent}", f"סניף {branch_id} — {msg}")
        return jsonify({'status': status, 'message': msg, 'duration': duration})

    except Exception as e:
        duration = round(time.time() - t0, 1)
        return jsonify({'status': 'error', 'message': str(e), 'duration': duration})


@app.route('/ops/logs/<int:branch_id>/<agent>')
@_ceo_required
def ops_logs(branch_id, agent):
    import re as _re
    if agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_employees'):
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
@_ceo_required
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
@_ceo_required
def api_ops_health():
    def _run(cmd):
        try:
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            return r.stdout.strip()
        except Exception as e:
            return str(e)

    svc1 = _run("systemctl is-active makolet-chain")
    svc2 = _run("systemctl is-active makolet-chain-scheduler")
    disk = _run("df -h /opt/makolet-chain --output=used,size,pcent | tail -1")
    memory = _run("free -m | awk 'NR==2{printf \"%s/%s\", $3, $2}'")
    uptime = _run("uptime -p")
    deploy_ago = _run("git -C /opt/makolet-chain log -1 --format='%ar'")
    deploy_msg = _run("git -C /opt/makolet-chain log -1 --format='%s'")

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

    services_ok = svc1 == 'active' and svc2 == 'active'
    disk_status = 'ok' if disk_pct < 70 else ('warning' if disk_pct < 90 else 'error')

    return jsonify({
        'services': {'app': svc1, 'scheduler': svc2, 'ok': services_ok},
        'disk': {'raw': disk_display, 'pct': disk_pct, 'status': disk_status},
        'memory': memory,
        'uptime': uptime,
        'last_deploy': last_deploy,
    })


@app.route('/admin/branches')
@_ceo_required
def admin_branches():
    db = get_db()
    branches = db.execute('SELECT * FROM branches ORDER BY id').fetchall()
    users = db.execute(
        "SELECT u.*, GROUP_CONCAT(ub.branch_id) as branch_ids "
        "FROM users u LEFT JOIN user_branches ub ON u.id = ub.user_id "
        "GROUP BY u.id ORDER BY u.id"
    ).fetchall()
    return render_template('admin_branches.html',
                           branches=[dict(b) for b in branches],
                           users=[dict(u) for u in users],
                           **_page_context('admin'))


@app.route('/api/admin/branches', methods=['POST'])
@_ceo_required
def api_admin_branch_create():
    data = request.get_json()
    db = get_db()
    max_id = db.execute('SELECT MAX(id) FROM branches').fetchone()[0] or 126
    new_id = max_id + 1
    db.execute(
        '''INSERT INTO branches (id, name, city, active, aviv_user_id, aviv_password,
           bilboy_user, bilboy_pass, gmail_label, franchise_supplier, iec_contract)
           VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)''',
        (new_id, data.get('name', ''), data.get('city', ''),
         data.get('aviv_user_id', ''), data.get('aviv_password', ''),
         data.get('bilboy_user', ''), data.get('bilboy_pass', ''),
         data.get('gmail_label', ''),
         data.get('franchise_supplier', 'זיכיונות המכולת בע"מ'),
         data.get('iec_contract', '')))
    db.commit()
    manager_email = data.get('manager_email', '').strip()
    manager_name = data.get('manager_name', '').strip()
    if manager_email and manager_name:
        temp_password = secrets.token_urlsafe(8)
        pw_hash = generate_password_hash(temp_password)
        db.execute(
            "INSERT OR IGNORE INTO users (name, email, password_hash, role) VALUES (?,?,?,'manager')",
            (manager_name, manager_email, pw_hash))
        db.commit()
        user_row = db.execute('SELECT id FROM users WHERE email=?', (manager_email,)).fetchone()
        if user_row:
            db.execute('INSERT OR IGNORE INTO user_branches (user_id, branch_id) VALUES (?,?)',
                       (user_row['id'], new_id))
            db.commit()
        return jsonify({'ok': True, 'branch_id': new_id, 'temp_password': temp_password})
    return jsonify({'ok': True, 'branch_id': new_id})


@app.route('/api/admin/branches/<int:branch_id>')
@_ceo_required
def api_admin_branch_get(branch_id):
    db = get_db()
    row = db.execute('SELECT * FROM branches WHERE id=?', (branch_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(dict(row))


@app.route('/api/admin/branches/<int:branch_id>', methods=['PUT'])
@_ceo_required
def api_admin_branch_update(branch_id):
    data = request.get_json()
    db = get_db()
    fields = ['name', 'city', 'active', 'aviv_user_id', 'aviv_password',
              'bilboy_user', 'bilboy_pass', 'gmail_label', 'franchise_supplier', 'iec_contract']
    updates = {f: data[f] for f in fields if f in data}
    if not updates:
        return jsonify({'ok': True})
    sql = 'UPDATE branches SET ' + ', '.join(f + '=?' for f in updates) + ' WHERE id=?'
    db.execute(sql, list(updates.values()) + [branch_id])
    db.commit()
    return jsonify({'ok': True})


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


# Initialize DB and seed admin on import (Gunicorn loads app:app)
init_db()
seed_admin()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
