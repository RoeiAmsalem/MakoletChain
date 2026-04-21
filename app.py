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
            # Return JSON 401 for API requests, redirect for page requests
            if request.path.startswith('/api/') or request.is_json:
                return jsonify({'error': 'יש להתחבר מחדש', 'redirect': '/login'}), 401
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
    bid = session.get('branch_id')
    if bid:
        return bid
    # Fallback: first assigned branch
    user_branches = session.get('user_branches', [])
    if user_branches:
        return user_branches[0]
    # Admin with no user_branches rows: fall back to first branch in DB
    if session.get('user_role') == 'admin':
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

    # UPDATED 2026-04-18: Always use API-only rows (CSV path retired).
    # Only include hours for ACTIVE employees (inactive employees excluded from salary).
    rows = db.execute('''
        SELECT eh.employee_name, eh.total_hours, eh.total_salary, eh.source,
               e.hourly_rate, e.id as emp_id
        FROM employee_hours eh
        JOIN employees e ON (
            e.branch_id = eh.branch_id AND e.name = eh.employee_name AND e.active = 1
        )
        WHERE eh.branch_id = ? AND eh.month = ? AND eh.source = 'aviv_api'
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
    role = session.get('user_role')
    if role == 'admin':
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

    # Fixed expenses (% rows computed live from final income) + electricity
    fixed_data = _get_fixed_total(branch_id, month, income, db)
    fixed = fixed_data['total']

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

    # Latest electricity invoice for the strip
    latest_elec = db.execute(
        "SELECT period_label, amount, due_date FROM electricity_invoices "
        "WHERE branch_id = ? ORDER BY due_date DESC LIMIT 1",
        (branch_id,)
    ).fetchone()
    # IEC last sync time
    iec_sync = db.execute(
        "SELECT iec_last_sync_at FROM branches WHERE id = ?", (branch_id,)
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
        'iec_last_sync_at': iec_sync['iec_last_sync_at'] if iec_sync and iec_sync['iec_last_sync_at'] else None,
    })


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
            'count': count
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
    hours_rows = db.execute(
        "SELECT employee_name, total_hours, total_salary, source FROM employee_hours "
        "WHERE branch_id = ? AND month = ? AND source = 'aviv_api'",
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
            h_row = db.execute(
                "SELECT COALESCE(SUM(total_hours), 0) as hours, COALESCE(SUM(total_salary), 0) as salary, "
                "COUNT(*) as cnt FROM employee_hours WHERE branch_id = ? AND month = ? AND source = 'aviv_api'",
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
    Returns: {amount: float, source: 'real'|'estimate'|'none', estimate_basis: str|None}
    """
    if db is None:
        db = get_db()
    # Check if branch has IEC integration
    has_iec = db.execute(
        "SELECT iec_token FROM branches WHERE id = ?", (branch_id,)
    ).fetchone()
    if not has_iec or not has_iec['iec_token']:
        return {'amount': 0, 'source': 'none', 'estimate_basis': None}
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
                break  # found for this offset, no need to check other direction at same distance
        if best_distance <= offset:
            break  # found something at this distance, no need to search further
    if best_amount > 0:
        return {'amount': best_amount, 'source': 'estimate', 'estimate_basis': best_basis}
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
            'has_iec_token': bool(has_iec),
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

    if not branch_id or agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_employees', 'iec'):
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
    if agent not in ('bilboy', 'gmail', 'aviv_live', 'aviv_employees', 'iec'):
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


# ── IEC status & accuracy endpoints ─────────────────────────────────────

@app.route('/api/iec-status')
@_ceo_required
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
@_ceo_required
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
    if session.get('user_role') == 'admin':
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
            UPDATE branches SET iec_user_id = ?, iec_token = ?, iec_bp_number = ?, iec_contract_id = ?
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


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


# Initialize DB and seed admin on import (Gunicorn loads app:app)
init_db()
seed_admin()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
