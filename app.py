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
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_PATH, 'r') as f:
        conn.executescript(f.read())
    conn.close()


def seed_admin():
    """Seed the admin user if not exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    existing = conn.execute("SELECT id FROM users WHERE email = ?", ('admin@makolet.com',)).fetchone()
    if not existing:
        pw_hash = generate_password_hash('admin123')
        conn.execute(
            "INSERT INTO users (name, email, password_hash, role) VALUES (?, ?, ?, ?)",
            ('מנהל ראשי', 'admin@makolet.com', pw_hash, 'ceo')
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
    if role == 'ceo':
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
        if role == 'ceo' or bid in branches:
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

    # Fixed expenses
    fixed = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM fixed_expenses "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchone()[0]

    # Salary — prefer CSV data (total_salary), fallback to estimate
    csv_salary = db.execute(
        "SELECT COALESCE(SUM(total_salary), 0) as s, COUNT(*) as cnt FROM employee_hours "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchone()
    if csv_salary['cnt'] > 0 and csv_salary['s'] > 0:
        salary = csv_salary['s']
    else:
        # Estimate: hours_this_month × weighted avg rate from employees table
        branch_row = db.execute(
            "SELECT hours_this_month, avg_hourly_rate FROM branches WHERE id = ?",
            (branch_id,)
        ).fetchone()
        if branch_row and branch_row['avg_hourly_rate'] and branch_row['avg_hourly_rate'] > 0:
            salary = (branch_row['hours_this_month'] or 0) * branch_row['avg_hourly_rate']
        else:
            # Try weighted avg from employees table
            emp_rate = db.execute(
                "SELECT AVG(hourly_rate) as r FROM employees WHERE branch_id=? AND active=1 AND hourly_rate>0",
                (branch_id,)
            ).fetchone()
            if emp_rate and emp_rate['r'] and branch_row:
                salary = (branch_row['hours_this_month'] or 0) * emp_rate['r']
            else:
                salary = 0

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
            'SELECT amount, transactions, last_updated FROM live_sales WHERE branch_id = ? AND date = ?',
            (branch_id, today)
        ).fetchone()

        if live_row and live_row['amount']:
            live_amount_today = live_row['amount']
            # If no Z-report for today, add live amount to income
            if not has_z:
                income += live_amount_today
    else:
        live_row = None

    profit = income - goods - fixed - salary

    live = None
    if live_row:
        live = {
            'amount': live_row['amount'],
            'transactions': live_row['transactions'],
            'last_updated': live_row['last_updated'],
        }

    return jsonify({
        'income': income,
        'goods': goods,
        'fixed': fixed,
        'salary': salary,
        'profit': profit,
        'live': live,
        'has_z': has_z,
        'live_amount_today': live_amount_today,
        'branch_id': branch_id,
        'month': month,
    })


@app.route('/api/history')
@login_required
def api_history():
    """Return last 6 months of data for chart + table."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))

    year, mon = map(int, month.split('-'))
    months = []
    for i in range(5, -1, -1):
        m = mon - i
        y = year
        while m <= 0:
            m += 12
            y -= 1
        m_str = f'{y:04d}-{m:02d}'
        label = f'{m}/{y}'
        months.append({'month': m_str, 'label': label})

    db = get_db()
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
        fix = db.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM fixed_expenses WHERE branch_id = ? AND month = ?",
            (branch_id, ms)
        ).fetchone()[0]
        csv_sal = db.execute(
            "SELECT COALESCE(SUM(total_salary), 0) as s, COUNT(*) as cnt FROM employee_hours "
            "WHERE branch_id = ? AND month = ?",
            (branch_id, ms)
        ).fetchone()
        if csv_sal['cnt'] > 0 and csv_sal['s'] > 0:
            sal = csv_sal['s']
        else:
            br = db.execute("SELECT hours_this_month, avg_hourly_rate FROM branches WHERE id=?", (branch_id,)).fetchone()
            sal = ((br['hours_this_month'] or 0) * (br['avg_hourly_rate'] or 0)) if br and ms == _now_il().strftime('%Y-%m') else 0
        profit = inc - gds - fix - sal
        result.append({
            'label': m['label'],
            'month': ms,
            'income': inc,
            'goods': gds,
            'fixed': fix,
            'salary': sal,
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

    # CSV hours for this month from employee_hours
    hours_rows = db.execute(
        "SELECT employee_name, total_hours, total_salary, source FROM employee_hours "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchall()
    hours_map = {r['employee_name']: dict(r) for r in hours_rows}
    csv_processed = len(hours_map) > 0

    # Branch KPI data
    branch_row = db.execute(
        "SELECT hours_this_month, avg_hourly_rate, hours_updated_at FROM branches WHERE id = ?",
        (branch_id,)
    ).fetchone()
    hours_this_month = (branch_row['hours_this_month'] or 0) if branch_row else 0
    avg_hourly_rate = (branch_row['avg_hourly_rate'] or 0) if branch_row else 0
    hours_updated_at = (branch_row['hours_updated_at'] or '') if branch_row else ''

    # Match employees to CSV hours + calculate salary
    for emp in employees:
        matched = _match_employee_hours(emp['name'], hours_map)
        if matched:
            emp['hours'] = matched['total_hours']
            emp['salary'] = matched['total_salary']
        else:
            emp['hours'] = 0
            emp['salary'] = 0

    # Calculate salary cost
    total_salary = sum(e['salary'] for e in employees)
    if csv_processed and total_salary > 0:
        salary_cost = total_salary
    else:
        # Estimate from employees with rates
        rates = [e['hourly_rate'] for e in employees if e['hourly_rate'] > 0]
        weighted_rate = sum(rates) / len(rates) if rates else 0
        salary_cost = hours_this_month * weighted_rate if weighted_rate else 0

    # History: last 6 months
    year, mon = map(int, month.split('-'))
    history = []
    for i in range(5, -1, -1):
        m = mon - i
        y = year
        while m <= 0:
            m += 12
            y -= 1
        m_str = f'{y:04d}-{m:02d}'
        h_row = db.execute(
            "SELECT COALESCE(SUM(total_hours), 0) as hours, COALESCE(SUM(total_salary), 0) as salary, "
            "COUNT(*) as cnt FROM employee_hours WHERE branch_id = ? AND month = ?",
            (branch_id, m_str)
        ).fetchone()
        h_hours = h_row['hours']
        h_salary = h_row['salary']
        h_source = 'csv' if h_row['cnt'] > 0 else 'משוער'
        h_rate = round(h_salary / h_hours, 2) if h_hours > 0 and h_salary > 0 else avg_hourly_rate
        history.append({
            'month': m_str, 'hours': h_hours, 'salary': h_salary,
            'avg_rate': h_rate, 'source': h_source,
        })

    return jsonify({
        'employees': employees,
        'hours_this_month': hours_this_month,
        'avg_hourly_rate': avg_hourly_rate,
        'hours_updated_at': hours_updated_at,
        'salary_cost': salary_cost,
        'csv_processed': csv_processed,
        'history': history,
    })


def _match_employee_hours(emp_name: str, hours_map: dict) -> dict | None:
    """Match an employee name to CSV hours data.
    Try: exact match, then strip branch suffix, then first-two-words match."""
    if emp_name in hours_map:
        return hours_map[emp_name]
    # Strip branch suffix words (e.g. "איינשטיין", "אינשטיין")
    for csv_name, data in hours_map.items():
        # Check if CSV name starts with employee name
        if csv_name.startswith(emp_name):
            return data
        if emp_name.startswith(csv_name):
            return data
    # First two words match
    emp_words = emp_name.split()[:2]
    if len(emp_words) >= 2:
        for csv_name, data in hours_map.items():
            csv_words = csv_name.split()[:2]
            if emp_words == csv_words:
                return data
    return None


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
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employees/<int:emp_id>', methods=['DELETE'])
@login_required
def api_employees_delete(emp_id):
    """Soft-delete an employee (set active=0)."""
    db = get_db()
    row = db.execute("SELECT branch_id FROM employees WHERE id = ?", (emp_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    branch_id = get_branch_id()
    if row['branch_id'] != branch_id:
        return jsonify({'error': 'forbidden'}), 403
    db.execute("UPDATE employees SET active=0 WHERE id=?", (emp_id,))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses', methods=['GET'])
@login_required
def api_fixed_expenses_list():
    """List fixed expenses for a branch + month."""
    branch_id = get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    rows = db.execute(
        "SELECT id, name, amount, expense_type, pct_value, locked FROM fixed_expenses "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


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
    """Update a fixed expense amount."""
    data = request.get_json()
    amount = float(data.get('amount', 0))
    db = get_db()
    db.execute("UPDATE fixed_expenses SET amount = ? WHERE id = ?", (amount, exp_id))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses/<int:exp_id>', methods=['DELETE'])
@login_required
def api_fixed_expenses_delete(exp_id):
    """Delete a fixed expense."""
    db = get_db()
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
        if session.get('user_role') != 'ceo':
            abort(403)
        return f(*args, **kwargs)
    return decorated


@app.route('/ops')
@_ceo_required
def ops():
    return render_template('ops.html')


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


def _convert_run_times(row_dict):
    """Convert started_at and finished_at to Israel time in a dict."""
    row_dict['started_at'] = _to_il_time(row_dict.get('started_at'))
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
                d['started_at'] = _to_il_time(d.get('started_at'))
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
    last_nightly = _to_il_time(last_nightly_row['started_at']) if last_nightly_row else ''

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

    if not branch_id or agent not in ('bilboy', 'gmail', 'aviv_live'):
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
        else:
            from agents.aviv_live import run_aviv_live
            result = run_aviv_live(int(branch_id))
            msg = f"₪{result.get('amount', 0):,.0f} ({result.get('transactions', 0)} tx)"

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
    if agent not in ('bilboy', 'gmail', 'aviv_live'):
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


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


# Initialize DB and seed admin on import (Gunicorn loads app:app)
init_db()
seed_admin()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
