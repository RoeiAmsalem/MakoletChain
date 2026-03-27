import io
import os
import sqlite3
from datetime import datetime, date
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo

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
        return render_template('login.html', error=None)

    email = request.form.get('email', '').strip()
    password = request.form.get('password', '')

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ? AND active = 1", (email,)).fetchone()

    if user and check_password_hash(user['password_hash'], password):
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


def _get_branch_id():
    bid = request.args.get('branch_id')
    if bid:
        session['branch_id'] = int(bid)
    else:
        bid = session.get('branch_id', 126)
    return int(bid)


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
    branch_id = _get_branch_id()
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

    # Salary
    salary = db.execute(
        "SELECT COALESCE(SUM(hours * rate), 0) FROM employee_hours "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchone()[0]

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
    branch_id = _get_branch_id()
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
        sal = db.execute(
            "SELECT COALESCE(SUM(hours * rate), 0) FROM employee_hours WHERE branch_id = ? AND month = ?",
            (branch_id, ms)
        ).fetchone()[0]
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
    branch_id = _get_branch_id()
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
    """List employees for a branch + month."""
    branch_id = _get_branch_id()
    month = request.args.get('month', _now_il().strftime('%Y-%m'))
    db = get_db()
    rows = db.execute(
        "SELECT id, employee_name, hours, rate, locked FROM employee_hours "
        "WHERE branch_id = ? AND month = ?",
        (branch_id, month)
    ).fetchall()
    return jsonify({'employees': [dict(r) for r in rows]})


@app.route('/api/employees', methods=['POST'])
@login_required
def api_employees_create():
    """Add a new employee entry for a branch + month."""
    data = request.get_json()
    branch_id = data.get('branch_id', _get_branch_id())
    month = data.get('month', _now_il().strftime('%Y-%m'))
    name = data.get('employee_name', '').strip()
    rate = float(data.get('rate', 0))
    if not name:
        return jsonify({'error': 'name required'}), 400
    db = get_db()
    db.execute(
        "INSERT INTO employee_hours (branch_id, month, employee_name, hours, rate) VALUES (?, ?, ?, 0, ?)",
        (branch_id, month, name, rate)
    )
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employees/<int:emp_id>', methods=['PUT'])
@login_required
def api_employees_update(emp_id):
    """Update an employee entry."""
    data = request.get_json()
    db = get_db()
    row = db.execute("SELECT * FROM employee_hours WHERE id = ?", (emp_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    name = data.get('employee_name', row['employee_name'])
    rate = float(data.get('rate', row['rate']))
    hours = float(data.get('hours', row['hours']))
    db.execute(
        "UPDATE employee_hours SET employee_name = ?, rate = ?, hours = ? WHERE id = ?",
        (name, rate, hours, emp_id)
    )
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/employees/<int:emp_id>', methods=['DELETE'])
@login_required
def api_employees_delete(emp_id):
    """Delete an employee entry."""
    db = get_db()
    db.execute("DELETE FROM employee_hours WHERE id = ?", (emp_id,))
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/fixed-expenses', methods=['GET'])
@login_required
def api_fixed_expenses_list():
    """List fixed expenses for a branch + month."""
    branch_id = _get_branch_id()
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
    branch_id = data.get('branch_id', _get_branch_id())
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
    branch_id = _get_branch_id()
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
    branch_id = _get_branch_id()
    pdf_path = os.path.join(PDF_BASE, str(branch_id), f"z_{sale_date}.pdf")
    if not os.path.isfile(pdf_path):
        abort(404)
    return send_file(pdf_path, mimetype='application/pdf')


@app.route('/api/sales/pdf-image/<sale_date>/<int:page>')
@login_required
def api_sales_pdf_image(sale_date, page):
    """Render a PDF page as PNG image using PyMuPDF."""
    branch_id = _get_branch_id()
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
    branch_id = _get_branch_id()
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


@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


# Initialize DB and seed admin on import (Gunicorn loads app:app)
init_db()
seed_admin()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
