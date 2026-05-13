"""Tests for /goods page — grouped view ordering."""
import os
import sys
import sqlite3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app
from werkzeug.security import generate_password_hash


@pytest.fixture
def client():
    app.config['TESTING'] = True
    test_db = os.path.join(os.path.dirname(__file__), 'test_goods.db')

    import app as app_module
    original_db = app_module.DB_PATH
    if os.path.exists(test_db):
        os.remove(test_db)
    app_module.DB_PATH = test_db
    app_module.init_db()

    conn = sqlite3.connect(test_db, timeout=30)
    conn.execute("INSERT OR REPLACE INTO branches (id, name, city, active) VALUES (126, 'איינשטיין', 'תל אביב', 1)")
    conn.execute(
        "INSERT INTO users (id, name, email, password_hash, role, active) VALUES "
        "(2, 'Manager', 'mgr@test.com', ?, 'manager', 1)",
        (generate_password_hash('test123'),))
    conn.execute("INSERT INTO user_branches (user_id, branch_id) VALUES (2, 126)")
    # Three suppliers, mixed dates within month 2026-05
    rows = [
        # (supplier, ref, amount, date, doc_type)
        ('סופר א', '1001', 100.00, '2026-05-01', 3),
        ('סופר א', '1002', 200.00, '2026-05-02', 3),  # supplier A total = 300
        ('סופר ב', '2001', 500.00, '2026-05-03', 3),
        ('סופר ב', '2002', 250.00, '2026-05-04', 2),  # supplier B total = 750
        ('סופר ג', '3001', 50.00, '2026-05-05', 3),   # supplier C total = 50
    ]
    for sup, ref, amt, dt, dtype in rows:
        conn.execute(
            "INSERT INTO goods_documents (branch_id, doc_date, supplier, ref_number, amount, doc_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (126, dt, sup, ref, amt, dtype))
    conn.commit()
    conn.close()

    with app.test_client() as c:
        yield c

    app_module.DB_PATH = original_db
    if os.path.exists(test_db):
        os.remove(test_db)


def _login(client, email='mgr@test.com', password='test123'):
    return client.post('/login', data={'email': email, 'password': password},
                       follow_redirects=False)


def test_grouped_view_orders_suppliers_by_total_desc(client):
    _login(client)
    # Set month via query, switch to grouped view
    client.get('/goods?month=2026-05')
    res = client.get('/goods?view=grouped')
    assert res.status_code == 200
    html = res.get_data(as_text=True)
    # Suppliers should appear in order: ב (750) → א (300) → ג (50)
    idx_b = html.find('סופר ב')
    idx_a = html.find('סופר א')
    idx_c = html.find('סופר ג')
    assert idx_b != -1 and idx_a != -1 and idx_c != -1, \
        f"expected all suppliers in HTML — found b={idx_b} a={idx_a} c={idx_c}"
    assert idx_b < idx_a < idx_c, \
        f"expected suppliers ordered by total DESC (ב < א < ג positions), got b={idx_b} a={idx_a} c={idx_c}"
