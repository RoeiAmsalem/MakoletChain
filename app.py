import os
import sqlite3
from flask import Flask, jsonify, g

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'db', 'makolet_chain.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'db', 'schema.sql')


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
    """Run schema.sql to create all tables."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_PATH, 'r') as f:
        conn.executescript(f.read())
    conn.close()


@app.route('/')
def health():
    return jsonify({'status': 'ok', 'project': 'MakoletChain'})


@app.route('/api/branches')
def list_branches():
    db = get_db()
    rows = db.execute('SELECT id, name, city, active FROM branches').fetchall()
    branches = [dict(row) for row in rows]
    return jsonify(branches)


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)
