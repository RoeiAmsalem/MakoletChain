"""Surface-level brand tests: navbar has "קופה שקופה" + ring logo, branch
switcher element is still present in base.html, page titles use the new brand,
favicon link is set."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402

REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
BASE_HTML = os.path.join(REPO_ROOT, 'templates', 'base.html')

BRAND_NAME = 'קופה שקופה'


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


def _read_base():
    with open(BASE_HTML, 'r', encoding='utf-8') as f:
        return f.read()


def test_navbar_has_brand_name():
    """Brand mark in the navbar is the logo ALONE — no visible text span next
    to it. The product name lives in <title>, the manifest, and the img alt
    (for accessibility). The brand-name span must be gone from base.html."""
    html = _read_base()
    assert 'class="brand-name"' not in html, \
        'navbar brand text span removed — logo-only brand mark'
    # Accessibility: name still surfaces via alt + aria-label
    assert f'alt="{BRAND_NAME}"' in html
    assert f'aria-label="{BRAND_NAME}"' in html
    # And <title> still uses the brand
    assert f'{{% block title %}}{BRAND_NAME}{{% endblock %}}' in html


def test_navbar_has_logo():
    html = _read_base()
    assert 'class="brand-logo"' in html
    assert 'icons/icon-192.png' in html


def test_brand_link_wraps_logo_and_wordmark():
    """The .navbar-brand anchor must wrap BOTH the logo and the wordmark, so
    the whole brand lockup is one tap-target to home."""
    html = _read_base()
    import re
    m = re.search(
        r'<a[^>]*href="/"[^>]*class="navbar-brand"[^>]*>(.*?)</a>',
        html,
        re.DOTALL,
    )
    assert m, 'navbar-brand anchor not found'
    # And it must point to "/" (home) — already required by the regex above
    inner = m.group(1)
    assert 'class="brand-logo"' in inner, \
        'brand-logo must live inside the navbar-brand anchor'
    assert 'class="brand-wordmark"' in inner, \
        'brand-wordmark must live inside the navbar-brand anchor'


def test_navbar_has_wordmark():
    """The 'קופה שקופה' wordmark image sits between the logo and the store
    name in the navbar brand cluster."""
    html = _read_base()
    assert 'class="brand-wordmark"' in html
    assert 'icons/wordmark.png' in html
    # Order check: logo appears before wordmark, wordmark appears before the
    # store-name element (branch-select OR branch-name-pill).
    logo_idx = html.find('class="brand-logo"')
    wm_idx = html.find('class="brand-wordmark"')
    name_idx = min(
        html.find('id="branch-select"'),
        html.find('class="branch-name-pill"'),
    )
    assert 0 < logo_idx < wm_idx < name_idx, \
        'expected DOM order: brand-logo → brand-wordmark → store-name element'


def test_branch_switcher_intact():
    """The branch switcher element must still be present in base.html — it is
    load-bearing for admin and multi-branch managers."""
    html = _read_base()
    assert 'id="branch-select"' in html
    assert 'onchange="switchBranch' in html
    assert 'loadBranchSwitcher' in html


def test_favicon_link_present():
    html = _read_base()
    assert 'rel="icon"' in html
    assert '/static/icons/icon-192.png' in html


def test_page_title_rebranded(client):
    """The login page renders the new brand in <title>."""
    res = client.get('/login')
    assert res.status_code == 200
    body = res.data.decode('utf-8')
    assert f'<title>התחברות - {BRAND_NAME}</title>' in body
    assert 'MakoletChain' not in body


def test_no_makoletchain_in_templates():
    """Sweep all rendered templates — no leftover 'MakoletChain' brand string."""
    tpl_dir = os.path.join(REPO_ROOT, 'templates')
    offenders = []
    for root, _, files in os.walk(tpl_dir):
        for name in files:
            if not name.endswith('.html'):
                continue
            path = os.path.join(root, name)
            with open(path, 'r', encoding='utf-8') as f:
                if 'MakoletChain' in f.read():
                    offenders.append(path)
    assert not offenders, f'MakoletChain still present in: {offenders}'
