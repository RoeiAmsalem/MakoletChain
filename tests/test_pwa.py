"""PWA tests: manifest + service worker + icons + base.html hooks.

Note: the service worker fetch handler and install-banner JS are browser
runtime — they can't be unit-tested here. We verify the static surface only:
files exist, are served, and base.html links them.
"""
import json
import os
import struct
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app import app  # noqa: E402


REPO_ROOT = os.path.join(os.path.dirname(__file__), '..')
ICON_DIR = os.path.join(REPO_ROOT, 'static', 'icons')


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


def _png_size(path):
    """Return (width, height) from a PNG file header. Raises if not a PNG."""
    with open(path, 'rb') as f:
        head = f.read(24)
    assert head[:8] == b'\x89PNG\r\n\x1a\n', f'{path} is not a PNG'
    width, height = struct.unpack('>II', head[16:24])
    return width, height


def test_manifest_served(client):
    resp = client.get('/static/manifest.json')
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data['display'] == 'standalone'
    assert data['start_url'].startswith('/')
    assert isinstance(data.get('icons'), list) and len(data['icons']) >= 2
    purposes = ' '.join(i.get('purpose', '') for i in data['icons'])
    assert 'maskable' in purposes


def test_sw_served_with_headers(client):
    resp = client.get('/static/sw.js')
    assert resp.status_code == 200
    ct = resp.headers.get('Content-Type', '')
    assert 'javascript' in ct, f'sw.js Content-Type is {ct!r}'
    assert resp.headers.get('Service-Worker-Allowed') == '/'
    body = resp.data.decode('utf-8')
    # Sanity: the financial-data safety rule must be present.
    assert '/api/' in body
    assert 'network' in body.lower()


def test_sw_root_alias(client):
    """Also accessible at /sw.js with the same header — defense in depth."""
    resp = client.get('/sw.js')
    assert resp.status_code == 200
    assert resp.headers.get('Service-Worker-Allowed') == '/'


def test_icons_exist():
    expected = {
        'icon-192.png': (192, 192),
        'icon-512.png': (512, 512),
        'icon-512-maskable.png': (512, 512),
        'icon-180.png': (180, 180),
    }
    for name, dims in expected.items():
        path = os.path.join(ICON_DIR, name)
        assert os.path.exists(path), f'missing {name}'
        assert _png_size(path) == dims, f'{name} wrong dims'


def test_base_html_has_manifest_link():
    base = os.path.join(REPO_ROOT, 'templates', 'base.html')
    with open(base, 'r', encoding='utf-8') as f:
        html = f.read()
    assert 'rel="manifest"' in html
    assert '/static/manifest.json' in html
    assert 'apple-touch-icon' in html
    assert '/static/icons/icon-180.png' in html
    assert 'theme-color' in html
    assert 'apple-mobile-web-app-capable' in html
    assert "navigator.serviceWorker.register('/static/sw.js'" in html


def test_base_html_has_install_button():
    """The install trigger is now a small navbar button — the bottom banner
    was removed in favor of a persistent, less-intrusive UI."""
    base = os.path.join(REPO_ROOT, 'templates', 'base.html')
    with open(base, 'r', encoding='utf-8') as f:
        html = f.read()
    # New navbar button present
    assert 'id="pwa-install-btn"' in html
    # Old bottom banner must be gone — banner element + its dismiss button
    assert 'pwa-install-banner' not in html
    assert 'pwa-dismiss-btn' not in html
    # Sanity: button sits inside the navbar block
    nav_start = html.find('<nav class="navbar"')
    nav_end = html.find('</nav>', nav_start)
    assert nav_start != -1 and nav_end != -1
    btn_pos = html.find('id="pwa-install-btn"')
    assert nav_start < btn_pos < nav_end, 'install button must live inside .navbar'


def test_base_html_has_mobile_install_menu_item():
    """The hamburger menu must contain an install menu item — on mobile the
    inline navbar button is hidden via CSS, so this is the install entry point.
    Both UI entries call the same triggerInstall() handler."""
    base = os.path.join(REPO_ROOT, 'templates', 'base.html')
    with open(base, 'r', encoding='utf-8') as f:
        html = f.read()
    # Menu item exists and lives inside the mobile-nav drawer
    assert 'id="pwa-install-menu-item"' in html
    mobile_nav_start = html.find('class="mobile-nav"')
    mobile_nav_end = html.find('</div>', mobile_nav_start)
    assert mobile_nav_start != -1 and mobile_nav_end != -1
    item_pos = html.find('id="pwa-install-menu-item"')
    assert mobile_nav_start < item_pos < mobile_nav_end, \
        'install menu item must live inside .mobile-nav'
    # Desktop button hidden at ≤768px (CSS media query)
    assert '#pwa-install-btn { display: none !important; }' in html \
        or '#pwa-install-btn{display:none !important;}' in html.replace(' ', '')
    # Single shared handler — not two duplicate install blocks
    assert html.count('deferredPrompt.prompt()') == 1


def test_ios_instructions_modal_present():
    """iOS modal markup must still be in base.html — reused by the new button."""
    base = os.path.join(REPO_ROOT, 'templates', 'base.html')
    with open(base, 'r', encoding='utf-8') as f:
        html = f.read()
    assert 'id="pwa-ios-modal"' in html
    assert 'id="pwa-ios-close"' in html
    assert 'id="pwa-ios-ok"' in html
    # The Hebrew step text — verifies the modal body wasn't accidentally stripped.
    assert 'הוסף למסך הבית' in html


def test_sw_never_caches_api():
    """Static analysis of sw.js — the file must contain the /api/* network-only
    guard. A regression here would be the worst kind of bug (stale financial
    data), so we assert the literal guard exists."""
    sw = os.path.join(REPO_ROOT, 'static', 'sw.js')
    with open(sw, 'r', encoding='utf-8') as f:
        src = f.read()
    assert "url.pathname.startsWith('/api/')" in src
    assert 'event.respondWith(fetch(req))' in src
    # And the cache list must not include any /api/ entry.
    assert '/api/' not in src.split('SHELL_ASSETS')[1].split(']')[0]
