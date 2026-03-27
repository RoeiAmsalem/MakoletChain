import os
import urllib.request
import urllib.parse

BRRR_URL = os.getenv('BRRR_URL', '')


def notify(title: str, message: str):
    """Send push notification to Roei's phone via brrr."""
    if not BRRR_URL:
        return
    try:
        url = f"{BRRR_URL}?title={urllib.parse.quote(title)}&message={urllib.parse.quote(message)}"
        urllib.request.urlopen(url, timeout=5)
    except Exception as e:
        print(f"brrr notification failed: {e}")
