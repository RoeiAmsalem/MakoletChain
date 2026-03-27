import os
import urllib.request
import urllib.parse


def notify(title: str, message: str):
    """Send push notification to Roei's phone via brrr."""
    brrr_url = os.getenv('BRRR_URL', '')
    if not brrr_url:
        return
    try:
        url = f"{brrr_url}?title={urllib.parse.quote(title)}&message={urllib.parse.quote(message)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'MakoletChain/1.0'})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"brrr notification failed: {e}")
