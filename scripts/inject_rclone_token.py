#!/usr/bin/env python3
"""Inject an `rclone authorize` result into a remote WITHOUT printing the token.

Modern `rclone authorize "drive" "..."` emits a base64 "paste" blob (between the
'--->' and '<---End paste' markers), NOT a bare {access_token:...} JSON line. This
reads the captured raw output, decodes that blob, extracts the inner token JSON,
and runs `rclone config update <remote> token ...` so the credential goes file ->
config via code only — never to screen, logs, or chat. Then it shreds the raw
capture. Used for first setup and for future token rotations.

Usage: python3 scripts/inject_rclone_token.py [RAW_FILE] [REMOTE]
  RAW_FILE default: /root/rclone_authorize_raw.txt
  REMOTE   default: gdrive
"""
import sys, os, re, base64, json, subprocess

RAW = sys.argv[1] if len(sys.argv) > 1 else "/root/rclone_authorize_raw.txt"
REMOTE = sys.argv[2] if len(sys.argv) > 2 else "gdrive"

raw = open(RAW).read()
m = re.search(r"--->\s*(.*?)\s*<---End paste", raw, re.S)
if not m:
    sys.exit(f"ERROR: no rclone paste-blob found between markers in {RAW}")

blob = "".join(m.group(1).split())            # strip any wrapped whitespace/newlines
pad = "=" * (-len(blob) % 4)                   # restore base64 padding if trimmed
try:
    decoded = base64.urlsafe_b64decode(blob + pad)
except Exception:
    decoded = base64.b64decode(blob + pad)

token = json.loads(decoded)["token"]           # inner JSON string rclone stores

subprocess.run(
    ["rclone", "config", "update", REMOTE, "token", token],
    check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)

# Shred the raw capture so the token doesn't linger on disk.
try:
    subprocess.run(["shred", "-u", RAW], check=True)
except Exception:
    os.remove(RAW)

print(f"token injected into '{REMOTE}' and raw capture removed (token never printed)")
