"""Billing motor layer D: the payment-reminder email (cron-driven, daily 08:30 IL).

Emails every ACTIVE-billed manager who is in the paywall WARNING state — unpaid
this month, inside the grace window. Locked managers get nothing (they already
see the lock screen); exempt/paid/ok managers get nothing. ONE email per
manager per month, tracked in manager_billing.reminder_sent_month (set only
after SMTP accepts the send, so a failed send retries the next morning).

State selection REUSES the paywall state machine exactly — the same
mb.active=1 AND u.active=1 join as _billing_alert_pass, then
_billing_state(...) == 'warning'. No new state math, and ZERO SUMIT calls:
_billing_state reads only local manager_billing rows.

Transport: Gmail SMTP (smtp.gmail.com:587, STARTTLS) as kupashkufaa@gmail.com.
Creds from .env: BILLING_GMAIL_USER + BILLING_GMAIL_APP_PASSWORD.
(Deliberately NOT GMAIL_APP_PASSWORD — that var already holds
makoletdashboard@gmail.com's IMAP password for gmail_agent.)

Gates, in order (same cron+gate pattern as billing_sweep):
  - BILLING_REMINDER_ENABLED (default TRUE) — kill switch.
  - IL hour == BILLING_REMINDER_HOUR (default 08). Cron on the UTC box fires
    at both 05:30 and 06:30 UTC; this gate lets exactly one run through at
    08:30 IL year-round across DST shifts.

DRY-RUN: when creds are missing OR BILLING_REMINDER_DRY_RUN != 'false', the
job logs "would send to X" instead of sending and does NOT set the
once-per-month flag. Staging runs dry by default; prod must explicitly set
BILLING_REMINDER_DRY_RUN=false.

SMTP failure: log + ONE 🟠 brrr for the whole run (never crashes the job);
the unsent managers retry tomorrow because their flag was never set.
"""
import os
import smtplib
import sys
from email.message import EmailMessage
from email.utils import formataddr

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# IL hour the daily reminder runs at. Overridable for on-demand staging runs
# (BILLING_REMINDER_HOUR=<current IL hour> to force a run outside 08:xx).
REMINDER_HOUR = 8

SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_TIMEOUT = 30

SENDER_NAME = 'קופה שקופה'

SUBJECT = 'קופה שקופה — תזכורת תשלום'

BODY = '''\
שלום {name},

זוהי הודעה אוטומטית ממערכת קופה שקופה.

המנוי שלך לחודש זה טרם שולם. כדי להמשיך להשתמש במערכת ללא הפרעה, יש להסדיר את התשלום בימים הקרובים.

לתשלום היכנסו לעמוד החשבון שלכם:
https://app.makoletdashboard.com/account

התשלום מאובטח ולוקח פחות מדקה. קבלה תישלח למייל אוטומטית.

לשאלות או בעיות: kupashkufaa@gmail.com | 052-3455860

תודה,
קופה שקופה
'''


def _smtp_creds():
    return (os.environ.get('BILLING_GMAIL_USER', '').strip(),
            os.environ.get('BILLING_GMAIL_APP_PASSWORD', '').strip())


def _dry_run():
    """Dry unless creds are present AND BILLING_REMINDER_DRY_RUN=false —
    missing config can never cause a real send."""
    user, password = _smtp_creds()
    if not user or not password:
        return True
    return os.environ.get('BILLING_REMINDER_DRY_RUN', 'true').strip().lower() \
        not in ('false', '0', 'no')


def _send_email(to_addr, name):
    """One real SMTP send. Raises on any failure — the caller decides what a
    failure means (no flag, one brrr per run)."""
    user, password = _smtp_creds()
    msg = EmailMessage()
    msg['Subject'] = SUBJECT
    msg['From'] = formataddr((SENDER_NAME, user))
    msg['To'] = to_addr
    msg.set_content(BODY.format(name=name))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def run_pass(db):
    """Select + send + mark. Returns a summary dict.

    Selection is the paywall's own machinery: the _billing_alert_pass join
    (mb.active=1 AND u.active=1) filtered to _billing_state == 'warning'.
    locked/exempt/ok are excluded by the state itself. The once-per-month
    dedup (reminder_sent_month == current month) is checked AFTER the state
    filter and set per-send, committed immediately, only on SMTP success.
    """
    from app import _billing_state, _billing_today
    from utils.notify import notify

    month = _billing_today().strftime('%Y-%m')
    dry = _dry_run()
    sent, would_send, failed = [], [], []
    skipped_already_sent = 0

    for row in db.execute(
            "SELECT mb.user_id, mb.reminder_sent_month, u.name, u.email, u.role "
            "FROM manager_billing mb JOIN users u ON u.id = mb.user_id "
            "WHERE mb.active = 1 AND u.active = 1").fetchall():
        st = _billing_state(row['user_id'], row['role'], row['email'], db)
        if st.get('state') != 'warning':
            continue
        if row['reminder_sent_month'] == month:
            skipped_already_sent += 1
            continue
        name = row['name'] or row['email']
        if dry:
            print(f"[billing-reminder] DRY-RUN — would send to "
                  f"{name} <{row['email']}> (days_left={st.get('days_left')})")
            would_send.append((row['user_id'], name, row['email']))
            continue
        try:
            _send_email(row['email'], name)
        except Exception as e:
            print(f"[billing-reminder] send FAILED for {name} "
                  f"<{row['email']}>: {e}")
            failed.append((row['user_id'], name, str(e)))
            continue
        db.execute(
            "UPDATE manager_billing SET reminder_sent_month=? WHERE user_id=?",
            (month, row['user_id']))
        db.commit()
        print(f"[billing-reminder] sent to {name} <{row['email']}>")
        sent.append((row['user_id'], name, row['email']))

    if failed:
        first_err = failed[0][2][:200]
        notify('Billing reminder emails failed',
               f'{len(failed)} of {len(failed) + len(sent)} payment-reminder '
               f'emails failed this morning (first error: {first_err}). '
               f'Unsent managers retry tomorrow.', medium=True)

    return {'month': month, 'dry_run': dry, 'sent': sent,
            'would_send': would_send, 'failed': failed,
            'skipped_already_sent': skipped_already_sent}


def run_reminder():
    """Returns 'disabled' | 'outside-window' | 'ok' | 'failed'."""
    if os.environ.get('BILLING_REMINDER_ENABLED', 'true').strip().lower() in (
            'false', '0', 'no'):
        print('[billing-reminder] BILLING_REMINDER_ENABLED=false — skipped')
        return 'disabled'

    from app import app, get_db, _now_il

    reminder_hour = int(
        os.environ.get('BILLING_REMINDER_HOUR', str(REMINDER_HOUR))
        or REMINDER_HOUR)
    hour = _now_il().hour
    if hour != reminder_hour:
        print(f'[billing-reminder] not the daily {reminder_hour:02d}:xx IL '
              f'slot (hour={hour}) — skipped')
        return 'outside-window'

    with app.app_context():
        res = run_pass(get_db())
        print(f"[billing-reminder] {'DRY-RUN ' if res['dry_run'] else ''}done — "
              f"month={res['month']} sent={len(res['sent'])} "
              f"would_send={len(res['would_send'])} "
              f"failed={len(res['failed'])} "
              f"already_sent_this_month={res['skipped_already_sent']}")
        return 'failed' if res['failed'] else 'ok'


if __name__ == '__main__':
    rc = run_reminder()
    sys.exit(0 if rc in ('ok', 'disabled', 'outside-window') else 1)
