"""Billing motor layer D: the payment-reminder email (cron-driven, daily 08:30 IL).

Emails every ACTIVE-billed manager who is in the paywall WARNING state AND
within REMINDER_DAYS_LEFT days of lock — the mail lands on the
2-days-before-lock morning, not on day 1 of warning. Locked managers get
nothing (they already see the lock screen); exempt/paid/ok get nothing;
early-warning managers wait for their final-stretch morning. ONE email per
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
import html
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

# Send only on the final-stretch mornings: days_left <= 2 (2 days before lock).
# '<=' rather than '==' so a manager who crossed the threshold before the job
# existed — or while it was down — still gets their one reminder.
REMINDER_DAYS_LEFT = 2

SMTP_HOST = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_TIMEOUT = 30

SENDER_NAME = 'קופה שקופה'

SUBJECT = 'קופה שקופה — תזכורת תשלום'

ACCOUNT_URL = 'https://app.makoletdashboard.com/account'

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

# ── Lock-notification email (sent by the 09:10 SWEEP, not this 08:30 job) ──
# Fired via run_lock_pass() from scripts/billing_sweep.py right after the
# transition alerts, on the same fresh post-sync state. Same sender, same
# dry-run convention, same once-per-month dedup pattern (migration 040).

LOCKED_SUBJECT = 'קופה שקופה — הגישה הושהתה'

LOCKED_BODY = '''\
שלום {name},

הגישה למערכת קופה שקופה הושהתה זמנית עקב אי-תשלום המנוי החודשי.

הגישה תחודש באופן מיידי לאחר ביצוע התשלום:
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


def _body_html(name, body=BODY):
    """The given body as minimal RTL HTML — Hebrew plain text renders
    left-aligned in Gmail, so the real part is a single dir=rtl div.
    Paragraphs (blank-line separated) → <p>, the /account URL → a plain
    clickable link. No images, no styling beyond direction/font."""
    paras = []
    for para in body.format(name=name).strip().split('\n\n'):
        esc = html.escape(para).replace('\n', '<br>')
        esc = esc.replace(ACCOUNT_URL,
                          f'<a href="{ACCOUNT_URL}">{ACCOUNT_URL}</a>')
        paras.append(f'<p style="margin:0 0 1em">{esc}</p>')
    return ('<div dir="rtl" style="text-align:right; '
            'font-family:Arial,sans-serif; font-size:15px; line-height:1.6">'
            + '\n'.join(paras) + '</div>')


def _send_email(to_addr, name, subject=SUBJECT, body=BODY):
    """One real SMTP send. Raises on any failure — the caller decides what a
    failure means (no flag, one brrr per run)."""
    user, password = _smtp_creds()
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = formataddr((SENDER_NAME, user))
    msg['To'] = to_addr
    # multipart/alternative: plain text stays as fallback for old clients,
    # modern clients render the RTL HTML part.
    msg.set_content(body.format(name=name))
    msg.add_alternative(_body_html(name, body), subtype='html')
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def _email_pass(db, *, label, flag_col, subject, body, selects, fail_title):
    """Shared select + send + mark loop for both billing emails.

    Selection is the paywall's own machinery: the _billing_alert_pass join
    (mb.active=1 AND u.active=1), then selects(_billing_state(...)) decides
    eligibility — locked/exempt/ok/etc. are excluded by the state itself.
    Zero SUMIT calls: _billing_state reads only local rows. The once-per-month
    dedup (flag_col == current month) is checked AFTER the state filter and
    set per-send, committed immediately, ONLY on SMTP success — failures
    retry on the next run and fire ONE 🟠 brrr for the whole pass.
    flag_col is always a code literal (mig 039/040 columns), never input.
    """
    from app import _billing_state, _billing_today
    from utils.notify import notify

    month = _billing_today().strftime('%Y-%m')
    dry = _dry_run()
    sent, would_send, failed = [], [], []
    skipped_already_sent = 0

    for row in db.execute(
            f"SELECT mb.user_id, mb.{flag_col} AS flag, "
            "u.name, u.email, u.role "
            "FROM manager_billing mb JOIN users u ON u.id = mb.user_id "
            "WHERE mb.active = 1 AND u.active = 1").fetchall():
        st = _billing_state(row['user_id'], row['role'], row['email'], db)
        if not selects(st):
            continue
        if row['flag'] == month:
            skipped_already_sent += 1
            continue
        name = row['name'] or row['email']
        detail = (f" (days_left={st['days_left']})" if 'days_left' in st
                  else f" (state={st.get('state')})")
        if dry:
            print(f"[{label}] DRY-RUN — would send to "
                  f"{name} <{row['email']}>{detail}")
            would_send.append((row['user_id'], name, row['email']))
            continue
        try:
            _send_email(row['email'], name, subject=subject, body=body)
        except Exception as e:
            print(f"[{label}] send FAILED for {name} <{row['email']}>: {e}")
            failed.append((row['user_id'], name, str(e)))
            continue
        db.execute(
            f"UPDATE manager_billing SET {flag_col}=? WHERE user_id=?",
            (month, row['user_id']))
        db.commit()
        print(f"[{label}] sent to {name} <{row['email']}>")
        sent.append((row['user_id'], name, row['email']))

    if failed:
        first_err = failed[0][2][:200]
        notify(fail_title,
               f'{len(failed)} of {len(failed) + len(sent)} emails failed '
               f'(first error: {first_err}). Unsent managers retry on the '
               f'next run.', medium=True)

    return {'month': month, 'dry_run': dry, 'sent': sent,
            'would_send': would_send, 'failed': failed,
            'skipped_already_sent': skipped_already_sent}


def run_pass(db):
    """The 08:30 payment reminder: warning-state managers within
    REMINDER_DAYS_LEFT days of lock, once per month
    (manager_billing.reminder_sent_month)."""
    return _email_pass(
        db, label='billing-reminder', flag_col='reminder_sent_month',
        subject=SUBJECT, body=BODY,
        # missing days_left can't happen for 'warning'; default 0 fails toward
        # sending — a stray mail beats a manager locking with no reminder
        selects=lambda st: (st.get('state') == 'warning'
                            and st.get('days_left', 0) <= REMINDER_DAYS_LEFT),
        fail_title='Billing reminder emails failed')


def run_lock_pass(db):
    """The lock notification — called by the 09:10 sweep (billing_sweep.py)
    right after the transition alerts, on the same fresh post-sync state.

    state == 'locked' AND locked_email_sent_month != month → the mail goes
    out the first sweep that SEES the manager locked (their transition
    morning), never repeats while they stay locked that month, and a
    pay → re-lock in a later month gets exactly one more (new month = new
    flag). An SMTP failure leaves the flag unset — retried next sweep —
    and never crashes the sweep."""
    return _email_pass(
        db, label='billing-lock-email', flag_col='locked_email_sent_month',
        subject=LOCKED_SUBJECT, body=LOCKED_BODY,
        selects=lambda st: st.get('state') == 'locked',
        fail_title='Billing lock emails failed')


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
