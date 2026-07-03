# בטל מנוי — phase-2 spec (cancel button on /account)

Phase-1 research: 2026-07-03. **This is a spec — no write code exists yet.**
This will be MakoletChain's FIRST write against SUMIT.

## SUMIT API findings (from the official swagger + live read-only probes)

### The cancel endpoint exists

`POST /billing/recurring/cancel/` — "Cancel customer item"

Required body:
| field | type | notes |
|---|---|---|
| `Credentials` | object | CompanyID + APIKey, as everywhere |
| `Customer` | Typed_Customer | pass `{ID: <SUMIT customer id>}` ONLY — see safety |
| `RecurringCustomerItemID` | int64 | "מזהה כרטיס הוראת קבע" — the standing-order id |

Response `Data` is an EMPTY object — success is `Status == 0`; failures come as
`Status != 0` + `UserErrorMessage`.

### Finding the recurring id (read-only)

`POST /billing/recurring/listforcustomer/` — body `Customer` (+ optional
`IncludeInactive`, default false). Returns `RecurringItems[]`:
`ID` (the RecurringCustomerItemID to cancel), `Item` (product: name/price),
`UnitPrice`, `Quantity`, `Date_Start/Last/NextBilling/PreviousBilling`,
`Status`, `Description`.

`Status` enum: `Active(0), Cancelled(1), DisabledFailedBillingPayment(3),
FinishedExpired(9), GracePeriod(11), PendingForFirstPayment(12),
CancelledByCustomer(13), PendingRetry(14)`.

"Cancelled" is therefore directly visible in data we can already read:
the item flips out of the default (active-only) list and shows
`Status: Cancelled` with `IncludeInactive=true`. That is our post-cancel
verification read.

### Live probe result (2026-07-03, scripts/recurring_probe.py)

All 5 test customers (tags 26/29/30/31/33) have **0 recurring items** — the ₪1
demo page charges ONE-TIME. Each has a saved payment method (masked card), so
SUMIT retains the card, but no standing order exists. Example read (tag 33 →
customer 2095409276): `recurring items: 0`, saved card `…1016 exp 9/2030`.

**Consequence:** the monthly-recharge model requires the real ₪179 page to be a
RECURRING (הוראת קבע) page in SUMIT. Phase 2 cannot be tested — or shipped —
until one real subscription exists. First step of phase 2: create the ₪179
recurring page, run one ₪1-style recurring test payment, and re-run
`scripts/recurring_probe.py` to see the live `RecurringCustomerItem`.

### Idempotency / double-cancel — UNDOCUMENTED

The spec says nothing about cancelling twice or cancelling a non-existent
`RecurringCustomerItemID`. Assumed behavior is `Status != 0` +
`UserErrorMessage`, but phase 2 MUST verify empirically on a disposable test
subscription before the button ships: (a) cancel → re-cancel same id,
(b) cancel bogus id, (c) cancel an id belonging to ANOTHER customer (expect
refusal — this is a security property we rely on).

### Safety quirk to respect

`Typed_Customer` auto-creates entities: "Leave empty to create a new entity".
NEVER pass name/email in the cancel call — pass `{ID: <resolved id>}` only, so
a typo can't spawn a new customer card.

## UI

- `/account`, below the hero, visible only when `active=1` AND a live recurring
  item exists for the manager. To respect the SUMIT API quota, the
  recurring-item check at render time is cached per user (1h TTL, in-process
  dict like the layer-A rate limit); the click-time flow re-resolves fresh.
- Click → confirm dialog (mkAlert style):
  "המנוי יבוטל, הגישה נשמרת עד סוף החודש ששולם. לבטל?" → [ביטול מנוי] [חזרה]
- After success: grey hero "המנוי בוטל — הגישה נשמרת עד סוף החודש" + the pay
  button becomes "חידוש מנוי" (the ordinary tagged pay link — re-subscribing is
  just paying again).

## Server flow (`POST /api/account/cancel-subscription`)

1. `login_required`; role must be manager; rate-limit 1/min per user.
2. Resolve tag = `str(session['user_id'])` — NEVER from the request body.
3. Resolve the SUMIT customer id from the tag via the receipt-document join
   (same proven path as the sync). No receipt/customer → 404 "לא נמצא מנוי".
4. `listforcustomer {ID}` → filter `Status == Active` items matching OUR
   product (Item name/price == the ₪179 product). Require EXACTLY ONE — zero →
   404; more than one → 409 + brrr 🟠, no cancel (never guess).
5. `cancel {Customer: {ID}, RecurringCustomerItemID}` → require `Status == 0`.
6. Verification read: `listforcustomer` again → the item must be gone from the
   active list. Only then:
7. `manager_billing.cancelled_at = today` (new column, migration 038) + row in
   the new `billing_cancellations` log table (user_id, sumit_customer_id,
   recurring_item_id, cancelled_at, api_status) + brrr 🟡
   "Manager X cancelled their subscription".

## State machine change (`_billing_state`)

New terminal-ish state `cancelled`, checked right after the `active` check:

- `cancelled_at` set AND paid this month → `cancelled_grace`: full access, NO
  warning banner, grey /account hero. (Access remains until end of paid month.)
- `cancelled_at` set AND month > paid month → `exempt` + the sweep flips
  `active=0` (one-time, logged). The manager simply lapses — never
  warning/locked, no red banner.
- Layer-C alerts skip rows with `cancelled_at` (except the single 🟡 at cancel
  time, sent by the endpoint itself).

## Safety rails (summary)

- The cancel may ONLY target a recurring id resolved server-side from the
  session user's own tag — assert like the pay-link assert.
- `utils/sumit.py`: the read allowlist and write-tripwire stay UNTOUCHED. The
  cancel gets its own dedicated `cancel_recurring(customer_id, item_id)`
  function with a private single-endpoint allowlist
  (`{'/billing/recurring/cancel/'}`) — the generic `_post` still refuses every
  write, so no other write can ride in on this exception.
- Every attempt (success or fail) logged in `billing_cancellations`.
- Fail-open unchanged: a SUMIT error on cancel returns a friendly error and
  changes NOTHING locally.

## Phase-2 test list

1. Endpoint: happy path — mocked SUMIT returns one active item; cancel called
   with exactly that id; `cancelled_at` written; log row; 🟡 notify.
2. Tag isolation: session user A can never cancel B's item (forged body params
   ignored; resolution from session only).
3. Zero active items → 404, no write, nothing logged as success.
4. Two active items → 409 + 🟠, NO cancel call issued.
5. SUMIT cancel returns Status != 0 → no local write, friendly error.
6. Verification-read still shows the item active → treat as failure (no local
   write) + 🟠.
7. State machine: `cancelled_grace` this month (no banner), lapse next month
   (exempt + active auto-off, no lock/red).
8. Layer-C: no warning/lock alerts for cancelled rows; the cancel 🟡 fires once.
9. Rate limit on the endpoint.
10. utils/sumit tripwire: generic `_post('/billing/recurring/cancel/')` still
    REFUSES (the exception is only the dedicated function).
11. Live (staging): create a real ₪179-page recurring subscription → probe →
    cancel via the button → verify Status flips to Cancelled(1) — plus the
    empirical double-cancel/bogus-id/foreign-id matrix above.

## Open SUMIT gaps

- No recurring object exists yet for any test customer (one-time demo page) —
  the real ₪179 RECURRING page is a hard prerequisite.
- Double-cancel semantics undocumented (empirical test required).
- `Cancelled(1)` vs `CancelledByCustomer(13)`: which one an API cancel produces
  is undocumented — check on the first live test.
- The ActionsBilling quota (hit on 2026-07-03) prices every render-time
  recurring check — hence the 1h cache; confirm quota size with SUMIT support.
