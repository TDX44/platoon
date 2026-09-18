# Stripe billing — design

**Status:** draft for review. **Date:** 2026-09-17.
**Depends on:** the unit tree (`2026-09-16-unit-tree-design.md`), live in
production since 2026-09-17, and the platform admin dashboard (`/admin`).

## 1. What this builds

Each leader account pays for its own access: **2.99 a month or 19.99 a
year**, through Stripe. New accounts get a **14-day trial**, may **extend it
once by 7 days**, then get **3 days of grace**, then a **hard lock behind a
pricing screen** that sells the two prices. A global default says whether
accounts are billed at all, and the platform admin can **comp** any account
from `/admin`.

The model copies Resyrv's trial enforcement (its
`docs/superpowers/specs/2026-06-13-trial-enforcement-design.md` and the
cancellation incident in `docs/billing/subscription-cancellation-bug.md`)
with two additions Resyrv does not have: the extension and the grace period.
The state rules are one pure function with table tests so the same rules can
be ported to Resyrv afterwards.

Soldiers on the roster never sign in and are never billed. "Account" and
"user" below always mean a leader or owner account in `users`.

## 2. Decisions (recorded from the design conversation)

| # | Decision |
|---|---|
| D1 | Billing is **per account**, not per organisation. The merge and the read-only grants are separate projects and do not interact with billing. |
| D2 | Prices: one Stripe product, two prices, **2.99/month** and **19.99/year**, resolved by lookup key at runtime; no price lives in code. |
| D3 | Trial **14 days**, starting at the account's **first billed sign-in**, not at account creation, so today's five production accounts get a full trial. |
| D4 | **One** trial extension of **7 days**, one click, **no card required**, offered during the trial and during the first grace period. Never twice. |
| D5 | **3 days of grace** after the trial (or the extension), with full access and an urgent banner. |
| D6 | After grace: **hard lock**. Sign-in lands on a full-screen **pricing screen** with purchase buttons. Nothing else works. Reads are not carved out (Resyrv's lesson: a read carve-out leaked organisation data). |
| D7 | A renewing card that fails is **past due**: full access with a warning while Stripe retries; locked only when Stripe reports cancelled or unpaid. |
| D8 | `BILLING_DEFAULT` (`on` \| `off`) sets whether accounts are billed **by default**; it does not switch billing off. Each account has a tri-state `billing_mode`: `default`, `comped`, `billed`. The platform admin is always comped. |
| D9 | Cancellation is **at period end** and never flips local state; the webhook does, when Stripe reports the real transition. |
| D10 | No emails in v1 (the app has no mail sender). Banners and a once-a-day modal carry the reminders. |
| D11 | The Stripe SDK (`stripe`) is added to `requirements.txt`, pinned. Signature verification and Checkout are not worth hand-rolling on a payments path. |

## 3. States

`billing_state(row, now)` is a pure function of the account's row and the
clock (UTC). It is the only thing that decides access. Nothing stored is
trusted for the verdict except the dates and flags below.

```
effective_billed = row.billing_mode == 'billed'
               or (row.billing_mode == 'default' and BILLING_DEFAULT == 'on')

if platform admin or not effective_billed or row.billing_mode == 'comped':
    COMPED                                   # open, no banner
elif row.stripe_status in ('active', 'trialing'):
    ACTIVE                                   # open; 'trialing' only if Stripe
                                             # ever runs a card-backed trial
elif row.stripe_status == 'past_due':
    PAST_DUE                                 # open, warning banner
elif row.stripe_status in ('canceled', 'unpaid', 'incomplete_expired'):
    LOCKED (reason payment_required)
elif row.trial_ends_at is None:
    TRIAL, but the row is initialised first (see 4.2)
elif now < row.trial_ends_at:
    TRIAL   days_left = ceil((trial_ends_at - now) / 1 day)
elif now < row.trial_ends_at + GRACE_DAYS:
    GRACE   days_left likewise, from the grace end
else:
    LOCKED (reason trial_expired)
```

- A Stripe status outside the three lists above (`incomplete`, an abandoned
  checkout, or NULL) means "not subscribed" and the trial dates decide.
- `trial_ends_at` already includes the extension when it was taken
  (`extended_at` is set and `trial_ends_at` moved by `EXTENSION_DAYS`).
- The extension is **available** when `extended_at IS NULL` and the state is
  `TRIAL` or `GRACE` with reason `trial_expired`. Taking it in grace moves
  `trial_ends_at` to `now + 7 days`, so the account is back in `TRIAL`.
- `days_left` is whole days, rounded up, so "1 day left" is the last day and
  the banner never shows "0 days".
- Constants, env-overridable: `TRIAL_DAYS=14`, `EXTENSION_DAYS=7`,
  `GRACE_DAYS=3`.

Table tests cover every branch, every boundary at exactly the stamp, the
extension in trial and in grace, the extension refused when used, comped
overriding a locked Stripe status, `BILLING_DEFAULT` off with
`billing_mode='billed'`, and the platform admin.

## 4. Data

### 4.1 Table `subscriptions`

One row per account, created lazily (4.2). Tenant data: `root_id` and the
same RLS `tenant` policy as the other ten tables.

| column | type | notes |
|---|---|---|
| `user_id` | int PK, FK `users.id` ON DELETE CASCADE | |
| `root_id` | int NOT NULL | RLS |
| `billing_mode` | text NOT NULL DEFAULT 'default' | `default` \| `comped` \| `billed` |
| `trial_started_at` | timestamptz | set at the first billed sign-in |
| `trial_ends_at` | timestamptz | includes the extension once taken |
| `extended_at` | timestamptz | NULL until the one extension is used |
| `stripe_customer_id` | text UNIQUE | per Stripe mode, see 4.4 |
| `stripe_subscription_id` | text UNIQUE | |
| `stripe_status` | text | Stripe's own status string, mirrored, never invented locally |
| `stripe_price_lookup_key` | text | which price they are on |
| `current_period_end` | timestamptz | from Stripe |
| `cancel_at_period_end` | boolean NOT NULL DEFAULT false | from Stripe |
| `comped_by` | text | who flipped `billing_mode`, for the audit trail |
| `updated_at` | timestamptz NOT NULL | |

Table `stripe_events(event_id text PK, received_at timestamptz)` records
every webhook delivery id, so a replay is a no-op.

Backup v3 exports `subscriptions` with the tenant (column whitelist as for
the others); restore keeps `billing_mode`, the trial stamps and
`extended_at`, and **drops** the Stripe ids and status, because a restored
copy must not claim someone else's Stripe subscription. The restored account
therefore lands in whatever the dates say, usually `TRIAL` or `LOCKED`.

### 4.2 Row creation

In `_resolved_user()`, after `set_tenant`, for an **attached** user: if no
row exists, insert one with `ON CONFLICT DO NOTHING` (two workers can race).
If the account is effectively billed and `trial_started_at IS NULL`, stamp
`trial_started_at = now`, `trial_ends_at = now + TRIAL_DAYS`. Unattached
users have no tenant and no row; the create-unit screen is not gated.

If `BILLING_DEFAULT` is later flipped from `off` to `on`, accounts that never
had a trial start one at their next sign-in, not retroactively.

### 4.3 Cross-tenant access

The webhook has no session and no tenant. Following the `auth_*` and
`admin_*` pattern, cross-tenant work is done by SECURITY DEFINER functions in
`sql/billing_functions.sql`, EXECUTE revoked from PUBLIC and granted to
`platoon_app`:

- `billing_find_by_customer(stripe_customer_id) → (user_id, root_id)`
- `billing_apply_stripe(stripe_customer_id, stripe_subscription_id, stripe_status, price_lookup_key, current_period_end, cancel_at_period_end)` — upserts the mirrored columns by customer id and returns the user id. Only these columns; it can never touch `billing_mode` or the trial stamps.
- `billing_record_event(event_id) → boolean` — false when already recorded.
- `billing_set_mode(user_id, mode, by)` — the `/admin` comp toggle, called only behind `platform_admin_required`; the header comment and a grep test enforce that, as for `admin_*`.

No other code path may call them; a grep test ties each call site to the
decorator that guards it.

### 4.4 Stripe mode

`STRIPE_MODE=test|live`, with `STRIPE_TEST_SECRET_KEY`,
`STRIPE_LIVE_SECRET_KEY`, `STRIPE_TEST_WEBHOOK_SECRET`,
`STRIPE_LIVE_WEBHOOK_SECRET` all in `.env`. Dev runs `test`, production
`live`. Customer ids are per mode, so `stripe_customer_id` stores
`<mode>:<id>` and a mode switch simply creates a new customer on the next
checkout. The webhook verifies against the active mode's secret and answers
200 to events from the other mode without acting on them, so Stripe never
disables the endpoint.

## 5. Backend

### 5.1 The gate

The gate lives in `_resolved_user()`, which every auth decorator goes
through (`login_required`, `attached_required`, `owner_required`,
`platform_admin_required`), so a route cannot dodge it by picking a weaker
decorator. It computes `billing_state` once per request, caches it on `g`,
and when the state is `LOCKED` and the request path is not exempt, the
decorator answers **402** `{'error': 'subscription_required', 'billing':
{...}}` before the view runs.

Exempt path prefixes, because they are how a locked account gets out of the
lock: `/api/auth/`, `/api/me`, `/api/billing/`, `/api/admin/` (the admin is
comped anyway). The SPA and static files are not API routes and are never
gated. `GET /api/units` is **not** exempt: the pricing screen needs nothing
from it. `tests/test_smoke.py`'s
route-guard check is extended: every `/api/` route is either exempt by that
list or answers 402 for a locked account, proven with a fixture account whose
`trial_ends_at` is in the past.

### 5.2 `/api/me` and `/api/auth/sync`

Both gain `billing`:

```json
{"state": "TRIAL", "reason": null, "days_left": 9,
 "trial_ends_at": "...", "grace_ends_at": "...",
 "extension_available": true, "subscribed": false,
 "cancel_at_period_end": false, "current_period_end": null,
 "prices": [{"lookup_key": "platoon_leader_monthly", "amount": 299, "interval": "month"},
            {"lookup_key": "platoon_leader_annual",  "amount": 1999, "interval": "year"}],
 "portal_available": false}
```

Prices come from Stripe by lookup key, cached in-process for an hour, and
`[]` when Stripe is unreachable; the pricing screen then shows "Prices are
loading" and a retry, never a wrong amount.

### 5.3 Routes (all `login_required` + attached, none gated by billing)

| route | does |
|---|---|
| `POST /api/billing/extend` | the one extension; 409 when already used or not available; audit `BILLING_EXTEND` |
| `POST /api/billing/checkout {lookup_key}` | creates or reuses the Stripe customer (idempotency key `user:<id>:<mode>`), opens a hosted Checkout Session, `mode=subscription`, `client_reference_id=user_id`, metadata `user_id`, `root_id`, `mode`; success URL `/settings?billing=success`, cancel URL `/settings`; returns `{url}` |
| `POST /api/billing/portal` | Billing Portal session for card, invoices, cancel; returns `{url}`; 409 when no Stripe customer yet |
| `POST /api/billing/webhook` | see 5.4; no auth decorator, CSRF-irrelevant, signature is the auth |

Audit rows: `BILLING_EXTEND`, `BILLING_CHECKOUT_STARTED`,
`BILLING_ACTIVE`, `BILLING_PAST_DUE`, `BILLING_CANCELLED`, `BILLING_COMP`
(unit NULL, tenant-level, the way `LOGIN` rows are).

### 5.4 Webhook

1. `stripe.Webhook.construct_event` with the active mode's secret; 400 on a
   bad signature. Bodies are read raw, never through `get_json()`.
2. `billing_record_event(event.id)`; already seen → 200, stop.
3. Handled: `customer.subscription.created|updated|deleted` → `billing_apply_stripe` with Stripe's status, price lookup key, period end and `cancel_at_period_end`; `invoice.payment_failed` → status `past_due` (Stripe sends the subscription update too; both paths converge on the same row); `invoice.paid` → audit only. `checkout.session.completed` is acknowledged and ignored; the subscription events carry the truth.
4. Anything else → 200, ignored. Handler failure → 500 so Stripe retries;
   the event id is recorded only after success.

The webhook runs with no tenant and touches the database only through the
`billing_*` functions.

### 5.5 Account deletion

`delete_user` cancels the Stripe subscription immediately (best effort,
logged on failure) before the row cascades away, so nobody keeps paying for
a deleted account.

### 5.6 Admin

`GET /api/admin/overview` gains, per recent user, `billing_state` and
`billing_mode`, and totals of trialing, grace, locked, active and comped
accounts. `PUT /api/admin/users/<id>/billing_mode {mode}` behind
`platform_admin_required`, via `billing_set_mode`, logged with the admin's
`sub`.

## 6. Frontend

All new code is pure render helpers liftable under node, plus the wiring.
`currentUser.billing` is the only input.

- **Trial banner:** full-width strip above everything on every signed-in
  screen. "N days left in your trial." with "Add billing details" linking to
  Settings, Account, Billing. Not dismissible. Height is measured into a
  CSS variable so the fixed sidebar and bottom nav offset correctly.
- **Last three days:** the banner turns to the warning colour. Once per
  calendar day on the tenant's clock (`localStorage` key
  `platoon.billingSeen.<user_id>` = that date) a modal opens: "Keep your
  access", the end date, buttons "Choose a plan", "Extend my trial 7 days"
  (only while available), "Remind me tomorrow".
- **Grace banner:** danger colour. "Your trial ended. N days until access is
  paused." with "Choose a plan" and the extension link while available.
- **Past due banner:** warning colour. "We couldn't process your last
  payment." with "Update your card" (opens the portal).
- **Pricing screen:** its own screen like `#createUnitScreen`, shown by
  `routeAfterLogin()` when `state == LOCKED` and by `api()` on any 402.
  Heading by reason: "Your trial has ended." or "Your subscription is no
  longer active." Two cards, Monthly 2.99 and Annual 19.99 with "Save 44%",
  each with a purchase button that POSTs checkout and follows `url`. Below:
  "Manage billing" (portal) when a Stripe customer exists, "Sign out", and
  a line saying the roster is kept and nothing is deleted. On `?billing=
  success` the app polls `/api/me` every 2 s for up to 30 s until the state
  is `ACTIVE`, then routes home.
- **Billing page:** Settings, Account, new row "Billing" opening a card:
  state and its date ("Trial ends 1 Oct", "Grace until 4 Oct", "Renews 17
  Oct", "Ends 17 Oct, you will not be billed again"), the extension button
  while available, the two prices when not subscribed, and "Update card",
  "Invoices", "Cancel subscription" through the portal when subscribed.
  Comped accounts see "Complimentary access". Cancel goes through the
  portal; the app never sets a cancelled state itself.
- **Admin dashboard:** Recent users gains a Billing column showing the
  state, and a comp toggle per row; the totals strip gains the five counts.

## 7. Tests

- `tests/test_billing_state.py`: table tests for `billing_state` (section
  3), every boundary at the exact stamp.
- `tests/test_billing.py` (DB-backed, `platoon_app`, RLS on): row creation
  at first attached sign-in and not before; trial does not start while
  `BILLING_DEFAULT=off`; starts at next sign-in after it flips; extension
  once, in trial and in grace, 409 afterwards; the gate: every `/api/` route
  not on the exempt list answers 402 for a locked account and 200 for an
  active, comped and grace account; `/api/me` payload; checkout and portal
  with the Stripe SDK stubbed at one seam (session created with the right
  customer, lookup key, metadata and URLs); webhook: bad signature 400,
  replay is a no-op, each handled event lands on the right row and only the
  mirrored columns change, cross-tenant: an event for tenant B's customer
  never touches tenant A and RLS still hides B's row from A; comp overrides
  a cancelled Stripe status; deletion cancels at Stripe; admin toggle audited
  and refused to non-admins (404); backup export includes the row and
  restore drops the Stripe ids.
- `tests/test_billing_js.py`: banner copy for each state, hostile strings
  never become markup, the modal shows once per day, the pricing screen
  renders both prices and no purchase button when prices are empty.
- `tests/test_mobile_layout.py`: the banner and the pricing screen at the
  three widths, no page-level overflow, the bottom nav still reachable under
  the banner.
- Every new assertion is shown red by a named mutation, the project's
  standing rule.

## 8. Rollout

Operator steps before deploy (yours, in the Stripe dashboard):

1. Create product "Platoon Accountability, leader" with two recurring
   prices, lookup keys `platoon_leader_monthly` (2.99) and
   `platoon_leader_annual` (19.99), in **both** test and live mode.
2. Add webhook endpoints `https://platoondev.carr7.com/api/billing/webhook`
   (test) and `https://platoon.carr7.com/api/billing/webhook` (live)
   subscribed to `customer.subscription.*`, `invoice.paid`,
   `invoice.payment_failed`, `checkout.session.completed`; copy the signing
   secrets.
3. Enable the Customer Portal in both modes with card update, invoices and
   cancel-at-period-end.
4. Put the keys, secrets, `STRIPE_MODE` and `BILLING_DEFAULT=on` in each
   `.env`.

Deploy is the ordinary pull and rebuild; the boot change is additive (one
table, one index, four functions). On production the five existing accounts
start a 14-day trial at their next sign-in; comp them from `/admin` at any
point in those 14 days, or leave them to pay.

Dev rehearsal: a full pass on dev in Stripe test mode with the test card,
including a failed-card renewal simulated from the Stripe dashboard, a
cancel through the portal, and a webhook replay, before production.

## 9. Out of scope

Owner-covers-their-leaders pricing, per-organisation plans, promo codes,
emails, invoices inside the app (the portal has them), a card-required
extension, refunds on account deletion, and any interaction with the
organisation merge or grants.

## 10. Amendments from the code survey (2026-09-17, before planning)

A read-only survey of the tree found thirteen places where sections 3 to 7
did not match the code as it is. These rulings win over the text above.

| # | Ruling |
|---|---|
| A1 | `platform_admin_required` does not go through `_resolved_user()` and never will (it declares no tenant). The gate lives in the three tenant decorators (`login_required`, `attached_required`, `owner_required`) through one helper, `_billing_block()`. `g.billing` is `None` on admin routes and for unattached users; nothing may assume it exists. |
| A2 | The backup stays **version 3**. The four billing columns (`billing_mode`, `trial_started_at`, `trial_ends_at`, `extended_at`) ride on each row of the existing owner-only `users` list as optional keys, ISO-8601 strings. A file without them restores as today. Restore upserts a `subscriptions` row per restored user from those keys and never writes a Stripe column. |
| A3 | Colours: the trial and past-due banners use `--cp-amber` / `--cp-amber-bg`, the grace banner `--cp-red` / `--cp-red-bg`, the ordinary trial strip `--cp-steel-bg`. No new tokens. |
| A4 | The banner is a normal-flow element, `#billingBanner`, first child of `<body>`. Nothing is measured into a CSS variable: the sidebar is sticky, not fixed, and the bottom nav is bottom-anchored, so neither needs an offset. |
| A5 | `api()` gains a 402 branch: it reads `billing` off the body into `currentUser.billing`, calls `showBillingScreen(true)` and returns `null`. It never toasts or resyncs on a 402. Sign out is the existing `doLogout()`. |
| A6 | The pricing screen and the Settings → Billing page are one screen, `#billingScreen`, rendered by `pricingScreenHtml()` (locked) or `billingPageHtml()` (everything else). The Settings row opens it without pushing history; Back calls `routeAfterLogin()`, which lands wherever the URL still says. |
| A7 | `delete_user` reads the row's `stripe_subscription_id` before the DELETE and cancels it best-effort. There is no self-delete route today, so this covers an owner removing a leader and nothing else. |
| A8 | The webhook refuses a body over `WEBHOOK_MAX_BYTES` (64 KB) with 413 before reading it, and reads it with `request.get_data(cache=False)`. The route is added to the smoke test's `PUBLIC_API` list. |
| A9 | `requirements.txt` pins `stripe==15.6.1` (the first pin in the file, on purpose: a payments SDK is not something to float). The SDK is used through its module-level API (`stripe.Price.list`, `stripe.Customer.create`, `stripe.checkout.Session.create`, `stripe.billing_portal.Session.create`, `stripe.Subscription.cancel`, `stripe.Webhook.construct_event`), with `stripe.default_http_client = stripe.RequestsClient(timeout=5)` and `stripe.max_network_retries = 0`, because gunicorn runs two sync workers. |
| A10 | Billing time is **UTC**: `billing_rules.utcnow()` is the only clock the rules read, every billing column is `timestamptz`, and the pure function takes `now` as an argument. `app_now()` stays the duty-day clock and is not used for billing. The once-a-day modal uses the frontend's `getTodayStr()` (the tenant's day) only as a `localStorage` key. |
| A11 | Prices are cached per worker with a 1 h TTL on success and a 60 s TTL on failure or on an incomplete answer (fewer than both lookup keys), the same shape as the platform-admin verdict cache. The payload is `[]` in either failure case. |
| A12 | The rules live in `billing_rules.py`, a pure module with no Flask and no database import, so `tests/test_billing_state.py` runs without Postgres and the file can be copied to Resyrv. `server.py` imports it. |
| A13 | **No Stripe key for the active mode means billing is off**: every account is `COMPED`, the app logs one warning at boot, and no trial is started. This keeps CI, a fresh checkout and a mis-set `.env` from locking anyone out. `BILLING_DEFAULT=on` only means something once a key is present. |
| A14 | A `customer.subscription.*` event whose subscription id differs from the stored one is applied only when its status is `active` or `trialing` (a new subscription supersedes), or when nothing is stored yet. A late `deleted` for a superseded subscription therefore cannot lock an account that has since resubscribed. |
| A15 | The admin overview computes each account's state in Python from a new SECURITY DEFINER `admin_billing_rows()` (mode, trial stamps, Stripe status — never a Stripe id). For that display only, an account whose stored email is in `PLATFORM_ADMIN_EMAILS` counts as comped; the real verdict is still Clerk's. |
| A16 | A failed invoice can never re-open a cancelled account: `invoice.payment_failed` carries the invoice's subscription id, and `billing_apply_stripe` refuses to move a locked status (`canceled`, `unpaid`, `incomplete_expired`) to `past_due`. Stripe does not order deliveries, and `past_due` is an open state. |
