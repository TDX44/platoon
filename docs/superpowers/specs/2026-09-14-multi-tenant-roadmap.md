# Multi-tenant Platoon — decomposition and locked decisions

**Date:** 2026-09-14
**Status:** agreed; A0 specced separately

This records the decisions made while brainstorming "support squads, positions,
and multiple companies" so they do not get re-litigated. Each project below gets
its own spec and its own implementation plan.

## Why this is four projects

The original ask was three things — squads/positions, multi-tenancy, and
billing. Reading the code changed the shape:

`PLATOONS` is a hardcoded dict of three (`server.py:63`) and `platoon TEXT` is a
column on six tables — 198 references in the backend, 142 in the frontend. A
tree of units with `parent_id` handles *both* the hierarchy ask and the
tenancy ask, because **a tenant is just a root node**. A team leader signing up
gets a root whose `kind` is `team`; a 1SG gets a root that is a company with
platoons beneath it. One scoping mechanism, not two.

Row-level security then forced a fourth: RLS needs Postgres, and doing the unit
tree in SQLite and again in Postgres is the same migration twice.

| | Project | Ships to prod on its own |
|---|---|---|
| **A0** | Postgres lift-and-shift — same schema, same behaviour | yes |
| **A1** | Unit tree, access rewrite, delegation grants, RLS policies | yes |
| **B** | Positions and chain of command | yes |
| **C** | Billing | yes |

A0 ships separately on purpose: when something breaks in A1, that tells you it
is the tree and not psycopg.

## Locked decisions

### Tenancy is a tree; a tenant is a root node

One `units` table, adjacency list, `parent_id` nullable. `root_id` is a
denormalized cache of "which tree am I in", not a truth — the tree is the truth.
Under RLS it is also the tenant discriminator, which raises its importance: any
operation that moves a subtree must rewrite `root_id` transactionally.

### Units can be grafted together

A root may become the child of another root — units task-organize and
reorganize constantly. This requires a **consent handshake**: without one,
anyone who learns a unit's identifier could claim its roster. The absorbed side
approves, and the audit log records who approved it.

The schema supports grafting from day one (`parent_id` is nullable and mutable).
The graft *flow* is deliberately not in A1 — there is one tenant today and
nothing to graft to.

### Unit kinds are labels, not a ladder

`kind` comes from a list — company, platoon, squad, team, section, detachment,
flight, crew — and drives UI copy, icons and report grouping. Nothing enforces
what nests under what. Army structure is full of exceptions (Guard units, task
organization, attachments) and an enforced ladder blocks a real unit on day one.

### One attachment, cascading down

A user is attached to exactly one unit with a role. They see and edit that unit
and everything beneath it: a team leader sees four people, a PSG sees the
platoon and its squads, a 1SG sees the company.

**There is no global admin any more.** Today `is_admin = 1` makes
`has_platoon_access()` return `True` for every platoon (`server.py:753`). Ported
into a multi-tenant app that is one flag between a user and every other
company's roster. "Admin" becomes `owner`, meaning attached at the root of your
own tree. `has_platoon_access` is deleted, not extended.

Roles are `owner` and `leader`. The column is text so `viewer` can be added
later without a migration.

### Delegation is a loan, not a second attachment

A leader can lend their level of access to a subordinate without making them the
leader — a squad leader covering down to a team leader while on leave, a 1SG to
a PSG, at every echelon.

- **Attachment** — who you are. Permanent, exactly one, implies position.
- **Grant** — what you have been lent. Temporary, many, revocable, dated, audited.

"You can only give away what you hold" falls out of `can_access(grantor, unit)`:
a squad leader can grant squad rights because they have them; a team leader
cannot.

Grants carry `from_date` / `to_date`, so the predicate is
`from_date <= today AND (to_date IS NULL OR to_date >= today)` — **the same
window predicate `_derive_state` already uses for absences**. A PSG going on
leave sets the grant window to match the leave window and it expires itself.
Same mental model, edge cases already solved once.

`can_access` = your attachment's subtree, union the subtrees of your active
grants.

### RLS guards tenants; the application guards subtrees

Policies compare `root_id` against a session variable — one indexed integer per
row. No query can cross companies even with a bug in a `WHERE` clause. Who sees
which squad *inside* a tenant stays in `can_access()`.

RLS is the blast door; the application handles the interior doors. Enforcing
subtree visibility in policy would mean a recursive check per row on every read,
and would require delegation grants to be visible to the policy.

**The footgun to remember:** table owners and superusers bypass policies
silently. The application must connect as a non-owner role, or every protected
table needs `FORCE ROW LEVEL SECURITY`. Getting this wrong looks like it works.

### URLs survive

`2ndplatoon` is currently a frontend-only map (`index.html:3703`) while the
database stores `2nd`. In A1 the slug becomes a real column, seeded from that
map, so existing bookmarks keep working.

Slugs are unique per tenant (`UNIQUE(root_id, slug)`), not globally — otherwise
the second company to sign up cannot have a "2ndplatoon" either. A URL resolves
within the signed-in user's tree, which is unambiguous because a user belongs to
exactly one tree. No tenant segment in the URL.

## Environment

Development happens at **`platoondev.carr7.com`**, not `dev.platoon.carr7.com`.
Cloudflare Universal SSL covers `carr7.com` and `*.carr7.com`, one label deep. A
two-label subdomain is not covered and needs Advanced Certificate Manager at $10
a month.

Production is never reconfigured. Dev is a parallel stack on prodsrv02:

| | prod | dev |
|---|---|---|
| checkout | `/opt/homelab/platoon` | `/opt/homelab/platoon-dev` |
| branch | `main` | the project branch |
| compose project | `platoon` | `platoon-dev` (from the directory name) |
| port | 5000 | 5001 |
| database | its own | its own |
| tunnel | existing token | **its own tunnel and token** |
| Clerk | production instance | its own instance |

A second tunnel rather than a second hostname on the existing one: adding a
hostname would mean editing production's tunnel config in order to stand up
development, and the stated requirement is that production keeps working.

A separate Clerk instance means destructive migrations get rehearsed as a test
account rather than as the real 1SG.

Only one file differs between the checkouts: a gitignored
`docker-compose.override.yml` in dev that changes the published port.

`APP_ENV=dev` renders a visible banner in the UI. It exists so nobody marks a
real soldier present on dev at 0630, or tests a destructive path on prod
believing it is dev.

## Pricing (informational — decided in C, not here)

Raised during brainstorming, recorded so the arithmetic is not redone:

| | Gross | Fee | Net |
|---|---|---|---|
| $0.99/mo via Stripe | $0.99 | 2.9% + $0.30 | **$0.66** |
| $0.99/mo via Apple IAP | $0.99 | 15% (Small Business) | $0.84 |
| $4.99/yr via Stripe | $4.99 | 2.9% + $0.30 | $4.55 |

Stripe's fixed $0.30 takes a third of a dollar subscription; Apple's
percentage-only cut is *better* at micro-pricing, which is the opposite of the
usual assumption. Apple requires IAP for digital subscriptions in an iOS app; a
pure web app is exempt.

$4.99/yr against $0.99/mo is a 58% discount where the convention is roughly two
months free. Nearly everyone would take annual. If both are offered, $0.99/mo
and $9.99/yr are consistent with each other.

Flat-per-tenant pricing means a 1SG tracking 120 soldiers pays what a team
leader with 4 pays. Good for adoption; the heaviest user is the cheapest.

## Ordering

A0 → A1 → B → C. Each gets its own spec and plan.
