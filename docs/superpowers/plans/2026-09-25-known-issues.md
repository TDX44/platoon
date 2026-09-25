# Known issues after the 2026-09-25 improvements deploy

Left over from the audit release (main 607e70c). Each item is small and ships
on branch `known-issues`.

| # | Issue | Fix | Status |
|---|-------|-----|--------|
| 1 | A soldier marked late, then present, counts as away *today* on Availability and in duty rotation: `_end_running_absence()` now closes a same-day absence at today, and both queries let the dates decide regardless of state. | A `completed` row whose `to_date` is today no longer covers today. It can only be one that was ended by marking present, or an overlap loser whose winner still covers today. | |
| 2 | The edit-person modal resends `status: 'present'` from a stale tab, which ends an absence another leader has just booked. | The edit save sends no status; only the mark-present paths do. | |
| 3 | `saveToHistory()` marks a report as saved before the POST succeeds, so a failed save is never retried that day. | Record it only after a successful response. | |
| 4 | `/api/cron/notify` sends serially inside one gunicorn request (30s timeout, 10s per Resend call, 2 workers). | Cap sends per run, with the rest picked up on the next 5-minute tick. The send log already makes this safe. | |
| 5 | Restore: a row that falls back to a fresh id can collide with an explicit id inserted earlier in the same restore, before sequences are resynced, and is silently skipped. | Advance each sequence past the file's maximum id before inserting. | |
| 6 | Cloudflare still has the old `application/octet-stream` header cached for `.webp`. | Purge the cache for `images/site/*.webp`. | |
| 7 | Notifications are inert: Resend needs platoonmanager.com as a sending domain, and prod needs `RESEND_API_KEY`, `NOTIFY_FROM`, `CRON_SECRET` and the `platoon-notify` timer. | Blocked on the owner's Resend plan decision (Pro, or a second team). | blocked |
| 8 | Offline / PWA write queue. | Deliberately not built: `sw.js` unregisters itself, and queued offline marks risk stale accountability. Revisit only with a real need. | won't do |
