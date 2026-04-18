---
status: complete
completed: "2026-04-18T15:43:41Z"
quick_id: 260418-eu1
slug: improve-350-1-training-dashboard-platoon
---

# Summary

Improved the 350-1 training dashboard and added the requested report.

## Completed

- Dashboard now sorts personnel by platoon, rank, and name.
- Dashboard inserts platoon divider rows so records are grouped by actual tracker platoon.
- Added a 350-1 report button.
- Report page 1 shows missing, expired, and unknown items only.
- Report page 2 shows items expiring in the next 30 days only.
- Report supports print and HTML download.

## Verification

- `python3 -m py_compile server.py`
- `node --check /tmp/platoon-index-script.js`
