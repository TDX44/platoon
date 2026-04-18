---
status: complete
completed: "2026-04-18T16:41:28Z"
quick_id: 260418-g5y
slug: make-350-1-training-dashboard-and-report
---

# Summary

Made the 350-1 training view more human-readable.

## Completed

- Replaced the spreadsheet matrix with platoon-grouped Soldier action rows.
- Grouped each Soldier's records into fix-now, due-soon, current, and N/A sections.
- Reworded dashboard copy and summary cards into plain leader-facing language.
- Rewrote the report as a platoon/Soldier punch list instead of a tag table.
- Kept upload, filters, inline report, print, and HTML download.

## Verification

- `python3 -m py_compile server.py`
- `node --check /tmp/platoon-index-script.js`
