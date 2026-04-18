---
status: complete
completed: "2026-04-18T18:05:23Z"
---

# Summary

Renamed the training dashboard "fix now" wording to "expired" and changed the related status label from "Overdue" to "Expired".

Redesigned downloaded 350-1 report HTML as a standalone webpage with embedded CSS, a report header, responsive layout, readable report cards, aligned training tables, and print rules. The download still uses the generated report content and active filters.

Verification:

- `node --check /tmp/platoon-index-scripts.js`
- `git diff --check -- index.html .planning/quick/260418-180353-redesign-standalone-training-report/PLAN.md`
