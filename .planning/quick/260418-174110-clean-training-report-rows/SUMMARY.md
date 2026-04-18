---
status: complete
completed: "2026-04-18T17:43:22Z"
---

# Summary

Updated the generated 350-1 report so each Soldier appears as rank/name with platoon underneath, followed by an aligned table of training rows. Each row now has Training, Expired date, and Days delinquent columns, with missing dates called out clearly and due-soon items retaining the same aligned layout.

Verification:

- `node --check /tmp/platoon-index-scripts.js`
- `git diff --check -- index.html .planning/quick/260418-174110-clean-training-report-rows/PLAN.md`
