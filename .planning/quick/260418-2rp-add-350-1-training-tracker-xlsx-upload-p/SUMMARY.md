---
status: complete
completed: "2026-04-18T07:08:06Z"
quick_id: 260418-2rp
slug: add-350-1-training-tracker-xlsx-upload-p
---

# Summary

Added a 350-1 Training tracker MVP.

## Completed

- Added SQLite storage for training imports, requirements, and records.
- Added standard-library XLSX parsing for the fixed 350-1 tracker template.
- Added admin-only tracker upload and authenticated latest-training APIs.
- Added a Training screen with upload controls, summary cards, filters, and a status-coded HTML table.
- Included training data in admin backup/restore payloads.

## Verification

- `python3 -m py_compile server.py`
- `node --check /tmp/platoon-index-script.js`
- Parsed the provided workbook: 74 personnel, 20 requirements, 1480 records.
