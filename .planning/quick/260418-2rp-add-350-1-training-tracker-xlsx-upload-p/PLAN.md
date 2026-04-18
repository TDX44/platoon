---
status: in_progress
created: "2026-04-18"
quick_id: 260418-2rp
slug: add-350-1-training-tracker-xlsx-upload-p
---

# Add 350-1 Training Tracker Upload

Implement a scoped MVP for uploading the standard 350-1 Training Tracker XLSX and rendering the latest import as an HTML training dashboard.

## Tasks

- Add SQLite tables for training imports, requirements, and per-person training records.
- Add server-side XLSX parsing for the fixed tracker template using standard library ZIP/XML parsing.
- Add authenticated training APIs for viewing the latest import and admin-only tracker upload.
- Add a Training screen to the SPA with upload controls, summary cards, filters, and a clean table.
- Verify parsing against the provided example workbook and run syntax checks.
