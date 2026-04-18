# Summary

Added platoon-scoped browser paths for accountability and 350-1 training. The SPA now accepts routes such as `/2ndplatoon/accountability` and `/2ndplatoon/training`, while older query-string links still route correctly.

Training now opens in the active platoon context, includes a direct 350-1 button from the accountability view, and keeps the platoon filter in sync with the URL. Training report generation uses the active platoon and search filter, so a filtered platoon report only contains that platoon's missing, expired, and due-soon records.

Verification:

- `python3 -m py_compile server.py`
- `node --check /tmp/platoon-index-scripts.js`

Note: Flask route fallback behavior could not be exercised with `app.test_client()` in this shell because Flask is not installed in the local Python environment.
