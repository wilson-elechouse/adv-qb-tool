---
name: adv-qb-update
description: Refresh the local ADV QB rules cache from Collector using the stored local credential config, update latest.json and manifest.json, and report the refreshed snapshot paths. Use when Codex needs to sync local bill-rules data before running ADV QB workflows or to verify Collector connectivity.
---

# ADV QB Update

## Overview

Use this skill to pull the latest Collector `bill-rules` into the local cache used by `adv-qb-tool`.

Default local credential config:

- `references/config/collector-config.local.json`

Default local rules cache:

- `C:/Users/wilson-acer/.codex/skills/adv-qb-tool/tmp/collector_rules_cache`

## Run

From this skill directory, run:

```bash
python scripts/update_local_db.py
```

Dry-run connectivity check:

```bash
python scripts/update_local_db.py --dry-run
```

Override output directory:

```bash
python scripts/update_local_db.py --out-dir <path>
```

## Notes

1. This skill updates the local cache and `latest.json`; it does not rewrite the bundled default `references/config/bill-rules.xnofi.json`.
2. The wrapper reuses `adv-qb-tool/scripts/refresh_bill_rules.py`.
3. If the credential config changes, edit only `references/config/collector-config.local.json`.
