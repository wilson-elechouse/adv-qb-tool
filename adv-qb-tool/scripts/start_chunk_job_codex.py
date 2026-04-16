#!/usr/bin/env python3
import sys

from runtime_profile import run_profile_entry


if __name__ == "__main__":
    raise SystemExit(run_profile_entry("codex", "start_chunk_job.py", sys.argv[1:]))
