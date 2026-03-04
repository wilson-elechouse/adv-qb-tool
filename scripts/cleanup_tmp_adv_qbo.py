#!/usr/bin/env python3
import argparse
import json
import os
import time
from pathlib import Path
from typing import List, Tuple


def now_ts() -> float:
    return time.time()


def list_files(root: Path) -> List[Path]:
    out = []
    if not root.exists():
        return out
    for p in root.rglob('*'):
        if p.is_file():
            out.append(p)
    return out


def file_info(paths: List[Path]) -> List[Tuple[Path, float, int]]:
    info = []
    for p in paths:
        try:
            st = p.stat()
            info.append((p, st.st_mtime, st.st_size))
        except Exception:
            continue
    return info


def bytes_h(n: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    x = float(n)
    i = 0
    while x >= 1024 and i < len(units) - 1:
        x /= 1024
        i += 1
    return f"{x:.2f}{units[i]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='tmp/adv-qbo')
    ap.add_argument('--retention-days', type=int, default=5)
    ap.add_argument('--keep-recent-runs', type=int, default=100)
    ap.add_argument('--max-files', type=int, default=5000)
    ap.add_argument('--max-size-mb', type=int, default=2048)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--out', default='tmp/adv-qbo/cleanup_summary.json')
    args = ap.parse_args()

    root = Path(args.root).resolve()
    cutoff = now_ts() - args.retention_days * 86400

    if not root.exists():
        out = {'ok': True, 'root': str(root), 'note': 'root_not_found', 'deleted': 0}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    all_files = list_files(root)
    infos = file_info(all_files)
    total_size = sum(sz for _, _, sz in infos)
    total_files = len(infos)

    # keep recent run dirs
    run_dirs = [d for d in root.glob('run*') if d.is_dir()]
    run_dirs.sort(key=lambda d: d.stat().st_mtime if d.exists() else 0, reverse=True)
    keep_runs = set(str(d.resolve()) for d in run_dirs[:max(0, args.keep_recent_runs)])

    candidates = []
    protected = []
    for p, mt, sz in infos:
        sp = str(p.resolve())
        # protect archive and newest kept runs
        if 'archive' in p.parts:
            protected.append((p, mt, sz, 'archive'))
            continue
        parent_run = next((kr for kr in keep_runs if sp.startswith(kr + os.sep) or sp == kr), None)
        if parent_run:
            protected.append((p, mt, sz, 'recent_run'))
            continue
        if mt < cutoff:
            candidates.append((p, mt, sz, 'retention'))

    # pressure cleanup if over thresholds
    over_files = total_files > args.max_files
    over_size = total_size > args.max_size_mb * 1024 * 1024
    if over_files or over_size:
        # include oldest non-protected files until under threshold
        non_protected = [(p, mt, sz, 'pressure') for p, mt, sz in infos if all(str(p.resolve()) != str(x[0].resolve()) for x in protected)]
        non_protected.sort(key=lambda x: x[1])
        cur_files, cur_size = total_files, total_size
        already = set(str(x[0].resolve()) for x in candidates)
        for it in non_protected:
            if cur_files <= args.max_files and cur_size <= args.max_size_mb * 1024 * 1024:
                break
            p, mt, sz, _ = it
            sp = str(p.resolve())
            if sp in already:
                cur_files -= 1
                cur_size -= sz
                continue
            candidates.append((p, mt, sz, 'pressure'))
            already.add(sp)
            cur_files -= 1
            cur_size -= sz

    deleted = []
    freed = 0
    for p, mt, sz, reason in sorted(candidates, key=lambda x: x[1]):
        if args.dry_run:
            deleted.append({'path': str(p), 'size': sz, 'reason': reason, 'dry_run': True})
            freed += sz
            continue
        try:
            p.unlink(missing_ok=True)
            deleted.append({'path': str(p), 'size': sz, 'reason': reason})
            freed += sz
        except Exception as e:
            deleted.append({'path': str(p), 'size': sz, 'reason': reason, 'error': str(e)})

    # remove empty dirs
    if not args.dry_run:
        for d in sorted(root.rglob('*'), reverse=True):
            if d.is_dir():
                try:
                    next(d.iterdir())
                except StopIteration:
                    d.rmdir()
                except Exception:
                    pass

    summary = {
        'ok': True,
        'root': str(root),
        'retention_days': args.retention_days,
        'keep_recent_runs': args.keep_recent_runs,
        'max_files': args.max_files,
        'max_size_mb': args.max_size_mb,
        'dry_run': args.dry_run,
        'before': {
            'files': total_files,
            'size_bytes': total_size,
            'size_human': bytes_h(total_size),
        },
        'deleted_count': len(deleted),
        'freed_bytes': freed,
        'freed_human': bytes_h(freed),
        'deleted': deleted[:500],
        'deleted_truncated': len(deleted) > 500,
        'ts': int(now_ts()),
    }

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
