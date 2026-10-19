#!/usr/bin/env python3
"""Shared-code changes use full regression protection; prose uses whitespace checks."""
import argparse
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def selection(paths):
    return 'docs' if paths and all(Path(f).suffix in {'.md', '.txt'} and f not in {'coreq.md', 'AGENTS.md', 'CLAUDE.md'} for f in paths) else 'full'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', default=os.environ.get('CHECK_BASE', 'HEAD^'))
    p.add_argument('--full', action='store_true')
    p.add_argument('--plan', action='store_true')
    a = p.parse_args()
    try:
        paths = subprocess.check_output(['git', 'diff', '--name-only', '-z', a.base], cwd=ROOT).decode().split('\0')
        paths += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT).decode().split('\0')
        scope = selection(list(filter(None, paths)))
    except (subprocess.CalledProcessError, UnicodeError):
        scope = 'full'
    if a.full or os.environ.get('CHECK_FULL') == 'true':
        scope = 'full'
    if a.plan:
        print('scope='+scope)
        return
    started = time.monotonic()
    try:
        commands = [['git', 'diff', '--check'], [os.sys.executable, 'scripts/check.test.py']]
        if scope == 'full':
            commands += [['cargo', '+stable', 'test', '--locked']]
        for command in commands:
            print('+', ' '.join(command), flush=True)
            subprocess.run(command, cwd=ROOT, check=True, timeout=max(0.001, 900-(time.monotonic()-started)))
    finally:
        print(f'Selected gate elapsed: {time.monotonic()-started:.3f}s / 900s; no retries', flush=True)


if __name__ == '__main__':
    main()
