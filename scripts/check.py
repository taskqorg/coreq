#!/usr/bin/env python3
"""Shared-code changes use full regression protection; prose uses whitespace checks."""
import argparse
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
SPECQ = '7e9adf57287553176d8b3be61c2b22565543dc51'  # specq commit whose contract lint gates coreq.md
LINT = ROOT / '.specq' / SPECQ / 'engineering' / 'scripts' / 'contract_lint.py'


def selection(paths):
    return 'docs' if paths and all(Path(f).suffix in {'.md', '.txt'} and f not in {'coreq.md', 'AGENTS.md', 'CLAUDE.md'} for f in paths) else 'full'


def contract_lint():
    """Fetch the pinned specq lint once, then return the command that lints coreq.md."""
    if not LINT.exists():
        dest = str(LINT.parents[2])
        for command in [['git', 'init', '-q', dest],
                        ['git', '-C', dest, 'fetch', '-q', '--depth=1', 'https://github.com/taskqorg/specq', SPECQ],
                        ['git', '-C', dest, 'checkout', '-q', 'FETCH_HEAD']]:
            subprocess.run(command, check=True, timeout=120)
    return [os.sys.executable, '-I', str(LINT), 'coreq.md']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', default=os.environ.get('CHECK_BASE', 'HEAD^'))
    p.add_argument('--full', action='store_true')
    p.add_argument('--plan', action='store_true')
    p.add_argument('--lint', action='store_true', help='run only the specq contract lint')
    a = p.parse_args()
    if a.lint:
        subprocess.run(contract_lint(), cwd=ROOT, check=True)
        return
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
        commands = [['git', 'diff', '--check'], [os.sys.executable, 'scripts/check.test.py'], contract_lint()]
        if scope == 'full':
            commands += [['cargo', '+stable', 'test', '--locked']]
        for command in commands:
            print('+', ' '.join(command), flush=True)
            subprocess.run(command, cwd=ROOT, check=True, timeout=max(0.001, 900-(time.monotonic()-started)))
    finally:
        print(f'Selected gate elapsed: {time.monotonic()-started:.3f}s / 900s; no retries', flush=True)


if __name__ == '__main__':
    main()
