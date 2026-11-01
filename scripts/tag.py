#!/usr/bin/env python3
"""Tag only the checked main source; preserve every existing version tag."""
import os
import json
from pathlib import Path
import re
import subprocess
import tomllib


def main():
    sha = os.environ['CANDIDATE_SHA']
    if not re.fullmatch(r'[0-9a-f]{40}', sha):
        raise ValueError('invalid checked candidate SHA')
    repository = os.environ['GITHUB_REPOSITORY']
    run = os.environ['CHECKED_RUN']
    if not re.fullmatch(r'[0-9]+', run):
        raise ValueError('invalid checked workflow run')
    # Public CI evidence needs no additional workflow permission.
    response = subprocess.run(['curl', '-fsSL', '--max-time', '30', f'https://api.github.com/repos/{repository}/actions/runs/{run}/jobs?per_page=100'], check=True, text=True, capture_output=True)
    jobs = json.loads(response.stdout)['jobs']
    required = ['tests', 'rust (ubuntu-latest)', 'rust (macos-latest)', 'rust (windows-latest)']
    if any(not any(j['name'] == name and j['conclusion'] == 'success' and j['head_sha'] == sha for j in jobs) for name in required):
        raise ValueError('exact candidate lacks successful Linux, macOS and Windows checks')
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], check=True, text=True, capture_output=True).stdout.strip()
    if head != sha:
        raise ValueError('checkout differs from checked candidate')
    subprocess.run(['git', 'merge-base', '--is-ancestor', sha, 'origin/main'], check=True)
    version = tomllib.loads((Path(__file__).resolve().parents[1] / 'Cargo.toml').read_text())['package']['version']
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?', version):
        raise ValueError('invalid crate version')
    tag = f'refs/tags/v{version}'
    probe = ['git', 'ls-remote', '--exit-code', '--tags', 'origin', tag]
    existing = subprocess.run(probe, text=True, capture_output=True)
    if existing.returncode == 0:
        print(f'{tag} exists; preserved without replacement')
        return
    if existing.returncode != 2:
        existing.check_returncode()
    subprocess.run(['gh', 'api', '--method', 'POST', f'repos/{repository}/git/refs',
                    '-f', f'ref={tag}', '-f', f'sha={sha}'], check=True)
    readback = subprocess.run(probe, check=True, text=True, capture_output=True).stdout.split()
    if readback != [sha, tag]:
        raise ValueError('created tag readback differs from checked candidate')
    print(f'{tag} -> {sha}')


if __name__ == '__main__':
    main()
