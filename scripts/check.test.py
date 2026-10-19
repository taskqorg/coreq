"""Shared-code/config impact must not become a documentation-only pass."""
from pathlib import Path
import runpy

select = runpy.run_path(str(Path(__file__).with_name('check.py')))['selection']
for paths, expected in [([], 'full'), (['README.md'], 'docs'), (['docs/design/enrollment.md'], 'docs'),
                        (['coreq.md'], 'full'), (['AGENTS.md'], 'full'), (['src/process.rs'], 'full'),
                        (['Cargo.lock'], 'full'), (['instrument/src/lib.rs'], 'full'),
                        (['tests/automatic_capture.rs'], 'full'), (['unknown'], 'full')]:
    assert select(paths) == expected, (paths, select(paths), expected)
print('PASS shared-code and unknown impact select full regression protection')
