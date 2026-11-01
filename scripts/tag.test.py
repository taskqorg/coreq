#!/usr/bin/env python3
"""A8/R7: failed checks or reads cannot publish; existing tags never move."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('tag', Path(__file__).with_name('tag.py'))
tag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tag)
SHA = 'a' * 40
REF = 'refs/tags/v' + tag.tomllib.loads((Path(__file__).resolve().parents[1] / 'Cargo.toml').read_text())['package']['version']


def result(code=0, stdout=''):
    return subprocess.CompletedProcess([], code, stdout=stdout, stderr='')


class Tag(unittest.TestCase):
    def run_tag(self, replies):
        jobs = [{'name': name, 'conclusion': 'success', 'head_sha': SHA} for name in
                ['tests', 'rust (ubuntu-latest)', 'rust (macos-latest)', 'rust (windows-latest)']]
        with patch.dict(tag.os.environ, {'CANDIDATE_SHA': SHA, 'GITHUB_REPOSITORY': 'example/fixtureq', 'CHECKED_RUN': '123'}), \
                patch.object(tag.subprocess, 'run', side_effect=[result(stdout=tag.json.dumps({'jobs': jobs})), *replies]) as run:
            tag.main()
            return run.call_args_list

    def test_only_missing_tag_is_created_and_exact_sha_is_read_back(self):
        calls = self.run_tag([result(stdout=SHA), result(), result(2), result(), result(stdout=f'{SHA}\t{REF}\n')])
        self.assertEqual(calls[4].args[0], ['gh', 'api', '--method', 'POST', 'repos/example/fixtureq/git/refs', '-f', f'ref={REF}', '-f', f'sha={SHA}'])
        self.assertEqual(calls[-1].args[0], ['git', 'ls-remote', '--exit-code', '--tags', 'origin', REF])

    def test_existing_tag_is_preserved_even_at_another_commit(self):
        calls = self.run_tag([result(stdout=SHA), result(), result(stdout=f'{"b"*40}\t{REF}\n')])
        self.assertEqual(len(calls), 4)

    def test_unknown_remote_and_non_main_candidate_cannot_publish(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_tag([result(stdout=SHA), result(), result(128)])
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_tag([result(stdout=SHA), subprocess.CalledProcessError(1, ['git', 'merge-base'])])
        with self.assertRaises(ValueError):
            self.run_tag([result(stdout='b'*40)])

    def test_wrong_readback_is_not_success(self):
        with self.assertRaises(ValueError):
            self.run_tag([result(stdout=SHA), result(), result(2), result(), result(stdout=f'{"b"*40}\t{REF}\n')])

    def test_missing_or_other_sha_platform_checks_cannot_publish(self):
        for jobs in [[], [{'name': 'tests', 'conclusion': 'success', 'head_sha': 'b'*40}]]:
            with patch.dict(tag.os.environ, {'CANDIDATE_SHA': SHA, 'GITHUB_REPOSITORY': 'example/fixtureq', 'CHECKED_RUN': '123'}), \
                    patch.object(tag.subprocess, 'run', return_value=result(stdout=tag.json.dumps({'jobs': jobs}))) as run:
                with self.assertRaises(ValueError):
                    tag.main()
                self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
