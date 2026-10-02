"""Portable migration identity/path checks with synthetic data; zero network."""
import copy
import csv
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
CORE = PROJECT / 'scripts/dataset'
sys.path.insert(0, str(CORE))
from check_metrics_retention import load_cases, digest, read_json, write_json
from source_paths import SourcePaths, guard_output, load_mappings
import build_dataset

spec = importlib.util.spec_from_file_location('migration_synthetic_demo', PROJECT / 'examples/migration/synthetic_demo.py')
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class PortableMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='recshop-migration-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = demo.create_inputs(self.root / 'source inputs')
        self.ledger = self.inputs / 'accepted-ledger.json'

    def cli(self, script, *args, ok=True):
        r = subprocess.run([sys.executable, '-B', '-X', 'utf8', str(CORE / script), *map(str, args)],
                           cwd=str(self.root), capture_output=True, text=True, encoding='utf-8', timeout=40)
        if ok:
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        else:
            self.assertNotEqual(r.returncode, 0, r.stdout)
        return r

    def prepare(self, out=None, *extra):
        out = out or self.root / 'plan output/PLAN.json'
        self.cli('prepare_export.py', '--source-root', self.inputs, '--accepted-ledger', self.ledger,
                 '--out', out, *extra)
        return out

    def test_relative_sources_prepare_build_read_and_relocation(self):
        before = {p.relative_to(self.inputs).as_posix(): digest(p.read_bytes())
                  for p in self.inputs.rglob('*') if p.is_file()}
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'cached history')
        out = self.root / 'delivery package'
        self.cli('build_dataset.py', '--history', history, '--out', out, '--gap-s', '0')
        readback = self.cli('validate_dataset.py', out, '--expected', '1', '--hashes')
        summary = json.loads(readback.stdout.strip().splitlines()[-1])
        self.assertEqual(summary['algorithm_evaluation'], 'NOT_AVAILABLE_NOT_RUN')
        first = {p.relative_to(out).as_posix(): digest(p.read_bytes()) for p in out.rglob('*') if p.is_file()}
        self.cli('build_dataset.py', '--history', history, '--out', out, '--gap-s', '0')
        second = {p.relative_to(out).as_posix(): digest(p.read_bytes()) for p in out.rglob('*') if p.is_file()}
        self.assertEqual(first, second)
        moved = self.root / 'moved standalone package'
        shutil.copytree(out, moved)
        self.cli('validate_dataset.py', moved, '--hashes')
        original_manifest = (moved / 'MANIFEST.json').read_bytes()
        self.cli('validate_dataset.py', moved, '--report', moved / 'MANIFEST.json', ok=False)
        self.assertEqual((moved / 'MANIFEST.json').read_bytes(), original_manifest)
        case_dir = moved / read_json(moved / 'MANIFEST.json')['cases'][0]['path']
        self.assertEqual(read_json(case_dir / 'audit/collector-quality-original.json')['original_quality'], 'FAIL')
        self.assertTrue(read_json(case_dir / 'scripts/contract-original.json')['synthetic_example'])
        self.assertEqual(before, {p.relative_to(self.inputs).as_posix(): digest(p.read_bytes())
                                for p in self.inputs.rglob('*') if p.is_file()})

    def test_competing_attempt_and_wrong_hash_rejected(self):
        doc = read_json(self.ledger)
        doc['rows'].append(copy.deepcopy(doc['rows'][0]))
        write_json(self.ledger, doc)
        with self.assertRaisesRegex(ValueError, 'duplicate accepted slot'):
            load_cases(self.inputs, None, [self.ledger])
        doc['rows'].pop();doc['rows'][0]['attempt_id'] = 'competing-attempt'
        write_json(self.ledger, doc)
        with self.assertRaisesRegex(ValueError, 'wrong attempt'):
            load_cases(self.inputs, None, [self.ledger])
        doc['rows'][0]['attempt_id'] = demo.ATTEMPT
        doc['rows'][0]['evidence_refs'][0]['sha256'] = '0' * 64
        write_json(self.ledger, doc)
        with self.assertRaisesRegex(ValueError, 'SUMMARY hash mismatch'):
            load_cases(self.inputs, None, [self.ledger])

    def test_exact_relocation_map_preserves_contract_bytes(self):
        contract_path = self.inputs / 'native/demo/contract.json'
        contract = read_json(contract_path)
        contract['context']['evidence_root'] = 'Z:/retired-source/native/demo'
        write_json(contract_path, contract)
        write_json(self.inputs / 'runs/demo/recovery-input.json', {'contract': contract})
        doc = read_json(self.ledger)
        for ref in doc['rows'][0]['evidence_refs']:
            local = self.inputs / ref['path']
            ref['sha256'] = digest(local.read_bytes())
            ref['path'] = 'Z:/retired-source/' + ref['path']
        write_json(self.ledger, doc)
        mapping = self.root / 'path-map.json'
        write_json(mapping, {'mappings': [{'from': 'Z:/retired-source', 'to': 'source inputs'}]})
        snapshot = contract_path.read_bytes()
        plan = self.prepare(None, '--path-map', mapping)
        self.assertEqual(read_json(plan)['cases'][0]['native_root'], str((self.inputs / 'native/demo').resolve()))
        self.assertEqual(snapshot, contract_path.read_bytes())
        resolver = SourcePaths(self.inputs, load_mappings(mapping))
        self.assertEqual(resolver.resolve('Z:/retired-source/native/demo'), contract_path.parent.resolve())
        self.assertNotEqual(resolver.resolve('Z:/retired-source-other/native/demo'), contract_path.parent.resolve())

    def test_new_output_overlap_and_plan_overwrite_rejected(self):
        self.prepare()
        self.cli('prepare_export.py', '--source-root', self.inputs, '--accepted-ledger', self.ledger,
                 '--out', self.root / 'plan output/PLAN.json', ok=False)
        with self.assertRaisesRegex(ValueError, 'overlaps'):
            guard_output(self.inputs, [self.ledger])
        with self.assertRaisesRegex(ValueError, 'overlaps'):
            guard_output(self.inputs / 'native/demo/out', [self.inputs / 'native/demo'])
        existing = self.root / 'unowned';existing.mkdir();(existing / 'keep.txt').write_text('keep')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            guard_output(existing, resume_marker='PLAN.json')
        self.assertEqual((existing / 'keep.txt').read_text(), 'keep')

    def test_existing_package_rejects_competing_attempt_before_writes(self):
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'first cache')
        out = self.root / 'selected package'
        self.cli('build_dataset.py', '--history', history, '--out', out, '--gap-s', '0')
        before = {p.relative_to(out).as_posix(): digest(p.read_bytes()) for p in out.rglob('*') if p.is_file()}
        old_attempt = demo.ATTEMPT
        try:
            demo.ATTEMPT = 'synthetic-competing-demo-r1'
            second = demo.create_inputs(self.root / 'competing inputs')
            second_plan = self.root / 'competing plan/PLAN.json'
            self.cli('prepare_export.py', '--source-root', second, '--accepted-ledger', second / 'accepted-ledger.json', '--out', second_plan)
            second_history = demo.create_cache(second_plan, self.root / 'competing cache')
            failure = self.cli('build_dataset.py', '--history', second_history, '--out', out, '--gap-s', '0', ok=False)
            self.assertIn('existing slot selects another attempt', failure.stderr)
        finally:
            demo.ATTEMPT = old_attempt
        self.assertEqual(before, {p.relative_to(out).as_posix(): digest(p.read_bytes()) for p in out.rglob('*') if p.is_file()})

    def test_traversal_and_ambiguous_mapping_rejected(self):
        with self.assertRaisesRegex(ValueError, 'traversal'):
            SourcePaths(self.inputs).resolve('../other/SUMMARY.json')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            SourcePaths(self.inputs, [{'from': 'Z:/a', 'to': str(self.inputs)},
                                      {'from': 'z:/A', 'to': str(self.root)}])

    def test_csv_requires_explicit_summary_instead_of_historical_guess(self):
        row = dict(scenario_id='DEMO01', condition_id='synthetic-condition',
                   attempt_id=demo.ATTEMPT, state='P0_COUNTED',
                   summary_path='runs/demo/SUMMARY.json',
                   summary_sha256=digest((self.inputs / 'runs/demo/SUMMARY.json').read_bytes()))
        ledger = self.inputs / 'accepted.csv'
        with ledger.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(row));w.writeheader();w.writerow(row)
        self.assertEqual(len(load_cases(self.inputs, ledger, [])[0]), 1)
        row['summary_path'] = ''
        with ledger.open('w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(row));w.writeheader();w.writerow(row)
        with self.assertRaisesRegex(ValueError, 'explicit summary_path'):
            load_cases(self.inputs, ledger, [])

    def test_changed_operations_or_log_projection_rejected_before_output(self):
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'cache')
        for relative in ('artifacts/operations.json', 'artifacts/pre_fault/logs/projection.json'):
            with self.subTest(relative=relative):
                source = self.inputs / 'native/demo' / relative
                original = source.read_bytes()
                doc = read_json(source)
                if isinstance(doc, list):
                    doc[0]['message'] = 'REPLACED AFTER PLAN'
                else:
                    doc['operations'] = [{'instance_id': 'F1', 'action': 'inject',
                        'return_code': 0, 'timestamp_epoch_s': 1310,
                        'attempt_id': demo.ATTEMPT, 'run_id': 'synthetic-run'}]
                write_json(source, doc)
                out = self.root / ('reject-' + source.parent.name + source.stem)
                failure = self.cli('build_dataset.py', '--history', history, '--out', out, '--gap-s', '0', ok=False)
                self.assertIn('consumed input changed', failure.stderr)
                self.assertFalse(out.exists())
                source.write_bytes(original)

    def test_absent_input_appearing_after_plan_is_rejected(self):
        path = self.inputs / 'native/demo/artifacts/pre_fault/logs/projection.json'
        original = path.read_bytes()
        path.unlink()  # owned synthetic fixture only
        plan = self.prepare()
        case = read_json(plan)['cases'][0]
        row = next(r for r in case['consumed_inputs']['files'] if r['relative_path'].endswith('pre_fault/logs/projection.json'))
        self.assertFalse(row['exists'])
        history = demo.create_cache(plan, self.root / 'cache')
        path.write_bytes(original)
        out = self.root / 'unexpected-appearance'
        failure = self.cli('build_dataset.py', '--history', history, '--out', out, ok=False)
        self.assertIn('consumed input changed', failure.stderr)
        self.assertFalse(out.exists())

    def test_old_plan_without_consumed_manifest_requires_prepare(self):
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'cache')
        document = read_json(history / 'PLAN.json')
        del document['cases'][0]['consumed_inputs']
        write_json(history / 'PLAN.json', document)
        out = self.root / 'old-plan-output'
        failure = self.cli('build_dataset.py', '--history', history, '--out', out, ok=False)
        self.assertIn('prepare a new plan', failure.stderr)
        self.assertFalse(out.exists())

    def test_after_snapshot_replacement_cannot_change_consumed_bytes(self):
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'cache')
        document = read_json(plan)
        c = document['cases'][0]
        out = self.root / 'snapshot-stability'
        original_build = build_dataset._build_case
        log_path = self.inputs / 'native/demo/artifacts/pre_fault/logs/projection.json'
        ops_path = self.inputs / 'native/demo/artifacts/operations.json'
        original_log = log_path.read_bytes()
        original_ops = ops_path.read_bytes()
        def replace_after_capture(*args):
            logs = read_json(log_path);logs[0]['message'] = 'REPLACED AFTER SNAPSHOT';write_json(log_path, logs)
            ops = read_json(ops_path);ops['operations'] = [{'instance_id': 'F1', 'action': 'recover',
                'return_code': 0, 'timestamp_epoch_s': 1510}];write_json(ops_path, ops)
            return original_build(*args)
        with mock.patch.object(build_dataset, '_build_case', side_effect=replace_after_capture):
            entry, skipped = build_dataset.build_case(c, history, out, document['inputs'], build_dataset.code_sha())
        self.assertFalse(skipped)
        case_dir = out / entry['path']
        self.assertEqual((case_dir / 'raw/logs/native/pre_fault.json').read_bytes(), original_log)
        self.assertEqual((case_dir / 'raw/operations/operations_log.json').read_bytes(), original_ops)
        self.assertNotIn('REPLACED AFTER SNAPSHOT', (case_dir / 'raw/logs/pre_fault__catalog.log').read_text())
        self.assertEqual(read_json(case_dir / 'metadata.json')['faults'][0]['status'], 'not_assessed')

    def test_cached_receipt_replacement_is_rejected_before_case_output(self):
        plan = self.prepare()
        history = demo.create_cache(plan, self.root / 'cache')
        receipt = history / 'r01/DEMO01/resources.receipt.json'
        doc = read_json(receipt);doc['query'] = 'REPLACED QUERY';write_json(receipt, doc)
        out = self.root / 'receipt-replacement'
        failure = self.cli('build_dataset.py', '--history', history, '--out', out, ok=False)
        self.assertIn('consumed input changed', failure.stderr)
        self.assertFalse((out / 'MANIFEST.json').exists())
        self.assertFalse((out / 'traditional').exists())


if __name__ == '__main__':
    unittest.main()
