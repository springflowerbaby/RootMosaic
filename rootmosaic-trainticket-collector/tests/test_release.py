"""Offline release checks; no cluster access or fault injection."""
import json
import shutil
import subprocess
import tempfile
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TT = ROOT / 'workspace/trainticket'
RUNNER = TT / 'scripts/rebuild-v3-main-mr1-replicates.ps1'
POWERSHELL = shutil.which('powershell.exe')


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def ps(command):
    return subprocess.run([POWERSHELL, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-Command',
                           "$ErrorActionPreference='Stop'; " + command],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True, timeout=30, cwd=str(ROOT))


class ReleaseTests(unittest.TestCase):
    def test_plan_matches_topology_and_counts(self):
        plan = json.loads((TT / 'manifests/collection-plan.json').read_text())
        topology = json.loads((TT / 'manifests/k8s-v3.1-contacts-topology.json').read_text())
        names = {c['name'] for c in topology['components']}
        scenarios = plan['scenarios']
        self.assertEqual(len(names), 21)
        self.assertEqual(len({s['slot_id'] for s in scenarios}), 55)
        self.assertEqual(Counter(s['root_count'] for s in scenarios), {0: 5, 1: 18, 2: 24, 3: 8})
        self.assertEqual(len(scenarios) * plan['replicates_per_scenario'], plan['target_run_count'])
        modes = {'cpu', 'memory', 'network_delay', 'packet_loss', 'pod_kill',
                 'pod_failure', 'nginx_timeout', 'nginx_retry_disabled'}
        for scenario in scenarios:
            faults = scenario['fault_plan']
            self.assertEqual(len(faults), scenario['root_count'])
            self.assertEqual(len({f['fault_instance_id'] for f in faults}), len(faults))
            for fault in faults:
                self.assertIn(fault['target'], names)
                self.assertIn(fault['mode'], modes)

    @unittest.skipUnless(POWERSHELL, 'Windows PowerShell required')
    def test_powershell_syntax(self):
        for path in (TT / 'scripts').glob('*.ps1'):
            command = "$t=$null; $e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile(" + quote(path) + ", [ref]$t, [ref]$e); if($e.Count){ throw ($e | Out-String) }"
            result = ps(command)
            self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(POWERSHELL, 'Windows PowerShell required')
    def test_plan_only_is_read_only_even_with_initialize(self):
        before = {str(p.relative_to(ROOT)) for p in ROOT.rglob('*')}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'must-not-be-created'
            result = ps('& ' + quote(RUNNER) + ' -Slots S01,D01,T01 -Replicates r1 -PlanOnly -Initialize -DatasetDir ' + quote(output))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('target_runs=3', result.stdout)
            self.assertFalse(output.exists())
        after = {str(p.relative_to(ROOT)) for p in ROOT.rglob('*')}
        self.assertEqual(before, after)

    @unittest.skipUnless(POWERSHELL, 'Windows PowerShell required')
    def test_complete_plan(self):
        result = ps('& ' + quote(RUNNER) + ' -PlanOnly')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('target_runs=275', result.stdout)
        self.assertIn('B01/r1', result.stdout)
        self.assertIn('T08/r5', result.stdout)

    @unittest.skipUnless(POWERSHELL, 'Windows PowerShell required')
    def test_unknown_slot_is_rejected(self):
        result = ps('& ' + quote(RUNNER) + ' -Slots TYPO -PlanOnly')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Unknown slot', result.stderr)

    @unittest.skipUnless(POWERSHELL, 'Windows PowerShell required')
    def test_initialize_refuses_existing_index(self):
        with tempfile.TemporaryDirectory() as temporary:
            index = Path(temporary) / 'index.jsonl'
            content = '{"system":"trainticket","slot_id":"B01","replicate_id":"r1"}\n'
            index.write_text(content)
            result = ps("$env:TRAINTICKET_JWT_SECRET='test-only'; & " + quote(RUNNER) + ' -Slots B01 -Replicates r1 -Initialize -DatasetDir ' + quote(temporary))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('already contains samples', result.stderr)
            self.assertEqual(index.read_text(), content)


if __name__ == '__main__':
    unittest.main()
