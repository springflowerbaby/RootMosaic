"""Credential-isolated child entry for explicitly requested collection."""
from pathlib import Path
import os
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.collection import environment, scenario_runner

def main():
    if any(arg in ('-h', '--help') for arg in sys.argv[1:]):
        scenario_runner.main()
        return
    environment.apply(scenario_runner)
    if sys.argv[1:2] != ['preview']:
        environment.require_windows_execution()
        environment.resolve_cli("kubectl")
        if sys.argv[1:2] == ["run"]:
            environment.resolve_cli("docker")
        os.environ.update(environment.credentials())
    scenario_runner.main()

if __name__ == '__main__':
    main()
