from pathlib import Path

import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"


def test_daily_workflow_grants_each_job_only_what_it_needs():
    flow = yaml.safe_load((WORKFLOWS / "daily.yml").read_text(encoding="utf-8"))
    assert "permissions" not in flow  # no workflow-wide grant
    assert flow["jobs"]["run"]["permissions"] == {"contents": "write"}
    assert flow["jobs"]["deploy"]["permissions"] == {"pages": "write", "id-token": "write"}
