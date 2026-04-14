"""README TOC internal links resolve to heading anchors (see scripts/verify_readme_anchors.py)."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_readme_toc_anchors():
    script = ROOT / "scripts" / "verify_readme_anchors.py"
    r = subprocess.run(
        [sys.executable, str(script)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stdout + r.stderr
