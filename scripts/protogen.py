#!/usr/bin/env python3
"""Regenerate Yellowstone gRPC stubs from proto/ into src/trenches/pb/.

protoc emits absolute imports that only resolve if the output directory is on
sys.path. Rewriting them to package-relative imports is the single local
modification, reapplied here so the stubs are never hand-edited.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "src" / "trenches" / "pb"
PROTOS = sorted((ROOT / "proto").glob("*.proto"))

PATTERNS = (
    (re.compile(r"^import (\w+_pb2) as (\w+)$", re.M), r"from . import \1 as \2"),
    (re.compile(r"^from (\w+_pb2) import \*$", re.M), r"from .\1 import *"),
    (re.compile(r"^import (\w+_pb2)$", re.M), r"from . import \1"),
)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc", f"-I{ROOT / 'proto'}",
        f"--python_out={OUT}", f"--pyi_out={OUT}", f"--grpc_python_out={OUT}",
        *[str(p) for p in PROTOS],
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603
    if result.returncode:
        sys.stderr.write(result.stderr)
        return result.returncode

    for path in OUT.glob("*_pb2*.py"):
        text = original = path.read_text()
        for pattern, repl in PATTERNS:
            text = pattern.sub(repl, text)
        if text != original:
            path.write_text(text)
            print(f"rewrote imports in {path.name}")
    print(f"generated {len(PROTOS)} proto file(s) into {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
