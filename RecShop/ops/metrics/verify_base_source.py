"""Build-time read-only baseline guard. Does not import application code."""
from pathlib import Path, PurePosixPath
import hashlib
import os
import re


def verify(source_path: str, expected_sha256: str) -> None:
    path = PurePosixPath(source_path)
    if path.parts[:3] != ("/", "app", "services") or ".." in path.parts or path.suffix != ".py":
        raise ValueError("overlay destination outside approved service tree")
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ValueError("explicit source baseline hash required")
    actual = hashlib.sha256(Path(source_path).read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise ValueError("base image source differs from reviewed baseline; refusing COPY overlay")


if __name__ == "__main__":
    verify(os.environ["SERVICE_DESTINATION"], os.environ["EXPECTED_BASE_SOURCE_SHA256"])
