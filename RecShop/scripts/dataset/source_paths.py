"""Explicit source path resolution and non-overlapping migration destinations.

No files are discovered by basename. A relocation map changes path resolution,
never the bytes of an original ledger, contract, or observation.
"""
from __future__ import annotations

import json
from pathlib import Path, PurePosixPath, PureWindowsPath


def _key(value):
    text = str(value).replace('\\', '/').rstrip('/')
    if not text or '\x00' in text:
        raise ValueError('empty or invalid source path')
    if '..' in PurePosixPath(text).parts:
        raise ValueError('parent traversal is not a source path mapping')
    windows = bool(PureWindowsPath(text).drive)
    if windows and not PureWindowsPath(text).is_absolute():
        raise ValueError('drive-relative source paths are ambiguous')
    return text, text.casefold() if windows else text


def load_mappings(path):
    """Read {mappings:[{from:absolute-prefix,to:path}]} with map-relative targets."""
    if path is None:
        return []
    p = Path(path).resolve()
    doc = json.loads(p.read_text(encoding='utf-8-sig'))
    if set(doc) != {'mappings'} or not isinstance(doc['mappings'], list):
        raise ValueError('path map must contain only a mappings list')
    rows = []
    for row in doc['mappings']:
        if set(row) != {'from', 'to'}:
            raise ValueError('each path mapping requires from and to')
        target = Path(row['to'])
        if not target.is_absolute():
            target = p.parent / target
        rows.append({'from': row['from'], 'to': str(target.resolve())})
    return rows


class SourcePaths:
    def __init__(self, source_root, mappings=()):
        self.root = Path(source_root).resolve()
        self.mappings = []
        seen = set()
        for row in mappings:
            src, key = _key(row['from'])
            if not (PurePosixPath(src).is_absolute() or PureWindowsPath(src).is_absolute()):
                raise ValueError('mapping source must be an absolute prefix')
            if key in seen:
                raise ValueError('duplicate source mapping')
            seen.add(key)
            dst = Path(row['to'])
            if not dst.is_absolute():
                raise ValueError('resolved mapping target must be absolute')
            self.mappings.append({'from': src, 'to': str(dst.resolve())})

    @classmethod
    def from_case(cls, case):
        cfg = case.get('source_resolution')
        if cfg is None:
            # Programmatic callers with absolute evidence paths need no map.
            return cls(Path.cwd())
        return cls(cfg['source_root'], cfg.get('mappings', []))

    def config(self):
        return {'source_root': str(self.root), 'mappings': self.mappings,
                'relative_basis': 'source_root', 'mapping_rule': 'longest_exact_path_prefix'}

    def resolve(self, value):
        text, key = _key(value)
        matches = []
        for row in self.mappings:
            prefix, prefix_key = _key(row['from'])
            if key == prefix_key or key.startswith(prefix_key + '/'):
                matches.append((len(prefix_key), prefix, row['to']))
        if matches:
            _, prefix, destination = max(matches)
            root = Path(destination).resolve()
            suffix = text[len(prefix):].lstrip('/')
            result = (root / suffix).resolve()
            if result != root and not result.is_relative_to(root):
                raise ValueError('mapped source escapes destination root')
            return result
        path = Path(text)
        if path.is_absolute():
            return path.resolve()
        if PureWindowsPath(text).is_absolute():
            raise ValueError('foreign absolute source needs an explicit path map')
        result = (self.root / path).resolve()
        if result != self.root and not result.is_relative_to(self.root):
            raise ValueError('relative source escapes source root')
        return result


def protected_sources(plan):
    paths = [Path(__file__).resolve().parent]
    paths.extend(Path(ref['path']) for ref in plan.get('inputs', []))
    for case in plan.get('cases', []):
        paths.append(Path(case['summary_path']).parent)
        paths.append(Path(case.get('recovery_input_path',
                                   Path(case['summary_path']).parent / 'recovery-input.json')).parent)
        if case.get('native_root'):
            paths.append(Path(case['native_root']))
    return paths


def guard_output(out, protected=(), *, resume_marker=None, file_output=False):
    """Require a new destination, or an owned resumable directory, disjoint from inputs."""
    out = Path(out).resolve()
    if out == Path(out.anchor):
        raise ValueError('filesystem root cannot be a migration destination')
    for source in [Path(__file__).resolve().parent, *map(Path, protected)]:
        source = source.resolve()
        if out == source or out.is_relative_to(source) or source.is_relative_to(out):
            raise ValueError('migration output overlaps a protected source: ' + str(source))
    if out.exists():
        if file_output or not resume_marker or not out.is_dir() or not (out / resume_marker).is_file():
            raise ValueError('output already exists without matching migration ownership')
    return out
