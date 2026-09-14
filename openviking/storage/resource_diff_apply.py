# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Apply a :class:`DiffPlan` against a target, uploading only what changed.

This is the backend-agnostic executor that turns a classified plan into target
writes/deletes:

- ``unchanged`` files are left untouched — the whole point of the diff.
- ``added`` / ``modified`` files are read from the parse output store and written
  to the target; md5 is computed from those bytes at the upload point (no extra
  read) so it can feed the subsequent embedding.
- ``needs_body_compare`` files (md5 unknown on some side) read both bodies once
  and resolve to unchanged or an upload.
- ``deleted`` / ``orphan_vectors`` are removed; ``structural`` (file<->dir flip)
  deletes the stale node before the replacement is written.

The ``target`` and ``store`` are duck-typed so AGFS (viking temp -> target cp)
and local (local dir -> AGFS target upload) reuse the same logic. The caller is
responsible for the completeness gate before invoking this (a plan built from an
incomplete snapshot never carries deletions — see build_diff_plan).
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence, Tuple

from openviking.parse.parsers.upload_utils import ARTIFACT_MANIFEST_NAME
from openviking.storage.viking_fs._diff_plan import CONTROL_BASENAMES, DiffPlan

logger = logging.getLogger(__name__)


def _upload_concurrency() -> int:
    """Resolve the upload fan-out width.

    Uploads here are the same class of independent per-file remote writes as the
    parse-time staging pass, so they share one knob: whatever
    ``upload_utils._UPLOAD_CONCURRENCY`` is set to (config/benchmark overrides
    included) governs both, avoiding a second constant that could drift.
    """
    from openviking.parse.parsers import upload_utils

    return max(1, int(getattr(upload_utils, "_UPLOAD_CONCURRENCY", 8)))


def _is_business_file(rel_path: str) -> bool:
    return rel_path.rsplit("/", 1)[-1] not in CONTROL_BASENAMES


class _PrefixedReadStore:
    """Wraps a store so reads re-add a doc_rel prefix to target-relative paths.

    ``apply_full_artifact_upload`` classifies files by their target-relative path
    (doc_rel stripped) but the artifact stores them under ``<doc_rel>/...``. The
    shared ``_upload`` helper reads by the key it is handed, so this wrapper puts
    the prefix back on read, letting the initial-import path reuse the concurrent
    uploader without a bespoke read call.
    """

    def __init__(self, store: Any, prefix: str) -> None:
        self._store = store
        self._prefix = prefix

    async def read_bytes(self, ref: Any, rel_path: str) -> bytes:
        return await self._store.read_bytes(ref, f"{self._prefix}{rel_path}")


def _covered_by_tree_delete(rel_path: str, roots: List[str]) -> bool:
    return any(rel_path == root or rel_path.startswith(root.rstrip("/") + "/") for root in roots)


@dataclass
class ApplyResult:
    """What the executor actually performed, for reporting and md5 hand-off."""

    uploaded: List[str] = field(default_factory=list)
    added: List[str] = field(default_factory=list)
    added_dirs: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    deleted_dirs: List[str] = field(default_factory=list)
    orphan_vectors: List[str] = field(default_factory=list)
    structural: List[str] = field(default_factory=list)
    repair: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    abstracts_by_rel: Dict[str, str] = field(default_factory=dict)
    # rel_path -> md5 of the bytes just uploaded; handed to the embedding stage
    # so the next diff can skip by fingerprint.
    md5_by_rel: Dict[str, str] = field(default_factory=dict)


async def _upload(rel_path: str, *, store, artifact_ref, target) -> None:
    """Read one file from the store and write it to the target, returning md5.

    Runs as an independent task so callers can fan out uploads concurrently; the
    result is returned rather than mutating shared state so parallel writers do
    not race on the ``ApplyResult`` collections.
    """
    data = await store.read_bytes(artifact_ref, rel_path)
    await target.write_file(rel_path, data)


async def _upload_concurrent(
    rel_paths: Sequence[str],
    *,
    store,
    artifact_ref,
    target,
    result: ApplyResult,
    concurrency: int | None = None,
) -> None:
    """Upload ``rel_paths`` with bounded concurrency, recording md5 per file.

    Each file is an independent remote write, so uploads fan out under a
    semaphore instead of one serial await. The first failure propagates (via
    ``gather``) so the caller still marks the task failed rather than reporting
    partial success as done.
    """
    if not rel_paths:
        return
    sem = asyncio.Semaphore(concurrency if concurrency is not None else _upload_concurrency())

    async def _one(rel_path: str) -> str:
        async with sem:
            md5 = await _upload(
                rel_path, store=store, artifact_ref=artifact_ref, target=target
            )
        return rel_path

    for rel_path in await asyncio.gather(*(_one(rel) for rel in rel_paths)):
        result.uploaded.append(rel_path)


async def apply_diff_plan(
    plan: DiffPlan,
    *,
    store: Any,
    artifact_ref: Any,
    target: Any,
    delete_vectors: bool = True,
) -> ApplyResult:
    """Execute ``plan`` against ``target``, uploading only changed files.

    Structural replacements are removed first so a file<->dir flip never leaves
    stale content under the same URI. Deletions come from a plan that already
    passed the completeness gate, so an unreadable file is never deleted here.
    Any target write/delete failure propagates; the caller marks the task failed
    rather than reporting partial success as done.
    """
    result = ApplyResult(
        added=list(plan.added),
        modified=list(plan.modified),
        repair=list(plan.repair),
        files=sorted(plan.new_files),
        md5_by_rel=dict(plan.new_md5s),
        abstracts_by_rel=dict(plan.file_abstracts),
    )
    body_compare_equal = 0
    body_compare_modified = 0
    body_compare_read_failed = 0
    body_compare_samples: List[str] = []

    # Structural replacements: delete the stale node up front. The replacement is
    # written by its added/modified classification below.
    structural_roots = sorted(
        plan.structural,
        key=lambda value: (value.count("/"), value),
    )
    structural_roots = [
        path
        for index, path in enumerate(structural_roots)
        if not _covered_by_tree_delete(path, structural_roots[:index])
    ]
    for rel_path in structural_roots:
        await target.delete_file(rel_path)
        if delete_vectors:
            await target.delete_vector(rel_path)
        result.structural.append(rel_path)

    for rel_path in sorted(plan.added_dirs, key=lambda value: (value.count("/"), value)):
        await target.mkdir(rel_path)
        result.added_dirs.append(rel_path)

    await _upload_concurrent(
        [*plan.added, *plan.modified],
        store=store,
        artifact_ref=artifact_ref,
        target=target,
        result=result,
    )

    for rel_path in plan.needs_body_compare:
        new_bytes = await store.read_bytes(artifact_ref, rel_path)
        try:
            old_bytes = await target.read_file(rel_path)
        except Exception as exc:
            old_bytes = None
            body_compare_read_failed += 1
            if len(body_compare_samples) < 5:
                body_compare_samples.append(f"{rel_path}(read_failed={exc})")
        if old_bytes is not None and old_bytes == new_bytes:
            result.unchanged.append(rel_path)
            body_compare_equal += 1
            continue
        await target.write_file(rel_path, new_bytes)
        result.uploaded.append(rel_path)
        result.modified.append(rel_path)
        result.md5_by_rel[rel_path] = plan.new_md5s.get(rel_path, "")
        body_compare_modified += 1
        if len(body_compare_samples) < 5:
            body_compare_samples.append(f"{rel_path}(content_different)")

    if plan.needs_body_compare:
        logger.info(
            "[IncrementalDiff] body_compare total=%d equal=%d modified=%d "
            "read_failed=%d samples=%s",
            len(plan.needs_body_compare),
            body_compare_equal,
            body_compare_modified,
            body_compare_read_failed,
            body_compare_samples,
        )

    for rel_path in plan.repair:
        new_bytes = await store.read_bytes(artifact_ref, rel_path)
        try:
            old_bytes = await target.read_file(rel_path)
        except Exception:
            old_bytes = None
        if old_bytes is not None and old_bytes == new_bytes:
            result.md5_by_rel.setdefault(rel_path, plan.new_md5s.get(rel_path, ""))
            continue
        await target.write_file(rel_path, new_bytes)
        result.uploaded.append(rel_path)
        result.md5_by_rel[rel_path] = plan.new_md5s.get(rel_path, "")

    result.unchanged.extend(plan.unchanged)

    for rel_path in plan.deleted:
        if not _covered_by_tree_delete(rel_path, structural_roots):
            await target.delete_file(rel_path)
        if delete_vectors:
            await target.delete_vector(rel_path)
        result.deleted.append(rel_path)

    deleted_dir_roots: List[str] = []
    for rel_path in sorted(plan.deleted_dirs, key=lambda value: (value.count("/"), value)):
        if _covered_by_tree_delete(rel_path, [*structural_roots, *deleted_dir_roots]):
            continue
        deleted_dir_roots.append(rel_path)
        await target.delete_file(rel_path)
        result.deleted_dirs.append(rel_path)

    for rel_path in plan.orphan_vectors:
        if delete_vectors:
            await target.delete_vector(rel_path)
        result.orphan_vectors.append(rel_path)

    return result


async def apply_full_artifact_upload(
    *,
    store: Any,
    artifact_ref: Any,
    doc_rel: str,
    target: Any,
    root_is_file: bool = False,
) -> ApplyResult:
    """Upload every file under ``doc_rel`` in the artifact to the target.

    Initial import is "the plan is all added": there is no existing target to
    diff against, so every business file below the document root is uploaded.
    Paths are made target-relative by stripping the ``doc_rel`` prefix (e.g. the
    ``repository`` wrapper), and md5 is computed from the final stored bytes at
    the upload point, exactly like the incremental path.
    """
    result = ApplyResult()
    manifest_md5s: Dict[str, str] = {}
    base = doc_rel.strip("/")
    prefix = f"{base}/" if base else ""
    try:
        raw_manifest = await store.read_bytes(artifact_ref, ARTIFACT_MANIFEST_NAME)
        loaded_manifest = json.loads(raw_manifest.decode("utf-8"))
        if isinstance(loaded_manifest, dict):
            manifest_md5s = {
                (key[len(prefix) :] if prefix and key.startswith(prefix) else key): str(value)
                for key, value in loaded_manifest.items()
            }
    except Exception:
        logger.info(
            "[IncrementalDiff] artifact manifest unavailable during full upload; vector md5 remains empty"
        )
    if root_is_file:
        data = await store.read_bytes(artifact_ref, base)
        written = await target.write_file("", data)
        result.uploaded.append("")
        result.added.append("")
        result.files.append("")
        result.md5_by_rel[""] = manifest_md5s.get("", "")
        return result

    directories: set[str] = set()
    upload_targets: List[str] = []

    async def _walk(rel: str) -> None:
        for entry in await store.list(artifact_ref, rel):
            if entry.is_dir:
                target_rel = entry.rel_path[len(prefix) :] if prefix else entry.rel_path
                if target_rel:
                    directories.add(target_rel)
                await _walk(entry.rel_path)
                continue
            target_rel = entry.rel_path[len(prefix) :] if prefix else entry.rel_path
            if not _is_business_file(target_rel):
                continue
            upload_targets.append(target_rel)

    await _walk(base)
    # Uploads are independent remote writes; fan them out concurrently. The walk
    # yields target-relative paths, and reads re-add the doc_rel prefix so the
    # store still resolves the artifact-relative source.
    prefixed_store = _PrefixedReadStore(store, prefix)
    await _upload_concurrent(
        upload_targets,
        store=prefixed_store,
        artifact_ref=artifact_ref,
        target=target,
        result=result,
    )
    result.files.extend(upload_targets)
    result.md5_by_rel.update(
        {rel_path: manifest_md5s[rel_path] for rel_path in upload_targets if rel_path in manifest_md5s}
    )
    for rel_path in sorted(directories, key=lambda value: (value.count("/"), value)):
        await target.mkdir(rel_path)
        result.added_dirs.append(rel_path)
    result.files.sort()
    return result


__all__ = ["ApplyResult", "apply_diff_plan", "apply_full_artifact_upload"]
