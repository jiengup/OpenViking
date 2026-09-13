# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import json

import pytest

from openviking.parse.image_rewrite import rewrite_artifact_image_uris
from openviking.parse.output import LocalParseOutputStore
from openviking.parse.parsers.upload_utils import ARTIFACT_MANIFEST_NAME
from openviking.utils.content_hash import content_md5


@pytest.mark.asyncio
async def test_rewrite_artifact_uses_final_uri_and_refreshes_manifest_md5(tmp_path):
    store = LocalParseOutputStore(local_root=str(tmp_path / "artifacts"))
    ref = await store.create_artifact()
    markdown = b"# Guide\n\n![diagram](diagram.png)\n"
    await store.write_bytes(ref, "repository/docs/guide.md", markdown)
    await store.write_bytes(ref, "repository/docs/diagram.png", b"png")
    await store.write_text(
        ref,
        "repository/.image_mappings.json",
        json.dumps({"docs/guide.md": {"diagram.png": "diagram.png"}}),
    )
    await store.write_text(
        ref,
        ARTIFACT_MANIFEST_NAME,
        json.dumps({"repository/docs/guide.md": content_md5(markdown)}),
    )

    rewritten = await rewrite_artifact_image_uris(
        store,
        ref,
        doc_rel="repository",
        target_root_uri="viking://resources/repo",
    )

    final = await store.read_bytes(ref, "repository/docs/guide.md")
    assert final == b"# Guide\n\n![diagram](viking://resources/repo/docs/diagram.png)\n"
    manifest = json.loads((await store.read_bytes(ref, ARTIFACT_MANIFEST_NAME)).decode())
    assert manifest["repository/docs/guide.md"] == content_md5(final)
    assert rewritten == {"docs/guide.md"}
