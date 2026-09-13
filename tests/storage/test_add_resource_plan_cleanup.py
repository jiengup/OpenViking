# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.storage.queuefs.add_resource_msg import AddResourceMsg, AddResourcePhase
from openviking.storage.queuefs.add_resource_processor import AddResourceProcessor
from openviking_cli.session.user_id import UserIdentifier


@pytest.mark.asyncio
async def test_cancelled_post_process_cleans_agfs_plan_artifact():
    viking_fs = SimpleNamespace(
        delete_temp=AsyncMock(),
        _async_agfs=SimpleNamespace(),
    )
    processor = AddResourceProcessor(
        SimpleNamespace(),
        asyncio.get_running_loop(),
        "add_resource",
        viking_fs,
    )
    msg = AddResourceMsg(
        task_id="task-1",
        job_phase=AddResourcePhase.POST_PROCESS,
        root_uri="viking://resources/repo",
        account_id="acct",
        user_id="alice",
        role="user",
        prepared={
            "root_uri": "viking://resources/repo",
            "artifact_ref": {
                "backend": "agfs",
                "root": "viking://temp/task-1",
                "resource_rel": "repository",
                "root_type": "dir",
            },
            "plan_artifact_committed": True,
        },
    )
    ctx = RequestContext(user=UserIdentifier("acct", "alice"), role=Role.USER)

    await processor._release_cancelled_resources(msg, ctx)

    viking_fs.delete_temp.assert_awaited_once_with("viking://temp/task-1", ctx=ctx)
