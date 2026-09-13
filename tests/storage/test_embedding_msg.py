# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

from uuid import uuid4

import pytest

from openviking.storage.queuefs.embedding_msg import EmbeddingMsg, EmbeddingOperation
from openviking.telemetry.request_wait_tracker import RequestWaitTracker


def test_embedding_msg_roundtrip_preserves_id_for_request_wait_tracker():
    telemetry_id = f"tm_{uuid4().hex}"
    tracker = RequestWaitTracker.get_instance()
    tracker.register_request(telemetry_id)

    try:
        msg = EmbeddingMsg(
            "hello",
            {"uri": "viking://user/default/skills/demo"},
            telemetry_id=telemetry_id,
        )
        tracker.register_embedding_root(telemetry_id, msg.id)

        restored = EmbeddingMsg.from_dict(msg.to_dict())

        assert restored.id == msg.id
        tracker.mark_embedding_done(telemetry_id, restored.id)
        assert tracker.is_complete(telemetry_id)
    finally:
        tracker.cleanup(telemetry_id)


def test_legacy_embedding_msg_defaults_to_embed_and_upsert():
    restored = EmbeddingMsg.from_dict(
        {
            "message": "hello",
            "context_data": {"uri": "viking://resources/demo"},
        }
    )

    assert restored.operation is EmbeddingOperation.EMBED_AND_UPSERT


def test_embedding_update_fields_roundtrip():
    msg = EmbeddingMsg.for_update_fields(
        record_id="record-1",
        fields={"md5": "new-md5", "updated_at": "2026-09-13T00:00:00Z"},
        context_data={
            "uri": "viking://resources/demo/a.py",
            "account_id": "account-a",
            "level": 2,
        },
    )

    restored = EmbeddingMsg.from_json(msg.to_json())

    assert restored.operation is EmbeddingOperation.UPDATE_FIELDS
    assert restored.record_ids == ["record-1"]
    assert restored.update_fields == {
        "md5": "new-md5",
        "updated_at": "2026-09-13T00:00:00Z",
    }
    assert restored.message is None


def test_embedding_delete_roundtrip():
    msg = EmbeddingMsg.for_delete(
        record_ids=["record-1", "record-2"],
        context_data={
            "uri": "viking://resources/demo",
            "account_id": "account-a",
        },
    )

    restored = EmbeddingMsg.from_json(msg.to_json())

    assert restored.operation is EmbeddingOperation.DELETE
    assert restored.record_ids == ["record-1", "record-2"]
    assert restored.message is None


def test_embedding_update_fields_rejects_vector_fields():
    with pytest.raises(ValueError, match="vector"):
        EmbeddingMsg.for_update_fields(
            record_id="record-1",
            fields={"vector": [0.1, 0.2]},
            context_data={
                "uri": "viking://resources/demo/a.py",
                "account_id": "account-a",
                "level": 2,
            },
        )
