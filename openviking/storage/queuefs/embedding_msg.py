# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4


class EmbeddingOperation(str, Enum):
    EMBED_AND_UPSERT = "embed_and_upsert"
    UPDATE_FIELDS = "update_fields"
    DELETE = "delete"


_UPDATE_FIELD_ALLOWLIST = frozenset(
    {"md5", "content", "abstract", "updated_at", "active_count", "tags", "search_tags"}
)


@dataclass
class EmbeddingMsg:
    message: Optional[Union[str, List[Dict[str, Any]]]]
    context_data: Dict[str, Any]
    id: str = field(default_factory=lambda: str(uuid4()))
    telemetry_id: str = ""
    operation: EmbeddingOperation = EmbeddingOperation.EMBED_AND_UPSERT
    record_ids: List[str] = field(default_factory=list)
    update_fields: Dict[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        message: Optional[Union[str, List[Dict[str, Any]]]],
        context_data: Dict[str, Any],
        telemetry_id: str = "",
        operation: EmbeddingOperation | str = EmbeddingOperation.EMBED_AND_UPSERT,
        record_ids: Optional[List[str]] = None,
        update_fields: Optional[Dict[str, Any]] = None,
    ):
        self.id = str(uuid4())
        self.message = message
        self.context_data = context_data
        self.telemetry_id = telemetry_id
        self.operation = EmbeddingOperation(operation)
        self.record_ids = list(record_ids or [])
        self.update_fields = dict(update_fields or {})
        self._validate()

    def _validate(self) -> None:
        if self.operation is EmbeddingOperation.EMBED_AND_UPSERT:
            if not isinstance(self.message, (str, list)):
                raise ValueError("embed_and_upsert requires an embedding message")
            return
        if self.message is not None:
            raise ValueError(f"{self.operation.value} does not accept an embedding message")
        if not self.record_ids:
            raise ValueError(f"{self.operation.value} requires record_ids")
        if self.operation is EmbeddingOperation.DELETE:
            if self.update_fields:
                raise ValueError("delete does not accept update_fields")
            return
        if len(self.record_ids) != 1:
            raise ValueError("update_fields requires exactly one record id")
        unknown = set(self.update_fields) - _UPDATE_FIELD_ALLOWLIST
        if unknown:
            raise ValueError(f"update_fields contains forbidden fields: {sorted(unknown)}")
        if not self.update_fields:
            raise ValueError("update_fields must not be empty")

    @classmethod
    def for_update_fields(
        cls,
        *,
        record_id: str,
        fields: Dict[str, Any],
        context_data: Dict[str, Any],
        telemetry_id: str = "",
    ) -> "EmbeddingMsg":
        return cls(
            message=None,
            context_data=context_data,
            telemetry_id=telemetry_id,
            operation=EmbeddingOperation.UPDATE_FIELDS,
            record_ids=[record_id],
            update_fields=fields,
        )

    @classmethod
    def for_delete(
        cls,
        *,
        record_ids: List[str],
        context_data: Dict[str, Any],
        telemetry_id: str = "",
    ) -> "EmbeddingMsg":
        return cls(
            message=None,
            context_data=context_data,
            telemetry_id=telemetry_id,
            operation=EmbeddingOperation.DELETE,
            record_ids=record_ids,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert embedding message to dictionary format."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert embedding message to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EmbeddingMsg":
        """Create an embedding message object from dictionary."""
        obj = EmbeddingMsg(
            message=data["message"],
            context_data=data["context_data"],
            telemetry_id=data.get("telemetry_id", ""),
            operation=data.get("operation", EmbeddingOperation.EMBED_AND_UPSERT.value),
            record_ids=data.get("record_ids"),
            update_fields=data.get("update_fields"),
        )
        obj.id = data.get("id", obj.id)
        return obj

    @classmethod
    def from_json(cls, json_str: str) -> "EmbeddingMsg":
        """Safely create object from JSON string."""
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")
