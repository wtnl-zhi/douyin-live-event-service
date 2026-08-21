"""Minimal protobuf wire helpers for the first real Douyin comment flow.

The collector only needs the transport envelope, the response envelope and the
small subset of ``WebcastChatMessage`` fields required by the stable Event
contract. Keeping this decoder local avoids leaking Douyin protocol types into
the services and API layers.
"""

from __future__ import annotations

import gzip
from dataclasses import dataclass
from typing import Iterator


class DouyinProtocolError(ValueError):
    """Raised when a received frame cannot be decoded safely."""


WireValue = int | bytes


def _read_varint(data: bytes, position: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while position < len(data):
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, position
        shift += 7
        if shift > 63:
            raise DouyinProtocolError("varint is too long")
    raise DouyinProtocolError("truncated varint")


def _iter_fields(data: bytes) -> Iterator[tuple[int, int, WireValue]]:
    position = 0
    while position < len(data):
        key, position = _read_varint(data, position)
        field_number, wire_type = key >> 3, key & 0x07
        if field_number == 0:
            raise DouyinProtocolError("protobuf field number cannot be zero")

        if wire_type == 0:
            value, position = _read_varint(data, position)
        elif wire_type == 1:
            end = position + 8
            if end > len(data):
                raise DouyinProtocolError("truncated fixed64 field")
            value = data[position:end]
            position = end
        elif wire_type == 2:
            size, position = _read_varint(data, position)
            end = position + size
            if end > len(data):
                raise DouyinProtocolError("truncated length-delimited field")
            value = data[position:end]
            position = end
        elif wire_type == 5:
            end = position + 4
            if end > len(data):
                raise DouyinProtocolError("truncated fixed32 field")
            value = data[position:end]
            position = end
        else:
            raise DouyinProtocolError(f"unsupported protobuf wire type: {wire_type}")

        yield field_number, wire_type, value


def _first(data: bytes, field_number: int, default: WireValue | None = b"") -> WireValue | None:
    for number, _, value in _iter_fields(data):
        if number == field_number:
            return value
    return default


def _as_text(value: WireValue) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


@dataclass(frozen=True)
class PushFrame:
    log_id: int
    payload_encoding: str
    payload_type: str
    payload: bytes


@dataclass(frozen=True)
class ResponseMessage:
    method: str
    payload: bytes
    msg_id: int | None


@dataclass(frozen=True)
class PushResponse:
    messages: tuple[ResponseMessage, ...]
    internal_ext: bytes
    need_ack: bool


def decode_push_frame(data: bytes) -> tuple[PushFrame, PushResponse]:
    fields = list(_iter_fields(data))
    log_id = int(next((value for number, wire, value in fields if number == 2 and wire == 0), 0))
    payload_encoding = _as_text(
        next((value for number, wire, value in fields if number == 6 and wire == 2), b"")
    )
    payload_type = _as_text(
        next((value for number, wire, value in fields if number == 7 and wire == 2), b"")
    )
    payload = next((value for number, wire, value in fields if number == 8 and wire == 2), b"")
    if not isinstance(payload, bytes):
        raise DouyinProtocolError("PushFrame payload is not bytes")

    # Some server frames omit payloadEncoding even though the payload is still
    # gzip-compressed. The gzip magic bytes are a safe fallback for that case.
    if payload and (payload_encoding.lower() == "gzip" or payload.startswith(b"\x1f\x8b")):
        response_bytes = gzip.decompress(payload)
    else:
        response_bytes = payload

    messages: list[ResponseMessage] = []
    internal_ext = b""
    need_ack = False
    for number, wire, value in _iter_fields(response_bytes):
        if number == 1 and wire == 2 and isinstance(value, bytes):
            method = _as_text(_first(value, 1))
            message_payload = _first(value, 2)
            msg_id = _first(value, 3, default=None)
            messages.append(
                ResponseMessage(
                    method=method,
                    payload=message_payload if isinstance(message_payload, bytes) else b"",
                    msg_id=int(msg_id) if isinstance(msg_id, int) else None,
                )
            )
        elif number == 5 and wire == 2 and isinstance(value, bytes):
            internal_ext = value
        elif number == 9 and wire == 0:
            need_ack = bool(value)

    return (
        PushFrame(
            log_id=log_id,
            payload_encoding=payload_encoding,
            payload_type=payload_type,
            payload=payload,
        ),
        PushResponse(messages=tuple(messages), internal_ext=internal_ext, need_ack=need_ack),
    )


def parse_chat_message(
    message: ResponseMessage,
    *,
    room_title: str | None = None,
) -> dict[str, object] | None:
    """Convert one protobuf chat payload into a raw, parser-friendly dict."""

    if message.method.lower() != "webcastchatmessage":
        return None

    common: dict[str, int] = {}
    user: dict[str, object] = {}
    content = ""
    event_time: int | None = None

    for number, wire, value in _iter_fields(message.payload):
        if number == 1 and wire == 2 and isinstance(value, bytes):
            for common_number, common_wire, common_value in _iter_fields(value):
                if common_wire == 0 and common_number in {2, 3, 4}:
                    common[common_number] = int(common_value)
        elif number == 2 and wire == 2 and isinstance(value, bytes):
            for user_number, user_wire, user_value in _iter_fields(value):
                if user_number == 1 and user_wire == 0:
                    user["id"] = str(user_value)
                elif user_number == 3 and user_wire == 2:
                    user["nickname"] = _as_text(user_value)
                elif user_number == 1028 and user_wire == 2:
                    user["id"] = _as_text(user_value)
        elif number == 3 and wire == 2 and isinstance(value, bytes):
            content = _as_text(value)
        elif number == 15 and wire == 0:
            event_time = int(value)

    if not content:
        return None

    room_id = common.get(3)
    timestamp = event_time or common.get(4)
    return {
        "method": message.method,
        "msg_id": message.msg_id,
        "room_id": str(room_id) if room_id is not None else "unknown-room",
        "room_title": room_title,
        "user": user,
        "content": content,
        "timestamp": timestamp,
        # Keep only decoded fields in the raw event. The original binary
        # payload can be large and is not safe to broadcast to API clients.
        "payload_size": len(message.payload),
    }


def _encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _encode_field(field_number: int, value: int | bytes) -> bytes:
    if isinstance(value, int):
        return _encode_varint(field_number << 3) + _encode_varint(value)
    return _encode_varint((field_number << 3) | 2) + _encode_varint(len(value)) + value


def encode_ack(log_id: int, internal_ext: bytes) -> bytes:
    return _encode_field(2, log_id) + _encode_field(7, b"ack") + _encode_field(8, internal_ext)


def encode_heartbeat() -> bytes:
    return _encode_field(7, b"hb")
