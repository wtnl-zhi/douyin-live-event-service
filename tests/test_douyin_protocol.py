import gzip

from collectors.douyin_protocol import (
    ResponseMessage,
    decode_push_frame,
    encode_ack,
    encode_heartbeat,
    parse_chat_message,
)


def field(number: int, value: int | bytes) -> bytes:
    def varint(number: int) -> bytes:
        result = bytearray()
        while number > 127:
            result.append((number & 127) | 128)
            number >>= 7
        result.append(number)
        return bytes(result)

    if isinstance(value, int):
        return varint(number << 3) + varint(value)
    return varint((number << 3) | 2) + varint(len(value)) + value


def test_decode_real_flow_subset_and_normalize_comment() -> None:
    common = field(2, 1001) + field(3, 2002) + field(4, 1_700_000_000)
    user = field(1, 3003) + field(3, "小明".encode())
    chat = (
        field(1, common)
        + field(2, user)
        + field(3, "你好".encode())
        + field(15, 1_700_000_001)
    )
    message = field(1, b"WebcastChatMessage") + field(2, chat) + field(3, 4004)
    response = field(1, message) + field(5, b"ack-payload") + field(9, 1)
    frame = field(2, 5005) + field(6, b"gzip") + field(7, b"pb") + field(8, gzip.compress(response))

    push_frame, decoded = decode_push_frame(frame)
    raw = parse_chat_message(decoded.messages[0], room_title="测试直播间")

    assert push_frame.log_id == 5005
    assert decoded.need_ack is True
    assert decoded.internal_ext == b"ack-payload"
    assert raw is not None
    assert raw["room_id"] == "2002"
    assert raw["room_title"] == "测试直播间"
    assert raw["user"] == {"id": "3003", "nickname": "小明"}
    assert raw["content"] == "你好"
    assert raw["timestamp"] == 1_700_000_001
    assert raw["payload_size"] == len(chat)
    assert "payload_hex" not in raw


def test_protocol_control_frames_and_non_comment_are_supported() -> None:
    assert encode_ack(12, b"ext")
    assert encode_heartbeat()
    assert parse_chat_message(ResponseMessage("WebcastGiftMessage", b"", 1)) is None
