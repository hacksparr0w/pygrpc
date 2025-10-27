from __future__ import annotations

import typing

from enum import Enum, auto
from io import BufferedIOBase, BytesIO

from ..common import Stream


__all__ = (
    "MessageFields",
    "MessageFieldType",
    "MessageType",
    "OptionalType",
    "PrimitiveType",
    "Type",
    "WireType",

    "get_wire_type",
    "read_bytes",
    "read_message",
    "read_message_field",
    "read_primitive",
    "read_string",
    "read_varint",
    "write",
    "write_bytes",
    "write_message",
    "write_message_field",
    "write_string",
    "write_varint"
)


class PrimitiveType(Enum):
    INT32 = auto()
    STRING = auto()


class WireType(Enum):
    VARINT = 0
    I64 = 1
    LEN = 2
    I32 = 5

    @classmethod
    def of(cls, value: int) -> WireType:
        for member in cls:
            if member.value == value:
                return member

        raise ValueError


type Type = PrimitiveType | MessageType
type MessageFieldType = Type | OptionalType[typing.Any, typing.Any]


class OptionalType[A: Type, B]:
    type: A
    value: B

    def __init__(self, type: A, value: B) -> None:
        self.type = type
        self.value = value


type MessageFields = dict[int, tuple[str, MessageFieldType]]


class MessageType:
    fields: MessageFields

    def __init__(self, fields: MessageFields) -> None:
        self.fields = fields


def get_wire_type(type: MessageFieldType) -> WireType:
    if isinstance(type, OptionalType):
        return get_wire_type(type.type)
    elif isinstance(type, MessageType):
        return WireType.LEN
    elif type == PrimitiveType.INT32:
        return WireType.VARINT
    elif type == PrimitiveType.STRING:
        return WireType.LEN

    raise NotImplementedError


def read_varint(stream: Stream) -> int:
    value = 0
    shift = 0

    while True:
        buf = stream.read(1)

        if not buf:
            raise EOFError

        payload = buf[0]
        value |= (payload & 0x7F) << shift
        shift += 0x07
        flag = payload & 0x80

        if not flag:
            break

    return value


def read_bytes(stream: Stream) -> bytes:
    size = read_varint(stream)
    value = stream.read(size)

    if len(value) != size:
        raise EOFError

    return value


def read_string(stream: Stream, encoding: str = "utf-8") -> str:
    return read_bytes(stream).decode(encoding)


def read_message_field(
    stream: Stream,
    fields: MessageFields
) -> tuple[int, typing.Any]:
    tag = read_varint(stream)
    field_number = tag >> 0x03
    _, field_type = fields[field_number]

    if isinstance(field_type, OptionalType):
        field_type = field_type.type

    if isinstance(field_type, MessageType):
        size = read_varint(stream)

        return read_message(stream, field_type, size)
    else:
        return read_primitive(stream, field_type)


def read_message(
    stream: Stream,
    message_type: MessageType,
    size: int
) -> dict[str, typing.Any]:
    fields = message_type.fields
    data = stream.read(size)

    if not data:
        raise EOFError

    substream = BytesIO(data)
    message = {}

    while True:
        try:
            field_number, field_value = read_message_field(substream, fields)
        except EOFError:
            break

        field_name, _ = fields[field_number]
        message[field_name] = field_value

    return message


def read_primitive(
    stream: Stream,
    type: PrimitiveType
) -> typing.Any:
    if type == PrimitiveType.INT32:
        return read_varint(stream)
    elif type == PrimitiveType.STRING:
        return read_string(stream)

    raise NotImplementedError


def write_varint(stream: Stream, value: int) -> None:
    buf = bytearray()

    while value >= 0x80:
        buf.append((value & 0x7F) | 0x80)
        value >>= 0x07

    buf.append(value)
    stream.write(buf)


def write_bytes(stream: Stream, value: bytes) -> None:
    write_varint(stream, len(value))
    stream.write(value)


def write_string(stream: Stream, value: str, encoding: str = "utf-8") -> None:
    write_bytes(value.encode(encoding))


def write_message_field(
    stream: Stream,
    field_type: MessageFieldType[typing.Any],
    field_number: int,
    value: typing.Any | None
) -> None:
    if isinstance(field_type, OptionalType) and value == field_type.value:
        return

    wire_type = get_wire_type(field_type)
    tag = (field_number << 0x03) | wire_type

    write_varint(stream, tag)
    write(stream, base_type, value)


def write_message(
    stream: BufferedIOBase,
    message_type: MessageType,
    value: dict[str, typing.Any]
) -> None:
    for field_number, (field_name, field_type) in message_type.fields.items():
        field_value = value[field_name]
        write_message_field(stream, field_type, field_number, field_value)


def write(stream: Stream, type: Type, value: typing.Any) -> None:
    if type == PrimitiveType.INT32:
        return write_varint(stream, value)
    elif type == PrimitiveType.STRING:
        return write_string(stream, value)
    elif isinstance(type, MessageType):
        return write_message(stream, type, value)

    raise NotImplementedError
