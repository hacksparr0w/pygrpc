from __future__ import annotations

import base64

from enum import Enum, auto
from io import BytesIO
from typing import Generator

import aiohttp

from pydantic import BaseModel, TypeAdapter

from .. import protobuf


__all__ = (
    "FrameExpectedError",
    "FrameId",
    "Headers",
    "ProtocolError",
    "Trailers",
    "UnexpectedFrameError",

    "decode_frames",
    "decode_trailers",
    "decode_unary_response",
    "encode_unary_request",
    "write_frame",
    "write_message_frame",
    "write_trailer_frame",
    "read_frame",
    "unary_unary_call"
)


class ProtocolError(Exception):
    pass


class FrameExpectedError(ProtocolError):
    pass


class UnexpectedFrameError(ProtocolError):
    pass


class UnknownFrameIdError(ProtocolError):
    pass


class FrameId(Enum):
    MESSAGE = b"\x00"
    TRAILER = b"\x80"

    @classmethod
    def of(cls, value: bytes) -> FrameId:
        for member in cls:
            if member.value == value:
                return member

        raise UnknownFrameIdError


type Headers = dict[str, str]
type Trailers = dict[str, str]


def write_frame(stream: Stream, id: FrameId, data: bytes) -> None:
    stream.write(id.value)
    stream.write(len(data).to_bytes(length=4, byteorder="big", signed=False))
    stream.write(data)


def write_message_frame(
    stream: Stream,
    type: protobuf.MessageType,
    message: BaseModel
) -> None:
    buf = BytesIO()
    protobuf.write_message(buf, type, message.model_dump())
    data = buf.getvalue()
    write_frame(stream, FrameId.MESSAGE, data)


def write_trailer_frame(
    stream: Stream,
    trailers: Trailers,
    encoding: str = "utf-8"
) -> None:
    buf = BytesIO()

    for k, v in trailers.items():
        buf.write(f"{k}: {v}\r\n".encode(encoding))

    data = buf.getvalue()

    write_frame(stream, FrameId.TRAILER, data)


def read_frame(stream: Stream) -> tuple[FrameId, bytes]:
    data = stream.read(1)

    if not data:
        raise EOFError

    frame_id = FrameId.of(data)

    data = stream.read(4)

    if len(data) != 4:
        raise EOFError

    size = int.from_bytes(data, byteorder="big", signed=False)
    data = stream.read(size)

    if len(data) != size:
        raise EOFError

    return (frame_id, data)


def decode_frames(data: bytes) -> Generator[tuple[FrameId, bytes]]:
    buf = BytesIO(data)

    while buf.tell() != len(data):
        yield read_frame(buf)


def decode_trailers(data: bytes, encoding: str = "utf-8") -> Trailers:
    text = data.decode(encoding)
    lines = text.split("\r\n")
    trailers = {}

    for line in lines[:-1]:
        key, value = line.split(":", maxsplit=1)
        key = key.strip()
        value = value.strip()

        trailers[key] = value

    return trailers


def decode_unary_response(
    data: bytes,
    type: protobuf.MessageType,
    model: Any
) -> tuple[BaseModel, Trailers]:
    frames = list(decode_frames(base64.b64decode(data)))

    if len(frames) == 0:
        raise FrameExpectedError
    if len(frames) == 1:
        frame_id, frame_data = frames[0]

        if frame_id != FrameId.MESSAGE:
            raise FrameExpectedError

        message = TypeAdapter(model).validate_python(
            protobuf.decode_message(
                frame_data,
                type
            )
        )

        trailers = {}
    elif len(frames) == 2:
        message_frame_id, message_frame_data = frames[0]
        trailer_frame_id, trailer_frame_data = frames[1]

        if message_frame_id != FrameId.MESSAGE or \
            trailer_frame_id != FrameId.TRAILER:

            raise UnexpectedFrameError
        
        message = TypeAdapter(model).validate_python(
            protobuf.decode_message(
                message_frame_data,
                type
            )
        )

        trailers = decode_trailers(trailer_frame_data)
    else:
        raise UnexpectedFrameError

    return message, trailers


def encode_unary_request(
    type: protobuf.MessageType,
    message: BaseModel,
    trailers: Trailers
) -> bytes:
    buf = BytesIO()

    write_message_frame(buf, type, message)

    if trailers:
        write_trailer_frame(buf, trailers)

    data = buf.getvalue()
    data = base64.b64encode(data)

    return data


async def unary_unary_call(
    url: str,
    request_type: protobuf.MessageType,
    request_message: BaseModel,
    request_headers: Headers,
    request_trailers: Trailers,
    response_type: protobuf.MessageType,
    response_model: Any
) -> tuple[BaseModel, Trailers]:
    request_headers = {
        **request_headers,
        "Accept": "application/grpc-web-text",
        "Content-Type": "application/grpc-web-text",
        "X-User-Agent": "grpc-web-javascript/0.1"
    }

    request_data = encode_unary_request(
        request_type,
        request_message,
        request_trailers
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url,
            data=request_data,
            headers=request_headers
        ) as response:

            response_data = await response.read()

    response_message, response_trailers = decode_unary_response(
        response_data,
        response_type,
        response_model
    )

    return response_message, response_trailers
