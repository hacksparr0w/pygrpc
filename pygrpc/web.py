import base64

from io import BytesIO

import aiohttp

from pydantic import BaseModel

from . import protobuf


__all__ = (
    "FrameExpectedError",
    "FrameId",
    "ProtocolError",
    "UnexpectedFrameError",

    "decode_trailers",
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


class FrameId(Enum):
    MESSAGE = b"\x00"
    TRAILER = b"\x80"

    @classmethod
    def of(cls, value: bytes) -> FrameId:
        for member in cls:
            if member.value == value:
                return member

        raise ValueError


def write_frame(stream: Stream, id: FrameId, data: bytes) -> None:
    stream.write(id.value)
    stream.write(len(data).to_bytes(byteorder="big", signed=False))
    stream.write(data)


def write_message_frame(
    stream: Stream,
    type: protobuf.MessageType,
    message: BaseModel
) -> None:
    buf = BytesIO()
    protobuf.write_message(buf, type, message.model_serialize())
    data = buf.getvalue()
    write_message_frame(stream, FrameId.MESSAGE, data)


def write_trailer_frame(
    stream: Stream,
    trailers: dict[str, str],
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


def decode_trailers(data: bytes, encoding: str = "utf-8") -> dict[str, str]:
    text = data.decode(encoding)
    lines = text.split("\r\n")
    trailers = {}

    for line in lines:
        key, value = line.split(":", maxsplit=1)
        key = key.strip()
        value = value.strip()

        trailers[key] = value

    return trailers


async def unary_unary_call[T: BaseModel](
    url: str,
    request_type: protobuf.MessageType,
    response_type: protobuf.MessageType,
    request_message: BaseModel,
    requet_headers: dict[str, str],
    request_trailers: dict[str, str]
) -> tuple[T, dict[str, str]]
    headers = {
        **headers,
        "Accept": "application/grpc-web-text",
        "Content-Type": "application/grpc-web-text",
        "X-User-Agent": "grpc-web-javascript/0.1"
    }

    buf = BytesIO()

    write_message(buf, request_type, request_message)

    if request_trailers:
        write_trailers(buf, request_trailers)

    data = buf.getvalue()
    data = base64.b64encode(data)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, headers=headers) as response:
            data = await response.read()

    data = base64.b64decode(data)
    buf = BytesIO(data)

    response_message = None
    response_trailers = {}

    while buf.tell() != len(data):
        frame_id, payload = read_frame(buf)

        if frame_id == FrameId.MESSAGE:
            if response_message is not None:
                raise UnexpectedFrameError

            response_message = protobuf.read_message(
                BytesIO(payload),
                response_type,
                len(payload)
            )
        elif frame_id == FrameId.TRAILER:
            if response_trailers:
                raise UnexpectedFrameError

            response_trailers = decode_trailers(payload)
        else:
            raise NotImplementedError

    if response_message is None:
        raise FrameExpectedError

    return response_message, response_trailers
