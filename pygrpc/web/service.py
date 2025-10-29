import inspect

from pydantic import BaseModel
from yarl import URL

from .. import protobuf
from . import protocol


__all__ = (
    "generate_method",
    "service"
)


def generate_method(template):
    rpc_name = template.__rpc_name__
    qualname = template.__qualname__
    signature = inspect.signature(template)
    parameters = signature.parameters
    response_annotation = signature.return_annotation

    if len(parameters) != 2:
        raise TypeError(
            f"RPC method '{qualname}' must have exactly 2 parameters"
        )

    request_annotation = list(parameters.values())[1].annotation

    if request_annotation is inspect._empty:
        raise TypeError(
            f"RPC method '{qualname}' requires a parameter annotation"
        )

    if response_annotation is inspect._empty:
        raise TypeError(
            f"RPC method '{qualname}' requires a return annotation"
        )

    request_type = protobuf.get_type(request_annotation)

    if not isinstance(request_type, protobuf.MessageType):
        raise TypeError(
            f"RPC request in method '{qualname}' must be of a message "
            f"type, '{request_annotation}' given"
        )

    response_type = protobuf.get_type(response_annotation)

    if not isinstance(response_type, protobuf.MessageType):
        raise TypeError(
            f"RPC response in method '{qualname}' must be of a message type, "
            f"'{response_annotation}' given"
        )

    async def run(self, message, headers={}, trailers={}):
        url = URL(self.url) / type(self).__service_name__ / rpc_name

        return await protocol.unary_unary_call(
            url,
            request_type,
            message,
            headers,
            trailers,
            response_type,
            response_annotation
        )

    return run


def service(name=None):
    def decorator(cls):
        nonlocal name

        class Service(cls):
            __service_name__ = name if name else cls.__name__

            def __init__(self, url, headers={}, trailers={}):
                self.url = url
                self.headers = headers
                self.trailers = trailers

        templates = inspect.getmembers(
            Service,
            predicate=lambda x: getattr(x, "__rpc__", False)
        )

        for name, template in templates:
            setattr(cls, name, generate_method(template))

        return Service

    return decorator
