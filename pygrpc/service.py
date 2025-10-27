import inspect

from pydantic import BaseModel
from yarl import URL

from . import protobuf
from . import web


def service(name=None):
    def decorator(cls):
        cls.__service_name__ = name if name else cls.__name__

        def __init__(self, url):
            self.url = url

        return cls


def rpc(name):
    def decorator(method):
        rpc_name = name if name else method.__name__
        signature = inspect.signature(method)
        parameters = signature.parameters
        response_annotation = signature.return_annotation

        if len(parameters != 2):
            raise TypeError("RPC method should have exactly 2 parameters")

        request_annotation = parameters[1].annotation

        if request_annotation is inspect._empty:
            raise TypeError("RPC method requres a parameter annotation")

        if response_annotation is inspect._empty:
            raise TypeError("RPC method requires a return annotation")
 
        request_type = protobuf.get_type(request_annotation)
        response_type = protobuf.get_type(response_annotation)

        if not isinstance(request_type, protobuf.MessageType):
            raise TypeError(
                "RPC request must be of a message type, "
                f"'{request_annotation}' given"
            )

        if not isinstance(response_type, protobuf.MessageType):
            raise TypeError(
                "RPC response must be of a message type, "
                f"'{response_annotation}' given"
            )

        async def run(self, message, headers={}, trailers={}):
            url = URL(self.url) / type(self).__service_name__ / rpc_name

            return await web.unary_unary_call(
                url,
                request_type,
                response_type,
                message,
                headers,
                trailers
            )

        return run

    return decorator
