__all__ = (
    "rpc",
)


def rpc(name=None):
    def decorator(method):
        method.__rpc__ = True
        method.__rpc_name__ = name if name else method.__name__

        return method

    return decorator
