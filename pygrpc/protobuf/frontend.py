import typing

from typing import Annotated, Any, TypeAliasType

from pydantic import BaseModel, PydanticUndefined
from pydantic_core import PydanticUndefined
from annotated_types import Interval

from . import codec


__all__ = (
    "FrontendError",
    "UnsupportedTypeError",

    "get_type",
    "get_model_type",
    "int32"
)


class FrontendError(Exception):
    pass


class UnsupportedTypeError(FrontnedError):
    pass


class UndefinedFieldNumberError(FrontendError):
    pass


class FieldNumber:
    value: int

    def __init__(self, value: int) -> None:
        self.value = value


type int32 = Annotated[
    int,
    codec.PrimitiveType.INT32,
    Interval(ge=-2 ** 3, le=2 ** 31 - 1)
]


def get_field_number(target: Any) -> int:
    if isinstance(target, TypeAliasType):
        return get_field_number(target.__value__)

    if typing.get_origin(target) is not Annotated:
        raise UndefinedFieldNumberError
    
    type_args = typing.get_args(target)
    field_numbers = filter(
        lambda x: isinstance(x, FieldNumber),
        type_args[1:]
    )

    try:
        return next(field_numbers).value
    except StopIteration:
        pass

    return get_field_number(type_args[0])


def get_model_type(model: type[BaseModel]) -> codec.MessageType:
    fields = {}

    for field_name, field_info in model.model_fields.items():
        field_annotation = field_info.annotation
        field_default = field_info.default
        field_number = get_field_number(field_annotation)
        field_type = get_type(field_annotation)

        if field_default is not PydanticUndefined:
            field_type = codec.OptionalType(field_type, field_default)

        fields[field_number] = (field_name, field_type)

    return MessageType(fields)


def get_type(target: Any) -> codec.Type:
    if isinstance(target, TypeAliasType):
        return get_type(target.__value__)

    if typing.get_origin(target) is Annotated:
        type_args = typing.get_args(target)
        primitive_types = filter(
            lambda x: isinstance(x, codec.PrimitiveType),
            type_args[1:]
        )

        try:
            return next(primitive_types)
        except StopIteration:
            pass

        try:
            return get_type(type_args[0])
        except UnsupportedTypeError:
            pass
    elif issubclass(target, BaseModel):
        return get_model_type(target)
    elif target is str:
        return codec.PrimitiveType.STRING

    raise UnsupportedTypeError
