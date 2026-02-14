"""
Insturments for defining encoding for data files
"""
from typing import Any
import pydantic

class SexpSerializer:
    """
    Mixing for parsing data as S-expressions. Class name is used as a tag
    """
    @pydantic.model_serializer
    def _serialize_sexp(self) -> list:
        cls = self.__class__
        return [cls.__name__]+[getattr(self, f) for f in cls.__pydantic_fields__]

    @pydantic.model_validator(mode='before')
    @classmethod
    def _parse_sexp(cls, data: Any) -> Any:
        fields = cls.__pydantic_fields__
        match data:
            case list():
                if len(data) != len(fields)+1:
                    raise ValueError("Invalid S-expression length")
                if data[0] != cls.__name__:
                    raise ValueError(f"Invalid class name {data[0]} expected {cls.__name__}")
                return {f:x for f,x in zip(cls.__pydantic_fields__, data[1:])}
            case _: return data
