"""
Standard data types
"""
from   typing import Any,Optional
import pydantic

import okaflow.encoding


@pydantic.dataclasses.dataclass
class Range(okaflow.encoding.SexpSerializer):
    a: float
    b: float

    def cut(self, x:Any) -> Any:
        return (self.a < x) & (x < self.b)

    def as_tuple(self) -> tuple[float,float]:
        return (self.a, self.b)


@pydantic.dataclasses.dataclass
class Less(okaflow.encoding.SexpSerializer):
    x: float

    def cut(self, x:Any) -> Any:
        return x < self.x


@pydantic.dataclasses.dataclass
class LessEq(okaflow.encoding.SexpSerializer):
    x: float

    def cut(self, x:Any) -> Any:
        return x <= self.x


@pydantic.dataclasses.dataclass
class Greater(okaflow.encoding.SexpSerializer):
    x: float

    def cut(self, x:Any) -> Any:
        return x > self.x

@pydantic.dataclasses.dataclass
class GreaterEq(okaflow.encoding.SexpSerializer):
    x: float

    def cut(self, x:Any) -> Any:
        return x >= self.x



## ----------------------------------------------------------------
__all__ = [
    "Range", "Less", "LessEq", "Greater", "GreaterEq"
]
