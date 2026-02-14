"""
Utilitites for working with data produced by oka-metadata library.

This module provides helper class `Meta` as untyped representation of metadata
and pydantic based conversion to python dataclasses.
"""
from   typing      import Any,Iterable,Union,TypeVar,ClassVar
import copy
import os
import sys
import json
import jsbeautifier
import pydantic


__all__ = [
    'Meta',
    'fromEnv', 'fromFile', 'fromStorePath', 'fromSavedMeta', 'fromStdin', 'toJSON', 'toPrettyJSON',
    'IsMeta'
]

## ================================================================
## Types
## ================================================================

JSON   = dict[str,Any] | list | str | int | float | bool | None
MetaTy = Union["Meta", list, str, int, float, bool, None]
M      = TypeVar('M', bound="IsMetaModel")


## ================================================================
## Metadata
## ================================================================

class Meta:
    """Helper object for metadata which allows to access fields of
    dictionaries using dot notation.
    """
    def __init__(self, dct: dict[str,Any]):
        "Create metadata object"
        self._dct = dct

    def get(self, k: str) -> Any:
        "Return field or None if there's no such key"
        return _asMeta(self._dct.get(k))

    def delete(self, path: list[str]) -> "Meta":
        "Delete given keys from metadata"
        match path:
            case [k]:
                dct = copy.copy(self._dct)
                dct.pop(k,None)
                return Meta(dct)
            case [k,*rest] if k in self._dct:
                match self[k]:
                    case Meta() as m:
                        dct    = copy.copy(self._dct)
                        dct[k] = m.delete(rest)
                        return Meta(dct)
                    case _:
                        raise Exception('Not a dictionary!')
        return self

    def __getitem__(self, k: str) -> Any:
        "Access using [_] operator"
        return _asMeta(self._dct[k])

    def __getattr__(self, k: str) -> Any:
        "Hack for access fields via dot"
        if k == "_dct":
            raise AttributeError
        return _asMeta(self._dct[k])

    def __iter__(self) -> Iterable[tuple[str, MetaTy]]:
        "Iteration is done over key-value pairs"
        for k,v in self._dct.items():
            yield (k, _asMeta(v))

    def __contains__(self, key: str) -> bool:
        return key in self._dct

    def __deepcopy__(self, memo: dict[int, Any] | None) -> "Meta":
        return Meta(copy.deepcopy(self._dct, memo))

    def __str__(self) -> str:
        return self._dct.__str__()

    def __repr__(self) -> str:
        return self._dct.__repr__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Meta):
            return False
        return self._dct == other._dct

    def _repr_pretty_(self, p: Any, _cycle: bool=False) -> str:
        opts = jsbeautifier.default_options()
        opts.indent_size = 2
        opts.wrap_line_length = 120
        return p.text(jsbeautifier.beautify(toJSON(self), opts)) # type: ignore


def fromEnv() -> Meta:
    """Decode metadata from environment OKA_META. This is standard
    convention for metadata passed to notebooks
    """
    return fromFile(os.environ['OKA_META'])

def fromFile(path: str) -> Meta:
    """Read metadata from file"""
    with open(path, encoding="utf-8") as f:
        return Meta(json.load(f))

def fromStorePath(path: str) -> Meta:
    "Read metadata which was used as path (meta.json)"
    return fromFile(path + "/meta.json")

def fromSavedMeta(path: str) -> Meta:
    "Read metadata which was produced by path (saved.json)"
    return fromFile(path + "/saved.json")

def fromStdin() -> Meta:
    "Read metadata from stdin. This is convention for worker scripts"
    return Meta(json.load(sys.stdin))


def toJSON(meta: "Meta") -> str:
    "Encode metadata to JSON"
    return json.dumps(meta, cls=_JSONEncoder)

def toPrettyJSON(meta: "Meta") -> str:
    "Encode metadata to JSON and beautify it"
    return jsbeautifier.beautify(toJSON(meta), _opts) # type: ignore



class _JSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        # pylint: disable=W0212
        if isinstance(o, Meta):
            return o._dct
        return super().default(o)

_opts = jsbeautifier.default_options()
_opts.indent_size      = 2
_opts.wrap_line_length = 120


def _asMeta(x: Any) -> MetaTy:
    "Convert JSON data type to metadata wrapper"
    match x:
        case Meta()  as x:  return x
        case dict()  as x:  return Meta(x)
        case list()  as xs: return [_asMeta(x) for x in xs]
        case int()   as x:  return x
        case float() as x:  return x
        case bool()  as x:  return x
        case str()   as x:  return x
        case None    as x:  return x
        case _: raise Exception(f"Malformed metadata. Type: {type(x)}")


## ----------------------------------------------------------------
## Pydantic
## ----------------------------------------------------------------

class IsMetaModel(pydantic.BaseModel):
    meta_location: ClassVar[list[str]] = []

    @classmethod
    def fromMeta(cls: type[M], meta: Meta) -> M:
        "Construct data type from metadata"
        m = meta
        for key in cls.meta_location:
            assert key in m
            v = m[key]
            assert isinstance(v,Meta)
            m = v
        return cls(**m._dct)

    @classmethod
    def fromMetaMaybe(cls: type[M], meta: Meta) -> M|None:
        "Construct data type from metadata"
        m = meta
        for key in cls.meta_location:
            if key not in m:
                return None
            v = m[key]
            assert isinstance(v,Meta)
            m = v
        return cls(**m._dct)


    @classmethod
    def fromSavedMeta(cls: type[M], path: str) -> M:
        "Construct data type from saved metadata"
        return cls.fromMeta(fromSavedMeta(path))

    def toJson(self) -> dict:
        "Convert data type into its metadata representation"
        meta = self.model_dump()
        for k in reversed(self.meta_location):
            meta = {k:meta}
        return meta

    def _repr_pretty_(self, p: Any, _cycle: bool=False) -> str:
        return Meta(self.model_dump())._repr_pretty_(p,_cycle)


def IsMeta(*prefix: str) -> type[IsMetaModel]:
    """Generate base model for a pyndatic class which will look up
    data in the given path"""
    class IsMetaCls(IsMetaModel):
        meta_location: ClassVar[list[str]] = list(prefix)
    return IsMetaCls
