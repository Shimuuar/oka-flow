"""
Simple and hopefully pythonic parser of
"""

from typing import Optional,Callable,TypeVar,Generic,Any,Protocol,TypeAlias
import itertools
import sys
import os
from dataclasses import dataclass

from . import metadata

__all__ = [
    'Args', 'S', 'Parser', 'Path', 'PathSavedMeta', 'Many', 'Some', 'Maybe', 'parse'
]

T = TypeVar('T', bound=metadata.IsMetaModel)

## ================================================================
## S expressions
## ================================================================

@dataclass
class Atom:
    atom: str

S: TypeAlias = None | str | Atom | list


def parseS(args: list[str]) -> S:
    """
    Parse S expression from list of arguments
    """
    def step(xs: list[str]) -> tuple[S, list[str]]:
        # Consume single S-expression from list and return it and rest of list
        match xs:
            case ["-", *rest]:
                return None, rest
            case ["(", *rest]:
                acc: list[S] = []
                while True:
                    match rest:
                        case [")", *rest]: return acc,rest
                        case _:
                            S,rest = step(rest)
                            acc.append(S)
            case [str() as s, *rest] if s.startswith("!"):
                return Atom(s[1:]), rest
            case [str() as s, *rest]:
                return s, rest
        raise Exception("Cannot parse S expression")
    #--
    match args:
        case []: return None
        case _:
            match step(args):
                case s,[]: return s
                case _:  raise Exception("Cannot pasre S exception")




## ================================================================
## Argument list
## ================================================================

@dataclass
class Args:
    """
    Output directory and absolute paths passed to workflow
    """
    out:  Optional[str]
    "Output directory. Not set when run as phony script"
    args: S
    "Parameters passed to a script"

    @staticmethod
    def fromArgs() -> "Args":
        "Create argument list from command line parameters"
        param = sys.argv[1:]
        S     = parseS(param)
        return Args(out=os.getcwd(), args=S)

    @staticmethod
    def fromEnv() -> "Args":
        out = os.environ.get('OKA_OUT')
        "Create argument list from environment"
        args: list[str] = []
        for i in itertools.count(1):
            match os.environ.get(f'OKA_ARG_{i}'):
                case None:
                    break
                case str() as arg:
                    args.append(arg)
        return Args(out=out, args=parseS(args))

    def setCWD(self) -> None:
        "Set working directory to output directory"
        assert self.out is not None
        os.chdir(self.out)


## ----------------------------------------------------------------
## Parser for arguments
## ----------------------------------------------------------------

class ParseError(Exception):
    "Thrown when parse error is encountered"
    def __init__(self, msg: str):
        super().__init__(msg)

class Parser(Protocol):
    "Base class for parsing sequences of store path"
    def parse(self, s: S) -> Any:
        pass

@dataclass
class Path(Parser):
    """Parse single path. If parameter is supplied it will be appended
       to store path
    """
    path: Optional[str] = None

    def parse(self, s: S) -> Any:
        match s:
            case str():
                match self.path:
                    case None: return s
                    case name: return os.path.join(s,name)
        raise ParseError("Expecting single parameter")


@dataclass
class PathSavedMeta(Parser, Generic[T]):
    """Parse single path. If parameter is supplied it will be appended
       to store path
    """
    ty: type[T]

    def parse(self, s: S) -> T:
        match s:
            case str():
                meta = metadata.fromSavedMeta(s)
                return self.ty.fromMeta(meta)
        raise ParseError("Expecting single parameter")

@dataclass
class Maybe(Parser):
    "Parse optional entry"
    parser: Parser|tuple

    def parse(self, s: S) -> Any:
        match s:
            case None: return None
            case _:    return _parse(s, self.parser)

@dataclass
class Many(Parser):
    "Parse many entries"
    parser: Parser|tuple

    def parse(self, s0: S) -> list[Any]:
        match s0:
            case list():
                return [_parse(s, self.parser) for s in s0]
        raise ParseError("Expecting list parameter")

@dataclass
class Some(Parser):
    "Parse many entries"
    parser: Parser|tuple

    def parse(self, s0: S) -> list[Any]:
        match s0:
            case []:
                raise ParseError("Expecting nonempty list parameter")
            case list():
                return [_parse(s, self.parser) for s in s0]
        raise ParseError("Expecting list parameter")


def _parse(s: S, p: Parser|tuple|None) -> Any:
    match p:
        case None:
            return None
        case tuple():
            match s:
                case list() as xs:
                    if len(xs) != len(p):
                        raise ParseError(
                            f"Exception: invalid length of list in S-expression. Got {len(xs)} expected {len(p)}")
                    return tuple((_parse(s, q) for s,q in zip(xs,p)))
        case _:
            return p.parse(s)

def parse(p: Parser|tuple, args: Args) -> Any:
    "Parse list of parameters"
    return _parse(args.args, p)
