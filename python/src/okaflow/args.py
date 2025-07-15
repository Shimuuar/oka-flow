"""
Simple and hopefully pythonic parser of
"""

from typing import Optional,Callable,TypeVar,Generic,Any,Protocol
import itertools
import sys
import os
from dataclasses import dataclass

__all__ = [
    'Args', 'Parser', 'ParserState', 'Path', 'Many', 'parse'
]

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
    args: list[str]
    "Parameters passed to a script"

    @staticmethod
    def fromArgs() -> "Args":
        "Create argument list from command line parameters"
        param = sys.argv[1:]
        for f in param:
            if not os.path.isdir(f):
                raise Exception(f"Parameter is not a directory: '{f}'")
        match param:
            case (out, *args): return Args(out=out, args=args)
            case _: raise Exception("Empty argument list")

    @staticmethod
    def fromEnv() -> "Args":
        "Create argument list from environment"
        args = []
        for i in itertools.count(1):
            match os.environ.get(f'OKA_ARG_{i}'):
                case None:
                    break
                case str() as arg:
                    if not os.path.isdir(arg):
                        raise Exception(f"Parameter is not a directory: '{arg}'")
                    args.append(arg)
        return Args(out=None, args=args)

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

@dataclass
class ParserState:
    off:  int
    args: list[str]

    def done(self) -> bool:
        return self.off >= len(self.args)

class Parser(Protocol):
    "Base class for parsing sequences of store path"
    def parse(self, p: ParserState) -> Any:
        pass

@dataclass
class Path(Parser):
    """Parse single path. If parameter is supplied it will be appended
       to store path
    """
    path: Optional[str] = None

    def parse(self, st: ParserState) -> Any:
        if st.done():
            raise ParseError("Not enough items")
        s       = st.args[st.off]
        st.off += 1
        match self.path:
            case None: return s
            case name: return os.path.join(s,name)

@dataclass
class Many(Parser):
    "Parse many entries"
    parser: Parser|tuple

    def parse(self, st: ParserState) -> list[Any]:
        acc: list[Any] = []
        while True:
            if st.done():
                return acc
            acc.append(_parse(st, self.parser))

@dataclass
class Some(Parser):
    "Parse many entries"
    parser: Parser|tuple

    def parse(self, st: ParserState) -> list[Any]:
        acc: list[Any] = []
        while True:
            if st.done():
                if not acc:
                    raise ParseError("Some: at least one element should be parsed")
                return acc
            acc.append(_parse(st, self.parser))



def _parse(st: ParserState, p: Parser|tuple) -> Any:
    match p:
        case tuple():
            return tuple((_parse(st, q) for q in p))
        case _:
            return p.parse(st)

def parse(p: Parser|tuple, args: Args) -> Any:
    "Parse list of parameters"
    st = ParserState(off=0, args=args.args)
    x  = _parse(st, p)
    if not st.done():
        raise ParseError(f"Not all arguments are consumed: {st.off} of {len(st.args)}")
    return x
