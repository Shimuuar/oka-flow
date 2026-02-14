
"""
"""
import sys
import pydantic
import pytest
from okaflow.args import parseS,Atom
import okaflow


@pydantic.dataclasses.dataclass
class SExp(okaflow.encoding.SexpSerializer):
    a: int
    b: int
    c: int

class Foo(okaflow.IsMeta("foo","bar")):
    x: int
    y: int

class Bar(okaflow.IsMeta("bar")):
    x: int
    s: SExp

def test_S():
    assert parseS([]) == None
    assert parseS(["-"]) == None
    assert parseS(["!atom"]) == Atom("atom")
    assert parseS(["/path"]) == "/path"
    # Lists
    assert parseS(["(",")"]) == []
    assert parseS(["(","AA","BB",")"]) == ["AA","BB"]
    assert parseS(["(","!AA","BB",")"]) == [Atom("AA"),"BB"]
    # Nested lists
    assert parseS(["(","AA","(","BB",")",")"]) == ["AA",["BB"]]
    assert parseS(["(","(","!AA",")","BB","(",")",")"]) == [[Atom("AA")],"BB",[]]


def test_regressions_S():
    assert parseS(['(', '-', ')']) == [None]
    assert parseS(['(', '(','/A','/B','/C',')', '/D', '-', ')']) == [['/A','/B','/C'], '/D', None]


def test_parse_IsMeta():
    js  = {"foo":{"bar":{"x":1,"y":2}}}
    foo = Foo.fromMeta(okaflow.Meta(js))
    assert foo          == Foo(x=1,y=2)
    assert foo.toJson() == js

def test_parse_Sexp():
    js  = {"bar":{"x":1, "s":["SExp",3,4,5]}}
    bar = Bar.fromMeta(okaflow.Meta(js))
    assert bar          == Bar(x=1, s=SExp(a=3,b=4,c=5))
    assert bar.toJson() == js
