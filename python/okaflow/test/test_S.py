"""
"""

import sys
import pytest
from okaflow.args import parseS,Atom

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
