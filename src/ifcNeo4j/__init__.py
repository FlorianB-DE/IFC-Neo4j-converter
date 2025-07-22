"""IFC Neo4j converter"""
from __future__ import annotations

__version__ = "0.0.1"

from .internal.ifc_neo4j_converter_AllSameNode import _ifc_neo4j_converter_all_same_class as convert_all_same_classes
from .internal.ifc_neo4j_converter_EachClass import _ifc_neo4j_converter_each_class as convert_each_class