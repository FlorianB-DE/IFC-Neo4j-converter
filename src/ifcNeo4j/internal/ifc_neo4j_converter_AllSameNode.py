from typing import Any
import ifcopenshell
from ifcopenshell import file
import sys
import time
import os
from neo4j import GraphDatabase, Driver, Session

# Load environment variables
neo4j_username = os.getenv("NEO4J_USERNAME", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
neo4j_uri = os.getenv("NEO4J_URI", "bolt://database:7687")

def _typeDict(key: str) -> list[str]:
    f = ifcopenshell.file()
    value: list[str] = f.create_entity(key).wrapped_data.get_attribute_names()
    return value

def _ifc_neo4j_converter_all_same_class(ifc_path: os.PathLike | str, driver: Driver | Session = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password)), ignored_classes: list[str] = ["IfcOwnerHistory"]):
    start = time.time()
    print("Start!")
    print(time.strftime("%Y/%m/%d %H:%M", time.strptime(time.ctime())))

    nodes: list[tuple[int, str, list[tuple[str, Any]]]] = []
    edges: list[tuple[int, str | int, str]] = []

    f: file = ifcopenshell.open(ifc_path)

    for el in f:
        cls = el.is_a()
        if cls in ignored_classes:
            continue
        tid = el.id()
        pairs:list[tuple[str, Any]] = []
        keys = []
        try:
            keys = [x for x in el.get_info() if x not in ["type", "id", "OwnerHistory"]]
        except RuntimeError:
            pass
        for key in keys:
            val = el.get_info()[key]
            if any(hasattr(val, "is_a") and val.is_a(thisTyp) for thisTyp in ["IfcBoolean", "IfcLabel", "IfcText", "IfcReal"]):
                val = val.wrappedValue
            if val and isinstance(val, tuple) and isinstance(val[0], (str, bool, float, int)):
                val = ",".join(str(x) for x in val)
            if not isinstance(val, (str, bool, float, int)):
                continue
            pairs.append((key, val))
        nodes.append((tid, cls, pairs))

        for i in range(len(el)):
            try:
                el[i]
            except RuntimeError as e:
                if str(e) != "Entity not found":
                    print("ID", tid, e, file=sys.stderr)
                continue
            if isinstance(el[i], ifcopenshell.entity_instance):
                if el[i].is_a() == "IfcOwnerHistory":
                    continue
                if el[i].id() != 0:
                    edges.append((tid, el[i].id(), _typeDict(cls)[i]))
                    continue
            try:
                iter(el[i])
            except TypeError:
                continue
            destinations = [x.id() for x in el[i] if isinstance(x, ifcopenshell.entity_instance)]
            for connectedTo in destinations:
                edges.append((tid, connectedTo, _typeDict(cls)[i]))

    if len(nodes) == 0:
        print("no nodes in file", file=sys.stderr)
        sys.exit(1)

    print("List creation process done in", round(time.time() - start, 2), "seconds.")
    print(time.strftime("%Y/%m/%d %H:%M", time.strptime(time.ctime())))

    with driver if isinstance(driver, Session) else driver.session() as session:
        # Delete all existing nodes/relationships
        session.run("MATCH (n) DETACH DELETE n")

        # Create nodes
        for nId, cls, pairs in nodes:
            props = {k: v for k, v in pairs}
            props.update({"nid": nId, "ClassName": cls})
            session.run("CREATE (n:IfcNode $props)", props=props)

        # Create index
        session.run("CREATE INDEX IF NOT EXISTS FOR (n:IfcNode) ON (n.nid)")

        # Create relationships
        for nId1, nId2, relType in edges:
            session.run("""
                MATCH (a:IfcNode {nid: $id1})
                MATCH (b:IfcNode {nid: $id2})
                CREATE (a)-[r:`%s`]->(b)
            """ % relType, parameters={"id1": nId1, "id2": nId2})

    print("All done. Total time:", round(time.time() - start, 2), "seconds.")
    print(time.strftime("%Y/%m/%d %H:%M", time.strptime(time.ctime())))
