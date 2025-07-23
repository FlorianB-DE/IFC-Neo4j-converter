from typing import Any
import ifcopenshell
import sys
import time
import os
from neo4j import Driver, GraphDatabase, ManagedTransaction, Session

# Load Neo4j credentials from environment or use defaults
neo4j_username = os.getenv("NEO4J_USERNAME", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
neo4j_uri = os.getenv("NEO4J_URI", "bolt://database:7687")
print(neo4j_uri, neo4j_username, neo4j_password)

# Path to IFC file
ifc_path = "ifc_files/IfcOpenHouse_original.ifc"


def typeDict(key: str) -> str:
    return f.create_entity(key).wrapped_data.get_attribute_names()


def _ifc_neo4j_converter_each_class(ifc_path: os.PathLike | str, driver: Driver | Session = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password)), ignored_classes: List[str] = ["IfcOwnerHistory"]):
    start = time.time()
    print("Start!")
    print(time.strftime("%Y/%m/%d %H:%M:%S", time.strptime(time.ctime())))

    # Load IFC
    f: ifcopenshell.file = ifcopenshell.open(ifc_path)
    nodes: list[tuple[int, str, list[tuple[str, Any]]]] = []
    edges: list[tuple[int, str, int, str, str]] = []

    for el in f:
        if el.is_a() == "IfcOwnerHistory":
            continue
        tid = el.id()
        cls = el.is_a()
        pairs: list[tuple[str, Any]] = []
        try:
            keys = [x for x in el.get_info() if x not in [
                "type", "id", "OwnerHistory"]]
        except RuntimeError:
            continue

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
                    edges.append(
                        (tid, cls, el[i].id(), el[i].is_a(), typeDict(cls)[i]))
                    continue
            try:
                iter(el[i])
            except TypeError:
                continue
            destinations = [x.id() for x in el[i] if isinstance(
                x, ifcopenshell.entity_instance)]
            destinations_cls = [x.is_a() for x in el[i] if isinstance(
                x, ifcopenshell.entity_instance)]
            for (connectedTo, connectedTo_cls) in zip(destinations, destinations_cls):
                edges.append(
                    (tid, cls, connectedTo, connectedTo_cls, typeDict(cls)[i]))

    if len(nodes) == 0:
        print("no nodes in file", file=sys.stderr)
        sys.exit(1)

    print("List creation process done. Time:", round(time.time() - start))

    # Helper functions
    def clear_database(tx: ManagedTransaction):
        tx.run("MATCH (n) DETACH DELETE n")

    def create_node(tx: ManagedTransaction, cls: str, nid: int | str, properties: list[tuple[str, Any]]):
        props: dict[str, str | int] = {"nid": nid, **{k: v for k, v in properties}}
        prop_str = ", ".join(f"{k}: ${k}" for k in props)
        tx.run(f"CREATE (n:{cls} {{{prop_str}}})", props)

    def create_relationship(tx: ManagedTransaction, id1: str | int, cls1: str, id2: int | str, cls2: str, relType: str):
        query = f"""
            MATCH (a:{cls1} {{nid: $id1}})
            MATCH (b:{cls2} {{nid: $id2}})
            CREATE (a)-[r:`{relType}`]->(b)
        """
        tx.run(query, id1=id1, id2=id2)

    # Begin Neo4j session
    with driver if isinstance(driver, Session) else driver.session() as session:
        session.execute_write(clear_database)

        for nId, cls, pairs in nodes:
            session.execute_write(create_node, cls, nId, pairs)

        for id1, cls1, id2, cls2, relType in edges:
            session.execute_write(create_relationship, id1,
                                  cls1, id2, cls2, relType)

    print("All done. Time:", round(time.time() - start))
