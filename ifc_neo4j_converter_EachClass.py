import ifcopenshell
import sys
import time
import os
from neo4j import GraphDatabase

# Load Neo4j credentials from environment or use defaults
neo4j_username = os.getenv("NEO4J_USERNAME", "neo4j")
neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
neo4j_uri = os.getenv("NEO4J_URI", "bolt://database:7687")
print(neo4j_uri, neo4j_username, neo4j_password)

# Path to IFC file
ifc_path = "ifc_files/IfcOpenHouse_original.ifc"

def typeDict(key):
    return f.create_entity(key).wrapped_data.get_attribute_names()

start = time.time()
print("Start!")
print(time.strftime("%Y/%m/%d %H:%M:%S", time.strptime(time.ctime())))

# Load IFC
f = ifcopenshell.open(ifc_path)
nodes = []
edges = []

for el in f:
    if el.is_a() == "IfcOwnerHistory":
        continue
    tid = el.id()
    cls = el.is_a()
    pairs = []
    try:
        keys = [x for x in el.get_info() if x not in ["type", "id", "OwnerHistory"]]
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
                edges.append((tid, cls, el[i].id(), el[i].is_a(), typeDict(cls)[i]))
                continue
        try:
            iter(el[i])
        except TypeError:
            continue
        destinations = [x.id() for x in el[i] if isinstance(x, ifcopenshell.entity_instance)]
        destinations_cls = [x.is_a() for x in el[i] if isinstance(x, ifcopenshell.entity_instance)]
        for (connectedTo, connectedTo_cls) in zip(destinations, destinations_cls):
            edges.append((tid, cls, connectedTo, connectedTo_cls, typeDict(cls)[i]))

if len(nodes) == 0:
    print("no nodes in file", file=sys.stderr)
    sys.exit(1)

print("List creation process done. Time:", round(time.time() - start))

# Connect to Neo4j using official driver
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))

driver.execute_query

# Helper functions
def clear_database(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def create_node(tx, cls, nid, properties):
    props = { "nid": nid, **{k: v for k, v in properties} }
    prop_str = ", ".join(f"{k}: ${k}" for k in props)
    tx.run(f"CREATE (n:{cls} {{{prop_str}}})", props)

def create_relationship(tx, id1, cls1, id2, cls2, relType):
    query = f"""
        MATCH (a:{cls1} {{nid: $id1}})
        MATCH (b:{cls2} {{nid: $id2}})
        CREATE (a)-[r:`{relType}`]->(b)
    """
    tx.run(query, id1=id1, id2=id2)

# Begin Neo4j session
with driver.session() as session:
    session.execute_write(clear_database)

    for nId, cls, pairs in nodes:
        session.execute_write(create_node, cls, nId, pairs)

    for id1, cls1, id2, cls2, relType in edges:
        session.execute_write(create_relationship, id1, cls1, id2, cls2, relType)

driver.close()

print("All done. Time:", round(time.time() - start))