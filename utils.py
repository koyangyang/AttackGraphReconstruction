from py2neo import Graph
from global_config import neo4j_url, neo4j_user, neo4j_pass, neo4j_database


def connect_to_neo4j():
    return Graph(neo4j_url, auth=(neo4j_user, neo4j_pass), name=neo4j_database)
